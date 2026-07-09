package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/pager"

	"cosmossdk.io/log"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mv1 "pkg.akt.dev/go/node/market/v1"

	"github.com/akash-network/provider/operator/common"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	akashclientset "github.com/akash-network/provider/pkg/client/clientset/versioned"
	"github.com/akash-network/provider/tools/fromctx"
)

// VolumeState is the JSON shape served for a single volume; the daemon's
// storage operator client consumes it for the bid engine's local pre-checks.
type VolumeState struct {
	Name          string `json:"name"`
	Owner         string `json:"owner"`
	VID           string `json:"vid"`
	Class         string `json:"class"`
	Size          string `json:"size"`
	Phase         string `json:"phase"`
	PVName        string `json:"pv-name,omitempty"`
	AttachedLease string `json:"attached-lease,omitempty"`
	RetainedUntil string `json:"retained-until,omitempty"`
}

type storageOperator struct {
	ctx        context.Context
	kc         kubernetes.Interface
	ac         akashclientset.Interface
	ns         string
	volNS      string
	cfg        common.OperatorConfig
	log        log.Logger
	server     common.OperatorHTTP
	reconciler *reconciler
	chain      ChainClient
	resync     time.Duration
	flagState  common.PrepareFlagFn
}

func newStorageOperator(ctx context.Context, logger log.Logger, ns, volNS string, cfg common.OperatorConfig, chain ChainClient, resync time.Duration) (*storageOperator, error) {
	kc, err := fromctx.KubeClientFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	ac, err := fromctx.AkashClientFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	opHTTP, err := common.NewOperatorHTTP()
	if err != nil {
		return nil, err
	}

	op := &storageOperator{
		ctx:        ctx,
		kc:         kc,
		ac:         ac,
		ns:         ns,
		volNS:      volNS,
		cfg:        cfg,
		log:        logger,
		server:     opHTTP,
		reconciler: newReconciler(kc, ac, ns, volNS, chain, logger),
		chain:      chain,
		resync:     resync,
	}

	op.flagState = op.server.AddPreparedEndpoint("/state", op.prepareState)

	op.server.GetRouter().HandleFunc("/health", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(rw, "OK")
	})

	op.server.GetRouter().HandleFunc("/volume/{owner}/{vid}", op.handleVolumeGet).Methods(http.MethodGet)

	return op, nil
}

func (op *storageOperator) run(parentCtx context.Context) error {
	op.log.Info("storage operator start", "namespace", op.ns, "volumes-namespace", op.volNS)

	for {
		lastAttempt := time.Now()
		err := op.monitorUntilError(parentCtx)
		if errors.Is(err, context.Canceled) {
			op.log.Debug("storage operator terminate")
			return err
		}

		op.log.Error("observation stopped", "err", err)

		elapsed := time.Since(lastAttempt)
		if elapsed < op.cfg.RetryDelay {
			select {
			case <-parentCtx.Done():
				return parentCtx.Err()
			case <-time.After(op.cfg.RetryDelay):
			}
		}
	}
}

func (op *storageOperator) monitorUntilError(parentCtx context.Context) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// restart recovery: the chain is re-listed in full - anything closed
	// while the operator was down is stamped now, before live observation
	if err := op.resyncChain(ctx); err != nil {
		return err
	}

	events, err := op.observeVolumes(ctx)
	if err != nil {
		return err
	}

	var chainch <-chan interface{}
	if op.chain != nil {
		chainch, err = op.chain.Events(ctx, "operator-storage")
		if err != nil {
			return err
		}
	}

	if err := op.server.PrepareAll(); err != nil {
		return err
	}

	resyncTicker := time.NewTicker(op.resync)
	defer resyncTicker.Stop()

	prepareTicker := time.NewTicker(op.cfg.WebRefreshInterval)
	defer prepareTicker.Stop()

	for {
		prepareData := false

		select {
		case <-ctx.Done():
			return ctx.Err()
		case vol, ok := <-events:
			if !ok {
				return common.ErrObservationStopped
			}

			if err := op.reconciler.reconcile(ctx, vol); err != nil {
				op.log.Error("reconciling volume", "volume", vol.Name, "err", err)
			}

			prepareData = true
		case ev, ok := <-chainch:
			if !ok {
				return common.ErrObservationStopped
			}

			if err := op.applyChainEvent(ctx, ev); err != nil {
				op.log.Error("applying chain event", "err", err)
			}

			prepareData = true
		case <-resyncTicker.C:
			// the sweep is what fires GC deadlines with no event traffic,
			// and re-verifies chain state for frozen adoptions
			if err := op.resyncChain(ctx); err != nil {
				op.log.Error("chain resync", "err", err)
			}

			if err := op.reconcileAll(ctx); err != nil {
				return err
			}

			prepareData = true
		case <-prepareTicker.C:
			prepareData = true
		}

		if prepareData {
			op.flagState()
			if err := op.server.PrepareAll(); err != nil {
				op.log.Error("preparing web data failed", "err", err)
			}
		}
	}
}

// observeVolumes lists then watches the Volume CRDs; every add/update lands
// the object on the returned channel for reconciliation.
func (op *storageOperator) observeVolumes(ctx context.Context) (<-chan *crd.Volume, error) {
	var lastResourceVersion string

	phpager := pager.New(func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
		resources, err := op.ac.AkashV2beta2().Volumes(op.ns).List(ctx, opts)

		if err == nil && len(resources.GetResourceVersion()) != 0 {
			lastResourceVersion = resources.GetResourceVersion()
		}

		return resources, err
	})

	data := make([]crd.Volume, 0, 16)
	err := phpager.EachListItem(ctx, metav1.ListOptions{}, func(obj runtime.Object) error {
		vol := obj.(*crd.Volume)
		data = append(data, *vol)
		return nil
	})
	if err != nil {
		return nil, err
	}

	op.log.Info("starting volume watch", "resourceVersion", lastResourceVersion, "existing", len(data))

	watcher, err := op.ac.AkashV2beta2().Volumes(op.ns).Watch(ctx, metav1.ListOptions{
		ResourceVersion: lastResourceVersion,
	})
	if err != nil {
		return nil, err
	}

	output := make(chan *crd.Volume)

	go func() {
		defer close(output)
		defer watcher.Stop()

		for i := range data {
			select {
			case output <- &data[i]:
			case <-ctx.Done():
				return
			}
		}

		data = nil

		for {
			select {
			case result, ok := <-watcher.ResultChan():
				if !ok {
					return
				}

				switch result.Type {
				case watch.Added, watch.Modified:
				default:
					continue
				}

				vol, valid := result.Object.(*crd.Volume)
				if !valid {
					continue
				}

				select {
				case output <- vol:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return output, nil
}

func (op *storageOperator) reconcileAll(ctx context.Context) error {
	list, err := op.ac.AkashV2beta2().Volumes(op.ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for i := range list.Items {
		if err := op.reconciler.reconcile(ctx, &list.Items[i]); err != nil {
			op.log.Error("reconciling volume", "volume", list.Items[i].Name, "err", err)
		}
	}

	return nil
}

// resyncChain compares every volume CRD against the chain's active-lease
// set: a volume whose own lease is gone is stamped for retention, and a
// stale attachment record is cleared. Missed events are healed here.
func (op *storageOperator) resyncChain(ctx context.Context) error {
	if op.chain == nil {
		return nil
	}

	active, err := op.chain.ActiveLeases(ctx)
	if err != nil {
		return err
	}

	list, err := op.ac.AkashV2beta2().Volumes(op.ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for i := range list.Items {
		vol := &list.Items[i]

		if lease := vol.Status.AttachedLease; lease != "" && !active[lease] {
			if err := op.detachVolume(ctx, vol, lease); err != nil {
				op.log.Error("detaching volume", "volume", vol.Name, "err", err)
				continue
			}
		}

		if lid, err := vol.Spec.LeaseID.FromCRD(); err == nil && !active[lid.String()] {
			switch vol.Status.Phase {
			case crd.VolumePhaseRetained, crd.VolumePhaseReleasing, crd.VolumePhaseAdopting:
				// already accounted for or frozen
			case crd.VolumePhaseExporting:
				// lease closed while the operator was down: start the
				// retention clock but keep the export freeze
				if err := op.stampExportingRetention(ctx, vol); err != nil {
					op.log.Error("stamping exporting retention", "volume", vol.Name, "err", err)
				}
			default:
				if err := op.retainVolume(ctx, vol); err != nil {
					op.log.Error("retaining volume", "volume", vol.Name, "err", err)
				}
			}
		}
	}

	return nil
}

func (op *storageOperator) applyChainEvent(ctx context.Context, ev interface{}) error {
	switch ev := ev.(type) {
	case *mv1.EventLeaseClosed:
		return op.applyLeaseClosed(ctx, ev.ID)
	case *mv1.EventLeaseReclaimStarted:
		return op.applyReclaimStarted(ctx, ev)
	case *mv1.EventVolumeAttached:
		return op.applyAttachment(ctx, ev.Volume, ev.LeaseID.String())
	case *mv1.EventVolumeDetached:
		return op.applyDetachment(ctx, ev.Volume, ev.LeaseID.String())
	case *dv1.EventVolumeAdopted:
		return op.applyAdoption(ctx, ev)
	default:
		return nil
	}
}

// applyLeaseClosed handles both directions of a lease close: the volume's
// own lease (start the retention clock) and an attached compute lease
// (clear the attachment record; the reconciler re-parks the PV).
func (op *storageOperator) applyLeaseClosed(ctx context.Context, lid mv1.LeaseID) error {
	list, err := op.ac.AkashV2beta2().Volumes(op.ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	closed := lid.String()

	for i := range list.Items {
		vol := &list.Items[i]

		if vol.Status.AttachedLease == closed {
			if err := op.detachVolume(ctx, vol, closed); err != nil {
				return err
			}

			continue
		}

		if vlid, err := vol.Spec.LeaseID.FromCRD(); err == nil && vlid.String() == closed {
			switch vol.Status.Phase {
			case crd.VolumePhaseRetained, crd.VolumePhaseReleasing, crd.VolumePhaseAdopting:
			case crd.VolumePhaseExporting:
				// a migration in flight: the retention clock starts (the
				// source holds through window + retention) but the phase
				// stays Exporting so GC remains frozen until the deadline
				if err := op.stampExportingRetention(ctx, vol); err != nil {
					return err
				}
			default:
				if err := op.retainVolume(ctx, vol); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// applyReclaimStarted freezes the reclaimed volume's CRD into Exporting:
// GC is structurally excluded while the phase holds, and the transfer
// server will serve the destination's pulls. Only volume reclaim reasons
// reach here (processEvent filters).
func (op *storageOperator) applyReclaimStarted(ctx context.Context, ev *mv1.EventLeaseReclaimStarted) error {
	list, err := op.ac.AkashV2beta2().Volumes(op.ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	reclaimed := ev.ID.String()

	for i := range list.Items {
		vol := &list.Items[i]

		vlid, err := vol.Spec.LeaseID.FromCRD()
		if err != nil || vlid.String() != reclaimed {
			continue
		}

		if vol.Status.Phase == crd.VolumePhaseExporting {
			return nil
		}

		uvol := vol.DeepCopy()
		uvol.Status.Phase = crd.VolumePhaseExporting

		op.log.Info("volume reclaim started; export window open", "volume", vol.Name,
			"reason", ev.Reason.String(), "deadline", ev.Deadline)

		if _, err := op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{}); err != nil {
			return err
		}

		return nil
	}

	return nil
}

// stampExportingRetention starts the retention clock on an Exporting
// volume whose own lease closed, without leaving the Exporting freeze:
// the destination may still be pulling the final diff. The reconciler
// thaws the phase to Retained once the deadline passes.
func (op *storageOperator) stampExportingRetention(ctx context.Context, vol *crd.Volume) error {
	if vol.Status.RetainedUntil != nil {
		return nil
	}

	retention, err := time.ParseDuration(vol.Spec.Retention)
	if err != nil {
		return fmt.Errorf("%w: volume %s retention %q: %s", ErrVolumeOperator, vol.Name, vol.Spec.Retention, err.Error())
	}

	uvol := vol.DeepCopy()
	uvol.Status.AttachedLease = ""

	retainedUntil := metav1.NewTime(time.Now().Add(retention))
	uvol.Status.RetainedUntil = &retainedUntil

	op.log.Info("exporting volume lease closed; retention clock started",
		"volume", uvol.Name, "retained-until", retainedUntil)

	updated, err := op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	*vol = *updated

	return nil
}

func (op *storageOperator) applyAttachment(ctx context.Context, ref dv1.VolumeRef, lease string) error {
	name := crd.VolumeName(ref.Owner, ref.Name)

	vol, err := op.ac.AkashV2beta2().Volumes(op.ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			op.log.Error("attach event for unknown volume", "name", name, "ref", ref.String())
			return nil
		}

		return err
	}

	if vol.Status.AttachedLease == lease {
		return nil
	}

	uvol := vol.DeepCopy()
	uvol.Status.AttachedLease = lease

	_, err = op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})

	return err
}

func (op *storageOperator) applyDetachment(ctx context.Context, ref dv1.VolumeRef, lease string) error {
	name := crd.VolumeName(ref.Owner, ref.Name)

	vol, err := op.ac.AkashV2beta2().Volumes(op.ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	if vol.Status.AttachedLease != lease {
		return nil
	}

	return op.detachVolume(ctx, vol, lease)
}

// applyAdoption moves the CRD spec to the adopting deployment. The
// reconciler notices the spec/PV identity split and performs the verified
// re-bind; until then the volume is frozen in Adopting.
func (op *storageOperator) applyAdoption(ctx context.Context, ev *dv1.EventVolumeAdopted) error {
	name := crd.VolumeName(ev.ID.Owner, ev.Vid)

	vol, err := op.ac.AkashV2beta2().Volumes(op.ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	dseq := strconv.FormatUint(ev.ID.DSeq, 10)
	if vol.Spec.GroupID.DSeq == dseq {
		return nil
	}

	uvol := vol.DeepCopy()
	uvol.Spec.GroupID = crd.VolumeGroupID{
		DSeq: dseq,
		GSeq: ev.ID.GSeq,
	}
	uvol.Status.Phase = crd.VolumePhaseAdopting

	op.log.Info("volume adoption recorded", "volume", name, "dseq", dseq)

	_, err = op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})

	return err
}

func (op *storageOperator) detachVolume(ctx context.Context, vol *crd.Volume, lease string) error {
	uvol := vol.DeepCopy()
	uvol.Status.AttachedLease = ""

	op.log.Info("volume detach recorded", "volume", vol.Name, "lease", lease)

	updated, err := op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	*vol = *updated

	return nil
}

// retainVolume starts the retention clock: retainedUntil = closedAt +
// retention. The chain does not expose the lease's closed_at yet (chain-sdk
// follow-up); until it does the event receipt time stands in, which only
// ever extends the window.
func (op *storageOperator) retainVolume(ctx context.Context, vol *crd.Volume) error {
	uvol := vol.DeepCopy()
	uvol.Status.AttachedLease = ""

	now := metav1.NewTime(time.Now())

	if uvol.Spec.Reclaim == dv1.VolumeReclaimDelete.String() {
		uvol.Status.Phase = crd.VolumePhaseReleasing
		uvol.Status.RetainedUntil = &now
	} else {
		retention, err := time.ParseDuration(uvol.Spec.Retention)
		if err != nil {
			return fmt.Errorf("%w: volume %s retention %q: %s", ErrVolumeOperator, uvol.Name, uvol.Spec.Retention, err.Error())
		}

		retainedUntil := metav1.NewTime(now.Add(retention))
		uvol.Status.Phase = crd.VolumePhaseRetained
		uvol.Status.RetainedUntil = &retainedUntil
	}

	op.log.Info("volume lease closed", "volume", uvol.Name, "phase", uvol.Status.Phase, "retained-until", uvol.Status.RetainedUntil)

	updated, err := op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	*vol = *updated

	return nil
}

func volumeState(vol *crd.Volume) VolumeState {
	state := VolumeState{
		Name:          vol.Name,
		Owner:         vol.Spec.Owner,
		VID:           vol.Spec.VID,
		Class:         vol.Spec.Class,
		Size:          vol.Spec.Size,
		Phase:         string(vol.Status.Phase),
		PVName:        vol.Status.PVName,
		AttachedLease: vol.Status.AttachedLease,
	}

	if vol.Status.RetainedUntil != nil {
		state.RetainedUntil = vol.Status.RetainedUntil.UTC().Format(time.RFC3339)
	}

	return state
}

func (op *storageOperator) prepareState(pd common.PreparedResult) error {
	list, err := op.ac.AkashV2beta2().Volumes(op.ns).List(op.ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	// retained/parked byte accounting is served here until inventory.v1
	// StorageInfo grows volumes_capable/retained_bytes (chain-sdk follow-up)
	var retainedBytes uint64

	states := make([]VolumeState, 0, len(list.Items))
	for i := range list.Items {
		vol := &list.Items[i]
		states = append(states, volumeState(vol))

		if vol.Status.Phase == crd.VolumePhaseRetained {
			if size, err := strconv.ParseUint(vol.Spec.Size, 10, 64); err == nil {
				retainedBytes += size
			}
		}
	}

	result := struct {
		Volumes       []VolumeState `json:"volumes"`
		RetainedBytes uint64        `json:"retained-bytes"`
	}{
		Volumes:       states,
		RetainedBytes: retainedBytes,
	}

	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(result); err != nil {
		return err
	}

	pd.Set(buf.Bytes())

	return nil
}

func (op *storageOperator) handleVolumeGet(rw http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	name := crd.VolumeName(vars["owner"], vars["vid"])

	vol, err := op.ac.AkashV2beta2().Volumes(op.ns).Get(req.Context(), name, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		op.log.Error("fetching volume", "name", name, "err", err)
		rw.WriteHeader(http.StatusInternalServerError)

		return
	}

	rw.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(rw).Encode(volumeState(vol)); err != nil {
		op.log.Error("encoding volume state", "err", err)
	}
}
