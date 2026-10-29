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
	"k8s.io/client-go/util/retry"

	"cosmossdk.io/log"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mv1 "pkg.akt.dev/go/node/market/v1"

	"github.com/akash-network/provider/bucket"
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
	Access        string `json:"access,omitempty"`
	Bucket        string `json:"bucket,omitempty"`
	DSeq          string `json:"dseq,omitempty"`
	ImportFrom    string `json:"import-from,omitempty"`
	ReplicaOf     string `json:"replica-of,omitempty"`
	SyncedAt      string `json:"synced-at,omitempty"`
	Syncing       bool   `json:"syncing,omitempty"`
	Promoted      bool   `json:"promoted,omitempty"`
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

func newStorageOperator(ctx context.Context, logger log.Logger, ns, volNS string, cfg common.OperatorConfig, chain ChainClient, resync time.Duration, nodeHint bool, buckets bucket.Backend) (*storageOperator, error) {
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

	op.reconciler.nodeHint = nodeHint
	op.reconciler.buckets = buckets

	op.flagState = op.server.AddPreparedEndpoint("/state", op.prepareState)

	op.server.GetRouter().HandleFunc("/health", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(rw, "OK")
	})

	op.server.GetRouter().HandleFunc("/volume/{owner}/{vid}", op.handleVolumeGet).Methods(http.MethodGet)

	return op, nil
}

func (op *storageOperator) run(parentCtx context.Context) error {
	op.log.Info("storage operator start", "ns", op.ns, "volumes_ns", op.volNS)

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

	op.log.Info("starting volume watch", "resource_version", lastResourceVersion, "existing", len(data))

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
// set: a volume whose own lease is gone is stamped for retention, a stale
// attachment record is cleared, and a live volume's size is raised to what
// its group holds on chain. Missed events are healed here.
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

		lid, err := vol.Spec.LeaseID.FromCRD()
		if err != nil || !volumeLeaseIsCurrent(vol, lid) {
			continue
		}

		if active[lid.String()] {
			// a resize both the daemon and the operator missed leaves the
			// claim short of what the tenant pays for
			if err := op.raiseSize(ctx, vol, lid); err != nil {
				op.log.Error("catching up the volume's size", "volume", vol.Name, "err", err)
			}

			continue
		}

		switch {
		case vol.Status.Phase == crd.VolumePhaseRetained,
			vol.Status.Phase == crd.VolumePhaseReleasing,
			vol.Status.Phase == crd.VolumePhaseAdopting:
			// already accounted for or frozen
		case vol.Status.Phase == crd.VolumePhaseExporting && vol.Status.RetainedUntil != nil:
			// its close is recorded
		default:
			if err := op.settleInactiveLease(ctx, vol, lid); err != nil {
				op.log.Error("settling volume lease", "volume", vol.Name, "err", err)
			}
		}
	}

	return nil
}

// settleInactiveLease records what became of a volume whose own lease is
// no longer active, as the chain has it, when the operator missed the
// events that said so. A lease in its reclamation window is not closed: a
// volume moving away exports through it, and any other reclamation is
// settled by its close. A closed lease starts the retention clock, frozen
// in Exporting for a volume that moves, and the volume is retained
// otherwise.
func (op *storageOperator) settleInactiveLease(ctx context.Context, vol *crd.Volume, lid mv1.LeaseID) error {
	leases, err := op.chain.VolumeLeases(ctx, vol.Spec.Owner, vol.Spec.VID)
	if err != nil {
		return err
	}

	var (
		state mv1.Lease_State
		moves bool
	)

	for _, lease := range leases {
		if lease.Provider == lid.Provider && lease.DSeq == lid.DSeq && lease.OSeq == lid.OSeq && lease.Policy.Vid == vol.Spec.VID {
			state, moves = lease.State, closedForMove(lease.Reason)
			break
		}
	}

	switch {
	case state == mv1.LeaseReclaiming:
		if moves && vol.Status.Phase != crd.VolumePhaseExporting {
			return op.exportVolume(ctx, vol)
		}

		return nil
	case moves, vol.Status.Phase == crd.VolumePhaseExporting:
		return op.stampExportingRetention(ctx, vol, lid)
	default:
		return op.retainVolume(ctx, vol, lid)
	}
}

// closedForMove reports whether a volume lease ends so that the volume moves:
// its order re-lists and the next provider copies it from this one.
func closedForMove(reason mv1.LeaseClosedReason) bool {
	return reason == mv1.LeaseClosedReasonVolumeMigrate || reason == mv1.LeaseClosedReasonVolumeEvict
}

// volumeLeaseIsCurrent reports whether the volume's recorded lease still
// belongs to the deployment the spec names. An adoption moves Spec.GroupID
// to the adopting deployment (applyAdoption, then the reconciler's verified
// re-bind) before the daemon's DeployVolume re-records the lease; in that
// window the lease record is stale and the dead lease's closure must not
// start the retention clock on the adopted volume — surviving that closure
// is the whole point of the adoption.
func volumeLeaseIsCurrent(vol *crd.Volume, lid mv1.LeaseID) bool {
	return vol.Spec.GroupID.DSeq == strconv.FormatUint(lid.DSeq, 10)
}

// applyChainEvent applies a single chain event, retrying on k8s update
// conflicts: every handler re-reads current state per attempt and is
// idempotent. A dropped event is not always healed by the resync sweep
// (adoptions are only applied from the event), so a transient conflict
// with the reconciler must not lose one.
func (op *storageOperator) applyChainEvent(ctx context.Context, ev interface{}) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return op.applyChainEventOnce(ctx, ev)
	})
}

func (op *storageOperator) applyChainEventOnce(ctx context.Context, ev interface{}) error {
	switch ev := ev.(type) {
	case *mv1.EventLeaseClosed:
		return op.applyLeaseClosed(ctx, ev.ID, ev.Reason)
	case *mv1.EventLeaseReclaimStarted:
		return op.applyReclaimStarted(ctx, ev)
	case *mv1.EventLeaseResized:
		return op.applyLeaseResized(ctx, ev.ID)
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
func (op *storageOperator) applyLeaseClosed(ctx context.Context, lid mv1.LeaseID, reason mv1.LeaseClosedReason) error {
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

		if vlid, err := vol.Spec.LeaseID.FromCRD(); err == nil && vlid.String() == closed && volumeLeaseIsCurrent(vol, vlid) {
			switch vol.Status.Phase {
			case crd.VolumePhaseRetained, crd.VolumePhaseReleasing, crd.VolumePhaseAdopting:
			case crd.VolumePhaseExporting:
				// a migration in flight: the retention clock starts (the
				// source holds through window + retention) but the phase
				// stays Exporting so GC remains frozen until the deadline
				if err := op.stampExportingRetention(ctx, vol, lid); err != nil {
					return err
				}
			default:
				// a move whose reclamation went unseen exports all the same
				if closedForMove(reason) {
					if err := op.stampExportingRetention(ctx, vol, lid); err != nil {
						return err
					}

					continue
				}

				if err := op.retainVolume(ctx, vol, lid); err != nil {
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

		op.log.Info("volume reclaim started; export window open", "volume", vol.Name,
			"reason", ev.Reason.String(), "deadline", ev.Deadline)

		return op.exportVolume(ctx, vol)
	}

	return nil
}

// exportVolume freezes a volume into Exporting for its reclamation window.
func (op *storageOperator) exportVolume(ctx context.Context, vol *crd.Volume) error {
	uvol := vol.DeepCopy()
	uvol.Status.Phase = crd.VolumePhaseExporting

	updated, err := op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	*vol = *updated

	return nil
}

// applyLeaseResized records the size the chain now holds for a resized
// volume lease; the reconciler then grows the claim. The daemon records the
// resize as well, through DeployVolume, so this covers a daemon that missed
// the event. Like DeployVolume it never lowers the recorded size.
func (op *storageOperator) applyLeaseResized(ctx context.Context, lid mv1.LeaseID) error {
	if op.chain == nil {
		return nil
	}

	list, err := op.ac.AkashV2beta2().Volumes(op.ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for i := range list.Items {
		vol := &list.Items[i]

		vlid, err := vol.Spec.LeaseID.FromCRD()
		if err != nil || !vlid.Equals(lid) {
			continue
		}

		return op.raiseSize(ctx, vol, lid)
	}

	return nil
}

// raiseSize records the size the chain holds for the group of the volume's
// lease lid when it is larger than the recorded one; the reconciler then
// grows the claim, or a bucket's quota. It never lowers the record, as a PV
// only grows.
func (op *storageOperator) raiseSize(ctx context.Context, vol *crd.Volume, lid mv1.LeaseID) error {
	size, err := op.chain.VolumeSize(ctx, lid.GroupID())
	if err != nil {
		return err
	}

	if cur, err := strconv.ParseUint(vol.Spec.Size, 10, 64); err == nil && size <= cur {
		return nil
	}

	uvol := vol.DeepCopy()
	uvol.Spec.Size = strconv.FormatUint(size, 10)

	op.log.Info("volume resize recorded", "volume", vol.Name, "size", uvol.Spec.Size)

	updated, err := op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	*vol = *updated

	return nil
}

// stampExportingRetention starts the retention clock on a volume whose own
// lease lid closed for a move, freezing it in Exporting: the destination
// may still be pulling the final diff. The reconciler thaws the phase to
// Retained once the deadline passes.
func (op *storageOperator) stampExportingRetention(ctx context.Context, vol *crd.Volume, lid mv1.LeaseID) error {
	if vol.Status.Phase == crd.VolumePhaseExporting && vol.Status.RetainedUntil != nil {
		return nil
	}

	retention, err := time.ParseDuration(vol.Spec.Retention)
	if err != nil {
		return fmt.Errorf("%w: volume %s retention %q: %s", ErrVolumeOperator, vol.Name, vol.Spec.Retention, err.Error())
	}

	uvol := vol.DeepCopy()
	uvol.Status.Phase = crd.VolumePhaseExporting
	uvol.Status.AttachedLease = ""

	retainedUntil := metav1.NewTime(op.closedAt(ctx, lid).Add(retention))
	uvol.Status.RetainedUntil = &retainedUntil

	op.log.Info("exporting volume lease closed; retention clock started",
		"volume", uvol.Name, "retained_until", retainedUntil)

	updated, err := op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	*vol = *updated

	return nil
}

// volume reads the Volume CRD of owner/vid. A record of another volume
// that shares the name counts as missing, so no event or request can act on
// it.
func (op *storageOperator) volume(ctx context.Context, owner, vid string) (*crd.Volume, error) {
	name := crd.VolumeName(owner, vid)

	vol, err := op.ac.AkashV2beta2().Volumes(op.ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if !vol.Matches(owner, vid) {
		op.log.Error("volume name collision", "volume", name, "owner", owner, "vid", vid)
		return nil, kerrors.NewNotFound(crd.Resource("volumes"), name)
	}

	return vol, nil
}

func (op *storageOperator) applyAttachment(ctx context.Context, ref dv1.VolumeRef, lease string) error {
	vol, err := op.volume(ctx, ref.Owner, ref.Name)
	if err != nil {
		if kerrors.IsNotFound(err) {
			op.log.Error("attach event for unknown volume", "ref", ref.String())
			return nil
		}

		return err
	}

	if vol.Status.AttachedLease == lease {
		return nil
	}

	uvol := vol.DeepCopy()
	uvol.Status.AttachedLease = lease

	// compute attached to a replica promotes it. A sync round still writing
	// it lands first: nothing mounts the volume until it has (AttachVolume)
	if uvol.IsReplica() {
		uvol.Status.Promoted = true
	}

	_, err = op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})

	return err
}

func (op *storageOperator) applyDetachment(ctx context.Context, ref dv1.VolumeRef, lease string) error {
	vol, err := op.volume(ctx, ref.Owner, ref.Name)
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
	vol, err := op.volume(ctx, ev.ID.Owner, ev.Vid)
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

	op.log.Info("volume adoption recorded", "volume", vol.Name, "dseq", dseq)

	_, err = op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})

	return err
}

func (op *storageOperator) detachVolume(ctx context.Context, vol *crd.Volume, lease string) error {
	uvol := vol.DeepCopy()
	uvol.Status.AttachedLease = ""

	op.log.Info("volume detach recorded", "volume", vol.Name, "detached_lease", lease)

	updated, err := op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	*vol = *updated

	return nil
}

// retainVolume starts the retention clock of a volume whose own lease lid
// closed: retainedUntil = closedAt + retention.
func (op *storageOperator) retainVolume(ctx context.Context, vol *crd.Volume, lid mv1.LeaseID) error {
	uvol := vol.DeepCopy()
	uvol.Status.AttachedLease = ""

	closedAt := metav1.NewTime(op.closedAt(ctx, lid))

	if uvol.Spec.Reclaim == dv1.VolumeReclaimDelete.String() {
		uvol.Status.Phase = crd.VolumePhaseReleasing
		uvol.Status.RetainedUntil = &closedAt
	} else {
		retention, err := time.ParseDuration(uvol.Spec.Retention)
		if err != nil {
			return fmt.Errorf("%w: volume %s retention %q: %s", ErrVolumeOperator, uvol.Name, uvol.Spec.Retention, err.Error())
		}

		retainedUntil := metav1.NewTime(closedAt.Add(retention))
		uvol.Status.Phase = crd.VolumePhaseRetained
		uvol.Status.RetainedUntil = &retainedUntil
	}

	op.log.Info("volume lease closed", "volume", uvol.Name, "phase", uvol.Status.Phase, "retained_until", uvol.Status.RetainedUntil)

	updated, err := op.ac.AkashV2beta2().Volumes(op.ns).Update(ctx, uvol, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	*vol = *updated

	return nil
}

// closedAt is when lid closed, which a volume's retention runs from: the
// chain's closed_at, the deadline adoption is held to as well. A lease the
// chain closed before it recorded closed_at, or a chain that cannot be
// read, leaves the time the operator observed the close, which only ever
// extends the window.
func (op *storageOperator) closedAt(ctx context.Context, lid mv1.LeaseID) time.Time {
	observed := time.Now()

	if op.chain == nil {
		return observed
	}

	closedAt, err := op.chain.LeaseClosedAt(ctx, lid)
	if err != nil {
		op.log.Error("reading when the volume lease closed; retention runs from now", "lid", lid, "err", err)
		return observed
	}

	if closedAt.IsZero() {
		return observed
	}

	return closedAt
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
		Access:        vol.Spec.Access,
		Bucket:        vol.Status.Bucket,
		DSeq:          vol.Spec.GroupID.DSeq,
		ImportFrom:    vol.Spec.ImportFrom,
		ReplicaOf:     vol.Spec.ReplicaOf,
		Syncing:       vol.Status.Syncing || vol.AwaitsFirstCopy(),
		Promoted:      vol.Status.Promoted,
	}

	if vol.Status.RetainedUntil != nil {
		state.RetainedUntil = vol.Status.RetainedUntil.UTC().Format(time.RFC3339)
	}

	if vol.Status.SyncedAt != nil {
		state.SyncedAt = vol.Status.SyncedAt.UTC().Format(time.RFC3339)
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

	vol, err := op.volume(req.Context(), vars["owner"], vars["vid"])
	if err != nil {
		if kerrors.IsNotFound(err) {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		op.log.Error("fetching volume", "owner", vars["owner"], "vid", vars["vid"], "err", err)
		rw.WriteHeader(http.StatusInternalServerError)

		return
	}

	rw.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(rw).Encode(volumeState(vol)); err != nil {
		op.log.Error("encoding volume state", "err", err)
	}
}
