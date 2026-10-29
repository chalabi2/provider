package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	"cosmossdk.io/log"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mv1 "pkg.akt.dev/go/node/market/v1"
	vtv1 "pkg.akt.dev/go/volume/v1"

	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	akashclientset "github.com/akash-network/provider/pkg/client/clientset/versioned"
	akashv2beta2 "github.com/akash-network/provider/pkg/client/clientset/versioned/typed/akash.network/v2beta2"
)

// errNotFollowing flags a replica whose record moved on under a round: to
// another lease, to another primary, or out of the phases a replica syncs
// in. The scan stops its sync.
var errNotFollowing = errors.New("volume replication: volume no longer follows its primary here")

// ReplicasConfig wires the replicas a provider keeps in sync with their
// primaries.
type ReplicasConfig struct {
	// Volumes reads the Volume CRDs in Namespace and records each round.
	Volumes   akashclientset.Interface
	Namespace string
	// Self is this provider's address.
	Self string
	// Chain answers which provider holds each primary.
	Chain ChainState
	// Source locates where a replica's data lands on this side.
	Source VolumeSource
	// Driver applies what is pulled.
	Driver Driver
	// Dial connects to a primary's provider's VolumeTransfer endpoint.
	Dial func(ctx context.Context, provider string) (vtv1.VolumeTransferClient, io.Closer, error)
	// SpoolDir holds the streams in flight; empty uses the OS temp dir.
	SpoolDir string
	// Scan paces the reads of the Volume CRDs.
	Scan time.Duration
	// Interval paces each replica's rounds (--volume-replica-sync-interval).
	Interval time.Duration
}

// Replicas keeps every replica leased here in sync with its primary
// (DESIGN.md §9.4). A replica is a volume whose group names another as
// replica_of; it parks as Importing until its first whole copy lands, then
// as Provisioned, and each round copies what changed at the primary since.
// The replica only receives: it pulls from the provider holding the
// primary's active lease, as the chain has it, and serves its primary
// nothing.
//
// A round marks the record Syncing before it writes the volume, and
// AttachVolume refuses a record so marked, both on the record's resource
// version: compute never mounts a volume mid-write, and no round starts on
// a volume compute attached. Compute attaching promotes the replica, and
// its sync stops for good. It stops too once the replica lease ends, which
// moves the record out of the phases a replica syncs in. The CRDs and the
// chain hold all its state, so a restart resumes each replica with a full
// copy.
type Replicas struct {
	cfg ReplicasConfig
	log log.Logger

	mu      sync.Mutex
	running map[string]*replicaRun
	wg      sync.WaitGroup
}

type replicaRun struct {
	cancel context.CancelFunc
}

// NewReplicas builds the replica side of volume replication.
func NewReplicas(cfg ReplicasConfig, logger log.Logger) *Replicas {
	if cfg.Scan <= 0 {
		cfg.Scan = defaultMigrationScan
	}

	if cfg.Interval <= 0 {
		cfg.Interval = defaultReplicaInterval
	}

	return &Replicas{
		cfg:     cfg,
		log:     logger.With("module", "volume-replicas"),
		running: make(map[string]*replicaRun),
	}
}

// Run reconciles the replica syncs with the Volume CRDs until ctx ends, then
// stops them all and waits for them.
func (r *Replicas) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.Scan)
	defer ticker.Stop()

	defer r.wg.Wait()

	for {
		if err := r.reconcile(ctx); err != nil && ctx.Err() == nil {
			r.log.Error("reconciling volume replicas", "err", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// reconcile starts a sync for every replica that follows its primary here
// without one, and stops every sync whose replica no longer does.
func (r *Replicas) reconcile(ctx context.Context) error {
	list, err := r.cfg.Volumes.AkashV2beta2().Volumes(r.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	following := make(map[string]*crd.Volume)

	for i := range list.Items {
		vol := &list.Items[i]

		if replicaFollows(vol) {
			following[migrationKey(vol)] = vol
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for key, run := range r.running {
		if following[key] == nil {
			run.cancel()
			delete(r.running, key)
		}
	}

	for key, vol := range following {
		if r.running[key] == nil {
			r.start(ctx, key, vol)
		}
	}

	return nil
}

// replicaFollows reports whether a volume is a replica this provider copies
// its primary into: a disk with replica_of, its PV parked here, under its
// own live lease (Importing or Provisioned), and never promoted. A replica
// that compute attached while a round was writing it still follows until
// that round lands: nothing mounted it meanwhile, and it holds no whole
// copy until then.
func replicaFollows(vol *crd.Volume) bool {
	if !vol.IsReplica() || vol.IsBucket() || vol.Spec.ImportFrom != "" || vol.Status.PVName == "" {
		return false
	}

	switch vol.Status.Phase {
	case crd.VolumePhaseImporting, crd.VolumePhaseProvisioned, crd.VolumePhaseAttached:
	default:
		return false
	}

	if vol.Status.Syncing {
		return true
	}

	return !vol.Status.Promoted && vol.Status.AttachedLease == ""
}

// start runs vol's sync; r.mu is held.
func (r *Replicas) start(ctx context.Context, key string, vol *crd.Volume) {
	rctx, cancel := context.WithCancel(ctx)
	run := &replicaRun{cancel: cancel}
	r.running[key] = run

	r.wg.Add(1)

	go func() {
		defer r.wg.Done()
		defer cancel()

		if err := r.follow(rctx, vol.DeepCopy()); err != nil && rctx.Err() == nil {
			r.log.Error("replica sync ended", "volume", vol.Name, "err", err)
		}

		r.mu.Lock()
		if r.running[key] == run {
			delete(r.running, key)
		}
		r.mu.Unlock()
	}()
}

// follow keeps vol in sync with its primary until the replica is promoted
// or ctx ends.
func (r *Replicas) follow(ctx context.Context, vol *crd.Volume) error {
	f, err := r.newFollower(ctx, vol)
	if err != nil {
		return err
	}
	defer f.disconnect()

	f.log.Info("replica sync started", "synced_at", vol.Status.SyncedAt, "syncing", vol.Status.Syncing)

	return RunReplica(ctx, ReplicaDeps{
		Sync:     f.round,
		Interval: r.cfg.Interval,
	}, f.log)
}

func (r *Replicas) newFollower(ctx context.Context, vol *crd.Volume) (*follower, error) {
	primary, err := dv1.ParseVolumeRef(vol.Spec.ReplicaOf)
	if err != nil {
		return nil, fmt.Errorf("%w: volume %s replica_of %q: %s", ErrReplication, vol.Name, vol.Spec.ReplicaOf, err.Error())
	}

	lid, err := vol.Spec.LeaseID.FromCRD()
	if err != nil {
		return nil, err
	}

	maxBytes, err := transferBound(vol.Spec.Size)
	if err != nil {
		return nil, err
	}

	info, err := r.cfg.Source.Resolve(ctx, vol.Spec.Owner, vol.Spec.VID)
	if err != nil {
		return nil, err
	}

	return &follower{
		r:        r,
		name:     vol.Name,
		owner:    vol.Spec.Owner,
		vid:      vol.Spec.VID,
		lid:      lid,
		primary:  primary,
		ref:      info.Ref,
		maxBytes: maxBytes,
		log:      r.log.With("volume", vol.Name, "lid", lid, "primary", primary.String()),
	}, nil
}

// follower syncs one replica: its rounds, and the connection to the
// provider holding its primary.
type follower struct {
	r        *Replicas
	name     string
	owner    string
	vid      string
	lid      mv1.LeaseID
	primary  dv1.VolumeRef
	ref      Ref
	maxBytes uint64
	log      log.Logger

	// source is the provider client pulls from; a primary that moved is
	// pulled from its new provider, afresh
	source string
	client vtv1.VolumeTransferClient
	closer io.Closer
	syncer *Syncer
	// fenced reports that the round in flight marked the record Syncing
	fenced bool
}

// round runs one sync round: find the primary's provider on chain, pull
// what changed there, and land it in the volume while nothing can attach
// it.
func (f *follower) round(ctx context.Context) (string, error) {
	if err := f.stillFollowing(ctx); err != nil {
		return "", err
	}

	leases, err := f.r.cfg.Chain.VolumeLeases(ctx, f.primary.Owner, f.primary.Name)
	if err != nil {
		return "", err
	}

	// the record follows the replica lease's close; the chain may be ahead
	if !leaseActive(leases, f.lid) {
		return "", fmt.Errorf("%w: replica lease %s is not active", ErrReplication, f.lid)
	}

	// the source is the chain's answer, which is the importer's gate: the
	// provider of the primary's active lease
	source, found := ReplicaSource(leases, f.primary)
	if !found {
		return "", fmt.Errorf("%w: primary %s has no active lease to copy from", ErrReplication, f.primary)
	}

	if source == f.r.cfg.Self {
		return "", fmt.Errorf("%w: primary %s is leased on this provider; a replica copies only from another", ErrReplication, f.primary)
	}

	if err := f.connect(ctx, source); err != nil {
		return "", err
	}

	f.fenced = false

	applied, err := f.syncer.SyncOnce(ctx)
	if err != nil {
		if f.fenced && !errors.Is(err, ErrReplicaDone) {
			// the volume may hold part of a round: the next one copies it
			// whole, and the record stays Syncing until it lands
			f.syncer = f.newSyncer()
		}

		return "", err
	}

	if applied == "" {
		return "", nil
	}

	return applied, f.landed(ctx, f.syncer.Taken())
}

// connect dials the primary's provider, again once the primary moved.
func (f *follower) connect(ctx context.Context, source string) error {
	if f.client != nil && f.source == source {
		return nil
	}

	f.disconnect()

	client, closer, err := f.r.cfg.Dial(ctx, source)
	if err != nil {
		return err
	}

	f.source, f.client, f.closer = source, client, closer
	f.syncer = f.newSyncer()

	f.log.Info("replica copying from its primary's provider", "source", source)

	return nil
}

// newSyncer starts the pull from the primary over: its first round copies
// the volume whole. Its writes go through the round's fence.
func (f *follower) newSyncer() *Syncer {
	return &Syncer{
		Client: f.client,
		Driver: &fencedDriver{
			Driver: f.r.cfg.Driver,
			fence:  f.fence,
		},
		Ref:       f.ref,
		Owner:     f.primary.Owner,
		Vid:       f.primary.Name,
		DSeq:      f.primary.DSeq,
		Requester: f.r.cfg.Self,
		SpoolDir:  f.r.cfg.SpoolDir,
		MaxBytes:  f.maxBytes,
	}
}

func (f *follower) disconnect() {
	if f.closer != nil {
		_ = f.closer.Close()
	}

	f.source, f.client, f.closer, f.syncer = "", nil, nil, nil
}

func (f *follower) volumes() akashv2beta2.VolumeInterface {
	return f.r.cfg.Volumes.AkashV2beta2().Volumes(f.r.cfg.Namespace)
}

// record reads the volume's record and checks it is still this replica,
// under this lease, following this primary.
func (f *follower) record(ctx context.Context) (*crd.Volume, error) {
	vol, err := f.volumes().Get(ctx, f.name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	lid, err := vol.Spec.LeaseID.FromCRD()
	if err != nil || !lid.Equals(f.lid) || !vol.Matches(f.owner, f.vid) || vol.Spec.ReplicaOf != f.primary.String() {
		return nil, fmt.Errorf("%w: %s", errNotFollowing, f.name)
	}

	return vol, nil
}

// stillFollowing ends the sync for good once compute attached the replica
// and no round is left to land, recording the promotion if only the
// attachment was.
func (f *follower) stillFollowing(ctx context.Context) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		vol, err := f.record(ctx)
		if err != nil {
			return err
		}

		if vol.Status.Syncing || (vol.Status.AttachedLease == "" && !vol.Status.Promoted) {
			return nil
		}

		if !vol.Status.Promoted {
			uvol := vol.DeepCopy()
			uvol.Status.Promoted = true

			if _, err := f.volumes().Update(ctx, uvol, metav1.UpdateOptions{}); err != nil {
				return err
			}
		}

		return ErrReplicaDone
	})
}

// fence marks the record Syncing before a round writes the volume. It is
// written on the record's resource version, as AttachVolume writes the
// attachment, so exactly one of them wins a race: a round never writes a
// volume compute attached, and compute never attaches one mid-round.
func (f *follower) fence(ctx context.Context) error {
	f.fenced = true

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		vol, err := f.record(ctx)
		if err != nil {
			return err
		}

		// the first copy, or a round that never landed: nothing mounted
		// the volume since
		if vol.Status.Syncing {
			return nil
		}

		if vol.Status.Promoted || vol.Status.AttachedLease != "" {
			return ErrReplicaDone
		}

		if !replicaFollows(vol) {
			return fmt.Errorf("%w: %s is %s", errNotFollowing, f.name, vol.Status.Phase)
		}

		uvol := vol.DeepCopy()
		uvol.Status.Syncing = true

		_, err = f.volumes().Update(ctx, uvol, metav1.UpdateOptions{})

		return err
	})
}

// landed records a round that landed whole: the replica holds its primary
// as it was at taken, nothing is being written, and a replica whose first
// copy this was may attach.
func (f *follower) landed(ctx context.Context, taken time.Time) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		vol, err := f.record(ctx)
		if err != nil {
			return err
		}

		uvol := vol.DeepCopy()
		synced := metav1.NewTime(taken)
		uvol.Status.SyncedAt = &synced
		uvol.Status.Syncing = false

		if uvol.Status.Phase == crd.VolumePhaseImporting {
			uvol.Status.Phase = crd.VolumePhaseProvisioned
		}

		_, err = f.volumes().Update(ctx, uvol, metav1.UpdateOptions{})

		return err
	})
}

// fencedDriver raises the round's fence before the first byte is written.
type fencedDriver struct {
	Driver
	fence func(ctx context.Context) error
}

func (d *fencedDriver) Import(ctx context.Context, ref Ref, snapshot, fromSnapshot string, r io.Reader) error {
	if err := d.fence(ctx); err != nil {
		return err
	}

	return d.Driver.Import(ctx, ref, snapshot, fromSnapshot, r)
}
