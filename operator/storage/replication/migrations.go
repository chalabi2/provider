package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	"cosmossdk.io/log"

	mv1 "pkg.akt.dev/go/node/market/v1"
	vtv1 "pkg.akt.dev/go/volume/v1"

	"github.com/akash-network/provider/bucket"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	akashclientset "github.com/akash-network/provider/pkg/client/clientset/versioned"
)

// ErrNotImporting flags a volume that left the Importing phase, or moved to
// another lease, before its copy could be parked.
var ErrNotImporting = errors.New("volume replication: volume is not importing")

const (
	defaultMigrationScan = 15 * time.Second

	// migrationPatience is how long a source may fail to serve before the
	// lease is given back, whatever the pace of the rounds
	migrationPatience = 30 * time.Minute

	// transferOverheadBytes is what a full volume's export may carry beyond
	// its data: archive headers and padding for the file backend.
	transferOverheadBytes = 64 << 20
)

// MigrationsConfig wires the migrations a provider runs as a destination.
type MigrationsConfig struct {
	// Volumes reads and parks the Volume CRDs in Namespace.
	Volumes   akashclientset.Interface
	Namespace string
	// Self is this provider's address.
	Self string
	// Chain answers which leases a volume had, and which provider held it.
	Chain ChainState
	// Source locates where an imported volume's data lands on this side.
	Source VolumeSource
	// Driver applies what is pulled.
	Driver Driver
	// Buckets receives the objects of the buckets moved here; nil on a
	// provider that serves none.
	Buckets bucket.Backend
	// Dial connects to a source provider's VolumeTransfer endpoint.
	Dial func(ctx context.Context, source string) (vtv1.VolumeTransferClient, io.Closer, error)
	// Evict starts a volume_evict reclamation of a lease whose copy cannot
	// complete.
	Evict func(ctx context.Context, lid mv1.LeaseID) error
	// Sweep drops what the driver keeps for volumes that are gone. Optional.
	Sweep func(live map[string]bool) error
	// SpoolDir holds the streams in flight; empty uses the OS temp dir.
	SpoolDir string
	// Scan paces the reads of the Volume CRDs.
	Scan time.Duration
	// Interval and MaxFailures pace each migration (MigrationDeps). Zero
	// MaxFailures allows failures for migrationPatience.
	Interval    time.Duration
	MaxFailures int
}

// Migrations copies in every volume moved to this provider (DESIGN.md
// §9.3). The storage operator parks a volume won on a re-listed order as
// Importing; Migrations runs one migration for each, and stops it once the
// volume leaves that phase, because the copy landed or the lease is going
// away. The CRDs and the chain hold all its state, so a restart resumes
// every migration with a full pull.
type Migrations struct {
	cfg MigrationsConfig
	log log.Logger

	mu      sync.Mutex
	running map[string]*migrationRun
	wg      sync.WaitGroup
}

type migrationRun struct {
	cancel context.CancelFunc
}

// NewMigrations builds the destination side of volume migrations.
func NewMigrations(cfg MigrationsConfig, logger log.Logger) *Migrations {
	if cfg.Scan <= 0 {
		cfg.Scan = defaultMigrationScan
	}

	if cfg.Interval <= 0 {
		cfg.Interval = defaultMigrationInterval
	}

	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = max(1, int(migrationPatience/cfg.Interval))
	}

	return &Migrations{
		cfg:     cfg,
		log:     logger.With("module", "volume-migrations"),
		running: make(map[string]*migrationRun),
	}
}

// Run reconciles the migrations with the Volume CRDs until ctx ends, then
// stops them all and waits for them.
func (m *Migrations) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.Scan)
	defer ticker.Stop()

	defer m.wg.Wait()

	for {
		if err := m.reconcile(ctx); err != nil && ctx.Err() == nil {
			m.log.Error("reconciling volume migrations", "err", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// reconcile starts a migration for every Importing volume without one, and
// stops every migration whose volume no longer imports under its lease.
func (m *Migrations) reconcile(ctx context.Context) error {
	list, err := m.cfg.Volumes.AkashV2beta2().Volumes(m.cfg.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	live := make(map[string]bool, len(list.Items))
	importing := make(map[string]*crd.Volume)

	for i := range list.Items {
		vol := &list.Items[i]
		live[vol.Name] = true

		if vol.Status.Phase == crd.VolumePhaseImporting && vol.Spec.ImportFrom != "" {
			importing[migrationKey(vol)] = vol
		}
	}

	m.mu.Lock()

	for key, run := range m.running {
		if importing[key] == nil {
			run.cancel()
			delete(m.running, key)
		}
	}

	for key, vol := range importing {
		if m.running[key] == nil {
			m.start(ctx, key, vol)
		}
	}

	m.mu.Unlock()

	if m.cfg.Sweep != nil {
		return m.cfg.Sweep(live)
	}

	return nil
}

func migrationKey(vol *crd.Volume) string {
	return vol.Name + "@" + vol.Spec.LeaseID.DSeq + "/" + strconv.FormatUint(uint64(vol.Spec.LeaseID.OSeq), 10)
}

// start runs vol's migration; m.mu is held.
func (m *Migrations) start(ctx context.Context, key string, vol *crd.Volume) {
	rctx, cancel := context.WithCancel(ctx)
	run := &migrationRun{cancel: cancel}
	m.running[key] = run

	m.wg.Add(1)

	go func() {
		defer m.wg.Done()
		defer cancel()

		if err := m.migrate(rctx, vol.DeepCopy()); err != nil && rctx.Err() == nil {
			m.log.Error("volume migration ended", "volume", vol.Name, "err", err)
		}

		m.mu.Lock()
		if m.running[key] == run {
			delete(m.running, key)
		}
		m.mu.Unlock()
	}()
}

// migrate copies vol in from the providers that held it before.
func (m *Migrations) migrate(ctx context.Context, vol *crd.Volume) error {
	lid, err := vol.Spec.LeaseID.FromCRD()
	if err != nil {
		return err
	}

	leases, err := m.cfg.Chain.VolumeLeases(ctx, vol.Spec.Owner, vol.Spec.VID)
	if err != nil {
		return err
	}

	// a lease already being reclaimed has nothing left to copy into
	if !leaseActive(leases, lid) {
		return nil
	}

	maxBytes, err := transferBound(vol.Spec.Size)
	if err != nil {
		return err
	}

	logger := m.log.With("volume", vol.Name, "lid", lid)

	var copier func(client vtv1.VolumeTransferClient) syncRound

	if vol.IsBucket() {
		if copier, err = m.bucketCopier(vol, lid.DSeq, maxBytes, logger); err != nil {
			return err
		}
	} else if copier, err = m.imageCopier(ctx, vol, lid.DSeq, maxBytes, logger); err != nil {
		return err
	}

	rounds := make(map[string]syncRound)

	var closers []io.Closer

	defer func() {
		for _, closer := range closers {
			_ = closer.Close()
		}
	}()

	round := func(ctx context.Context, source string) (syncRound, error) {
		if r := rounds[source]; r != nil {
			return r, nil
		}

		if err := AuthorizeImport(ctx, m.cfg.Chain, vol.Spec.Owner, vol.Spec.VID, source, false); err != nil {
			return nil, err
		}

		client, closer, err := m.cfg.Dial(ctx, source)
		if err != nil {
			return nil, err
		}

		closers = append(closers, closer)

		r := copier(client)
		rounds[source] = r

		return r, nil
	}

	sources := MigrationSources(leases, vol.Spec.VID, lid.DSeq, m.cfg.Self, vol.Status.LocalCopy)

	logger.Info("volume migration started", "sources", sources, "local_copy", vol.Status.LocalCopy, "bucket", vol.IsBucket())

	return RunMigration(ctx, MigrationDeps{
		Sources: sources,
		Sync: func(ctx context.Context, source string) (bool, error) {
			r, err := round(ctx, source)
			if err != nil {
				return false, err
			}

			return r(ctx, source)
		},
		Park: func(ctx context.Context) error {
			return m.park(ctx, vol.Name, lid)
		},
		Evict: func(ctx context.Context) error {
			return m.cfg.Evict(ctx, lid)
		},
		KeepLocal:   vol.Status.LocalCopy,
		Interval:    m.cfg.Interval,
		MaxFailures: m.cfg.MaxFailures,
	}, logger)
}

// syncRound runs one round of a migration against the source it was made
// for, and reports whether the copy here is whole.
type syncRound func(ctx context.Context, source string) (bool, error)

// imageCopier pulls a mounted volume's image through the driver.
func (m *Migrations) imageCopier(ctx context.Context, vol *crd.Volume, dseq, maxBytes uint64, logger log.Logger) (func(vtv1.VolumeTransferClient) syncRound, error) {
	info, err := m.cfg.Source.Resolve(ctx, vol.Spec.Owner, vol.Spec.VID)
	if err != nil {
		return nil, err
	}

	return func(client vtv1.VolumeTransferClient) syncRound {
		s := &Syncer{
			Client:    client,
			Driver:    m.cfg.Driver,
			Ref:       info.Ref,
			Owner:     vol.Spec.Owner,
			Vid:       vol.Spec.VID,
			DSeq:      dseq,
			Requester: m.cfg.Self,
			SpoolDir:  m.cfg.SpoolDir,
			MaxBytes:  maxBytes,
		}

		return func(ctx context.Context, source string) (bool, error) {
			if applied, err := s.SyncOnce(ctx); err != nil {
				return false, err
			} else if applied != "" {
				logger.Info("volume snapshot landed", "source", source, "snapshot", applied, "final", s.Final())
			}

			return s.Final(), nil
		}
	}, nil
}

// bucketCopier copies a bucket's objects into the bucket here.
func (m *Migrations) bucketCopier(vol *crd.Volume, dseq, maxBytes uint64, logger log.Logger) (func(vtv1.VolumeTransferClient) syncRound, error) {
	if m.cfg.Buckets == nil {
		return nil, fmt.Errorf("%w: bucket %s moved here, but this provider serves no buckets", ErrReplication, vol.Name)
	}

	ref := bucket.RefFor(vol.Name, vol.Spec.Owner, vol.Spec.VID)

	return func(client vtv1.VolumeTransferClient) syncRound {
		s := &BucketSyncer{
			Client: client,
			Open: func(ctx context.Context) (bucket.Objects, error) {
				return m.cfg.Buckets.Objects(ctx, ref)
			},
			Owner:     vol.Spec.Owner,
			Vid:       vol.Spec.VID,
			DSeq:      dseq,
			Requester: m.cfg.Self,
			SpoolDir:  m.cfg.SpoolDir,
			MaxBytes:  maxBytes,
		}

		return func(ctx context.Context, source string) (bool, error) {
			complete, err := s.SyncOnce(ctx)
			if err != nil {
				if s.Objects() > 0 {
					logger.Info("bucket objects landing", "source", source, "objects", s.Objects(), "bytes", s.Bytes())
				}

				return false, err
			}

			if complete {
				logger.Info("bucket objects landed", "source", source, "objects", s.Objects(), "bytes", s.Bytes())
			}

			return complete, nil
		}
	}, nil
}

// park records the copy as whole: the volume is Provisioned, and nothing
// marks it as still arriving.
func (m *Migrations) park(ctx context.Context, name string, lid mv1.LeaseID) error {
	vols := m.cfg.Volumes.AkashV2beta2().Volumes(m.cfg.Namespace)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		vol, err := vols.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		current, err := vol.Spec.LeaseID.FromCRD()
		if err != nil || !current.Equals(lid) || vol.Status.Phase != crd.VolumePhaseImporting {
			return fmt.Errorf("%w: %s", ErrNotImporting, name)
		}

		uvol := vol.DeepCopy()
		uvol.Spec.ImportFrom = ""
		uvol.Status.LocalCopy = false
		uvol.Status.Phase = crd.VolumePhaseProvisioned

		_, err = vols.Update(ctx, uvol, metav1.UpdateOptions{})

		return err
	})
}

// leaseActive reports whether lid is on chain and active.
func leaseActive(leases []VolumeLease, lid mv1.LeaseID) bool {
	for _, lease := range leases {
		if lease.Provider == lid.Provider && lease.DSeq == lid.DSeq && lease.OSeq == lid.OSeq {
			return lease.State == mv1.LeaseActive
		}
	}

	return false
}

// transferBound is the most a source may send for a volume of size bytes.
func transferBound(size string) (uint64, error) {
	bytes, err := strconv.ParseUint(size, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: volume size %q: %s", ErrReplication, size, err.Error())
	}

	return bytes + bytes/2 + transferOverheadBytes, nil
}
