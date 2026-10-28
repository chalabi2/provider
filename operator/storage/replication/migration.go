package replication

import (
	"context"
	"errors"
	"time"

	"cosmossdk.io/log"
)

// MigrationDeps are the injected seams the destination-side migration
// runner drives (DESIGN.md §9.3). Everything chain- or cluster-facing is
// behind a function so the state machine is testable with fakes.
type MigrationDeps struct {
	// Sources are the providers that may hold the volume's data, the most
	// recent first (MigrationSources).
	Sources []string

	// Sync performs one sync round against source (Syncer.SyncOnce in
	// production): the first call lands the full export, later calls what
	// changed since. final reports that this side now holds the source's
	// final snapshot, so the copy is whole.
	Sync func(ctx context.Context, source string) (final bool, err error)

	// Park flips the destination Volume CRD from Importing to Provisioned:
	// the volume is whole and may attach.
	Park func(ctx context.Context) error

	// Evict gives the lease back when the copy cannot complete: a
	// volume_evict reclamation, after which the order re-lists while the
	// sources keep the data through their retention.
	Evict func(ctx context.Context) error

	// KeepLocal reports that this side holds a whole copy from its own
	// earlier lease, older than every source. If none of the sources ever
	// held the volume, that copy is the latest, and the volume parks with
	// it rather than being evicted.
	KeepLocal bool

	// Interval paces the sync rounds.
	Interval time.Duration

	// MaxFailures bounds consecutive failed rounds before the migration is
	// abandoned and the lease evicted.
	MaxFailures int
}

const (
	defaultMigrationInterval = time.Minute
	defaultMaxFailures       = 30
)

// ErrNoSource flags a migration with no provider to copy from.
var ErrNoSource = errors.New("volume replication: no provider holds the volume")

// RunMigration drives a volume migration on the destination provider:
//
//	full export -> rounds while the source runs -> source final (its lease
//	closed, its workloads stopped) -> final snapshot landed -> CRD Parked
//
// A source that never finished importing the volume itself refuses with
// ReasonImportIncomplete, and the runner moves on to the one before it. A
// source that cannot serve for MaxFailures rounds in a row ends the
// migration: the destination evicts its lease so the order re-lists, and
// the sources hold the data through their retention. The context ending
// (the daemon stopping, or the lease leaving Importing) ends the runner
// without evicting. Nothing is held that is not re-derivable: restarted,
// the runner pulls a full export again and converges the same way.
func RunMigration(ctx context.Context, deps MigrationDeps, logger log.Logger) error {
	interval := deps.Interval
	if interval <= 0 {
		interval = defaultMigrationInterval
	}

	maxFailures := deps.MaxFailures
	if maxFailures <= 0 {
		maxFailures = defaultMaxFailures
	}

	abandon := func(cause error) error {
		logger.Error("migration abandoned; evicting the volume lease", "err", cause)

		if err := deps.Evict(ctx); err != nil {
			logger.Error("evicting the volume lease", "err", err)
		}

		return cause
	}

	keepLocal := func(cause error) error {
		if err := deps.Park(ctx); err != nil {
			return abandon(err)
		}

		logger.Info("no provider since held the volume; keeping the copy here", "cause", cause)

		return nil
	}

	if len(deps.Sources) == 0 {
		if deps.KeepLocal {
			return keepLocal(ErrNoSource)
		}

		return abandon(ErrNoSource)
	}

	source := 0
	failures := 0

	for {
		final, err := deps.Sync(ctx, deps.Sources[source])
		if err == nil && final {
			err = deps.Park(ctx)
			if err == nil {
				logger.Info("migration complete; volume parked", "source", deps.Sources[source])
				return nil
			}
		}

		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err == nil:
			failures = 0
		case IsImportIncomplete(err):
			logger.Info("source never finished importing the volume; trying the one before it", "source", deps.Sources[source])

			source++
			failures = 0

			if source == len(deps.Sources) {
				if deps.KeepLocal {
					return keepLocal(err)
				}

				return abandon(err)
			}

			continue
		default:
			failures++

			logger.Error("migration round", "err", err, "source", deps.Sources[source], "failures", failures)

			if failures >= maxFailures {
				return abandon(err)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// ReplicaDeps drive the replica_of steady-state sync (DESIGN.md §9.4):
// the replica's operator pulls periodic diffs from the primary for as
// long as the replica lease lives. Paid work on both sides - the period
// comes from provider config.
type ReplicaDeps struct {
	// Sync performs one sync round against the primary.
	Sync func(ctx context.Context) (string, error)

	// Interval is the sync period (provider config).
	Interval time.Duration
}

const defaultReplicaInterval = 15 * time.Minute

// RunReplica pulls the initial full export then periodic diffs until the
// context ends (the replica lease closing tears the context down). Sync
// failures are logged and retried on the next tick - replica lag is
// observable via VolumeTransfer.Status, never chain-attested.
func RunReplica(ctx context.Context, deps ReplicaDeps, logger log.Logger) error {
	interval := deps.Interval
	if interval <= 0 {
		interval = defaultReplicaInterval
	}

	for {
		if applied, err := deps.Sync(ctx); err != nil {
			logger.Error("replica sync", "err", err)
		} else if applied != "" {
			logger.Info("replica synced", "snapshot", applied)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}
