package replication

import (
	"context"
	"time"

	"cosmossdk.io/log"
)

// MigrationDeps are the injected seams the destination-side migration
// runner drives (DESIGN.md §9.3). Everything chain- or cluster-facing is
// behind a function so the state machine is testable with fakes.
type MigrationDeps struct {
	// Sync performs one sync round against the source (Syncer.SyncOnce
	// in production): the first call lands the full export, later calls
	// land diffs. Returns the applied snapshot, or "" when in sync.
	Sync func(ctx context.Context) (string, error)

	// SourceClosed reports whether the source's volume lease has closed
	// (cascade-detach ran; the image is quiesced). The final export-diff
	// happens after this turns true.
	SourceClosed func(ctx context.Context) (bool, error)

	// Park flips the destination Volume CRD to Parked and reports ready.
	Park func(ctx context.Context) error

	// CloseBid gives the order back: if the import cannot complete, the
	// destination closes its bid and the order re-lists while the source
	// holds data through window + retention.
	CloseBid func(ctx context.Context) error

	// Interval paces the periodic diff pulls while the source is live.
	Interval time.Duration

	// MaxFailures bounds consecutive sync failures before the migration
	// is abandoned (bid closed).
	MaxFailures int
}

const (
	defaultMigrationInterval = 5 * time.Minute
	defaultMaxFailures       = 5
)

// RunMigration drives a volume migration on the destination provider:
//
//	full export -> periodic diffs -> source lease closed (quiesce) ->
//	final export-diff -> CRD Parked
//
// Any terminal failure closes the destination's bid so the order
// re-lists; the source holds data through window + retention either way.
// The runner holds no state that is not re-derivable: restarted, it
// re-pulls a full export and converges the same way.
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
		logger.Error("migration abandoned; closing bid", "err", cause)

		if err := deps.CloseBid(ctx); err != nil {
			logger.Error("closing bid", "err", err)
		}

		return cause
	}

	failures := 0

	for {
		closed, err := deps.SourceClosed(ctx)
		if err != nil {
			return abandon(err)
		}

		if _, err := deps.Sync(ctx); err != nil {
			failures++

			logger.Error("migration sync", "err", err, "failures", failures)

			if failures >= maxFailures {
				return abandon(err)
			}
		} else {
			failures = 0

			if closed {
				// the source was already quiesced when this round pulled:
				// that round was the final export-diff
				if err := deps.Park(ctx); err != nil {
					return abandon(err)
				}

				logger.Info("migration complete; volume parked")

				return nil
			}
		}

		select {
		case <-ctx.Done():
			return abandon(ctx.Err())
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
