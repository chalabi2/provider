package replication

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"pkg.akt.dev/go/testutil"
)

// fakeMigration is the fake-CRD world the migration runner drives: sync
// rounds mutate counters instead of volumes.
type fakeMigration struct {
	syncs        int
	syncErrs     map[int]error // 1-based round -> error
	closedAfter  int           // source lease closes after this many syncs
	parked       bool
	bidClosed    bool
	sourceErr    error
	sourceClosed bool
}

func (f *fakeMigration) deps() MigrationDeps {
	return MigrationDeps{
		Sync: func(_ context.Context) (string, error) {
			f.syncs++
			if err := f.syncErrs[f.syncs]; err != nil {
				return "", err
			}

			return fmt.Sprintf("snap-%08d", f.syncs), nil
		},
		SourceClosed: func(_ context.Context) (bool, error) {
			if f.sourceErr != nil {
				return false, f.sourceErr
			}

			return f.sourceClosed || (f.closedAfter > 0 && f.syncs >= f.closedAfter), nil
		},
		Park: func(_ context.Context) error {
			f.parked = true
			return nil
		},
		CloseBid: func(_ context.Context) error {
			f.bidClosed = true
			return nil
		},
		Interval:    time.Millisecond,
		MaxFailures: 3,
	}
}

func TestMigrationHappyPath(t *testing.T) {
	// round 1: full pull, source still live; round 2: diff, source still
	// live; round 3 observes the source closed BEFORE pulling - that pull
	// is the final export-diff and the volume parks
	f := &fakeMigration{closedAfter: 2}

	err := RunMigration(context.Background(), f.deps(), testutil.Logger(t))
	require.NoError(t, err)

	require.True(t, f.parked)
	require.False(t, f.bidClosed)
	require.Equal(t, 3, f.syncs)
}

func TestMigrationImmediateCutover(t *testing.T) {
	// the source was already closed when the migration started
	// (cascade-detach ran at close): one final full pull, then park
	f := &fakeMigration{sourceClosed: true}

	err := RunMigration(context.Background(), f.deps(), testutil.Logger(t))
	require.NoError(t, err)

	require.True(t, f.parked)
	require.False(t, f.bidClosed)
	require.Equal(t, 1, f.syncs)
}

func TestMigrationTransientFailureRecovers(t *testing.T) {
	f := &fakeMigration{
		closedAfter: 3,
		syncErrs:    map[int]error{2: errors.New("stream broke")},
	}

	err := RunMigration(context.Background(), f.deps(), testutil.Logger(t))
	require.NoError(t, err)

	require.True(t, f.parked)
	require.False(t, f.bidClosed)
}

func TestMigrationExhaustedFailuresClosesBid(t *testing.T) {
	boom := errors.New("import keeps failing")

	f := &fakeMigration{
		syncErrs: map[int]error{1: boom, 2: boom, 3: boom},
	}

	err := RunMigration(context.Background(), f.deps(), testutil.Logger(t))
	require.ErrorIs(t, err, boom)

	// the destination gives the order back; the source holds through
	// window + retention
	require.True(t, f.bidClosed)
	require.False(t, f.parked)
	require.Equal(t, 3, f.syncs)
}

func TestMigrationSourceProbeFailureClosesBid(t *testing.T) {
	boom := errors.New("chain unreachable")

	f := &fakeMigration{sourceErr: boom}

	err := RunMigration(context.Background(), f.deps(), testutil.Logger(t))
	require.ErrorIs(t, err, boom)
	require.True(t, f.bidClosed)
	require.False(t, f.parked)
}

func TestMigrationContextCancelClosesBid(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	f := &fakeMigration{}

	deps := f.deps()
	deps.Sync = func(_ context.Context) (string, error) {
		f.syncs++
		cancel()
		return "snap-00000001", nil
	}

	err := RunMigration(ctx, deps, testutil.Logger(t))
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, f.bidClosed)
	require.False(t, f.parked)
}

func TestReplicaLoopSyncsUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	syncs := 0

	deps := ReplicaDeps{
		Sync: func(_ context.Context) (string, error) {
			syncs++
			if syncs >= 3 {
				cancel()
			}

			return fmt.Sprintf("snap-%08d", syncs), nil
		},
		Interval: time.Millisecond,
	}

	err := RunReplica(ctx, deps, testutil.Logger(t))
	require.ErrorIs(t, err, context.Canceled)
	require.GreaterOrEqual(t, syncs, 3)
}

func TestReplicaLoopSurvivesSyncErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	syncs := 0

	deps := ReplicaDeps{
		Sync: func(_ context.Context) (string, error) {
			syncs++
			if syncs >= 3 {
				cancel()
			}

			return "", errors.New("primary unreachable")
		},
		Interval: time.Millisecond,
	}

	err := RunReplica(ctx, deps, testutil.Logger(t))
	require.ErrorIs(t, err, context.Canceled)
	require.GreaterOrEqual(t, syncs, 3)
}
