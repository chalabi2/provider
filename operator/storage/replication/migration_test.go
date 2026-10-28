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

// fakeMigration is the world the migration runner drives: each source
// answers its rounds from a script, and parking and evicting are recorded.
type fakeMigration struct {
	rounds   map[string][]roundResult
	synced   []string
	parkErrs []error
	parked   bool
	evicted  bool
}

type roundResult struct {
	final bool
	err   error
}

func (f *fakeMigration) deps(sources ...string) MigrationDeps {
	return MigrationDeps{
		Sources: sources,
		Sync: func(_ context.Context, source string) (bool, error) {
			f.synced = append(f.synced, source)

			script := f.rounds[source]
			if len(script) == 0 {
				return false, nil
			}

			round := script[0]
			if len(script) > 1 {
				f.rounds[source] = script[1:]
			}

			return round.final, round.err
		},
		Park: func(_ context.Context) error {
			if len(f.parkErrs) > 0 {
				err := f.parkErrs[0]
				f.parkErrs = f.parkErrs[1:]

				return err
			}

			f.parked = true

			return nil
		},
		Evict: func(_ context.Context) error {
			f.evicted = true
			return nil
		},
		Interval:    time.Millisecond,
		MaxFailures: 3,
	}
}

func incompleteRefusal() error {
	return importIncompleteError(authzOwner, authzVID)
}

func TestMigrationParksOnceTheSourceIsFinal(t *testing.T) {
	f := &fakeMigration{rounds: map[string][]roundResult{
		authzExporter: {{}, {}, {final: true}},
	}}

	err := RunMigration(context.Background(), f.deps(authzExporter), testutil.Logger(t))
	require.NoError(t, err)

	require.True(t, f.parked)
	require.False(t, f.evicted)
	require.Len(t, f.synced, 3)
}

func TestMigrationParksAtOnceFromAFinalSource(t *testing.T) {
	f := &fakeMigration{rounds: map[string][]roundResult{authzExporter: {{final: true}}}}

	err := RunMigration(context.Background(), f.deps(authzExporter), testutil.Logger(t))
	require.NoError(t, err)

	require.True(t, f.parked)
	require.Len(t, f.synced, 1)
}

func TestMigrationRecoversFromATransientFailure(t *testing.T) {
	f := &fakeMigration{rounds: map[string][]roundResult{
		authzExporter: {{err: errors.New("stream broke")}, {err: errors.New("stream broke")}, {final: true}},
	}}

	err := RunMigration(context.Background(), f.deps(authzExporter), testutil.Logger(t))
	require.NoError(t, err)

	require.True(t, f.parked)
	require.False(t, f.evicted)
}

func TestMigrationEvictsWhenTheSourceKeepsFailing(t *testing.T) {
	boom := errors.New("source unreachable")
	f := &fakeMigration{rounds: map[string][]roundResult{authzExporter: {{err: boom}}}}

	err := RunMigration(context.Background(), f.deps(authzExporter), testutil.Logger(t))
	require.ErrorIs(t, err, boom)

	require.True(t, f.evicted)
	require.False(t, f.parked)
	require.Len(t, f.synced, 3)
}

func TestMigrationNeverFallsBackPastASourceThatFails(t *testing.T) {
	boom := errors.New("source unreachable")
	f := &fakeMigration{rounds: map[string][]roundResult{authzExporter: {{err: boom}}}}

	err := RunMigration(context.Background(), f.deps(authzExporter, authzStranger), testutil.Logger(t))
	require.ErrorIs(t, err, boom)

	require.NotContains(t, f.synced, authzStranger)
	require.True(t, f.evicted)
}

func TestMigrationLooksPastASourceThatNeverFinishedImporting(t *testing.T) {
	f := &fakeMigration{rounds: map[string][]roundResult{
		authzExporter: {{err: incompleteRefusal()}},
		authzStranger: {{final: true}},
	}}

	err := RunMigration(context.Background(), f.deps(authzExporter, authzStranger), testutil.Logger(t))
	require.NoError(t, err)

	require.Equal(t, []string{authzExporter, authzStranger}, f.synced)
	require.True(t, f.parked)
}

func TestMigrationEvictsWhenNoSourceHoldsTheVolume(t *testing.T) {
	f := &fakeMigration{rounds: map[string][]roundResult{authzExporter: {{err: incompleteRefusal()}}}}

	err := RunMigration(context.Background(), f.deps(authzExporter), testutil.Logger(t))
	require.True(t, IsImportIncomplete(err))

	require.True(t, f.evicted)
	require.False(t, f.parked)
}

func TestMigrationKeepsTheCopyHereWhenNoSourceEverHeldTheVolume(t *testing.T) {
	f := &fakeMigration{rounds: map[string][]roundResult{authzExporter: {{err: incompleteRefusal()}}}}
	deps := f.deps(authzExporter)
	deps.KeepLocal = true

	require.NoError(t, RunMigration(context.Background(), deps, testutil.Logger(t)))

	require.True(t, f.parked)
	require.False(t, f.evicted)
}

func TestMigrationKeepsTheCopyHereWithoutASource(t *testing.T) {
	f := &fakeMigration{}
	deps := f.deps()
	deps.KeepLocal = true

	require.NoError(t, RunMigration(context.Background(), deps, testutil.Logger(t)))

	require.True(t, f.parked)
	require.Empty(t, f.synced)
}

func TestMigrationNeverKeepsTheCopyHereOverASourceThatFails(t *testing.T) {
	boom := errors.New("source unreachable")
	f := &fakeMigration{rounds: map[string][]roundResult{authzExporter: {{err: boom}}}}
	deps := f.deps(authzExporter)
	deps.KeepLocal = true

	require.ErrorIs(t, RunMigration(context.Background(), deps, testutil.Logger(t)), boom)

	require.True(t, f.evicted)
	require.False(t, f.parked)
}

func TestMigrationEvictsWhenKeepingTheCopyHereFails(t *testing.T) {
	f := &fakeMigration{parkErrs: []error{errors.New("conflict")}}
	deps := f.deps()
	deps.KeepLocal = true

	require.Error(t, RunMigration(context.Background(), deps, testutil.Logger(t)))

	require.True(t, f.evicted)
}

func TestMigrationEvictsWithoutASource(t *testing.T) {
	f := &fakeMigration{}

	err := RunMigration(context.Background(), f.deps(), testutil.Logger(t))
	require.ErrorIs(t, err, ErrNoSource)

	require.True(t, f.evicted)
	require.Empty(t, f.synced)
}

func TestMigrationRetriesParking(t *testing.T) {
	f := &fakeMigration{
		rounds:   map[string][]roundResult{authzExporter: {{final: true}}},
		parkErrs: []error{errors.New("conflict")},
	}

	err := RunMigration(context.Background(), f.deps(authzExporter), testutil.Logger(t))
	require.NoError(t, err)

	require.True(t, f.parked)
	require.Len(t, f.synced, 2)
}

func TestMigrationStopsWithoutEvictingWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	f := &fakeMigration{}
	deps := f.deps(authzExporter)
	deps.Sync = func(_ context.Context, _ string) (bool, error) {
		cancel()
		return false, context.Canceled
	}

	err := RunMigration(ctx, deps, testutil.Logger(t))
	require.ErrorIs(t, err, context.Canceled)

	require.False(t, f.evicted)
	require.False(t, f.parked)
}

func TestMigrationStopsBetweenRoundsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	f := &fakeMigration{}
	deps := f.deps(authzExporter)
	deps.Interval = time.Hour
	deps.Sync = func(_ context.Context, _ string) (bool, error) {
		cancel()
		return false, nil
	}

	err := RunMigration(ctx, deps, testutil.Logger(t))
	require.ErrorIs(t, err, context.Canceled)

	require.False(t, f.evicted)
}

func TestMigrationDefaultsItsPace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	f := &fakeMigration{rounds: map[string][]roundResult{authzExporter: {{final: true}}}}
	deps := f.deps(authzExporter)
	deps.Interval = 0
	deps.MaxFailures = 0
	defer cancel()

	require.NoError(t, RunMigration(ctx, deps, testutil.Logger(t)))
	require.True(t, f.parked)
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
