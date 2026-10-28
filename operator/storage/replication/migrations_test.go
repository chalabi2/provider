package replication

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mv1 "pkg.akt.dev/go/node/market/v1"
	"pkg.akt.dev/go/testutil"
	vtv1 "pkg.akt.dev/go/volume/v1"

	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	afake "github.com/akash-network/provider/pkg/client/clientset/versioned/fake"
)

const migrationsNS = "lease"

type migrationsWorld struct {
	ctx     context.Context
	source  *transferWorld
	vols    *afake.Clientset
	dst     *FileDriver
	dstRef  Ref
	m       *Migrations
	mu      sync.Mutex
	dialed  []string
	evicted []mv1.LeaseID
	swept   []map[string]bool
}

func newMigrationsWorld(t *testing.T, phase crd.VolumePhase) *migrationsWorld {
	t.Helper()

	source := transferScaffold(t)
	source.chain.leases = []VolumeLease{
		closedLease(authzExporter, 1, mv1.LeaseClosedReasonVolumeMigrate),
		destinationLease(mv1.LeaseActive),
	}
	writeTree(t, source.src, source.ref, map[string]string{"app.db": "rows the tenant wrote"})

	ctx, cancel := context.WithCancel(context.Background())

	w := &migrationsWorld{
		ctx:    ctx,
		source: source,
		vols:   afake.NewSimpleClientset(importingVolume(phase)),
		dst:    NewFileDriver(t.TempDir()),
	}
	w.dstRef = Ref{Name: crd.VolumeName(authzOwner, authzVID), Path: t.TempDir()}

	w.m = NewMigrations(MigrationsConfig{
		Volumes:   w.vols,
		Namespace: migrationsNS,
		Self:      authzRequester,
		Chain:     source.chain,
		Source:    &staticVolumeSource{info: VolumeInfo{Ref: w.dstRef, DSeq: authzDSeq}},
		Driver:    w.dst,
		Dial: func(_ context.Context, provider string) (vtv1.VolumeTransferClient, io.Closer, error) {
			w.mu.Lock()
			defer w.mu.Unlock()

			w.dialed = append(w.dialed, provider)

			return source.client, io.NopCloser(nil), nil
		},
		Evict: func(_ context.Context, lid mv1.LeaseID) error {
			w.mu.Lock()
			defer w.mu.Unlock()

			w.evicted = append(w.evicted, lid)

			return nil
		},
		Sweep: func(live map[string]bool) error {
			w.mu.Lock()
			defer w.mu.Unlock()

			w.swept = append(w.swept, live)

			return nil
		},
		SpoolDir:    t.TempDir(),
		Interval:    time.Millisecond,
		MaxFailures: 3,
	}, testutil.Logger(t))

	t.Cleanup(func() {
		cancel()
		w.m.wg.Wait()
	})

	return w
}

func destinationLease(state mv1.Lease_State) VolumeLease {
	lease := migrationLease(authzRequester, state)
	lease.OSeq = 2

	return lease
}

func importingVolume(phase crd.VolumePhase) *crd.Volume {
	return &crd.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      crd.VolumeName(authzOwner, authzVID),
			Namespace: migrationsNS,
		},
		Spec: crd.VolumeSpec{
			Owner:   authzOwner,
			VID:     authzVID,
			GroupID: crd.VolumeGroupID{DSeq: "100", GSeq: 1},
			Size:    "1073741824",
			LeaseID: crd.LeaseID{
				Owner:    authzOwner,
				DSeq:     "100",
				GSeq:     1,
				OSeq:     2,
				Provider: authzRequester,
			},
			ImportFrom: authzExporter,
		},
		Status: crd.VolumeStatus{
			Phase:  phase,
			PVName: "pvc-0002",
		},
	}
}

func (w *migrationsWorld) volume(t *testing.T) *crd.Volume {
	t.Helper()

	vol, err := w.vols.AkashV2beta2().Volumes(migrationsNS).Get(context.Background(), crd.VolumeName(authzOwner, authzVID), metav1.GetOptions{})
	require.NoError(t, err)

	return vol
}

func (w *migrationsWorld) setPhase(t *testing.T, phase crd.VolumePhase) {
	t.Helper()

	vol := w.volume(t)
	vol.Status.Phase = phase

	_, err := w.vols.AkashV2beta2().Volumes(migrationsNS).Update(context.Background(), vol, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func (w *migrationsWorld) running() int {
	w.m.mu.Lock()
	defer w.m.mu.Unlock()

	return len(w.m.running)
}

func (w *migrationsWorld) dials() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]string(nil), w.dialed...)
}

func (w *migrationsWorld) evictions() []mv1.LeaseID {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]mv1.LeaseID(nil), w.evicted...)
}

func TestMigrationsCopyAVolumeInAndParkIt(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)
	w.source.goFinal()

	require.NoError(t, w.m.reconcile(w.ctx))

	require.Eventually(t, func() bool { return w.volume(t).Status.Phase == crd.VolumePhaseProvisioned }, 5*time.Second, time.Millisecond)
	require.Empty(t, w.volume(t).Spec.ImportFrom)
	require.Equal(t, map[string]string{"app.db": "rows the tenant wrote"}, readTree(t, w.dst, w.dstRef))
	require.Equal(t, []string{authzExporter}, w.dials())
	require.Empty(t, w.evictions())
	require.Eventually(t, func() bool { return w.running() == 0 }, 5*time.Second, time.Millisecond)
}

func TestMigrationsKeepCopyingUntilTheSourceIsFinal(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)

	require.NoError(t, w.m.reconcile(w.ctx))

	require.Eventually(t, func() bool {
		tree, err := tryReadTree(w.dst, w.dstRef)
		return err == nil && tree["app.db"] == "rows the tenant wrote"
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, crd.VolumePhaseImporting, w.volume(t).Status.Phase)

	writeTree(t, w.source.src, w.source.ref, map[string]string{"app.db": "the last write before the move"})
	w.source.goFinal()

	require.Eventually(t, func() bool { return w.volume(t).Status.Phase == crd.VolumePhaseProvisioned }, 5*time.Second, time.Millisecond)
	require.Equal(t, map[string]string{"app.db": "the last write before the move"}, readTree(t, w.dst, w.dstRef))
}

func TestMigrationsLeaveVolumesThatAreNotImporting(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseProvisioned)

	require.NoError(t, w.m.reconcile(w.ctx))

	require.Zero(t, w.running())
	require.Empty(t, w.dials())
}

func TestMigrationsStopWithoutEvictingWhenTheVolumeStopsImporting(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)

	require.NoError(t, w.m.reconcile(w.ctx))
	require.Eventually(t, func() bool { return len(w.dials()) == 1 }, 5*time.Second, time.Millisecond)

	w.setPhase(t, crd.VolumePhaseExporting)
	require.NoError(t, w.m.reconcile(w.ctx))

	require.Zero(t, w.running())
	w.m.wg.Wait()
	require.Empty(t, w.evictions())
}

func TestMigrationsLeaveALeaseBeingReclaimed(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)
	w.source.chain.update(func(chain *fakeChainState) { chain.leases[1] = destinationLease(mv1.LeaseReclaiming) })

	require.NoError(t, w.m.reconcile(w.ctx))
	w.m.wg.Wait()

	require.Empty(t, w.dials())
	require.Empty(t, w.evictions())
}

func TestMigrationsEvictWhenNoProviderHeldTheVolume(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)
	w.source.chain.update(func(chain *fakeChainState) { chain.leases = []VolumeLease{destinationLease(mv1.LeaseActive)} })

	require.NoError(t, w.m.reconcile(w.ctx))
	w.m.wg.Wait()

	require.Len(t, w.evictions(), 1)
	require.Equal(t, uint32(2), w.evictions()[0].OSeq)
	require.Equal(t, crd.VolumePhaseImporting, w.volume(t).Status.Phase)
}

func TestMigrationsEvictWhenTheSourceNeverServes(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)
	w.source.vols.update(func(info *VolumeInfo) { info.Exportable = false })

	require.NoError(t, w.m.reconcile(w.ctx))
	w.m.wg.Wait()

	require.Len(t, w.evictions(), 1)
	require.Empty(t, readTree(t, w.dst, w.dstRef))
}

func TestMigrationsRetryWhenTheChainCannotBeRead(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)
	w.source.chain.update(func(chain *fakeChainState) { chain.err = errors.New("node unreachable") })

	require.NoError(t, w.m.reconcile(w.ctx))
	w.m.wg.Wait()

	require.Zero(t, w.running())
	require.Empty(t, w.evictions())

	w.source.chain.update(func(chain *fakeChainState) { chain.err = nil })
	w.source.goFinal()

	require.NoError(t, w.m.reconcile(w.ctx))
	require.Eventually(t, func() bool { return w.volume(t).Status.Phase == crd.VolumePhaseProvisioned }, 5*time.Second, time.Millisecond)
}

func TestMigrationsRunOncePerVolume(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)

	require.NoError(t, w.m.reconcile(w.ctx))
	require.NoError(t, w.m.reconcile(w.ctx))

	require.Equal(t, 1, w.running())
}

func TestMigrationsSweepWhatTheDriverKeepsForGoneVolumes(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseProvisioned)

	require.NoError(t, w.m.reconcile(w.ctx))

	require.Equal(t, []map[string]bool{{crd.VolumeName(authzOwner, authzVID): true}}, w.swept)
}

func TestMigrationsStopEverythingWhenRunEnds(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)
	w.m.cfg.Scan = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- w.m.Run(ctx) }()

	require.Eventually(t, func() bool { return len(w.dials()) > 0 }, 5*time.Second, time.Millisecond)
	cancel()

	require.ErrorIs(t, <-done, context.Canceled)
	require.Empty(t, w.evictions())
}

func TestMigrationsKeepTheCopyHereWhenNoProviderSinceHeldTheVolume(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)
	vol := w.volume(t)
	vol.Spec.LeaseID.OSeq = 3
	vol.Status.LocalCopy = true
	_, err := w.vols.AkashV2beta2().Volumes(migrationsNS).Update(context.Background(), vol, metav1.UpdateOptions{})
	require.NoError(t, err)

	w.source.chain.update(func(chain *fakeChainState) {
		chain.leases = []VolumeLease{
			closedLease(authzRequester, 1, mv1.LeaseClosedReasonVolumeMigrate),
			closedLease(authzExporter, 2, mv1.LeaseClosedReasonVolumeEvict),
			destinationLease(mv1.LeaseActive),
		}
		chain.leases[2].OSeq = 3
	})
	w.source.vols.update(func(info *VolumeInfo) { info.Incomplete = true })

	require.NoError(t, w.m.reconcile(w.ctx))
	w.m.wg.Wait()

	parked := w.volume(t)
	require.Equal(t, crd.VolumePhaseProvisioned, parked.Status.Phase)
	require.False(t, parked.Status.LocalCopy)
	require.Empty(t, parked.Spec.ImportFrom)
	require.Empty(t, w.evictions())
}

func TestMigrationsDefaultTheirPatienceToTime(t *testing.T) {
	m := NewMigrations(MigrationsConfig{Interval: 5 * time.Second}, testutil.Logger(t))
	require.Equal(t, 360, m.cfg.MaxFailures)

	m = NewMigrations(MigrationsConfig{}, testutil.Logger(t))
	require.Equal(t, defaultMigrationInterval, m.cfg.Interval)
	require.Equal(t, 30, m.cfg.MaxFailures)

	m = NewMigrations(MigrationsConfig{Interval: time.Hour}, testutil.Logger(t))
	require.Equal(t, 1, m.cfg.MaxFailures)
}

func TestParkRefusesAVolumeThatMovedOn(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)
	lid, err := importingVolume(crd.VolumePhaseImporting).Spec.LeaseID.FromCRD()
	require.NoError(t, err)

	w.setPhase(t, crd.VolumePhaseExporting)
	require.ErrorIs(t, w.m.park(context.Background(), crd.VolumeName(authzOwner, authzVID), lid), ErrNotImporting)

	w.setPhase(t, crd.VolumePhaseImporting)
	lid.OSeq = 3
	require.ErrorIs(t, w.m.park(context.Background(), crd.VolumeName(authzOwner, authzVID), lid), ErrNotImporting)
}

func TestTransferBoundLeavesRoomForArchiveOverhead(t *testing.T) {
	bound, err := transferBound("1073741824")
	require.NoError(t, err)
	require.Equal(t, uint64(1073741824+536870912+transferOverheadBytes), bound)

	_, err = transferBound("lots")
	require.ErrorIs(t, err, ErrReplication)
}
