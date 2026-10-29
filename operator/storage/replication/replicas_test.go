package replication

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mv1 "pkg.akt.dev/go/node/market/v1"
	"pkg.akt.dev/go/testutil"
	vtv1 "pkg.akt.dev/go/volume/v1"

	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	afake "github.com/akash-network/provider/pkg/client/clientset/versioned/fake"
)

const replicasNS = "lease"

// replicasWorld is a replica's provider (authzRequester) following a
// primary served by an in-process transfer server (authzExporter).
type replicasWorld struct {
	ctx     context.Context
	primary *replicaWorld
	vols    *afake.Clientset
	dst     Driver
	dstFile *FileDriver
	dstRef  Ref
	r       *Replicas

	mu      sync.Mutex
	dialed  []string
	dialErr []error
	exports []string
}

func newReplicasWorld(t *testing.T, opts ...func(*replicasWorld)) *replicasWorld {
	t.Helper()

	primary := replicaScaffold(t, 1)
	writeTree(t, primary.src, primary.ref, map[string]string{"app.db": "the primary's rows"})

	ctx, cancel := context.WithCancel(context.Background())

	w := &replicasWorld{
		ctx:     ctx,
		primary: primary,
		vols:    afake.NewSimpleClientset(replicaVolume()),
		dstFile: NewFileDriver(t.TempDir()),
	}
	w.dst = w.dstFile
	w.dstRef = Ref{Name: replicaVolume().Name, Path: t.TempDir()}

	for _, opt := range opts {
		opt(w)
	}

	w.r = NewReplicas(ReplicasConfig{
		Volumes:   w.vols,
		Namespace: replicasNS,
		Self:      authzRequester,
		Chain:     primary.chain,
		Source:    &staticVolumeSource{info: VolumeInfo{Ref: w.dstRef}},
		Driver:    w.dst,
		Dial: func(_ context.Context, provider string) (vtv1.VolumeTransferClient, io.Closer, error) {
			w.mu.Lock()
			defer w.mu.Unlock()

			w.dialed = append(w.dialed, provider)

			if len(w.dialErr) > 0 {
				err := w.dialErr[0]
				w.dialErr = w.dialErr[1:]

				return nil, nil, err
			}

			return &recordingClient{VolumeTransferClient: primary.client, w: w}, io.NopCloser(nil), nil
		},
		SpoolDir: t.TempDir(),
		Interval: time.Millisecond,
	}, testutil.Logger(t))

	t.Cleanup(func() {
		cancel()
		w.r.wg.Wait()
	})

	return w
}

// recordingClient records the diff base of every export the replica asks
// for.
type recordingClient struct {
	vtv1.VolumeTransferClient
	w *replicasWorld
	// onExport runs before each export, as the export starts
	onExport func()
}

func (c *recordingClient) Export(ctx context.Context, in *vtv1.ExportRequest, opts ...grpc.CallOption) (vtv1.VolumeTransfer_ExportClient, error) {
	c.w.mu.Lock()
	c.w.exports = append(c.w.exports, in.FromSnapshot)
	c.w.mu.Unlock()

	if c.onExport != nil {
		c.onExport()
	}

	return c.VolumeTransferClient.Export(ctx, in, opts...)
}

func replicaVolume() *crd.Volume {
	vid := authzVID + "-replica"

	return &crd.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      crd.VolumeName(authzOwner, vid),
			Namespace: replicasNS,
		},
		Spec: crd.VolumeSpec{
			Owner:   authzOwner,
			VID:     vid,
			GroupID: crd.VolumeGroupID{DSeq: "150", GSeq: 1},
			Size:    "1073741824",
			LeaseID: crd.LeaseID{
				Owner:    authzOwner,
				DSeq:     "150",
				GSeq:     1,
				OSeq:     1,
				Provider: authzRequester,
			},
			ReplicaOf: dv1.VolumeRef{Owner: authzOwner, DSeq: authzDSeq, GSeq: 1, Name: authzVID}.String(),
		},
		Status: crd.VolumeStatus{
			Phase:   crd.VolumePhaseImporting,
			PVName:  "pvc-replica",
			Syncing: true,
		},
	}
}

func (w *replicasWorld) volume(t *testing.T) *crd.Volume {
	t.Helper()

	vol, err := w.vols.AkashV2beta2().Volumes(replicasNS).Get(context.Background(), replicaVolume().Name, metav1.GetOptions{})
	require.NoError(t, err)

	return vol
}

func (w *replicasWorld) update(t *testing.T, fn func(*crd.Volume)) {
	t.Helper()

	vol := w.volume(t)
	fn(vol)

	_, err := w.vols.AkashV2beta2().Volumes(replicasNS).Update(context.Background(), vol, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func (w *replicasWorld) follower(t *testing.T) *follower {
	t.Helper()

	f, err := w.r.newFollower(w.ctx, w.volume(t))
	require.NoError(t, err)

	t.Cleanup(f.disconnect)

	return f
}

func (w *replicasWorld) running() int {
	w.r.mu.Lock()
	defer w.r.mu.Unlock()

	return len(w.r.running)
}

func (w *replicasWorld) dials() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]string(nil), w.dialed...)
}

func (w *replicasWorld) exportBases() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]string(nil), w.exports...)
}

func (w *replicasWorld) tree(t *testing.T) map[string]string {
	t.Helper()

	tree, err := tryReadTree(w.dstFile, w.dstRef)
	if err != nil {
		return nil
	}

	return tree
}

// writePrimary changes the primary's data, past the time its provider
// takes a new snapshot for replicas.
func (w *replicasWorld) writePrimary(t *testing.T, rows string) {
	t.Helper()

	writeTree(t, w.primary.src, w.primary.ref, map[string]string{"app.db": rows})
	w.primary.advance(2 * time.Minute)
}

func TestReplicasCopyThePrimaryInAndPark(t *testing.T) {
	w := newReplicasWorld(t)

	require.NoError(t, w.r.reconcile(w.ctx))

	require.Eventually(t, func() bool { return w.volume(t).Status.Phase == crd.VolumePhaseProvisioned }, 5*time.Second, time.Millisecond)

	vol := w.volume(t)
	require.False(t, vol.Status.Syncing)
	require.NotNil(t, vol.Status.SyncedAt)
	require.WithinDuration(t, time.Now(), vol.Status.SyncedAt.Time, time.Minute)
	require.Equal(t, map[string]string{"app.db": "the primary's rows"}, w.tree(t))
	require.Equal(t, authzExporter, w.dials()[0])
}

func TestReplicasFollowThePrimary(t *testing.T) {
	w := newReplicasWorld(t)

	require.NoError(t, w.r.reconcile(w.ctx))
	require.Eventually(t, func() bool { return w.volume(t).Status.Phase == crd.VolumePhaseProvisioned }, 5*time.Second, time.Millisecond)

	w.writePrimary(t, "rows written since the first copy")

	require.Eventually(t, func() bool {
		return w.tree(t)["app.db"] == "rows written since the first copy"
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, 1, w.running())
}

func TestReplicasDateWhatTheyHoldByThePrimarysSnapshot(t *testing.T) {
	w := newReplicasWorld(t)
	f := w.follower(t)

	_, err := f.round(w.ctx)
	require.NoError(t, err)

	first := w.volume(t).Status.SyncedAt.Time

	// the primary's provider hands out the same snapshot within the minute
	w.primary.advance(30 * time.Second)

	applied, err := f.round(w.ctx)
	require.NoError(t, err)
	require.Empty(t, applied)
	require.True(t, first.Equal(w.volume(t).Status.SyncedAt.Time), "a round that copied nothing leaves the replica as old as it was")
}

func TestReplicasStopForGoodOnceComputeAttaches(t *testing.T) {
	w := newReplicasWorld(t)
	f := w.follower(t)

	_, err := f.round(w.ctx)
	require.NoError(t, err)

	// compute attaches, as the chain's attach event records it
	w.update(t, func(vol *crd.Volume) {
		vol.Status.AttachedLease = testutil.LeaseID(t).String()
		vol.Status.Phase = crd.VolumePhaseAttached
	})

	w.writePrimary(t, "rows the promoted replica must never see")

	_, err = f.round(w.ctx)
	require.ErrorIs(t, err, ErrReplicaDone)
	require.True(t, w.volume(t).Status.Promoted, "the promotion outlives the attachment")
	require.Equal(t, map[string]string{"app.db": "the primary's rows"}, w.tree(t))

	// detached, it never follows its primary again
	w.update(t, func(vol *crd.Volume) {
		vol.Status.AttachedLease = ""
		vol.Status.Phase = crd.VolumePhaseProvisioned
	})

	require.NoError(t, w.r.reconcile(w.ctx))
	require.Zero(t, w.running())

	_, err = f.round(w.ctx)
	require.ErrorIs(t, err, ErrReplicaDone)
	require.Equal(t, map[string]string{"app.db": "the primary's rows"}, w.tree(t))
}

// TestReplicaRoundLosesToAnAttachment: compute attaches while a round
// pulls. The round's fence finds the attachment and writes nothing.
func TestReplicaRoundLosesToAnAttachment(t *testing.T) {
	w := newReplicasWorld(t)
	f := w.follower(t)

	_, err := f.round(w.ctx)
	require.NoError(t, err)

	w.writePrimary(t, "rows the promoted replica must never see")

	f.client.(*recordingClient).onExport = func() {
		w.update(t, func(vol *crd.Volume) {
			vol.Status.AttachedLease = testutil.LeaseID(t).String()
			vol.Status.Promoted = true
		})
	}

	_, err = f.round(w.ctx)
	require.ErrorIs(t, err, ErrReplicaDone)

	vol := w.volume(t)
	require.False(t, vol.Status.Syncing)
	require.Equal(t, map[string]string{"app.db": "the primary's rows"}, w.tree(t))
}

// TestReplicaAttachedMidRoundLandsItsRound: compute attached while a round
// was writing the replica. Nothing mounted it - the record says it is being
// written - so the round lands, and only then does the sync stop.
func TestReplicaAttachedMidRoundLandsItsRound(t *testing.T) {
	w := newReplicasWorld(t)
	w.update(t, func(vol *crd.Volume) {
		vol.Status.AttachedLease = testutil.LeaseID(t).String()
		vol.Status.Promoted = true
		vol.Status.Phase = crd.VolumePhaseAttached
	})

	require.NoError(t, w.r.reconcile(w.ctx))

	require.Eventually(t, func() bool { return !w.volume(t).Status.Syncing }, 5*time.Second, time.Millisecond)
	require.Equal(t, map[string]string{"app.db": "the primary's rows"}, w.tree(t))
	require.Eventually(t, func() bool { return w.running() == 0 }, 5*time.Second, time.Millisecond)
}

func TestReplicasRetryWhileThePrimarysProviderIsUnreachable(t *testing.T) {
	w := newReplicasWorld(t, func(w *replicasWorld) {
		w.dialErr = []error{errors.New("connection refused"), errors.New("connection refused")}
	})

	require.NoError(t, w.r.reconcile(w.ctx))

	require.Eventually(t, func() bool { return w.volume(t).Status.Phase == crd.VolumePhaseProvisioned }, 5*time.Second, time.Millisecond)
	require.GreaterOrEqual(t, len(w.dials()), 3)
}

func TestReplicasCopyFromWhereThePrimaryMoved(t *testing.T) {
	w := newReplicasWorld(t)
	f := w.follower(t)

	_, err := f.round(w.ctx)
	require.NoError(t, err)

	w.primary.chain.update(func(chain *fakeChainState) {
		chain.leases[0].State = mv1.LeaseClosed
		chain.leases[0].Reason = mv1.LeaseClosedReasonVolumeMigrate

		moved := migrationLease(authzStranger, mv1.LeaseActive)
		moved.OSeq = 2
		chain.leases = append(chain.leases, moved)
	})

	w.writePrimary(t, "rows written at the primary's new provider")

	_, err = f.round(w.ctx)
	require.NoError(t, err)

	require.Equal(t, []string{authzExporter, authzStranger}, w.dials())
	require.Equal(t, "", w.exportBases()[len(w.exportBases())-1], "a new provider's copy starts whole")
}

func TestReplicasWaitWhileThePrimaryHasNoActiveLease(t *testing.T) {
	w := newReplicasWorld(t)
	w.primary.chain.update(func(chain *fakeChainState) { chain.leases[0].State = mv1.LeaseReclaiming })

	_, err := w.follower(t).round(w.ctx)
	require.ErrorIs(t, err, ErrReplication)
	require.Empty(t, w.dials())
	require.True(t, w.volume(t).Status.Syncing)
}

func TestReplicasNeverCopyFromThisProvider(t *testing.T) {
	w := newReplicasWorld(t)
	w.primary.chain.update(func(chain *fakeChainState) { chain.leases[0].Provider = authzRequester })

	_, err := w.follower(t).round(w.ctx)
	require.ErrorIs(t, err, ErrReplication)
	require.Empty(t, w.dials())
}

func TestReplicasStopWhenTheReplicaLeaseCloses(t *testing.T) {
	w := newReplicasWorld(t)
	w.primary.chain.update(func(chain *fakeChainState) { chain.leases[1].State = mv1.LeaseClosed })

	// the chain is ahead of the record: no round runs
	_, err := w.follower(t).round(w.ctx)
	require.ErrorIs(t, err, ErrReplication)
	require.Empty(t, w.dials())

	require.NoError(t, w.r.reconcile(w.ctx))
	require.Equal(t, 1, w.running())

	// the operator retains the closed replica: its sync stops
	w.update(t, func(vol *crd.Volume) { vol.Status.Phase = crd.VolumePhaseRetained })

	require.NoError(t, w.r.reconcile(w.ctx))
	require.Zero(t, w.running())
	w.r.wg.Wait()
	require.Empty(t, w.tree(t))
}

// failingDriver fails the imports it is told to, after they began.
type failingDriver struct {
	Driver
	mu    sync.Mutex
	fails int
}

func (d *failingDriver) Import(ctx context.Context, ref Ref, snapshot, fromSnapshot string, r io.Reader) error {
	d.mu.Lock()
	fail := d.fails > 0
	if fail {
		d.fails--
	}
	d.mu.Unlock()

	if fail {
		return errors.New("disk full")
	}

	return d.Driver.Import(ctx, ref, snapshot, fromSnapshot, r)
}

// TestReplicaRoundThatFailsWritingCopiesWholeNext: a round that failed
// after it began writing leaves the record Syncing, so nothing attaches the
// part-written volume, and the next round copies the volume whole.
func TestReplicaRoundThatFailsWritingCopiesWholeNext(t *testing.T) {
	failing := &failingDriver{}

	w := newReplicasWorld(t, func(w *replicasWorld) {
		failing.Driver = w.dstFile
		w.dst = failing
	})
	f := w.follower(t)

	_, err := f.round(w.ctx)
	require.NoError(t, err)

	w.writePrimary(t, "rows of the round that fails")

	failing.fails = 1

	_, err = f.round(w.ctx)
	require.Error(t, err)
	require.True(t, w.volume(t).Status.Syncing, "the part-written volume is no whole copy: nothing attaches it")
	require.True(t, w.volume(t).Incomplete())

	_, err = f.round(w.ctx)
	require.NoError(t, err)

	bases := w.exportBases()
	require.Len(t, bases, 3)
	require.Empty(t, bases[0])
	require.NotEmpty(t, bases[1], "the second round asked for a diff")
	require.Empty(t, bases[2], "after a failed write the round copies the volume whole")

	require.False(t, w.volume(t).Status.Syncing)
	require.Equal(t, map[string]string{"app.db": "rows of the round that fails"}, w.tree(t))
}

func TestReplicasFollowOnlyReplicasTheyMayWrite(t *testing.T) {
	for name, set := range map[string]func(*crd.Volume){
		"not a replica":   func(vol *crd.Volume) { vol.Spec.ReplicaOf = "" },
		"a bucket":        func(vol *crd.Volume) { vol.Spec.Access = dv1.VolumeAccessBucket.String() },
		"promoted":        func(vol *crd.Volume) { vol.Status.Syncing = false; vol.Status.Promoted = true },
		"attached":        func(vol *crd.Volume) { vol.Status.Syncing = false; vol.Status.AttachedLease = "lease" },
		"moving here":     func(vol *crd.Volume) { vol.Spec.ImportFrom = authzStranger },
		"not provisioned": func(vol *crd.Volume) { vol.Status.PVName = ""; vol.Status.Phase = crd.VolumePhasePending },
		"lease closing":   func(vol *crd.Volume) { vol.Status.Phase = crd.VolumePhaseExporting },
		"lease closed":    func(vol *crd.Volume) { vol.Status.Phase = crd.VolumePhaseRetained },
		"being adopted":   func(vol *crd.Volume) { vol.Status.Phase = crd.VolumePhaseAdopting },
		"being destroyed": func(vol *crd.Volume) { vol.Status.Phase = crd.VolumePhaseReleasing },
		"promoted, never synced": func(vol *crd.Volume) {
			vol.Status.Syncing = false
			vol.Status.Promoted = true
			vol.Status.Phase = crd.VolumePhaseProvisioned
		},
	} {
		t.Run(name, func(t *testing.T) {
			vol := replicaVolume()
			set(vol)

			require.False(t, replicaFollows(vol))
		})
	}

	require.True(t, replicaFollows(replicaVolume()))
}

func TestReplicasStartOncePerReplicaAndStopWithRun(t *testing.T) {
	w := newReplicasWorld(t)
	w.r.cfg.Scan = time.Millisecond

	ctx, cancel := context.WithCancel(w.ctx)
	done := make(chan error, 1)

	go func() { done <- w.r.Run(ctx) }()

	require.Eventually(t, func() bool { return w.running() == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, w.r.reconcile(ctx))
	require.Equal(t, 1, w.running())

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, w.running())
}

func TestReplicasDefaultTheirPace(t *testing.T) {
	r := NewReplicas(ReplicasConfig{}, testutil.Logger(t))
	require.Equal(t, defaultReplicaInterval, r.cfg.Interval)
	require.Equal(t, defaultMigrationScan, r.cfg.Scan)
}
