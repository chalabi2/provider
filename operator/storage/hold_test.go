package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mv1 "pkg.akt.dev/go/node/market/v1"
	"pkg.akt.dev/go/testutil"
	vtv1 "pkg.akt.dev/go/volume/v1"

	"github.com/akash-network/provider/operator/storage/replication"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

const testMoveHold = 168 * time.Hour

// movedAway is vol after its lease closed at closedAt for a move: frozen in
// Exporting, retained for retention and held for the provider it moved to
// to close + max(retention, testMoveHold).
func movedAway(vol *crd.Volume, retention time.Duration, closedAt time.Time) *crd.Volume {
	retainedUntil := metav1.NewTime(closedAt.Add(retention))
	heldUntil := metav1.NewTime(closedAt.Add(max(retention, testMoveHold)))

	vol.Spec.Retention = retention.String()
	vol.Status.Phase = crd.VolumePhaseExporting
	vol.Status.RetainedUntil = &retainedUntil
	vol.Status.HeldUntil = &heldUntil

	return vol
}

// copiedTo records that the final copy of vol reached provider at at.
func copiedTo(vol *crd.Volume, provider string, at time.Time) *crd.Volume {
	exported := metav1.NewTime(at)

	vol.Status.ExportedTo = provider
	vol.Status.ExportedAt = &exported

	return vol
}

// newLease is the lease the provider a volume moved to won on its re-listed
// order.
func newLease(lid mv1.LeaseID, provider string, state mv1.Lease_State) replication.VolumeLease {
	return replication.VolumeLease{
		Provider: provider,
		DSeq:     lid.DSeq,
		OSeq:     lid.OSeq + 1,
		State:    state,
		Policy:   dv1.VolumePolicy{Vid: testVID},
	}
}

func TestStampBoundsTheMoveHold(t *testing.T) {
	closedAt := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)

	for name, tc := range map[string]struct {
		retention time.Duration
		hold      time.Duration
		held      time.Duration
	}{
		"no retention":            {0, testMoveHold, testMoveHold},
		"retention past the hold": {720 * time.Hour, testMoveHold, 720 * time.Hour},
		"no hold":                 {24 * time.Hour, 0, 24 * time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			lid := testutil.LeaseID(t)
			vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
			vol.Spec.Retention = tc.retention.String()
			vol.Status.PVName = testPV

			s := makeOpScaffold(t, closedOnChain(lid, closedAt), []runtime.Object{vol})
			s.op.moveHold = tc.hold

			require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid, mv1.LeaseClosedReasonVolumeMigrate))

			stored := getOpVolume(t, s, vol.Name)
			require.Equal(t, crd.VolumePhaseExporting, stored.Status.Phase)
			require.True(t, closedAt.Add(tc.retention).Equal(stored.Status.RetainedUntil.Time))
			require.True(t, closedAt.Add(tc.held).Equal(stored.Status.HeldUntil.Time))
		})
	}
}

// TestMoveHoldKeepsAVolumeUntilItsCopyArrived: a volume that moved away is
// kept for the provider it moved to until a final export reached it whole
// and the grace passed, never before its retention, and never past close
// + max(retention, hold).
func TestMoveHoldKeepsAVolumeUntilItsCopyArrived(t *testing.T) {
	closedAt := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	dest := testutil.AccAddress(t).String()

	at := func(d time.Duration) time.Time { return closedAt.Add(d) }

	for name, tc := range map[string]struct {
		retention   time.Duration
		exportedAt  time.Duration // zero: never copied
		destination mv1.Lease_State
		noChain     bool
		incomplete  bool
		legacy      bool
		now         time.Duration
		released    bool
	}{
		"retention 0, not copied yet":                 {0, 0, mv1.LeaseActive, false, false, false, time.Hour, false},
		"retention 0, copied, within the grace":       {0, 2 * time.Hour, mv1.LeaseActive, false, false, false, 2*time.Hour + 30*time.Minute, false},
		"retention 0, copied, past the grace":         {0, 2 * time.Hour, mv1.LeaseActive, false, false, false, 3*time.Hour + time.Minute, true},
		"copied, but its new provider gave up":        {0, 2 * time.Hour, mv1.LeaseClosed, false, false, false, 3*time.Hour + time.Minute, false},
		"copied, its new provider gave up, the bound": {0, 2 * time.Hour, mv1.LeaseClosed, false, false, false, testMoveHold + time.Minute, true},
		"never copied, before the bound":              {0, 0, mv1.LeaseActive, false, false, false, testMoveHold - time.Minute, false},
		"never copied, at the bound":                  {0, 0, mv1.LeaseActive, false, false, false, testMoveHold + time.Minute, true},
		"retention past the hold, before it ends":     {720 * time.Hour, 2 * time.Hour, mv1.LeaseActive, false, false, false, 700 * time.Hour, false},
		"retention past the hold, once it ends":       {720 * time.Hour, 2 * time.Hour, mv1.LeaseActive, false, false, false, 720*time.Hour + time.Minute, true},
		"copied, the chain out of reach":              {0, 2 * time.Hour, mv1.LeaseActive, true, false, false, 3*time.Hour + time.Minute, false},
		"copied, the chain out of reach, the bound":   {0, 2 * time.Hour, mv1.LeaseActive, true, false, false, testMoveHold + time.Minute, true},
		"never whole here":                            {0, 0, mv1.LeaseActive, false, true, false, time.Minute, true},
		"stamped before holds existed":                {0, 0, mv1.LeaseActive, false, false, true, time.Minute, true},
	} {
		t.Run(name, func(t *testing.T) {
			lid := testutil.LeaseID(t)
			vol := movedAway(testVolume(t, lid, crd.VolumePhaseExporting), tc.retention, closedAt)
			vol.Status.PVName = testPV

			if tc.exportedAt != 0 {
				copiedTo(vol, dest, at(tc.exportedAt))
			}

			if tc.incomplete {
				vol.Spec.ImportFrom = testutil.AccAddress(t).String()
			}

			if tc.legacy {
				vol.Status.HeldUntil = nil
			}

			var chain *fakeChain
			if !tc.noChain {
				chain = &fakeChain{leases: []replication.VolumeLease{newLease(lid, dest, tc.destination)}}
			}

			s := makeScaffold(t, chain, nil, []runtime.Object{vol})
			s.now = at(tc.now)

			require.NoError(t, s.r.reconcile(context.Background(), vol))

			want := crd.VolumePhaseExporting
			if tc.released {
				want = crd.VolumePhaseRetained
			}

			require.Equal(t, want, getVolume(t, s, vol.Name).Status.Phase)
		})
	}
}

func TestMoveHoldKeepsABucketUntilItsCopyArrived(t *testing.T) {
	closedAt := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	dest := testutil.AccAddress(t).String()

	lid := testutil.LeaseID(t)
	vol := movedAway(provisioned(testBucketVolume(t, lid, crd.VolumePhaseExporting), crd.VolumePhaseExporting), 0, closedAt)

	s, fake, ref := bucketScaffold(t, &fakeChain{leases: []replication.VolumeLease{newLease(lid, dest, mv1.LeaseActive)}}, vol)
	ensureFakeBucket(t, fake, ref)
	s.now = closedAt.Add(time.Hour)

	// retention 0 and nothing copied yet: held, and sealed for the copy
	after, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseExporting, after.Status.Phase)
	require.True(t, fake.Exists(ref))

	_, err = s.ac.AkashV2beta2().Volumes(testNS).Update(context.Background(), copiedTo(after, dest, closedAt.Add(2*time.Hour)), metav1.UpdateOptions{})
	require.NoError(t, err)

	s.now = closedAt.Add(2*time.Hour + 30*time.Minute)

	after, err = reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseExporting, after.Status.Phase, "the new provider may still verify the copy")

	s.now = closedAt.Add(3*time.Hour + time.Minute)

	after, err = reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseRetained, after.Status.Phase)

	// suspended, then deleted the single destruction way
	after, err = reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Nil(t, after)
	require.False(t, fake.Exists(ref))
}

// TestMoveHoldKeepsItsCopiedMarkerOverARestart: the marker lives on the
// record. The operator's restart recovery leaves it, and a reconciler that
// never saw the export releases the volume from it.
func TestMoveHoldKeepsItsCopiedMarkerOverARestart(t *testing.T) {
	ctx := context.Background()
	closedAt := time.Now().Add(-4 * time.Hour).Truncate(time.Second)
	dest := testutil.AccAddress(t).String()

	lid := testutil.LeaseID(t)
	vol := movedAway(testVolume(t, lid, crd.VolumePhaseExporting), 0, closedAt)
	vol.Status.PVName = testPV

	chain := closedOnChain(lid, closedAt)
	chain.leases = []replication.VolumeLease{
		{Provider: lid.Provider, DSeq: lid.DSeq, OSeq: lid.OSeq, State: mv1.LeaseClosed, Reason: mv1.LeaseClosedReasonVolumeMigrate, Policy: dv1.VolumePolicy{Vid: testVID}},
		newLease(lid, dest, mv1.LeaseActive),
	}

	s := makeOpScaffold(t, chain, []runtime.Object{vol})
	s.op.moveHold = testMoveHold

	// the daemon's transfer server records the copy that reached dest
	copied := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	require.NoError(t, replication.NewCRDVolumeSource(s.ac, s.op.kc, testNS, nil).RecordExport(ctx, vol.Spec.Owner, testVID, dest, copied))

	// the operator restarts: its recovery re-lists the chain
	require.NoError(t, s.op.resyncChain(ctx))

	stored := getOpVolume(t, s, vol.Name)
	require.Equal(t, dest, stored.Status.ExportedTo)
	require.True(t, copied.Equal(stored.Status.ExportedAt.Time))
	require.True(t, closedAt.Add(testMoveHold).Equal(stored.Status.HeldUntil.Time))

	fresh := newReconciler(s.op.kc, s.ac, testNS, testVolNS, chain, testutil.Logger(t))
	require.NoError(t, fresh.reconcile(ctx, stored))
	require.Equal(t, crd.VolumePhaseRetained, getOpVolume(t, s, vol.Name).Status.Phase)
}

// exportStream collects what an export sends.
type exportStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent int
}

func (s *exportStream) Context() context.Context { return s.ctx }

func (s *exportStream) Send(chunk *vtv1.ExportChunk) error {
	s.sent += len(chunk.Data)
	return nil
}

// TestRetentionZeroVolumeMovesEndToEnd: a volume leased with no retention
// is kept past its close until the provider it moves to has its final
// copy, then released and destroyed once that provider had the grace to
// verify it.
func TestRetentionZeroVolumeMovesEndToEnd(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	closedAt := now.Add(-10 * time.Minute).Truncate(time.Second)
	dest := testutil.AccAddress(t).String()

	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Spec.Retention = "0s"
	vol.Status.PVName = testPV

	data := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(data, "app.db"), []byte("the tenant's rows"), 0o600))

	pv := testPVObj(vol, testVolNS, vol.Name)
	pv.Spec.PersistentVolumeSource = corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: data}}

	chain := closedOnChain(lid, closedAt)
	chain.leases = []replication.VolumeLease{
		{Provider: lid.Provider, DSeq: lid.DSeq, OSeq: lid.OSeq, State: mv1.LeaseClosed, Reason: mv1.LeaseClosedReasonVolumeMigrate, Policy: dv1.VolumePolicy{Vid: testVID}},
		newLease(lid, dest, mv1.LeaseActive),
	}

	s := makeOpScaffoldWithKube(t, chain, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})
	s.op.moveHold = testMoveHold

	// the source's lease closed for a move: with no retention, only the
	// hold keeps the volume
	require.NoError(t, s.op.applyLeaseClosed(ctx, lid, mv1.LeaseClosedReasonVolumeMigrate))
	require.NoError(t, s.op.reconcileAll(ctx))
	require.Equal(t, crd.VolumePhaseExporting, getOpVolume(t, s, vol.Name).Status.Phase)

	// the provider holding the new lease pulls the final copy through this
	// provider's transfer server
	volumes := replication.NewCRDVolumeSource(s.ac, s.op.kc, testNS, nil)
	srv := replication.NewServer(replication.NewFileDriver(t.TempDir()), chain, volumes, lid.Provider, testutil.Logger(t),
		replication.WithPeerID(func(context.Context) (string, error) { return dest, nil }),
		replication.WithExportRecorder(volumes),
	)

	st, err := srv.Status(ctx, &vtv1.StatusRequest{Owner: vol.Spec.Owner, Vid: testVID, DSeq: lid.DSeq, Requester: dest})
	require.NoError(t, err)
	require.True(t, st.Final)

	stream := &exportStream{ctx: ctx}
	require.NoError(t, srv.Export(&vtv1.ExportRequest{Owner: vol.Spec.Owner, Vid: testVID, DSeq: lid.DSeq, Requester: dest}, stream))
	require.Equal(t, int(st.Size_), stream.sent)

	copied := getOpVolume(t, s, vol.Name)
	require.Equal(t, dest, copied.Status.ExportedTo)

	// the new provider verifies the copy within the grace
	s.op.reconciler.now = func() time.Time { return now.Add(30 * time.Minute) }
	require.NoError(t, s.op.reconcileAll(ctx))
	require.Equal(t, crd.VolumePhaseExporting, getOpVolume(t, s, vol.Name).Status.Phase)

	// then the volume goes, the single destruction way
	s.op.reconciler.now = func() time.Time { return now.Add(exportGrace + time.Minute) }
	require.NoError(t, s.op.reconcileAll(ctx))
	require.Equal(t, crd.VolumePhaseRetained, getOpVolume(t, s, vol.Name).Status.Phase)

	require.NoError(t, s.op.reconcileAll(ctx))

	_, err = s.ac.AkashV2beta2().Volumes(testNS).Get(ctx, vol.Name, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err), "the volume outlived its hold")

	_, err = s.op.kc.CoreV1().PersistentVolumes().Get(ctx, testPV, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err))
}
