package storage

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	sdkmath "cosmossdk.io/math"
	abci "github.com/cometbft/cometbft/abci/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mv1 "pkg.akt.dev/go/node/market/v1"
	"pkg.akt.dev/go/testutil"

	"github.com/akash-network/provider/operator/storage/replication"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	afake "github.com/akash-network/provider/pkg/client/clientset/versioned/fake"
)

type opScaffold struct {
	op *storageOperator
	ac *afake.Clientset
}

type fakeChainClient struct {
	fakeChain
	active map[string]bool

	size      uint64
	sizeCalls int
}

func (f *fakeChainClient) ActiveLeases(_ context.Context) (map[string]bool, error) {
	return f.active, nil
}

func (f *fakeChainClient) VolumeSize(_ context.Context, _ dv1.GroupID) (uint64, error) {
	f.sizeCalls++
	return f.size, nil
}

func (f *fakeChainClient) Events(_ context.Context, _ string) (<-chan interface{}, error) {
	ch := make(chan interface{})
	close(ch)
	return ch, nil
}

func makeOpScaffold(t *testing.T, chain ChainClient, aobjs []runtime.Object) *opScaffold {
	t.Helper()

	kc := kfake.NewSimpleClientset()
	ac := afake.NewSimpleClientset(aobjs...)

	log := testutil.Logger(t)

	return &opScaffold{
		op: &storageOperator{
			ctx:        context.Background(),
			kc:         kc,
			ac:         ac,
			ns:         testNS,
			volNS:      testVolNS,
			log:        log,
			reconciler: newReconciler(kc, ac, testNS, testVolNS, chain, log),
			chain:      chain,
		},
		ac: ac,
	}
}

func TestApplyLeaseClosedRetainsVolume(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid, mv1.LeaseClosedReasonOwner))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseRetained, uvol.Status.Phase)
	require.NotNil(t, uvol.Status.RetainedUntil)

	// retainedUntil = closedAt + retention (event receipt stands in for
	// closedAt until the chain exposes it)
	expected := time.Now().Add(168 * time.Hour)
	require.WithinDuration(t, expected, uvol.Status.RetainedUntil.Time, time.Minute)
}

func TestApplyLeaseClosedReclaimDeleteReleases(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Spec.Reclaim = dv1.VolumeReclaimDelete.String()
	vol.Status.PVName = testPV

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid, mv1.LeaseClosedReasonOwner))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseReleasing, uvol.Status.Phase)
	require.NotNil(t, uvol.Status.RetainedUntil)
	require.WithinDuration(t, time.Now(), uvol.Status.RetainedUntil.Time, time.Minute)
}

func TestApplyLeaseClosedDetachesComputeLease(t *testing.T) {
	lid := testutil.LeaseID(t)
	computeLid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseAttached)
	vol.Status.PVName = testPV
	vol.Status.AttachedLease = computeLid.String()

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), computeLid, mv1.LeaseClosedReasonOwner))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, "", uvol.Status.AttachedLease)
	// the volume's own lease is untouched
	require.NotEqual(t, crd.VolumePhaseRetained, uvol.Status.Phase)
}

func TestApplyLeaseClosedIgnoresFrozenPhases(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseAdopting)
	vol.Status.PVName = testPV

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid, mv1.LeaseClosedReasonOwner))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseAdopting, uvol.Status.Phase)
	require.Nil(t, uvol.Status.RetainedUntil)
}

func TestApplyLeaseClosedExportingStampsRetention(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseExporting)
	vol.Status.PVName = testPV

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid, mv1.LeaseClosedReasonOwner))

	// the retention clock starts (source holds through window + retention)
	// but the phase stays Exporting: GC remains frozen while the
	// destination pulls the final diff
	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseExporting, uvol.Status.Phase)
	require.NotNil(t, uvol.Status.RetainedUntil)
	require.WithinDuration(t, time.Now().Add(168*time.Hour), uvol.Status.RetainedUntil.Time, time.Minute)
}

func TestApplyReclaimStartedFreezesExporting(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	require.NoError(t, s.op.applyChainEvent(context.Background(), &mv1.EventLeaseReclaimStarted{
		ID:     lid,
		Reason: mv1.LeaseClosedReasonVolumeEvict,
	}))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseExporting, uvol.Status.Phase)
	require.Nil(t, uvol.Status.RetainedUntil)
}

func TestApplyReclaimStartedIgnoresOtherLeases(t *testing.T) {
	lid := testutil.LeaseID(t)
	otherLid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	require.NoError(t, s.op.applyChainEvent(context.Background(), &mv1.EventLeaseReclaimStarted{
		ID:     otherLid,
		Reason: mv1.LeaseClosedReasonVolumeMigrate,
	}))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseProvisioned, uvol.Status.Phase)
}

func TestResyncChainStampsExportingRetention(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseExporting)
	vol.Status.PVName = testPV

	// the volume lease closed while the operator was down
	chain := &fakeChainClient{active: map[string]bool{}}

	s := makeOpScaffold(t, chain, []runtime.Object{vol})

	require.NoError(t, s.op.resyncChain(context.Background()))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseExporting, uvol.Status.Phase)
	require.NotNil(t, uvol.Status.RetainedUntil)
}

func TestApplyAttachDetachEvents(t *testing.T) {
	lid := testutil.LeaseID(t)
	computeLid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	ref := dv1.VolumeRef{
		Owner: lid.Owner,
		DSeq:  lid.DSeq,
		GSeq:  lid.GSeq,
		Name:  testVID,
	}

	require.NoError(t, s.op.applyChainEvent(context.Background(), &mv1.EventVolumeAttached{
		LeaseID: computeLid,
		Volume:  ref,
	}))

	require.Equal(t, computeLid.String(), getOpVolume(t, s, vol.Name).Status.AttachedLease)

	require.NoError(t, s.op.applyChainEvent(context.Background(), &mv1.EventVolumeDetached{
		LeaseID: computeLid,
		Volume:  ref,
		Reason:  mv1.LeaseClosedReasonOwner,
	}))

	require.Equal(t, "", getOpVolume(t, s, vol.Name).Status.AttachedLease)
}

// TestVolumeNameCollisionIgnored: events and requests for a volume whose
// name collides with another owner's record leave that record alone.
func TestVolumeNameCollisionIgnored(t *testing.T) {
	lid := testutil.LeaseID(t)
	victimLid := testutil.LeaseID(t)
	computeLid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseAttached)
	vol.Spec.Owner = victimLid.Owner
	vol.Spec.VID = "victim-data"
	vol.Status.PVName = testPV
	vol.Status.AttachedLease = victimLid.String()

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	ref := dv1.VolumeRef{Owner: lid.Owner, DSeq: lid.DSeq, GSeq: lid.GSeq, Name: testVID}

	require.NoError(t, s.op.applyChainEvent(context.Background(), &mv1.EventVolumeAttached{LeaseID: computeLid, Volume: ref}))
	require.NoError(t, s.op.applyChainEvent(context.Background(), &mv1.EventVolumeDetached{
		LeaseID: victimLid,
		Volume:  ref,
		Reason:  mv1.LeaseClosedReasonOwner,
	}))
	require.NoError(t, s.op.applyChainEvent(context.Background(), &dv1.EventVolumeAdopted{
		ID:      dv1.GroupID{Owner: lid.Owner, DSeq: lid.DSeq + 7, GSeq: 1},
		Adopted: ref,
		Vid:     testVID,
	}))

	after := getOpVolume(t, s, vol.Name)
	require.Equal(t, vol.Spec, after.Spec)
	require.Equal(t, vol.Status, after.Status)

	req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/volume/"+lid.Owner+"/"+testVID, nil), map[string]string{"owner": lid.Owner, "vid": testVID})
	rec := httptest.NewRecorder()
	s.op.handleVolumeGet(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestApplyAdoptionEventMovesSpec(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseRetained)
	vol.Status.PVName = testPV

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	newDSeq := lid.DSeq + 7

	require.NoError(t, s.op.applyChainEvent(context.Background(), &dv1.EventVolumeAdopted{
		ID: dv1.GroupID{
			Owner: lid.Owner,
			DSeq:  newDSeq,
			GSeq:  1,
		},
		Adopted: dv1.VolumeRef{
			Owner: lid.Owner,
			DSeq:  lid.DSeq,
			GSeq:  lid.GSeq,
			Name:  testVID,
		},
		Vid: testVID,
	}))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, strconv.FormatUint(newDSeq, 10), uvol.Spec.GroupID.DSeq)
	require.Equal(t, crd.VolumePhaseAdopting, uvol.Status.Phase)
}

// TestApplyLeaseClosedIgnoresAdoptedVolume: after an adoption moved the
// spec to the new deployment (but before the daemon re-recorded the lease),
// a close of the dead lease must not restart the retention clock — the
// volume just survived that close by being adopted.
func TestApplyLeaseClosedIgnoresAdoptedVolume(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV
	// applyAdoption moved the group; Spec.LeaseID still names the dead lease
	vol.Spec.GroupID.DSeq = strconv.FormatUint(lid.DSeq+7, 10)

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid, mv1.LeaseClosedReasonOwner))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseProvisioned, uvol.Status.Phase)
	require.Nil(t, uvol.Status.RetainedUntil)
}

// TestResyncChainIgnoresAdoptedVolume is the same guard on the resync
// sweep: the dead lease is not in the active set, but the spec has moved
// to the adopting deployment.
func TestResyncChainIgnoresAdoptedVolume(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV
	vol.Spec.GroupID.DSeq = strconv.FormatUint(lid.DSeq+7, 10)

	chain := &fakeChainClient{active: map[string]bool{}}

	s := makeOpScaffold(t, chain, []runtime.Object{vol})

	require.NoError(t, s.op.resyncChain(context.Background()))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseProvisioned, uvol.Status.Phase)
	require.Nil(t, uvol.Status.RetainedUntil)
}

// TestApplyChainEventRetriesOnConflict: a transient k8s update conflict
// (reconciler racing the event handler) must not drop the event — adoption
// events in particular are not healed by the resync sweep.
func TestApplyChainEventRetriesOnConflict(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseRetained)
	vol.Status.PVName = testPV

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	conflicted := false
	s.ac.PrependReactor("update", "volumes", func(ktesting.Action) (bool, runtime.Object, error) {
		if conflicted {
			return false, nil, nil
		}
		conflicted = true
		return true, nil, kerrors.NewConflict(
			schema.GroupResource{Group: "akash.network", Resource: "volumes"},
			vol.Name, fmt.Errorf("the object has been modified"))
	})

	newDSeq := lid.DSeq + 7

	require.NoError(t, s.op.applyChainEvent(context.Background(), &dv1.EventVolumeAdopted{
		ID: dv1.GroupID{
			Owner: lid.Owner,
			DSeq:  newDSeq,
			GSeq:  1,
		},
		Adopted: dv1.VolumeRef{
			Owner: lid.Owner,
			DSeq:  lid.DSeq,
			GSeq:  lid.GSeq,
			Name:  testVID,
		},
		Vid: testVID,
	}))

	require.True(t, conflicted)

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, strconv.FormatUint(newDSeq, 10), uvol.Spec.GroupID.DSeq)
	require.Equal(t, crd.VolumePhaseAdopting, uvol.Status.Phase)
}

func TestProcessEventPassesLeaseResized(t *testing.T) {
	ev := &mv1.EventLeaseResized{
		ID:        testutil.LeaseID(t),
		Price:     sdk.NewDecCoin("uact", sdkmath.NewInt(2)),
		PrevPrice: sdk.NewDecCoin("uact", sdkmath.NewInt(1)),
	}

	bev, err := sdk.TypedEventToEvent(ev)
	require.NoError(t, err)

	decoded, ok := processEvent(abci.Event(bev))
	require.True(t, ok)
	require.Equal(t, ev, decoded)
}

func TestApplyLeaseResizedGrowsVolume(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseAttached)
	vol.Status.PVName = testPV

	chain := &fakeChainClient{size: 322122547200}
	s := makeOpScaffold(t, chain, []runtime.Object{vol})

	require.NoError(t, s.op.applyChainEvent(context.Background(), &mv1.EventLeaseResized{ID: lid}))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, "322122547200", uvol.Spec.Size)
	require.Equal(t, crd.VolumePhaseAttached, uvol.Status.Phase)
}

func TestApplyLeaseResizedNeverShrinksVolume(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV

	chain := &fakeChainClient{size: 107374182400}
	s := makeOpScaffold(t, chain, []runtime.Object{vol})

	require.NoError(t, s.op.applyChainEvent(context.Background(), &mv1.EventLeaseResized{ID: lid}))
	require.Equal(t, vol.Spec.Size, getOpVolume(t, s, vol.Name).Spec.Size)
}

func TestApplyLeaseResizedIgnoresComputeLeases(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseAttached)
	vol.Status.PVName = testPV

	chain := &fakeChainClient{size: 322122547200}
	s := makeOpScaffold(t, chain, []runtime.Object{vol})

	// a lease no volume is recorded under, such as the attached compute lease
	require.NoError(t, s.op.applyChainEvent(context.Background(), &mv1.EventLeaseResized{ID: testutil.LeaseID(t)}))

	require.Equal(t, 0, chain.sizeCalls)
	require.Equal(t, vol.Spec.Size, getOpVolume(t, s, vol.Name).Spec.Size)
}

func TestResyncChainHealsMissedClose(t *testing.T) {
	lid := testutil.LeaseID(t)
	computeLid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseAttached)
	vol.Status.PVName = testPV
	vol.Status.AttachedLease = computeLid.String()

	// neither the volume lease nor the compute lease is active anymore
	chain := &fakeChainClient{active: map[string]bool{}}

	s := makeOpScaffold(t, chain, []runtime.Object{vol})

	require.NoError(t, s.op.resyncChain(context.Background()))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, "", uvol.Status.AttachedLease)
	require.Equal(t, crd.VolumePhaseRetained, uvol.Status.Phase)
	require.NotNil(t, uvol.Status.RetainedUntil)
}

func TestResyncChainLeavesActiveLeases(t *testing.T) {
	lid := testutil.LeaseID(t)
	computeLid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseAttached)
	vol.Status.PVName = testPV
	vol.Status.AttachedLease = computeLid.String()

	chain := &fakeChainClient{active: map[string]bool{
		lid.String():        true,
		computeLid.String(): true,
	}}

	s := makeOpScaffold(t, chain, []runtime.Object{vol})

	require.NoError(t, s.op.resyncChain(context.Background()))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, computeLid.String(), uvol.Status.AttachedLease)
	require.Equal(t, crd.VolumePhaseAttached, uvol.Status.Phase)
}

// TestApplyLeaseClosedForAMoveExports: a move whose reclamation the
// operator never saw still exports, rather than being retained, which would
// suspend a bucket before the next provider copied it.
func TestApplyLeaseClosedForAMoveExports(t *testing.T) {
	for _, reason := range []mv1.LeaseClosedReason{mv1.LeaseClosedReasonVolumeMigrate, mv1.LeaseClosedReasonVolumeEvict} {
		t.Run(reason.String(), func(t *testing.T) {
			lid := testutil.LeaseID(t)
			vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
			vol.Status.PVName = testPV

			s := makeOpScaffold(t, nil, []runtime.Object{vol})

			require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid, reason))

			uvol := getOpVolume(t, s, vol.Name)
			require.Equal(t, crd.VolumePhaseExporting, uvol.Status.Phase)
			require.NotNil(t, uvol.Status.RetainedUntil)
			require.WithinDuration(t, time.Now().Add(168*time.Hour), uvol.Status.RetainedUntil.Time, time.Minute)
		})
	}
}

// TestResyncChainSettlesALeaseAsTheChainHasIt: a volume whose own lease is
// no longer active, with the events that said why missed, is recorded from
// what the chain holds of that lease.
func TestResyncChainSettlesALeaseAsTheChainHasIt(t *testing.T) {
	for name, tc := range map[string]struct {
		was      crd.VolumePhase
		state    mv1.Lease_State
		reason   mv1.LeaseClosedReason
		phase    crd.VolumePhase
		retained bool
	}{
		"moving, in its window":        {crd.VolumePhaseProvisioned, mv1.LeaseReclaiming, mv1.LeaseClosedReasonVolumeMigrate, crd.VolumePhaseExporting, false},
		"evicted, in its window":       {crd.VolumePhaseProvisioned, mv1.LeaseReclaiming, mv1.LeaseClosedReasonVolumeEvict, crd.VolumePhaseExporting, false},
		"exporting, in its window":     {crd.VolumePhaseExporting, mv1.LeaseReclaiming, mv1.LeaseClosedReasonVolumeMigrate, crd.VolumePhaseExporting, false},
		"exporting, closed":            {crd.VolumePhaseExporting, mv1.LeaseClosed, mv1.LeaseClosedReasonVolumeMigrate, crd.VolumePhaseExporting, true},
		"moved":                        {crd.VolumePhaseProvisioned, mv1.LeaseClosed, mv1.LeaseClosedReasonVolumeMigrate, crd.VolumePhaseExporting, true},
		"closed by its owner":          {crd.VolumePhaseProvisioned, mv1.LeaseClosed, mv1.LeaseClosedReasonOwner, crd.VolumePhaseRetained, true},
		"out of funds":                 {crd.VolumePhaseProvisioned, mv1.LeaseInsufficientFunds, mv1.LeaseClosedReasonInsufficientFunds, crd.VolumePhaseRetained, true},
		"in another reclamation":       {crd.VolumePhaseProvisioned, mv1.LeaseReclaiming, mv1.LeaseClosedReasonUnstable, crd.VolumePhaseProvisioned, false},
		"exporting, gone from chain":   {crd.VolumePhaseExporting, mv1.LeaseStateInvalid, mv1.LeaseClosedReasonInvalid, crd.VolumePhaseExporting, true},
		"provisioned, gone from chain": {crd.VolumePhaseProvisioned, mv1.LeaseStateInvalid, mv1.LeaseClosedReasonInvalid, crd.VolumePhaseRetained, true},
	} {
		t.Run(name, func(t *testing.T) {
			lid := testutil.LeaseID(t)
			vol := testVolume(t, lid, tc.was)
			vol.Status.PVName = testPV

			chain := &fakeChainClient{active: map[string]bool{}}
			if tc.state != mv1.LeaseStateInvalid {
				chain.leases = []replication.VolumeLease{{
					Provider: lid.Provider,
					DSeq:     lid.DSeq,
					OSeq:     lid.OSeq,
					State:    tc.state,
					Reason:   tc.reason,
					Policy:   dv1.VolumePolicy{Vid: testVID},
				}}
			}

			s := makeOpScaffold(t, chain, []runtime.Object{vol})

			require.NoError(t, s.op.resyncChain(context.Background()))

			uvol := getOpVolume(t, s, vol.Name)
			require.Equal(t, tc.phase, uvol.Status.Phase)
			require.Equal(t, tc.retained, uvol.Status.RetainedUntil != nil)
		})
	}
}

func TestResyncChainWaitsForTheChainToSettleALease(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV

	chain := &fakeChainClient{active: map[string]bool{}}
	chain.leasesErr = fmt.Errorf("node down")

	s := makeOpScaffold(t, chain, []runtime.Object{vol})

	require.NoError(t, s.op.resyncChain(context.Background()))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseProvisioned, uvol.Status.Phase)
	require.Nil(t, uvol.Status.RetainedUntil)
}

func TestStorageCmdRegistered(t *testing.T) {
	cmd := Cmd()
	require.Equal(t, "storage", cmd.Use)
	require.NotNil(t, cmd.Flags().Lookup(FlagVolumesNS))
	require.NotNil(t, cmd.Flags().Lookup(FlagResyncInterval))
}

func getOpVolume(t *testing.T, s *opScaffold, name string) *crd.Volume {
	t.Helper()

	vol, err := s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)

	return vol
}

// TestApplyAttachmentPromotesAReplica: the chain's attach event, which
// usually reaches the operator before the daemon attaches, promotes a
// replica just as AttachVolume does.
func TestApplyAttachmentPromotesAReplica(t *testing.T) {
	lid := testutil.LeaseID(t)
	computeLid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Spec.ReplicaOf = dv1.VolumeRef{Owner: lid.Owner, DSeq: lid.DSeq + 1, GSeq: 1, Name: "primary-data"}.String()
	vol.Status.PVName = testPV

	plainLid := testutil.LeaseID(t)
	plain := testVolume(t, plainLid, crd.VolumePhaseProvisioned)
	plain.Status.PVName = testPV

	s := makeOpScaffold(t, nil, []runtime.Object{vol, plain})

	require.NoError(t, s.op.applyChainEvent(context.Background(), &mv1.EventVolumeAttached{
		LeaseID: computeLid,
		Volume:  dv1.VolumeRef{Owner: lid.Owner, DSeq: lid.DSeq, GSeq: lid.GSeq, Name: testVID},
	}))

	stored := getOpVolume(t, s, vol.Name)
	require.Equal(t, computeLid.String(), stored.Status.AttachedLease)
	require.True(t, stored.Status.Promoted)

	require.NoError(t, s.op.applyChainEvent(context.Background(), &mv1.EventVolumeAttached{
		LeaseID: computeLid,
		Volume:  dv1.VolumeRef{Owner: plainLid.Owner, DSeq: plainLid.DSeq, GSeq: plainLid.GSeq, Name: testVID},
	}))

	require.False(t, getOpVolume(t, s, plain.Name).Status.Promoted, "a volume that is no replica has nothing to promote")
}

func TestVolumeStateReportsAReplica(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseImporting)
	vol.Spec.ReplicaOf = dv1.VolumeRef{Owner: lid.Owner, DSeq: lid.DSeq + 1, GSeq: 1, Name: "primary-data"}.String()

	state := volumeState(vol)
	require.Equal(t, vol.Spec.ReplicaOf, state.ReplicaOf)
	require.True(t, state.Syncing, "no copy landed yet")
	require.Empty(t, state.SyncedAt)

	synced := metav1.NewTime(time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC))
	vol.Status.SyncedAt = &synced
	vol.Status.Phase = crd.VolumePhaseProvisioned

	state = volumeState(vol)
	require.False(t, state.Syncing)
	require.Equal(t, "2026-10-29T12:00:00Z", state.SyncedAt)
}
