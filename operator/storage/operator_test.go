package storage

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

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
}

func (f *fakeChainClient) ActiveLeases(_ context.Context) (map[string]bool, error) {
	return f.active, nil
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

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid))

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

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid))

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

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), computeLid))

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

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid))

	uvol := getOpVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseAdopting, uvol.Status.Phase)
	require.Nil(t, uvol.Status.RetainedUntil)
}

func TestApplyLeaseClosedExportingStampsRetention(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := testVolume(t, lid, crd.VolumePhaseExporting)
	vol.Status.PVName = testPV

	s := makeOpScaffold(t, nil, []runtime.Object{vol})

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid))

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

	require.NoError(t, s.op.applyLeaseClosed(context.Background(), lid))

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
