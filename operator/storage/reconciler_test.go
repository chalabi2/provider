package storage

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mtypes "pkg.akt.dev/go/node/market/v1"
	"pkg.akt.dev/go/testutil"

	"github.com/akash-network/provider/cluster/kube/builder"
	clusterutil "github.com/akash-network/provider/cluster/util"
	"github.com/akash-network/provider/operator/storage/replication"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	afake "github.com/akash-network/provider/pkg/client/clientset/versioned/fake"
)

const (
	testNS    = "lease"
	testVolNS = "akash-volumes"
	testVID   = "myapp-pgdata"
	testPV    = "pvc-8c1f0000-aaaa-bbbb-cccc-000000000001"
)

// scaffold builds a reconciler over simple fake clientsets. The simple
// tracker is required: the repo's generated apply-configuration schema
// carries no type definitions, so the field-managed tracker rejects CRD
// writes (same reasoning as cluster/kube/client_volume_test.go).
type scaffold struct {
	r     *reconciler
	kc    *kfake.Clientset
	ac    *afake.Clientset
	chain *fakeChain
	now   time.Time
}

type fakeChain struct {
	policy *dv1.VolumePolicy
	err    error
	calls  int

	leases      []replication.VolumeLease
	leasesErr   error
	leasesCalls int
}

func (f *fakeChain) GroupVolumePolicy(_ context.Context, _ dv1.GroupID) (*dv1.VolumePolicy, error) {
	f.calls++
	return f.policy, f.err
}

func (f *fakeChain) VolumeLeases(_ context.Context, _, _ string) ([]replication.VolumeLease, error) {
	f.leasesCalls++
	return f.leases, f.leasesErr
}

func makeScaffold(t *testing.T, chain *fakeChain, kobjs, aobjs []runtime.Object) *scaffold {
	t.Helper()

	kc := kfake.NewSimpleClientset(kobjs...)
	ac := afake.NewSimpleClientset(aobjs...)

	var cq ChainQuery
	if chain != nil {
		cq = chain
	}

	s := &scaffold{
		r:     newReconciler(kc, ac, testNS, testVolNS, cq, testutil.Logger(t)),
		kc:    kc,
		ac:    ac,
		chain: chain,
		now:   time.Now(),
	}

	s.r.now = func() time.Time { return s.now }

	return s
}

func testVolume(t *testing.T, lid mtypes.LeaseID, phase crd.VolumePhase) *crd.Volume {
	t.Helper()

	return &crd.Volume{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Volume",
			APIVersion: "akash.network/v2beta2",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      crd.VolumeName(lid.Owner, testVID),
			Namespace: testNS,
			Labels: map[string]string{
				builder.AkashManagedLabelName:   builder.ValTrue,
				builder.AkashComponentLabelName: builder.AkashComponentVolume,
			},
		},
		Spec: crd.VolumeSpec{
			Owner: lid.Owner,
			VID:   testVID,
			GroupID: crd.VolumeGroupID{
				DSeq: strconv.FormatUint(lid.DSeq, 10),
				GSeq: lid.GSeq,
			},
			Class:     "beta3",
			Size:      "214748364800",
			Reclaim:   dv1.VolumeReclaimRetain.String(),
			Retention: "168h0m0s",
			LeaseID:   crd.LeaseIDFromAkash(lid),
		},
		Status: crd.VolumeStatus{
			Phase: phase,
		},
	}
}

// testPVObj builds the backing PV as the provisioner + reconciler would have
// left it: Retain policy, identity labels, claimRef on the given PVC.
func testPVObj(vol *crd.Volume, claimNS, claimName string) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: testPV,
			Labels: map[string]string{
				builder.AkashManagedLabelName:   builder.ValTrue,
				builder.AkashComponentLabelName: builder.AkashComponentVolume,
				LabelVolumeOwner:                vol.Spec.Owner,
				LabelVolumeVID:                  vol.Spec.VID,
				LabelVolumeDSeq:                 vol.Spec.GroupID.DSeq,
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			StorageClassName:              RetainClass(vol.Spec.Class),
		},
	}

	if claimName != "" {
		pv.Spec.ClaimRef = &corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
			Namespace:  claimNS,
			Name:       claimName,
		}
	}

	return pv
}

func holderPVC(vol *crd.Volume, pvName string) *corev1.PersistentVolumeClaim {
	class := RetainClass(vol.Spec.Class)

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vol.Name,
			Namespace: testVolNS,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &class,
			VolumeName:       pvName,
		},
	}
}

// firstOrderLease is a lease on a volume group's first order, which starts
// the volume empty.
func firstOrderLease(t *testing.T) mtypes.LeaseID {
	t.Helper()

	lid := testutil.LeaseID(t)
	lid.OSeq = 1

	return lid
}

// relistedLease is a lease on the second order of a volume group, the one a
// migration re-lists.
func relistedLease(t *testing.T) mtypes.LeaseID {
	t.Helper()

	lid := testutil.LeaseID(t)
	lid.OSeq = 2

	return lid
}

// heldBefore is the closed lease of the provider that held the volume before
// lid's order re-listed.
func heldBefore(lid mtypes.LeaseID, provider string, oseq uint32) replication.VolumeLease {
	return replication.VolumeLease{
		Provider: provider,
		DSeq:     lid.DSeq,
		OSeq:     oseq,
		State:    mtypes.LeaseClosed,
		Reason:   mtypes.LeaseClosedReasonVolumeMigrate,
		Policy:   dv1.VolumePolicy{Vid: testVID},
	}
}

func getVolume(t *testing.T, s *scaffold, name string) *crd.Volume {
	t.Helper()

	vol, err := s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)

	return vol
}

func TestReconcileProvisionCreatesHolderPVC(t *testing.T) {
	lid := firstOrderLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	s := makeScaffold(t, nil, nil, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	// holder PVC created against the -retain class in the parking namespace
	pvc, err := s.kc.CoreV1().PersistentVolumeClaims(testVolNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, RetainClass("beta3"), *pvc.Spec.StorageClassName)
	require.Equal(t, "", pvc.Spec.VolumeName)
	require.Equal(t, vol.Spec.Size, pvc.Spec.Resources.Requests.Storage().AsDec().String())
	require.Equal(t, builder.AkashComponentVolume, pvc.Labels[builder.AkashComponentLabelName])

	// not bound: the volume stays Pending
	require.Equal(t, crd.VolumePhasePending, getVolume(t, s, vol.Name).Status.Phase)
}

func TestReconcileProvisionNodeHint(t *testing.T) {
	lid := firstOrderLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)

	classLabel := builder.AkashServiceCapabilityStorage + ".class.beta3"
	nodes := []runtime.Object{
		// unschedulable: skipped even though labeled
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{classLabel: "1"}},
			Spec:       corev1.NodeSpec{Unschedulable: true},
			Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
		},
		// ready + labeled: the deterministic pick (first by name)
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{classLabel: "1"}},
			Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-c", Labels: map[string]string{classLabel: "1"}},
			Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
		},
	}

	s := makeScaffold(t, nil, nodes, []runtime.Object{vol})
	s.r.nodeHint = true

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	pvc, err := s.kc.CoreV1().PersistentVolumeClaims(testVolNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "node-b", pvc.Annotations[pvcSelectedNodeAnnotation])
}

func TestReconcileProvisionNodeHintNoNodes(t *testing.T) {
	lid := firstOrderLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)

	s := makeScaffold(t, nil, nil, []runtime.Object{vol})
	s.r.nodeHint = true

	// no labeled node: provisioning errors instead of parking a claim the
	// provisioner can never satisfy
	require.ErrorIs(t, s.r.reconcile(context.Background(), vol), ErrVolumeOperator)
}

func TestReconcileProvisionPinsBoundPV(t *testing.T) {
	lid := firstOrderLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)

	// the binder bound the holder PVC to a freshly provisioned PV
	pv := testPVObj(vol, testVolNS, vol.Name)
	pv.Labels = nil

	s := makeScaffold(t, nil, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	// PV labeled with the volume identity
	upv, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, vol.Spec.Owner, upv.Labels[LabelVolumeOwner])
	require.Equal(t, testVID, upv.Labels[LabelVolumeVID])
	require.Equal(t, vol.Spec.GroupID.DSeq, upv.Labels[LabelVolumeDSeq])
	require.Equal(t, builder.ValTrue, upv.Labels[builder.AkashManagedLabelName])

	// parked
	uvol := getVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseProvisioned, uvol.Status.Phase)
	require.Equal(t, testPV, uvol.Status.PVName)
}

func TestReconcileAttachChoreography(t *testing.T) {
	lid := testutil.LeaseID(t)
	computeLid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV
	vol.Status.AttachedLease = computeLid.String()

	pv := testPVObj(vol, testVolNS, vol.Name)

	s := makeScaffold(t, nil, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	targetNS := clusterutil.LeaseIDToNamespace(computeLid)

	// target PVC exists in the lease namespace, pre-bound by volumeName
	tpvc, err := s.kc.CoreV1().PersistentVolumeClaims(targetNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, testPV, tpvc.Spec.VolumeName)

	// claimRef moved directly to the target PVC - never through Available
	upv, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, upv.Spec.ClaimRef)
	require.Equal(t, targetNS, upv.Spec.ClaimRef.Namespace)
	require.Equal(t, vol.Name, upv.Spec.ClaimRef.Name)

	// holder PVC deleted last
	_, err = s.kc.CoreV1().PersistentVolumeClaims(testVolNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err))

	require.Equal(t, crd.VolumePhaseAttached, getVolume(t, s, vol.Name).Status.Phase)
}

func TestReconcileDetachReparks(t *testing.T) {
	lid := testutil.LeaseID(t)
	computeLid := testutil.LeaseID(t)
	targetNS := clusterutil.LeaseIDToNamespace(computeLid)

	vol := testVolume(t, lid, crd.VolumePhaseAttached)
	vol.Status.PVName = testPV
	vol.Status.AttachedLease = "" // the daemon/chain watch cleared it

	// PV still claimed by the (stale) attach PVC
	pv := testPVObj(vol, targetNS, vol.Name)
	stale := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: vol.Name, Namespace: targetNS},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: testPV},
	}

	s := makeScaffold(t, nil, []runtime.Object{pv, stale}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	// holder PVC recreated, pre-bound
	hpvc, err := s.kc.CoreV1().PersistentVolumeClaims(testVolNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, testPV, hpvc.Spec.VolumeName)

	// claimRef re-parked directly on the holder
	upv, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, testVolNS, upv.Spec.ClaimRef.Namespace)
	require.Equal(t, vol.Name, upv.Spec.ClaimRef.Name)

	// the stale attach claim is gone
	_, err = s.kc.CoreV1().PersistentVolumeClaims(targetNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err))

	require.Equal(t, crd.VolumePhaseProvisioned, getVolume(t, s, vol.Name).Status.Phase)
}

// testVolumeSize is testVolume's recorded size.
const testVolumeSize = int64(214748364800)

// boundClaim is the claim in ns holding the PV, bound and requesting size.
func boundClaim(vol *crd.Volume, ns string, size int64) *corev1.PersistentVolumeClaim {
	pvc := holderPVC(vol, testPV)
	pvc.Namespace = ns
	pvc.Spec.Resources.Requests = corev1.ResourceList{
		corev1.ResourceStorage: *resource.NewQuantity(size, resource.DecimalSI),
	}
	pvc.Status.Phase = corev1.ClaimBound

	return pvc
}

func claimRequest(t *testing.T, s *scaffold, ns, name string) int64 {
	t.Helper()

	pvc, err := s.kc.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)

	return pvc.Spec.Resources.Requests.Storage().Value()
}

func TestReconcileGrowsParkedClaim(t *testing.T) {
	lid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV

	pv := testPVObj(vol, testVolNS, vol.Name)

	s := makeScaffold(t, nil, []runtime.Object{boundClaim(vol, testVolNS, testVolumeSize/2), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	require.Equal(t, testVolumeSize, claimRequest(t, s, testVolNS, vol.Name))
	require.Equal(t, crd.VolumePhaseProvisioned, getVolume(t, s, vol.Name).Status.Phase)
}

func TestReconcileGrowsAttachedClaim(t *testing.T) {
	lid := testutil.LeaseID(t)
	computeLid := testutil.LeaseID(t)
	targetNS := clusterutil.LeaseIDToNamespace(computeLid)

	vol := testVolume(t, lid, crd.VolumePhaseAttached)
	vol.Status.PVName = testPV
	vol.Status.AttachedLease = computeLid.String()

	pv := testPVObj(vol, targetNS, vol.Name)

	s := makeScaffold(t, nil, []runtime.Object{boundClaim(vol, targetNS, testVolumeSize/2), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	require.Equal(t, testVolumeSize, claimRequest(t, s, targetNS, vol.Name))

	// the claim on the PV was grown in place; nothing was re-parked
	_, err := s.kc.CoreV1().PersistentVolumeClaims(testVolNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err))
}

func TestReconcileGrowWaitsForBoundClaim(t *testing.T) {
	lid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV

	pvc := boundClaim(vol, testVolNS, testVolumeSize/2)
	pvc.Status.Phase = corev1.ClaimPending

	s := makeScaffold(t, nil, []runtime.Object{pvc, testPVObj(vol, testVolNS, vol.Name)}, []runtime.Object{vol})

	// the API refuses to resize an unbound claim
	require.NoError(t, s.r.reconcile(context.Background(), vol))
	require.Equal(t, testVolumeSize/2, claimRequest(t, s, testVolNS, vol.Name))
}

func TestReconcileGrowNeverShrinksClaim(t *testing.T) {
	lid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseProvisioned)
	vol.Status.PVName = testPV

	s := makeScaffold(t, nil, []runtime.Object{boundClaim(vol, testVolNS, 2*testVolumeSize), testPVObj(vol, testVolNS, vol.Name)}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))
	require.Equal(t, 2*testVolumeSize, claimRequest(t, s, testVolNS, vol.Name))
}

func TestReconcileRetainedStaysRetainedWhenParked(t *testing.T) {
	lid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseRetained)
	vol.Status.PVName = testPV
	until := metav1.NewTime(time.Now().Add(time.Hour))
	vol.Status.RetainedUntil = &until

	pv := testPVObj(vol, testVolNS, vol.Name)

	s := makeScaffold(t, nil, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	// still Retained - the GC clock keeps running; PV untouched
	require.Equal(t, crd.VolumePhaseRetained, getVolume(t, s, vol.Name).Status.Phase)

	upv, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, corev1.PersistentVolumeReclaimRetain, upv.Spec.PersistentVolumeReclaimPolicy)
}

func TestGCWaitsForDeadline(t *testing.T) {
	lid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseRetained)
	vol.Status.PVName = testPV
	until := metav1.NewTime(time.Now().Add(time.Hour))
	vol.Status.RetainedUntil = &until

	pv := testPVObj(vol, testVolNS, vol.Name)

	s := makeScaffold(t, nil, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	// deadline not reached: everything survives
	_, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseRetained, getVolume(t, s, vol.Name).Status.Phase)
}

func TestGCDestroysPastDeadline(t *testing.T) {
	lid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseRetained)
	vol.Status.PVName = testPV
	until := metav1.NewTime(time.Now().Add(-time.Hour))
	vol.Status.RetainedUntil = &until

	pv := testPVObj(vol, testVolNS, vol.Name)

	s := makeScaffold(t, nil, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	// track that the reclaim policy is flipped to Delete before the PV is
	// removed - that is what destroys the backing image via the provisioner
	var sawDeleteReclaim bool
	s.kc.PrependReactor("update", "persistentvolumes", func(action ktesting.Action) (bool, runtime.Object, error) {
		upd := action.(ktesting.UpdateAction).GetObject().(*corev1.PersistentVolume)
		if upd.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimDelete {
			sawDeleteReclaim = true
		}
		return false, nil, nil
	})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	require.True(t, sawDeleteReclaim)

	// PVC, PV and CRD are gone
	_, err := s.kc.CoreV1().PersistentVolumeClaims(testVolNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err))

	_, err = s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err))

	_, err = s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err))
}

func TestGCFrozenWhileAdopting(t *testing.T) {
	testGCFrozenInPhase(t, crd.VolumePhaseAdopting)
}

func TestGCFrozenWhileExporting(t *testing.T) {
	testGCFrozenInPhase(t, crd.VolumePhaseExporting)
}

func TestExportingThawsToRetainedPastDeadline(t *testing.T) {
	lid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseExporting)
	vol.Status.PVName = testPV
	until := metav1.NewTime(time.Now().Add(-time.Hour))
	vol.Status.RetainedUntil = &until

	pv := testPVObj(vol, testVolNS, vol.Name)

	s := makeScaffold(t, &fakeChain{}, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	// window + retention served: the export freeze thaws to Retained;
	// the next pass takes the single destruction path
	uvol, err := s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseRetained, uvol.Status.Phase)

	// the PV survived this pass
	_, err = s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestExportingHoldsBeforeDeadline(t *testing.T) {
	lid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseExporting)
	vol.Status.PVName = testPV
	until := metav1.NewTime(time.Now().Add(time.Hour))
	vol.Status.RetainedUntil = &until

	pv := testPVObj(vol, testVolNS, vol.Name)

	s := makeScaffold(t, &fakeChain{}, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	uvol, err := s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseExporting, uvol.Status.Phase)
}

func testGCFrozenInPhase(t *testing.T, phase crd.VolumePhase) {
	t.Helper()

	lid := testutil.LeaseID(t)

	vol := testVolume(t, lid, phase)
	vol.Status.PVName = testPV
	until := metav1.NewTime(time.Now().Add(-time.Hour))
	vol.Status.RetainedUntil = &until

	pv := testPVObj(vol, testVolNS, vol.Name)

	s := makeScaffold(t, &fakeChain{}, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	// deadline long past, but the phase freezes destruction
	_, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)

	_, err = s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestGCReleasingReclaimDelete(t *testing.T) {
	lid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseReleasing)
	vol.Spec.Reclaim = dv1.VolumeReclaimDelete.String()
	vol.Status.PVName = testPV
	until := metav1.NewTime(time.Now().Add(-time.Minute))
	vol.Status.RetainedUntil = &until

	pv := testPVObj(vol, testVolNS, vol.Name)

	s := makeScaffold(t, nil, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	_, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err))

	_, err = s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err))
}

// adoption: the CRD spec was moved to a new deployment while the PV label
// still records the dead one.

func adoptionScaffold(t *testing.T, chain *fakeChain) (*scaffold, *crd.Volume, string) {
	t.Helper()

	lid := testutil.LeaseID(t)

	vol := testVolume(t, lid, crd.VolumePhaseRetained)
	vol.Status.PVName = testPV
	until := metav1.NewTime(time.Now().Add(time.Hour))
	vol.Status.RetainedUntil = &until

	pv := testPVObj(vol, testVolNS, vol.Name)

	// the dead deployment's dseq stays on the PV; the spec moves ahead
	deadDSeq := vol.Spec.GroupID.DSeq
	vol.Spec.GroupID.DSeq = strconv.FormatUint(lid.DSeq+1, 10)

	s := makeScaffold(t, chain, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	return s, vol, deadDSeq
}

func TestReconcileAdoptVerified(t *testing.T) {
	var s *scaffold
	var vol *crd.Volume
	var deadDSeq string

	chain := &fakeChain{}
	s, vol, deadDSeq = adoptionScaffold(t, chain)

	dead, err := strconv.ParseUint(deadDSeq, 10, 64)
	require.NoError(t, err)

	chain.policy = &dv1.VolumePolicy{
		Vid: testVID,
		Adopt: &dv1.VolumeRef{
			Owner: vol.Spec.Owner,
			DSeq:  dead,
			GSeq:  1,
			Name:  testVID,
		},
	}

	require.NoError(t, s.r.reconcile(context.Background(), vol))
	require.Equal(t, 1, chain.calls)

	// PV identity rewritten to the adopting deployment
	upv, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, vol.Spec.GroupID.DSeq, upv.Labels[LabelVolumeDSeq])

	// re-parked; the retention clock is void
	uvol := getVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseProvisioned, uvol.Status.Phase)
	require.Nil(t, uvol.Status.RetainedUntil)
}

func TestReconcileAdoptRejectedOnMismatch(t *testing.T) {
	chain := &fakeChain{
		policy: &dv1.VolumePolicy{
			Vid: testVID,
			Adopt: &dv1.VolumeRef{
				Owner: "akash1foreignownerxxxxxxxxxxxxxxxxxxxxxxxxxx",
				DSeq:  424242,
				GSeq:  1,
				Name:  testVID,
			},
		},
	}

	s, vol, deadDSeq := adoptionScaffold(t, chain)

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	// rejected: PV identity untouched, retention deadline stands
	upv, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, deadDSeq, upv.Labels[LabelVolumeDSeq])

	uvol := getVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseRetained, uvol.Status.Phase)
	require.NotNil(t, uvol.Status.RetainedUntil)
}

func TestReconcileAdoptFrozenWithoutChain(t *testing.T) {
	s, vol, deadDSeq := adoptionScaffold(t, nil)

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	// no chain access: frozen in Adopting (GC frozen with it), no re-bind
	uvol := getVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseAdopting, uvol.Status.Phase)

	upv, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, deadDSeq, upv.Labels[LabelVolumeDSeq])
}

func TestReconcileMovedVolumeRecordsWhereItsDataIs(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	previous := testutil.AccAddress(t).String()
	chain := &fakeChain{leases: []replication.VolumeLease{heldBefore(lid, previous, 1)}}
	s := makeScaffold(t, chain, nil, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	stored := getVolume(t, s, vol.Name)
	require.Equal(t, previous, stored.Spec.ImportFrom)
	require.Equal(t, crd.VolumePhasePending, stored.Status.Phase)

	_, err := s.kc.CoreV1().PersistentVolumeClaims(testVolNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestReconcileMovedVolumeParksAsImporting(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	pv := testPVObj(vol, testVolNS, vol.Name)
	chain := &fakeChain{leases: []replication.VolumeLease{heldBefore(lid, testutil.AccAddress(t).String(), 1)}}
	s := makeScaffold(t, chain, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	stored := getVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseImporting, stored.Status.Phase)
	require.Equal(t, testPV, stored.Status.PVName)
}

func TestReconcileMovedVolumeAsksTheChainOnce(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	chain := &fakeChain{leases: []replication.VolumeLease{heldBefore(lid, testutil.AccAddress(t).String(), 1)}}
	s := makeScaffold(t, chain, nil, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))
	require.NoError(t, s.r.reconcile(context.Background(), getVolume(t, s, vol.Name)))

	require.Equal(t, 1, chain.leasesCalls)
}

func TestReconcileRelistedVolumeHeldHereLastStartsAsIs(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	pv := testPVObj(vol, testVolNS, vol.Name)
	chain := &fakeChain{leases: []replication.VolumeLease{heldBefore(lid, lid.Provider, 1)}}
	s := makeScaffold(t, chain, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	stored := getVolume(t, s, vol.Name)
	require.Empty(t, stored.Spec.ImportFrom)
	require.Equal(t, crd.VolumePhaseProvisioned, stored.Status.Phase)
}

func TestReconcileMovedVolumeWaitsForTheChain(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	s := makeScaffold(t, &fakeChain{leasesErr: kerrors.NewServiceUnavailable("node down")}, nil, []runtime.Object{vol})

	require.Error(t, s.r.reconcile(context.Background(), vol))

	require.Equal(t, crd.VolumePhasePending, getVolume(t, s, vol.Name).Status.Phase)
	_, err := s.kc.CoreV1().PersistentVolumeClaims(testVolNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err), "nothing is provisioned before the chain answers")
}

func TestReconcileMovedVolumeWaitsWithoutChainAccess(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	s := makeScaffold(t, nil, nil, []runtime.Object{vol})

	require.ErrorIs(t, s.r.reconcile(context.Background(), vol), ErrVolumeOperator)

	_, err := s.kc.CoreV1().PersistentVolumeClaims(testVolNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err))
}

func TestReconcileFirstOrderNeverAsksTheChain(t *testing.T) {
	lid := firstOrderLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	chain := &fakeChain{}
	s := makeScaffold(t, chain, nil, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	require.Zero(t, chain.leasesCalls)
}

func TestReconcileVolumeWonBackImportsWhatChangedElsewhere(t *testing.T) {
	lid := testutil.LeaseID(t)
	lid.OSeq = 3
	vol := testVolume(t, lid, crd.VolumePhasePending)
	vol.Status.PVName = testPV
	pv := testPVObj(vol, testVolNS, vol.Name)
	elsewhere := testutil.AccAddress(t).String()
	chain := &fakeChain{leases: []replication.VolumeLease{
		heldBefore(lid, lid.Provider, 1),
		heldBefore(lid, elsewhere, 2),
	}}
	s := makeScaffold(t, chain, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	stored := getVolume(t, s, vol.Name)
	require.Equal(t, elsewhere, stored.Spec.ImportFrom)
	require.Equal(t, crd.VolumePhaseImporting, stored.Status.Phase)
	require.True(t, stored.Status.LocalCopy, "the copy here stands if the newer one never arrived anywhere")
}

func TestReconcileMovedVolumeHasNoCopyToFallBackOn(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	chain := &fakeChain{leases: []replication.VolumeLease{heldBefore(lid, testutil.AccAddress(t).String(), 1)}}
	s := makeScaffold(t, chain, nil, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	require.False(t, getVolume(t, s, vol.Name).Status.LocalCopy)
}

func TestReconcileMovedVolumeLooksPastItsOwnEmptyLease(t *testing.T) {
	lid := testutil.LeaseID(t)
	lid.OSeq = 3
	vol := testVolume(t, lid, crd.VolumePhasePending)
	holder := testutil.AccAddress(t).String()
	chain := &fakeChain{leases: []replication.VolumeLease{
		heldBefore(lid, holder, 1),
		heldBefore(lid, lid.Provider, 2),
	}}
	s := makeScaffold(t, chain, nil, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	require.Equal(t, holder, getVolume(t, s, vol.Name).Spec.ImportFrom)
}

func TestReconcileUnfinishedImportWonAgainKeepsImporting(t *testing.T) {
	lid := testutil.LeaseID(t)
	lid.OSeq = 3
	vol := testVolume(t, lid, crd.VolumePhasePending)
	vol.Spec.ImportFrom = testutil.AccAddress(t).String()
	vol.Status.PVName = testPV
	pv := testPVObj(vol, testVolNS, vol.Name)
	chain := &fakeChain{}
	s := makeScaffold(t, chain, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	stored := getVolume(t, s, vol.Name)
	require.Equal(t, crd.VolumePhaseImporting, stored.Status.Phase)
	require.Equal(t, vol.Spec.ImportFrom, stored.Spec.ImportFrom)
	require.Zero(t, chain.leasesCalls)
}

func TestReconcileVolumeWonBackKeepsItsData(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	vol.Status.PVName = testPV
	pv := testPVObj(vol, testVolNS, vol.Name)
	chain := &fakeChain{leases: []replication.VolumeLease{heldBefore(lid, lid.Provider, 1)}}
	s := makeScaffold(t, chain, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	stored := getVolume(t, s, vol.Name)
	require.Empty(t, stored.Spec.ImportFrom)
	require.Equal(t, crd.VolumePhaseProvisioned, stored.Status.Phase)
}

func TestReconcileVolumeWonBackWaitsForTheChain(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	vol.Status.PVName = testPV
	pv := testPVObj(vol, testVolNS, vol.Name)
	s := makeScaffold(t, &fakeChain{leasesErr: kerrors.NewServiceUnavailable("node down")}, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.Error(t, s.r.reconcile(context.Background(), vol))

	require.Equal(t, crd.VolumePhasePending, getVolume(t, s, vol.Name).Status.Phase)
}

func TestReconcileImportingVolumeStaysImportingWhenReparked(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhaseImporting)
	vol.Spec.ImportFrom = testutil.AccAddress(t).String()
	vol.Status.PVName = testPV
	pv := testPVObj(vol, "lease-ns-gone", vol.Name)
	s := makeScaffold(t, &fakeChain{}, []runtime.Object{pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	require.Equal(t, crd.VolumePhaseImporting, getVolume(t, s, vol.Name).Status.Phase)
	upv, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, testVolNS, upv.Spec.ClaimRef.Namespace)
}

func TestReconcileUnfinishedImportStaysRetained(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhaseRetained)
	vol.Spec.ImportFrom = testutil.AccAddress(t).String()
	vol.Status.PVName = testPV
	until := metav1.NewTime(time.Now().Add(time.Hour))
	vol.Status.RetainedUntil = &until
	pv := testPVObj(vol, testVolNS, vol.Name)
	s := makeScaffold(t, &fakeChain{}, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	require.Equal(t, crd.VolumePhaseRetained, getVolume(t, s, vol.Name).Status.Phase)
}

func TestReconcileAdoptRejectedForAnUnfinishedImport(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhaseRetained)
	vol.Spec.ImportFrom = testutil.AccAddress(t).String()
	vol.Status.PVName = testPV
	pv := testPVObj(vol, testVolNS, vol.Name)
	pv.Labels[LabelVolumeDSeq] = "1"
	chain := &fakeChain{policy: &dv1.VolumePolicy{Vid: testVID, Adopt: &dv1.VolumeRef{Owner: vol.Spec.Owner, DSeq: 1, Name: testVID}}}
	s := makeScaffold(t, chain, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})

	require.NoError(t, s.r.reconcile(context.Background(), vol))

	require.Equal(t, crd.VolumePhaseRetained, getVolume(t, s, vol.Name).Status.Phase)
	require.Zero(t, chain.calls, "an unfinished copy is refused before the chain is asked")
	upv, err := s.kc.CoreV1().PersistentVolumes().Get(context.Background(), testPV, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "1", upv.Labels[LabelVolumeDSeq])
}

// pruneImportSource makes the API server drop spec.importFrom on every write,
// as a Volume CRD installed before moves existed does.
func pruneImportSource(s *scaffold) {
	s.ac.PrependReactor("update", "volumes", func(action ktesting.Action) (bool, runtime.Object, error) {
		action.(ktesting.UpdateAction).GetObject().(*crd.Volume).Spec.ImportFrom = ""
		return false, nil, nil
	})
}

func TestReconcileMovedVolumeWaitsForACRDThatKeepsItsSource(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	chain := &fakeChain{leases: []replication.VolumeLease{heldBefore(lid, testutil.AccAddress(t).String(), 1)}}
	s := makeScaffold(t, chain, nil, []runtime.Object{vol})
	pruneImportSource(s)

	err := s.r.reconcile(context.Background(), vol)

	require.ErrorContains(t, err, "apply the current CRD")
	require.Equal(t, crd.VolumePhasePending, getVolume(t, s, vol.Name).Status.Phase)
	_, err = s.kc.CoreV1().PersistentVolumeClaims(testVolNS).Get(context.Background(), vol.Name, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err), "nothing is provisioned for a volume whose source the CRD drops")
}

func TestReconcileVolumeWonBackWaitsForACRDThatKeepsItsSource(t *testing.T) {
	lid := relistedLease(t)
	vol := testVolume(t, lid, crd.VolumePhasePending)
	vol.Status.PVName = testPV
	pv := testPVObj(vol, testVolNS, vol.Name)
	chain := &fakeChain{leases: []replication.VolumeLease{heldBefore(lid, testutil.AccAddress(t).String(), 1)}}
	s := makeScaffold(t, chain, []runtime.Object{holderPVC(vol, testPV), pv}, []runtime.Object{vol})
	pruneImportSource(s)

	require.ErrorContains(t, s.r.reconcile(context.Background(), vol), "apply the current CRD")

	require.Equal(t, crd.VolumePhasePending, getVolume(t, s, vol.Name).Status.Phase)
}
