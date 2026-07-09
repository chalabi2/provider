package storage

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mtypes "pkg.akt.dev/go/node/market/v1"
	"pkg.akt.dev/go/testutil"

	"github.com/akash-network/provider/cluster/kube/builder"
	clusterutil "github.com/akash-network/provider/cluster/util"
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
}

func (f *fakeChain) GroupVolumePolicy(_ context.Context, _ dv1.GroupID) (*dv1.VolumePolicy, error) {
	f.calls++
	return f.policy, f.err
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

func getVolume(t *testing.T, s *scaffold, name string) *crd.Volume {
	t.Helper()

	vol, err := s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)

	return vol
}

func TestReconcileProvisionCreatesHolderPVC(t *testing.T) {
	lid := testutil.LeaseID(t)
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

func TestReconcileProvisionPinsBoundPV(t *testing.T) {
	lid := testutil.LeaseID(t)
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
