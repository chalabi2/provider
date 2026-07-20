package kube

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	dtypes "pkg.akt.dev/go/node/deployment/v1"
	dvbeta "pkg.akt.dev/go/node/deployment/v1beta5"
	mtypes "pkg.akt.dev/go/node/market/v1"
	attrtypes "pkg.akt.dev/go/node/types/attributes/v1"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"
	"pkg.akt.dev/go/testutil"

	"github.com/akash-network/provider/cluster/kube/builder"
	kubeclienterrors "github.com/akash-network/provider/cluster/kube/errors"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	afake "github.com/akash-network/provider/pkg/client/clientset/versioned/fake"
)

const testVolumeVID = "myapp-pgdata"

// volumeClientForTest builds a kube client over simple fake clientsets.
// The generated apply-configuration schema in this repo carries no type
// definitions (openapi schema is not wired into codegen), so the
// field-managed tracker used by fake.NewClientset rejects CRD writes;
// the simple tracker processes creates/updates as-is.
func volumeClientForTest(t *testing.T, kobjs []runtime.Object, aobjs []runtime.Object) *client {
	t.Helper()

	return &client{
		kc:                kfake.NewSimpleClientset(kobjs...),
		ac:                afake.NewSimpleClientset(aobjs...),
		ns:                testKubeClientNs,
		log:               testutil.Logger(t).With("mode", "test-kube-provider-client"),
		kubeContentConfig: &rest.Config{},
	}
}

func testVolumeGroup(t *testing.T, lid mtypes.LeaseID, reclaim dtypes.VolumePolicy_ReclaimPolicy) *dvbeta.Group {
	t.Helper()

	return &dvbeta.Group{
		ID: dtypes.GroupID{
			Owner: lid.Owner,
			DSeq:  lid.DSeq,
			GSeq:  lid.GSeq,
		},
		GroupSpec: dvbeta.GroupSpec{
			Name: "us-west",
			Volume: &dtypes.VolumePolicy{
				Vid:            testVolumeVID,
				Reclaim:        reclaim,
				Retention:      168 * time.Hour,
				MaxAttachments: 1,
			},
			Resources: dvbeta.ResourceUnits{
				{
					Resources: rtypes.Resources{
						ID: 1,
						CPU: &rtypes.CPU{
							Units: rtypes.NewResourceValue(0),
						},
						GPU: &rtypes.GPU{
							Units: rtypes.NewResourceValue(0),
						},
						Memory: &rtypes.Memory{
							Quantity: rtypes.NewResourceValue(0),
						},
						Storage: rtypes.Volumes{
							{
								Name:     testVolumeVID,
								Quantity: rtypes.NewResourceValue(200),
								Attributes: attrtypes.Attributes{
									{Key: "class", Value: "beta2"},
									{Key: "persistent", Value: "true"},
								},
							},
						},
						Endpoints: rtypes.Endpoints{},
					},
					Count: 1,
				},
			},
		},
	}
}

func testVolumeRef(lid mtypes.LeaseID) dtypes.VolumeRef {
	return dtypes.VolumeRef{
		Owner: lid.Owner,
		DSeq:  lid.DSeq,
		GSeq:  lid.GSeq,
		Name:  testVolumeVID,
	}
}

func TestDeployVolumeCreatesCRD(t *testing.T) {
	lid := testutil.LeaseID(t)
	kc := volumeClientForTest(t, nil, nil)

	err := kc.DeployVolume(context.Background(), lid, testVolumeGroup(t, lid, dtypes.VolumeReclaimRetain))
	require.NoError(t, err)

	name := crd.VolumeName(lid.Owner, testVolumeVID)
	obj, err := kc.ac.AkashV2beta2().Volumes(testKubeClientNs).Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)

	require.Equal(t, lid.Owner, obj.Spec.Owner)
	require.Equal(t, testVolumeVID, obj.Spec.VID)
	require.Equal(t, "beta2", obj.Spec.Class)
	require.Equal(t, "200", obj.Spec.Size)
	require.Equal(t, crd.VolumePhasePending, obj.Status.Phase)

	// labeled as managed volume component and by lease
	require.Equal(t, "true", obj.Labels[builder.AkashManagedLabelName])
	require.Equal(t, builder.AkashComponentVolume, obj.Labels[builder.AkashComponentLabelName])
	require.Equal(t, lid.Owner, obj.Labels[builder.AkashLeaseOwnerLabelName])
}

func TestDeployVolumeAdoptionRewritesLease(t *testing.T) {
	lid := testutil.LeaseID(t)
	kc := volumeClientForTest(t, nil, nil)

	require.NoError(t, kc.DeployVolume(context.Background(), lid, testVolumeGroup(t, lid, dtypes.VolumeReclaimRetain)))

	// same owner/vid, new deployment (adoption / migration re-lease)
	newLid := lid
	newLid.DSeq++

	require.NoError(t, kc.DeployVolume(context.Background(), newLid, testVolumeGroup(t, newLid, dtypes.VolumeReclaimRetain)))

	name := crd.VolumeName(lid.Owner, testVolumeVID)
	obj, err := kc.ac.AkashV2beta2().Volumes(testKubeClientNs).Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, newLid, mustLeaseIDFromCRD(t, obj.Spec.LeaseID))
}

func mustLeaseIDFromCRD(t *testing.T, id crd.LeaseID) mtypes.LeaseID {
	t.Helper()
	lid, err := id.FromCRD()
	require.NoError(t, err)
	return lid
}

func TestAttachDetachVolume(t *testing.T) {
	lid := testutil.LeaseID(t)
	kc := volumeClientForTest(t, nil, nil)

	require.NoError(t, kc.DeployVolume(context.Background(), lid, testVolumeGroup(t, lid, dtypes.VolumeReclaimRetain)))

	ref := testVolumeRef(lid)

	computeLid := testutil.LeaseID(t)
	computeLid.Owner = lid.Owner

	require.NoError(t, kc.AttachVolume(context.Background(), computeLid, ref))

	obj, err := kc.VolumeStatus(context.Background(), ref)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseAttached, obj.Status.Phase)
	require.Equal(t, computeLid.String(), obj.Status.AttachedLease)

	// RWO: a second lease cannot attach
	otherLid := computeLid
	otherLid.DSeq++
	err = kc.AttachVolume(context.Background(), otherLid, ref)
	require.ErrorIs(t, err, kubeclienterrors.ErrVolumeAttached)

	// detach by a lease that is not attached is a no-op
	require.NoError(t, kc.DetachVolume(context.Background(), otherLid, ref))
	obj, err = kc.VolumeStatus(context.Background(), ref)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseAttached, obj.Status.Phase)

	require.NoError(t, kc.DetachVolume(context.Background(), computeLid, ref))

	obj, err = kc.VolumeStatus(context.Background(), ref)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseProvisioned, obj.Status.Phase)
	require.Equal(t, "", obj.Status.AttachedLease)
}

func TestAttachVolumeNotFound(t *testing.T) {
	kc := volumeClientForTest(t, nil, nil)

	lid := testutil.LeaseID(t)
	err := kc.AttachVolume(context.Background(), lid, testVolumeRef(lid))
	require.ErrorIs(t, err, kubeclienterrors.ErrVolumeNotFound)
}

func TestTeardownVolumeRetains(t *testing.T) {
	lid := testutil.LeaseID(t)
	kc := volumeClientForTest(t, nil, nil)

	require.NoError(t, kc.DeployVolume(context.Background(), lid, testVolumeGroup(t, lid, dtypes.VolumeReclaimRetain)))

	before := time.Now()
	require.NoError(t, kc.TeardownVolume(context.Background(), lid))

	obj, err := kc.VolumeStatus(context.Background(), testVolumeRef(lid))
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseRetained, obj.Status.Phase)
	require.NotNil(t, obj.Status.RetainedUntil)

	// deadline is positive: closedAt + retention
	require.True(t, obj.Status.RetainedUntil.Time.After(before.Add(167*time.Hour)))
}

func TestTeardownVolumeReclaimDelete(t *testing.T) {
	lid := testutil.LeaseID(t)
	kc := volumeClientForTest(t, nil, nil)

	require.NoError(t, kc.DeployVolume(context.Background(), lid, testVolumeGroup(t, lid, dtypes.VolumeReclaimDelete)))
	require.NoError(t, kc.TeardownVolume(context.Background(), lid))

	obj, err := kc.VolumeStatus(context.Background(), testVolumeRef(lid))
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseReleasing, obj.Status.Phase)
}

func TestDeployedVolumesRebuild(t *testing.T) {
	lid := testutil.LeaseID(t)
	kc := volumeClientForTest(t, nil, nil)

	volumes, err := kc.DeployedVolumes(context.Background())
	require.NoError(t, err)
	require.Empty(t, volumes)

	require.NoError(t, kc.DeployVolume(context.Background(), lid, testVolumeGroup(t, lid, dtypes.VolumeReclaimRetain)))

	volumes, err = kc.DeployedVolumes(context.Background())
	require.NoError(t, err)
	require.Len(t, volumes, 1)

	require.Equal(t, lid, volumes[0].LeaseID)
	require.NotNil(t, volumes[0].Group.Volume)
	require.Equal(t, testVolumeVID, volumes[0].Group.Volume.Vid)
	require.Len(t, volumes[0].Group.Resources, 1)
	require.Equal(t, uint64(200), volumes[0].Group.Resources[0].Storage[0].Quantity.Value())
}

// TestTeardownLeaseLeavesVolumeObjects pins the AEP-87 invariant that
// TeardownLease is unchanged: it deletes the lease namespace (and manifest),
// while Retain PVs and Volume CRDs survive for the storage operator to
// re-park.
func TestTeardownLeaseLeavesVolumeObjects(t *testing.T) {
	lid := testutil.LeaseID(t)

	ns := builder.LidNS(lid)

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-volume-test",
			Labels: map[string]string{
				builder.AkashManagedLabelName:   "true",
				builder.AkashComponentLabelName: builder.AkashComponentVolume,
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		},
	}

	nsObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
		},
	}

	kc := volumeClientForTest(t, []runtime.Object{pv, nsObj}, nil)

	require.NoError(t, kc.DeployVolume(context.Background(), lid, testVolumeGroup(t, lid, dtypes.VolumeReclaimRetain)))

	require.NoError(t, kc.TeardownLease(context.Background(), lid))

	// the namespace is gone
	_, err := kc.kc.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
	require.Error(t, err)

	// the Retain PV survives
	gotPV, err := kc.kc.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, corev1.PersistentVolumeReclaimRetain, gotPV.Spec.PersistentVolumeReclaimPolicy)

	// the Volume CRD survives
	_, err = kc.VolumeStatus(context.Background(), testVolumeRef(lid))
	require.NoError(t, err)
}
