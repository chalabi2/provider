package replication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/akash-network/provider/bucket"
	"github.com/akash-network/provider/bucket/buckettest"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	afake "github.com/akash-network/provider/pkg/client/clientset/versioned/fake"
)

const (
	sourceNS      = "lease"
	sourcePV      = "pvc-0001"
	sourceVolNS   = "akash-volumes"
	sourceLeaseNS = "lease-namespace"
)

func TestSourceResolvesAParkedVolumeAsQuiesced(t *testing.T) {
	src := sourceWorld(t, sourceVolume(crd.VolumePhaseExporting, ""), sourcePVBoundTo(sourceVolNS))

	info, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)

	require.True(t, info.Exportable)
	require.True(t, info.Quiesced)
	require.False(t, info.Incomplete)
	require.Equal(t, "/var/local-path/pvc-0001", info.Ref.Path)
	require.Equal(t, authzDSeq, info.DSeq)
}

func TestSourceSeesAWorkloadStillUsingTheVolume(t *testing.T) {
	src := sourceWorld(t, sourceVolume(crd.VolumePhaseExporting, ""), sourcePVBoundTo(sourceLeaseNS), podMounting(corev1.PodRunning))

	info, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)

	require.False(t, info.Quiesced)
}

func TestSourceIgnoresAWorkloadThatStopped(t *testing.T) {
	src := sourceWorld(t, sourceVolume(crd.VolumePhaseExporting, ""), sourcePVBoundTo(sourceLeaseNS), podMounting(corev1.PodSucceeded))

	info, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)

	require.True(t, info.Quiesced)
}

func TestSourceWaitsForTheNodeToLetGoOfTheVolume(t *testing.T) {
	pvName := sourcePV
	attachment := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-attachment"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "rbd.csi.ceph.com",
			NodeName: "node-1",
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
	}

	src := sourceWorld(t, sourceVolume(crd.VolumePhaseExporting, ""), sourcePVBoundTo(sourceVolNS), attachment)

	info, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)

	require.False(t, info.Quiesced)
}

func TestSourceFallsBackToPodsWithoutAccessToAttachments(t *testing.T) {
	src := sourceWorld(t, sourceVolume(crd.VolumePhaseExporting, ""), sourcePVBoundTo(sourceVolNS))
	src.kc.(*kfake.Clientset).PrependReactor("list", "volumeattachments", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, kerrors.NewForbidden(schema.GroupResource{Group: "storage.k8s.io", Resource: "volumeattachments"}, "", nil)
	})

	info, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)

	require.True(t, info.Quiesced)
}

func TestSourceFailsWhenAttachmentsCannotBeRead(t *testing.T) {
	src := sourceWorld(t, sourceVolume(crd.VolumePhaseExporting, ""), sourcePVBoundTo(sourceVolNS))
	src.kc.(*kfake.Clientset).PrependReactor("list", "volumeattachments", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, kerrors.NewServiceUnavailable("apiserver restarting")
	})

	_, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.Error(t, err)
}

func TestSourceMarksAnUnfinishedImport(t *testing.T) {
	src := sourceWorld(t, sourceVolume(crd.VolumePhaseImporting, authzStranger), sourcePVBoundTo(sourceVolNS))

	info, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)

	require.True(t, info.Incomplete)
}

func TestSourceHasNothingToExportBeforeThePVExists(t *testing.T) {
	vol := sourceVolume(crd.VolumePhasePending, "")
	vol.Status.PVName = ""

	src := sourceWorld(t, vol)

	info, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)

	require.False(t, info.Exportable)
}

func TestSourceNeverExportsAVolumeBeingDestroyed(t *testing.T) {
	src := sourceWorld(t, sourceVolume(crd.VolumePhaseReleasing, ""), sourcePVBoundTo(sourceVolNS))

	info, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)

	require.False(t, info.Exportable)
}

func TestSourceRefusesAnotherOwnersVolumeUnderTheSameName(t *testing.T) {
	vol := sourceVolume(crd.VolumePhaseExporting, "")
	vol.Spec.Owner = authzStranger

	src := sourceWorld(t, vol, sourcePVBoundTo(sourceVolNS))

	_, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.ErrorIs(t, err, ErrReplication)
}

func sourceWorld(t *testing.T, vol *crd.Volume, objects ...runtime.Object) *crdVolumeSource {
	t.Helper()

	return &crdVolumeSource{
		ac: afake.NewSimpleClientset(vol),
		kc: kfake.NewSimpleClientset(objects...),
		ns: sourceNS,
	}
}

func sourceVolume(phase crd.VolumePhase, importFrom string) *crd.Volume {
	return &crd.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      crd.VolumeName(authzOwner, authzVID),
			Namespace: sourceNS,
		},
		Spec: crd.VolumeSpec{
			Owner:      authzOwner,
			VID:        authzVID,
			GroupID:    crd.VolumeGroupID{DSeq: "100", GSeq: 1},
			ImportFrom: importFrom,
		},
		Status: crd.VolumeStatus{
			Phase:  phase,
			PVName: sourcePV,
		},
	}
}

func sourcePVBoundTo(namespace string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: sourcePV},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: namespace, Name: crd.VolumeName(authzOwner, authzVID)},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/var/local-path/pvc-0001"},
			},
		},
	}
}

func podMounting(phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: sourceLeaseNS},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: crd.VolumeName(authzOwner, authzVID)},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// bucketSourceWorld resolves a bucket volume in phase whose bucket exists.
func bucketSourceWorld(t *testing.T, phase crd.VolumePhase, importFrom string) (*crdVolumeSource, *buckettest.Fake, bucket.Ref) {
	t.Helper()

	vol := sourceVolume(phase, importFrom)
	vol.Spec.Access = "bucket"
	vol.Status.PVName = ""

	ref := bucket.RefFor(vol.Name, authzOwner, authzVID)
	vol.Status.Bucket = ref.Bucket

	fake := buckettest.NewFake()
	require.NoError(t, fake.Ensure(context.Background(), bucket.Spec{Ref: ref, SizeBytes: 1 << 20}))

	src := sourceWorld(t, vol)
	src.buckets = fake

	return src, fake, ref
}

func TestSourceResolvesABucketMovingAway(t *testing.T) {
	src, fake, ref := bucketSourceWorld(t, crd.VolumePhaseExporting, "")

	info, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)
	require.True(t, info.Bucket)
	require.True(t, info.Exportable)
	require.True(t, info.Quiesced, "a bucket no key opens is quiesced")
	require.False(t, info.Incomplete)
	require.Equal(t, ref.UserID, info.Ref.Name)

	_, err = fake.CreateKey(context.Background(), ref, "app", bucket.KeyReadWrite)
	require.NoError(t, err)

	info, err = src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)
	require.False(t, info.Quiesced, "a key can still write")
}

func TestSourceExportsOnlyBucketsMovingAway(t *testing.T) {
	for _, phase := range []crd.VolumePhase{
		crd.VolumePhasePending,
		crd.VolumePhaseProvisioned,
		crd.VolumePhaseImporting,
		crd.VolumePhaseRetained,
		crd.VolumePhaseReleasing,
	} {
		src, _, _ := bucketSourceWorld(t, phase, "")

		info, err := src.Resolve(context.Background(), authzOwner, authzVID)
		require.NoError(t, err, phase)
		require.True(t, info.Bucket, phase)
		require.False(t, info.Exportable, phase)
	}
}

func TestSourceExportsNoBucketWithoutItsBackend(t *testing.T) {
	src, _, _ := bucketSourceWorld(t, crd.VolumePhaseExporting, "")
	src.buckets = nil

	info, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)
	require.False(t, info.Exportable)
}

func TestSourceReportsABucketThatNeverFinishedArriving(t *testing.T) {
	src, _, _ := bucketSourceWorld(t, crd.VolumePhaseExporting, authzStranger)

	info, err := src.Resolve(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)
	require.True(t, info.Incomplete)
}
