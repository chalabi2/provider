package replication

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"github.com/akash-network/provider/bucket"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	akashclientset "github.com/akash-network/provider/pkg/client/clientset/versioned"
)

// rbdCSIDriverSuffix identifies rook/ceph RBD-provisioned PVs by their
// CSI driver name (<cluster-id>.rbd.csi.ceph.com).
const rbdCSIDriverSuffix = "rbd.csi.ceph.com"

// crdVolumeSource resolves transfer requests against the local Volume
// CRDs and their backing PVs: the same records the storage operator
// reconciles, read fresh on every call (nothing only-in-memory).
type crdVolumeSource struct {
	ac akashclientset.Interface
	kc kubernetes.Interface
	ns string
	// buckets answers whether a key can still write to a bucket; nil when
	// the provider serves none
	buckets bucket.Backend
}

// VolumeRecords resolves local volumes from their records, and records
// the final copies that reached the provider a volume moved to.
type VolumeRecords interface {
	VolumeSource
	ExportRecorder
}

var _ VolumeRecords = (*crdVolumeSource)(nil)

// NewCRDVolumeSource resolves volumes from the Volume CRDs in namespace
// ns and their backing PVs, and buckets through buckets, which is nil on a
// provider that serves none.
func NewCRDVolumeSource(ac akashclientset.Interface, kc kubernetes.Interface, ns string, buckets bucket.Backend) VolumeRecords {
	return &crdVolumeSource{ac: ac, kc: kc, ns: ns, buckets: buckets}
}

// RecordExport keeps on the volume's record that its final copy reached
// provider to at at.
func (s *crdVolumeSource) RecordExport(ctx context.Context, owner, vid, to string, at time.Time) error {
	vols := s.ac.AkashV2beta2().Volumes(s.ns)
	name := crd.VolumeName(owner, vid)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		vol, err := vols.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if !vol.Matches(owner, vid) {
			return fmt.Errorf("%w: volume %s identity mismatch", ErrReplication, name)
		}

		exported := metav1.NewTime(at)

		uvol := vol.DeepCopy()
		uvol.Status.ExportedTo = to
		uvol.Status.ExportedAt = &exported

		_, err = vols.Update(ctx, uvol, metav1.UpdateOptions{})

		return err
	})
}

func (s *crdVolumeSource) Resolve(ctx context.Context, owner, vid string) (VolumeInfo, error) {
	name := crd.VolumeName(owner, vid)

	vol, err := s.ac.AkashV2beta2().Volumes(s.ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return VolumeInfo{}, err
	}

	if vol.Spec.Owner != owner || vol.Spec.VID != vid {
		return VolumeInfo{}, fmt.Errorf("%w: volume %s identity mismatch", ErrReplication, name)
	}

	dseq, err := strconv.ParseUint(vol.Spec.GroupID.DSeq, 10, 64)
	if err != nil {
		return VolumeInfo{}, fmt.Errorf("%w: volume %s dseq %q: %s", ErrReplication, name, vol.Spec.GroupID.DSeq, err.Error())
	}

	if vol.IsBucket() {
		return s.resolveBucket(ctx, vol, dseq)
	}

	info := VolumeInfo{
		Ref:  Ref{Name: name},
		DSeq: dseq,
		// Releasing is the only phase past the point of no return; a
		// Pending volume simply has no PV yet
		Exportable: vol.Status.PVName != "" && vol.Status.Phase != crd.VolumePhaseReleasing,
		Incomplete: vol.Incomplete(),
	}

	if vol.IsReplica() && !vol.Status.Promoted && vol.Status.SyncedAt != nil {
		info.SyncedAt = vol.Status.SyncedAt.Time
	}

	if vol.Status.PVName == "" {
		return info, nil
	}

	pv, err := s.kc.CoreV1().PersistentVolumes().Get(ctx, vol.Status.PVName, metav1.GetOptions{})
	if err != nil {
		return VolumeInfo{}, err
	}

	if info.Quiesced, err = s.quiesced(ctx, pv); err != nil {
		return VolumeInfo{}, err
	}

	switch {
	case pv.Spec.CSI != nil && strings.HasSuffix(pv.Spec.CSI.Driver, rbdCSIDriverSuffix):
		info.Ref.Pool = pv.Spec.CSI.VolumeAttributes["pool"]
		info.Ref.Image = pv.Spec.CSI.VolumeAttributes["imageName"]
	case pv.Spec.HostPath != nil:
		// local-path fallback: the FileDriver lays its image/snap state
		// out inside the volume's host path
		info.Ref.Path = pv.Spec.HostPath.Path
	case pv.Spec.Local != nil:
		info.Ref.Path = pv.Spec.Local.Path
	}

	return info, nil
}

// resolveBucket reports a bucket. Only one moving away exports: past its
// reclamation window a retained bucket is suspended, which the provider's
// own reads are refused by too. It is quiesced once no key is left to
// write to it.
func (s *crdVolumeSource) resolveBucket(ctx context.Context, vol *crd.Volume, dseq uint64) (VolumeInfo, error) {
	info := VolumeInfo{
		Ref:        Ref{Name: vol.Name},
		DSeq:       dseq,
		Bucket:     true,
		Exportable: vol.Status.Bucket != "" && vol.Status.Phase == crd.VolumePhaseExporting && s.buckets != nil,
		Incomplete: vol.Incomplete(),
	}

	if !info.Exportable {
		return info, nil
	}

	keys, err := s.buckets.ListKeys(ctx, bucket.RefFor(vol.Name, vol.Spec.Owner, vol.Spec.VID))
	if err != nil {
		return VolumeInfo{}, err
	}

	info.Quiesced = len(keys) == 0

	return info, nil
}

// quiesced reports that nothing can write to the PV any more: no pod mounts
// the claim it is bound to, and no node still has it attached. A pod is gone
// once its mounts are, but a node unstages the volume (flushing what the
// filesystem still caches) only afterwards, which the volume attachment's
// removal confirms for CSI volumes.
func (s *crdVolumeSource) quiesced(ctx context.Context, pv *corev1.PersistentVolume) (bool, error) {
	if claim := pv.Spec.ClaimRef; claim != nil {
		pods, err := s.kc.CoreV1().Pods(claim.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}

		for i := range pods.Items {
			if podMountsClaim(&pods.Items[i], claim.Name) {
				return false, nil
			}
		}
	}

	attachments, err := s.kc.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		// a provider not granted volume attachments falls back to the pods
		if kerrors.IsForbidden(err) {
			return true, nil
		}

		return false, err
	}

	for _, attachment := range attachments.Items {
		if name := attachment.Spec.Source.PersistentVolumeName; name != nil && *name == pv.Name {
			return false, nil
		}
	}

	return true, nil
}

func podMountsClaim(pod *corev1.Pod, claim string) bool {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false
	}

	for _, volume := range pod.Spec.Volumes {
		if pvc := volume.PersistentVolumeClaim; pvc != nil && pvc.ClaimName == claim {
			return true
		}
	}

	return false
}

// NewAutoDriver dispatches per volume: RBD-backed refs (pool/image
// resolved from the PV) go to the rbd stream-export backend, everything
// else takes the plain-file-copy fallback.
func NewAutoDriver(rbd, file Driver) Driver {
	return &autoDriver{rbd: rbd, file: file}
}

type autoDriver struct {
	rbd  Driver
	file Driver
}

var _ Driver = (*autoDriver)(nil)

func (d *autoDriver) pick(ref Ref) Driver {
	if ref.Pool != "" && ref.Image != "" && d.rbd != nil {
		return d.rbd
	}

	return d.file
}

func (d *autoDriver) Snapshot(ctx context.Context, ref Ref) (Snapshot, error) {
	return d.pick(ref).Snapshot(ctx, ref)
}

func (d *autoDriver) Snapshots(ctx context.Context, ref Ref) ([]Snapshot, error) {
	return d.pick(ref).Snapshots(ctx, ref)
}

func (d *autoDriver) Export(ctx context.Context, ref Ref, snapshot, fromSnapshot string) (io.ReadCloser, error) {
	return d.pick(ref).Export(ctx, ref, snapshot, fromSnapshot)
}

func (d *autoDriver) Import(ctx context.Context, ref Ref, snapshot, fromSnapshot string, r io.Reader) error {
	return d.pick(ref).Import(ctx, ref, snapshot, fromSnapshot, r)
}

func (d *autoDriver) Digest(ctx context.Context, ref Ref, snapshot string) (Digest, error) {
	return d.pick(ref).Digest(ctx, ref, snapshot)
}

func (d *autoDriver) Prune(ctx context.Context, ref Ref, keep int) error {
	return d.pick(ref).Prune(ctx, ref, keep)
}
