package replication

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

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
}

var _ VolumeSource = (*crdVolumeSource)(nil)

// NewCRDVolumeSource resolves volumes from the Volume CRDs in namespace
// ns and their backing PVs.
func NewCRDVolumeSource(ac akashclientset.Interface, kc kubernetes.Interface, ns string) VolumeSource {
	return &crdVolumeSource{ac: ac, kc: kc, ns: ns}
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

	info := VolumeInfo{
		Ref:  Ref{Name: name},
		DSeq: dseq,
		// Releasing is the only phase past the point of no return; a
		// Pending volume simply has no PV yet
		Exportable: vol.Status.PVName != "" && vol.Status.Phase != crd.VolumePhaseReleasing,
	}

	if vol.Status.PVName == "" {
		return info, nil
	}

	pv, err := s.kc.CoreV1().PersistentVolumes().Get(ctx, vol.Status.PVName, metav1.GetOptions{})
	if err != nil {
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
