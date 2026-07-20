// Package replication is the AEP-87 volume data plane (DESIGN.md §9,
// IMPLEMENTATION.md §5.5): the ReplicationDriver seam between the
// VolumeTransfer wire protocol and the storage backend, the chunked,
// resumable, per-chunk-sha256 export framing, and the migration/replica
// import flows.
//
// The chain coordinates identity, placement, money, and windows; this
// package moves bytes. It attests nothing about replica consistency.
package replication

import (
	"context"
	"errors"
	"io"
	"time"
)

const (
	// DefaultChunkSize is the export stream chunk size. Small enough to
	// keep per-chunk sha256 verification cheap to retry, large enough to
	// amortize the gRPC framing.
	DefaultChunkSize = 4 << 20

	// DriverStreamExport is the default, required driver: snapshot-based
	// stream export over VolumeTransfer. No Ceph cluster peering, no
	// WAN-exposed mons, no pool-scoped cephx credentials. The mainnet
	// posture.
	DriverStreamExport = "stream-export"

	// DriverRBDMirror is the opt-in snapshot-mode rbd-mirror driver for
	// mutually trusting provider pairs. Disabled by default; see
	// rbdmirror.go for the seam.
	DriverRBDMirror = "rbd-mirror"
)

var (
	// ErrReplication is the root of this package's error tree.
	ErrReplication = errors.New("volume replication")
	// ErrUnknownSnapshot flags a diff base the driver does not hold; the
	// caller falls back to a full export.
	ErrUnknownSnapshot = errors.New("volume replication: unknown snapshot")
	// ErrChecksumMismatch flags a chunk or image whose sha256 does not
	// match its declared digest. The transfer is aborted, never applied.
	ErrChecksumMismatch = errors.New("volume replication: checksum mismatch")
	// ErrNotImplemented flags a driver seam that is present but not built
	// (rbd-mirror).
	ErrNotImplemented = errors.New("volume replication: driver not implemented")
)

// Ref locates a volume's backing image for a driver. Name is always set
// (the Volume CRD name); the backend-specific locators are filled by the
// PV resolver for whichever backend backs the PV.
type Ref struct {
	// Name is the Volume CRD object name (volume-<hash>); the file
	// backend keys its on-disk layout off it.
	Name string
	// Path is the backing file/directory for the file backend
	// (local-path PVs). Empty selects the driver's base directory.
	Path string
	// Pool and Image locate the RBD image for the stream-export RBD
	// backend, resolved from the PV's CSI volumeAttributes.
	Pool  string
	Image string
}

// Snapshot is one point-in-time image the driver holds for a volume.
type Snapshot struct {
	// ID is the driver-scoped snapshot identifier, usable as a diff base.
	ID string
	// CreatedAt is when the snapshot was taken; sync lag derives from it.
	CreatedAt time.Time
}

// Digest identifies a full image: its size and whole-image sha256.
type Digest struct {
	Size   uint64
	SHA256 []byte
}

// Driver is the ReplicationDriver seam (IMPLEMENTATION.md §5.5). The
// exporter side snapshots and opens export streams; the importer side
// applies them. Export streams are source-immutable: they always read
// from a snapshot, never the live image.
type Driver interface {
	// Snapshot takes a new point-in-time snapshot of the volume image
	// and returns it.
	Snapshot(ctx context.Context, ref Ref) (Snapshot, error)

	// Snapshots lists the snapshots held for the volume, oldest first.
	Snapshots(ctx context.Context, ref Ref) ([]Snapshot, error)

	// Export opens the image byte stream at snapshot: the full image
	// when fromSnapshot is empty, else the incremental diff
	// fromSnapshot -> snapshot. ErrUnknownSnapshot when either id is
	// not held.
	Export(ctx context.Context, ref Ref, snapshot, fromSnapshot string) (io.ReadCloser, error)

	// Import applies a stream produced by Export against the same
	// (snapshot, fromSnapshot) pair: a full image write when
	// fromSnapshot is empty, else a diff on top of the previously
	// imported fromSnapshot. The imported state is recorded under
	// snapshot so it can serve as the next diff base.
	Import(ctx context.Context, ref Ref, snapshot, fromSnapshot string, r io.Reader) error

	// Digest returns the size and whole-image sha256 at snapshot.
	Digest(ctx context.Context, ref Ref, snapshot string) (Digest, error)

	// Prune drops snapshots older than keep, never dropping the most
	// recent one (it is the standing diff base).
	Prune(ctx context.Context, ref Ref, keep int) error
}
