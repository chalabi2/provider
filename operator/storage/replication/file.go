package replication

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// FileDriver is the plain-file-copy backend for local-path volumes: the
// stream-export fallback when there is no RBD image behind the PV, and the
// backend CI exercises (IMPLEMENTATION.md §8.3 - local-path proves the
// coordination and transfer plumbing, not RBD).
//
// Snapshots are whole-file copies; "diffs" degrade to a full copy of the
// target snapshot (a plain file has no block-diff format), which converges
// to the identical image - the protocol semantics hold, only the transfer
// is not incremental. Layout, per volume, under the base directory:
//
//	<base>/<name>/image                  the live image
//	<base>/<name>/snap/<id>              immutable snapshot copies
type FileDriver struct {
	base string
	mu   sync.Mutex
}

var _ Driver = (*FileDriver)(nil)

// NewFileDriver returns a file-copy driver rooted at base.
func NewFileDriver(base string) *FileDriver {
	return &FileDriver{base: base}
}

const (
	fileImageName   = "image"
	fileSnapDir     = "snap"
	fileSnapPrefix  = "snap-"
	fileSnapPadding = 8
)

func (d *FileDriver) root(ref Ref) string {
	if ref.Path != "" {
		return ref.Path
	}

	return filepath.Join(d.base, ref.Name)
}

// ImagePath returns the live image path for a volume; the write side of
// tests and the local-path resolver use it.
func (d *FileDriver) ImagePath(ref Ref) string {
	return filepath.Join(d.root(ref), fileImageName)
}

func (d *FileDriver) snapPath(ref Ref, id string) string {
	return filepath.Join(d.root(ref), fileSnapDir, id)
}

func (d *FileDriver) Snapshot(ctx context.Context, ref Ref) (Snapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	snaps, err := d.list(ref)
	if err != nil {
		return Snapshot{}, err
	}

	seq := 1
	if len(snaps) > 0 {
		last := snaps[len(snaps)-1].ID
		if n, err := strconv.Atoi(strings.TrimPrefix(last, fileSnapPrefix)); err == nil {
			seq = n + 1
		}
	}

	id := fmt.Sprintf("%s%0*d", fileSnapPrefix, fileSnapPadding, seq)

	if err := os.MkdirAll(filepath.Dir(d.snapPath(ref, id)), 0o750); err != nil {
		return Snapshot{}, err
	}

	if err := copyFile(d.ImagePath(ref), d.snapPath(ref, id)); err != nil {
		return Snapshot{}, fmt.Errorf("%w: snapshot %s: %s", ErrReplication, id, err.Error())
	}

	fi, err := os.Stat(d.snapPath(ref, id))
	if err != nil {
		return Snapshot{}, err
	}

	return Snapshot{ID: id, CreatedAt: fi.ModTime()}, nil
}

func (d *FileDriver) Snapshots(_ context.Context, ref Ref) ([]Snapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.list(ref)
}

func (d *FileDriver) list(ref Ref) ([]Snapshot, error) {
	entries, err := os.ReadDir(filepath.Join(d.root(ref), fileSnapDir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	snaps := make([]Snapshot, 0, len(entries))

	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), fileSnapPrefix) {
			continue
		}

		fi, err := e.Info()
		if err != nil {
			return nil, err
		}

		snaps = append(snaps, Snapshot{ID: e.Name(), CreatedAt: fi.ModTime()})
	}

	// zero-padded sequence ids sort oldest-first lexicographically
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].ID < snaps[j].ID })

	return snaps, nil
}

func (d *FileDriver) Export(_ context.Context, ref Ref, snapshot, fromSnapshot string) (io.ReadCloser, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if fromSnapshot != "" {
		if _, err := os.Stat(d.snapPath(ref, fromSnapshot)); err != nil {
			return nil, fmt.Errorf("%w: diff base %q", ErrUnknownSnapshot, fromSnapshot)
		}
	}

	// the file backend has no diff format: the "diff" is the full target
	// snapshot; import overwrites and converges identically
	f, err := os.Open(d.snapPath(ref, snapshot))
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %q", ErrUnknownSnapshot, snapshot)
	}
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (d *FileDriver) Import(_ context.Context, ref Ref, snapshot, fromSnapshot string, r io.Reader) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if fromSnapshot != "" {
		if _, err := os.Stat(d.snapPath(ref, fromSnapshot)); err != nil {
			return fmt.Errorf("%w: diff base %q", ErrUnknownSnapshot, fromSnapshot)
		}
	}

	if err := os.MkdirAll(filepath.Join(d.root(ref), fileSnapDir), 0o750); err != nil {
		return err
	}

	// land the stream as the immutable snapshot first, then copy it over
	// the live image: a broken import never corrupts the image
	tmp := d.snapPath(ref, snapshot) + ".partial"

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)

		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmp, d.snapPath(ref, snapshot)); err != nil {
		return err
	}

	return copyFile(d.snapPath(ref, snapshot), d.ImagePath(ref))
}

func (d *FileDriver) Digest(_ context.Context, ref Ref, snapshot string) (Digest, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	f, err := os.Open(d.snapPath(ref, snapshot))
	if os.IsNotExist(err) {
		return Digest{}, fmt.Errorf("%w: %q", ErrUnknownSnapshot, snapshot)
	}
	if err != nil {
		return Digest{}, err
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()

	size, err := io.Copy(h, f)
	if err != nil {
		return Digest{}, err
	}

	return Digest{Size: uint64(size), SHA256: h.Sum(nil)}, nil // nolint: gosec
}

func (d *FileDriver) Prune(_ context.Context, ref Ref, keep int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if keep < 1 {
		keep = 1
	}

	snaps, err := d.list(ref)
	if err != nil {
		return err
	}

	for len(snaps) > keep {
		if err := os.Remove(d.snapPath(ref, snaps[0].ID)); err != nil {
			return err
		}

		snaps = snaps[1:]
	}

	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	tmp := dst + ".tmp"

	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)

		return err
	}

	if err := out.Close(); err != nil {
		return err
	}

	return os.Rename(tmp, dst)
}
