package replication

import (
	"archive/tar"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FileDriver is the plain-copy backend for volumes on a local filesystem
// (local-path PVs): the stream-export fallback when no RBD image backs the
// PV, and the backend CI exercises (IMPLEMENTATION.md §8.3).
//
// A volume's data is the directory tree at Ref.Path (the PV's host path), or
// <base>/<name>/data when the Ref names no path. A snapshot is a tar archive
// of that tree, kept outside the volume so the tenant never sees it and it
// never counts against the volume. An archive has no diff format, so a
// "diff" is the whole target snapshot: every transfer is a full copy, and an
// import replaces the tree outright, which converges to the same data.
//
//	<base>/<name>/snap/<id>.tar   immutable snapshot archives
//	<base>/<name>/imported        the snapshot last imported, and its digest
//
// The tree belongs to a tenant and an import comes from another provider, so
// neither is trusted. Every read and write goes through an os.Root confined
// to the volume: no symlink, planted in the tree or carried in an archive,
// reaches a file outside it. Archives carry directories, regular files and
// symlinks only.
type FileDriver struct {
	base string
	mu   sync.Mutex
}

var _ Driver = (*FileDriver)(nil)

// NewFileDriver returns a file-copy driver keeping its snapshots under base.
func NewFileDriver(base string) *FileDriver {
	return &FileDriver{base: base}
}

const (
	fileDataDir     = "data"
	fileSnapDir     = "snap"
	fileSnapPrefix  = "snap-"
	fileSnapSuffix  = ".tar"
	fileSnapPadding = 8
	fileImported    = "imported"

	// fileStagingPrefix names the directory an import lands in before it
	// replaces the tree. It sits inside the volume so the final moves stay
	// on one filesystem.
	fileStagingPrefix = ".akash-import-"
)

// DataPath returns the directory holding a volume's data.
func (d *FileDriver) DataPath(ref Ref) string {
	if ref.Path != "" {
		return ref.Path
	}

	return filepath.Join(d.base, ref.Name, fileDataDir)
}

func (d *FileDriver) stateDir(ref Ref) string {
	return filepath.Join(d.base, ref.Name)
}

// snapPath locates a snapshot archive. Snapshot ids arrive from other
// providers, so one that is not a plain file name is never held.
func (d *FileDriver) snapPath(ref Ref, id string) (string, bool) {
	if id == "" || id != filepath.Base(id) || !filepath.IsLocal(id) {
		return "", false
	}

	return filepath.Join(d.stateDir(ref), fileSnapDir, id+fileSnapSuffix), true
}

func (d *FileDriver) Snapshot(_ context.Context, ref Ref) (Snapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	snaps, err := d.list(ref)
	if err != nil {
		return Snapshot{}, err
	}

	seq := 1
	if len(snaps) > 0 {
		last, _, _ := strings.Cut(strings.TrimPrefix(snaps[len(snaps)-1].ID, fileSnapPrefix), "-")
		if n, err := strconv.Atoi(last); err == nil {
			seq = n + 1
		}
	}

	// the sequence orders snapshots; the suffix keeps an id from naming
	// two different trees once older snapshots are gone, which would let a
	// destination take one for the other
	suffix, err := randomHex(4)
	if err != nil {
		return Snapshot{}, err
	}

	id := fmt.Sprintf("%s%0*d-%s", fileSnapPrefix, fileSnapPadding, seq, suffix)
	file, _ := d.snapPath(ref, id)

	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		return Snapshot{}, err
	}

	if err := writeArchive(d.DataPath(ref), file); err != nil {
		return Snapshot{}, fmt.Errorf("%w: snapshot %s: %s", ErrReplication, id, err.Error())
	}

	fi, err := os.Stat(file)
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
	entries, err := os.ReadDir(filepath.Join(d.stateDir(ref), fileSnapDir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	snaps := make([]Snapshot, 0, len(entries))

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, fileSnapPrefix) || !strings.HasSuffix(name, fileSnapSuffix) {
			continue
		}

		fi, err := e.Info()
		if err != nil {
			return nil, err
		}

		snaps = append(snaps, Snapshot{ID: strings.TrimSuffix(name, fileSnapSuffix), CreatedAt: fi.ModTime()})
	}

	// zero-padded sequence ids sort oldest-first lexicographically
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].ID < snaps[j].ID })

	return snaps, nil
}

func (d *FileDriver) Export(_ context.Context, ref Ref, snapshot, fromSnapshot string) (io.ReadCloser, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if fromSnapshot != "" && !d.holds(ref, fromSnapshot) {
		return nil, fmt.Errorf("%w: diff base %q", ErrUnknownSnapshot, fromSnapshot)
	}

	file, valid := d.snapPath(ref, snapshot)
	if !valid {
		return nil, fmt.Errorf("%w: %q", ErrUnknownSnapshot, snapshot)
	}

	// the file backend has no diff format: the "diff" is the full target
	// snapshot; import replaces the tree and converges identically
	f, err := os.Open(file)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %q", ErrUnknownSnapshot, snapshot)
	}
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (d *FileDriver) holds(ref Ref, snapshot string) bool {
	if file, valid := d.snapPath(ref, snapshot); valid {
		if _, err := os.Stat(file); err == nil {
			return true
		}
	}

	rec, found := d.readImported(ref)

	return found && rec.Snapshot == snapshot
}

func (d *FileDriver) Import(_ context.Context, ref Ref, snapshot, fromSnapshot string, r io.Reader) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if fromSnapshot != "" && !d.holds(ref, fromSnapshot) {
		return fmt.Errorf("%w: diff base %q", ErrUnknownSnapshot, fromSnapshot)
	}

	dataDir := d.DataPath(ref)
	if ref.Path == "" {
		if err := os.MkdirAll(dataDir, 0o750); err != nil {
			return err
		}
	}

	volume, err := os.OpenRoot(dataDir)
	if err != nil {
		return err
	}
	defer func() { _ = volume.Close() }()

	if err := removeStaging(volume); err != nil {
		return err
	}

	staging, err := stagingName()
	if err != nil {
		return err
	}

	if err := volume.Mkdir(staging, 0o700); err != nil {
		return err
	}

	digest := sha256.New()
	stream := &countingReader{r: io.TeeReader(r, digest)}

	if err := extractArchive(volume, staging, stream); err != nil {
		_ = volume.RemoveAll(staging)
		return err
	}

	// the digest covers the whole stream, the archive's end padding too
	if _, err := io.Copy(io.Discard, stream); err != nil {
		_ = volume.RemoveAll(staging)
		return err
	}

	if err := replaceTree(volume, staging); err != nil {
		return err
	}

	return d.writeImported(ref, importRecord{
		Snapshot: snapshot,
		SHA256:   hex.EncodeToString(digest.Sum(nil)),
		Size:     stream.n,
	})
}

func (d *FileDriver) Digest(_ context.Context, ref Ref, snapshot string) (Digest, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if file, valid := d.snapPath(ref, snapshot); valid {
		f, err := os.Open(file)
		switch {
		case err == nil:
			defer func() { _ = f.Close() }()

			h := sha256.New()

			size, err := io.Copy(h, f)
			if err != nil {
				return Digest{}, err
			}

			return Digest{Size: uint64(size), SHA256: h.Sum(nil)}, nil // nolint: gosec
		case !os.IsNotExist(err):
			return Digest{}, err
		}
	}

	if rec, found := d.readImported(ref); found && rec.Snapshot == snapshot {
		sum, err := hex.DecodeString(rec.SHA256)
		if err != nil {
			return Digest{}, fmt.Errorf("%w: import record of %s: %s", ErrReplication, ref.Name, err.Error())
		}

		return Digest{Size: rec.Size, SHA256: sum}, nil
	}

	return Digest{}, fmt.Errorf("%w: %q", ErrUnknownSnapshot, snapshot)
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
		file, _ := d.snapPath(ref, snaps[0].ID)
		if err := os.Remove(file); err != nil {
			return err
		}

		snaps = snaps[1:]
	}

	return nil
}

// stateDirName matches the directories the driver keeps per volume, named
// for the Volume CRD.
var stateDirName = regexp.MustCompile(`^volume-[0-9a-f]{12}$`)

// RemoveStale drops the snapshots and import records of every volume live
// does not name: the volume is gone, so nothing will export it again. It
// touches nothing but the driver's own per-volume directories, whatever
// else shares the base directory.
func (d *FileDriver) RemoveStale(live map[string]bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	entries, err := os.ReadDir(d.base)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, e := range entries {
		if !e.IsDir() || !stateDirName.MatchString(e.Name()) || live[e.Name()] {
			continue
		}

		if err := os.RemoveAll(filepath.Join(d.base, e.Name())); err != nil {
			return err
		}
	}

	return nil
}

// importRecord stands in for the snapshot an import landed: the tree holds
// its data, so a full copy of the archive would only take space.
type importRecord struct {
	Snapshot string `json:"snapshot"`
	SHA256   string `json:"sha256"`
	Size     uint64 `json:"size"`
}

func (d *FileDriver) readImported(ref Ref) (importRecord, bool) {
	buf, err := os.ReadFile(filepath.Join(d.stateDir(ref), fileImported))
	if err != nil {
		return importRecord{}, false
	}

	var rec importRecord
	if err := json.Unmarshal(buf, &rec); err != nil {
		return importRecord{}, false
	}

	return rec, true
}

func (d *FileDriver) writeImported(ref Ref, rec importRecord) error {
	buf, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(d.stateDir(ref), 0o700); err != nil {
		return err
	}

	file := filepath.Join(d.stateDir(ref), fileImported)

	if err := os.WriteFile(file+".tmp", buf, 0o600); err != nil {
		return err
	}

	return os.Rename(file+".tmp", file)
}

// writeArchive archives the tree at dir into file, landing it whole or not
// at all.
func writeArchive(dir, file string) error {
	tmp := file + ".partial"

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	if err := archiveTree(dir, f); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)

		return err
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	return os.Rename(tmp, file)
}

// archiveTree writes the tree at dir to w in a fixed order, so two archives
// of an unchanged tree are the same bytes.
func archiveTree(dir string, w io.Writer) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()

	tw := tar.NewWriter(w)

	err = fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, err error) error {
		// a live volume may lose an entry while it is walked
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}

		if name == "." {
			return nil
		}

		// a half-landed import is not the volume's data
		if entry.IsDir() && path.Dir(name) == "." && strings.HasPrefix(name, fileStagingPrefix) {
			return fs.SkipDir
		}

		return archiveEntry(root, tw, name)
	})
	if err != nil {
		return err
	}

	return tw.Close()
}

func archiveEntry(root *os.Root, tw *tar.Writer, name string) error {
	fi, err := root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	var (
		link string
		f    *os.File
	)

	switch {
	case fi.Mode().IsRegular():
		f, err = root.Open(name)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
	case fi.IsDir():
	case fi.Mode()&fs.ModeSymlink != 0:
		link, err = root.Readlink(name)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
	default:
		// devices, sockets and pipes are not data a volume carries
		return nil
	}

	hdr, err := tar.FileInfoHeader(fi, link)
	if err != nil {
		return err
	}

	hdr.Name = name
	if fi.IsDir() {
		hdr.Name += "/"
	}

	hdr.Uname, hdr.Gname = "", ""
	hdr.AccessTime, hdr.ChangeTime = time.Time{}, time.Time{}

	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}

	if f == nil {
		return nil
	}

	// a file that changed size since it was listed keeps the size its
	// header declared: a later snapshot carries what it holds by then
	n, err := io.CopyN(tw, f, hdr.Size)
	if errors.Is(err, io.EOF) {
		_, err = io.CopyN(tw, zeroReader{}, hdr.Size-n)
	}

	return err
}

// extractArchive lands the archive in r under the staging directory of the
// volume.
func extractArchive(volume *os.Root, staging string, r io.Reader) error {
	root, err := volume.OpenRoot(staging)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()

	type landedDir struct {
		name string
		hdr  *tar.Header
	}

	var dirs []landedDir

	tr := tar.NewReader(r)

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("%w: reading archive: %s", ErrReplication, err.Error())
		}

		name, err := archiveName(hdr)
		if err != nil {
			return err
		}

		if parent := path.Dir(name); parent != "." {
			if err := root.MkdirAll(parent, 0o700); err != nil {
				return err
			}
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			// writable until every entry has landed; its own mode last
			if err := root.MkdirAll(name, 0o700); err != nil {
				return err
			}

			dirs = append(dirs, landedDir{name: name, hdr: hdr})
		case tar.TypeReg:
			if err := extractFile(root, name, tr, hdr); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := root.Symlink(hdr.Linkname, name); err != nil {
				return err
			}

			ownAs(root, name, hdr)
		default:
			return fmt.Errorf("%w: archive entry %q is of type %q", ErrReplication, hdr.Name, hdr.Typeflag)
		}
	}

	// deepest first: setting a parent's mode could take away the access a
	// child needs
	for i := len(dirs) - 1; i >= 0; i-- {
		dir := dirs[i]

		if err := root.Chmod(dir.name, fs.FileMode(dir.hdr.Mode)&fs.ModePerm); err != nil { // nolint: gosec
			return err
		}

		if err := root.Chtimes(dir.name, dir.hdr.ModTime, dir.hdr.ModTime); err != nil {
			return err
		}

		ownAs(root, dir.name, dir.hdr)
	}

	return nil
}

// archiveName is where an entry lands in the tree, refused when that could
// be anywhere outside it.
func archiveName(hdr *tar.Header) (string, error) {
	name := strings.TrimSuffix(hdr.Name, "/")

	if name == "" || name == "." || !filepath.IsLocal(name) || strings.Contains(name, `\`) {
		return "", fmt.Errorf("%w: archive entry %q leaves the volume", ErrReplication, hdr.Name)
	}

	// a sparse entry expands past the bytes the stream carried
	for key := range hdr.PAXRecords {
		if strings.HasPrefix(key, "GNU.sparse.") {
			return "", fmt.Errorf("%w: archive entry %q is sparse", ErrReplication, hdr.Name)
		}
	}

	return name, nil
}

func extractFile(root *os.Root, name string, r io.Reader, hdr *tar.Header) error {
	// exclusive: an entry never writes through one that landed before it
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}

	if _, err := io.CopyN(f, r, hdr.Size); err != nil {
		_ = f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	if err := root.Chmod(name, fs.FileMode(hdr.Mode)&fs.ModePerm); err != nil { // nolint: gosec
		return err
	}

	if err := root.Chtimes(name, hdr.ModTime, hdr.ModTime); err != nil {
		return err
	}

	ownAs(root, name, hdr)

	return nil
}

// ownAs hands an entry to the owner the archive recorded. Only a privileged
// provider can; elsewhere the entry stays the provider's.
func ownAs(root *os.Root, name string, hdr *tar.Header) {
	_ = root.Lchown(name, hdr.Uid, hdr.Gid)
}

// replaceTree swaps the volume's contents for what landed in staging.
func replaceTree(volume *os.Root, staging string) error {
	current, err := fs.ReadDir(volume.FS(), ".")
	if err != nil {
		return err
	}

	for _, entry := range current {
		if entry.Name() == staging {
			continue
		}

		if err := volume.RemoveAll(entry.Name()); err != nil {
			return err
		}
	}

	landed, err := fs.ReadDir(volume.FS(), staging)
	if err != nil {
		return err
	}

	for _, entry := range landed {
		if err := volume.Rename(path.Join(staging, entry.Name()), entry.Name()); err != nil {
			return err
		}
	}

	return volume.Remove(staging)
}

// removeStaging clears what an interrupted import left behind.
func removeStaging(volume *os.Root) error {
	entries, err := fs.ReadDir(volume.FS(), ".")
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), fileStagingPrefix) {
			if err := volume.RemoveAll(entry.Name()); err != nil {
				return err
			}
		}
	}

	return nil
}

func stagingName() (string, error) {
	suffix, err := randomHex(8)
	if err != nil {
		return "", err
	}

	return fileStagingPrefix + suffix, nil
}

func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	return hex.EncodeToString(buf), nil
}

type countingReader struct {
	r io.Reader
	n uint64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += uint64(n) // nolint: gosec

	return n, err
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
