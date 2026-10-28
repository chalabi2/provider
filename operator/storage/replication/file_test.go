package replication

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeTree lays files out in the volume's data directory, keyed by slash
// path.
func writeTree(t *testing.T, d *FileDriver, ref Ref, files map[string]string) {
	t.Helper()

	dir := d.DataPath(ref)

	for name, content := range files {
		file := filepath.Join(dir, filepath.FromSlash(name))

		require.NoError(t, os.MkdirAll(filepath.Dir(file), 0o750))
		require.NoError(t, os.WriteFile(file, []byte(content), 0o600))
	}

	require.NoError(t, os.MkdirAll(dir, 0o750))
}

// readTree returns the volume's regular files by slash path, and its
// symlinks as "-> target".
func readTree(t *testing.T, d *FileDriver, ref Ref) map[string]string {
	t.Helper()

	tree := make(map[string]string)

	root, err := os.OpenRoot(d.DataPath(ref))
	require.NoError(t, err)

	defer func() { _ = root.Close() }()

	err = fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		switch {
		case entry.Type()&fs.ModeSymlink != 0:
			target, err := root.Readlink(name)
			if err != nil {
				return err
			}

			tree[name] = "-> " + target
		case entry.Type().IsRegular():
			content, err := root.ReadFile(name)
			if err != nil {
				return err
			}

			tree[name] = string(content)
		}

		return nil
	})
	require.NoError(t, err)

	return tree
}

// archiveOf builds an archive from headers, each with its content.
func archiveOf(t *testing.T, entries ...archiveEntryFixture) []byte {
	t.Helper()

	buf := &bytes.Buffer{}
	tw := tar.NewWriter(buf)

	for _, entry := range entries {
		hdr := entry.hdr
		hdr.Size = int64(len(entry.content))

		if hdr.Mode == 0 {
			hdr.Mode = 0o644
		}

		require.NoError(t, tw.WriteHeader(&hdr))

		_, err := tw.Write([]byte(entry.content))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())

	return buf.Bytes()
}

type archiveEntryFixture struct {
	hdr     tar.Header
	content string
}

func regularEntry(name, content string) archiveEntryFixture {
	return archiveEntryFixture{hdr: tar.Header{Name: name, Typeflag: tar.TypeReg}, content: content}
}

// moveVolume snapshots ref on src and imports it into dstRef on dst.
func moveVolume(t *testing.T, src *FileDriver, ref Ref, dst *FileDriver, dstRef Ref) {
	t.Helper()

	ctx := context.Background()

	snap, err := src.Snapshot(ctx, ref)
	require.NoError(t, err)

	rc, err := src.Export(ctx, ref, snap.ID, "")
	require.NoError(t, err)

	defer func() { _ = rc.Close() }()

	require.NoError(t, dst.Import(ctx, dstRef, snap.ID, "", rc))
}

func TestFileDriverMovesTheWholeTree(t *testing.T) {
	src := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa"}
	writeTree(t, src, ref, map[string]string{
		"PG_VERSION":          "16",
		"base/1/1259":         "catalog",
		"pg_wal/000000010000": "wal",
	})
	require.NoError(t, os.Symlink("base/1/1259", filepath.Join(src.DataPath(ref), "current")))
	require.NoError(t, os.Mkdir(filepath.Join(src.DataPath(ref), "empty"), 0o750))

	dst := NewFileDriver(t.TempDir())
	dstRef := Ref{Name: ref.Name, Path: t.TempDir()}

	moveVolume(t, src, ref, dst, dstRef)

	require.Equal(t, map[string]string{
		"PG_VERSION":          "16",
		"base/1/1259":         "catalog",
		"pg_wal/000000010000": "wal",
		"current":             "-> base/1/1259",
	}, readTree(t, dst, dstRef))
	require.DirExists(t, filepath.Join(dstRef.Path, "empty"))
}

func TestFileDriverKeepsSnapshotsOutsideTheVolume(t *testing.T) {
	ctx := context.Background()

	d := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa", Path: t.TempDir()}
	writeTree(t, d, ref, map[string]string{"app.db": "rows"})

	snap, err := d.Snapshot(ctx, ref)
	require.NoError(t, err)
	require.Regexp(t, `^snap-00000001-[0-9a-f]{8}$`, snap.ID)

	require.Equal(t, map[string]string{"app.db": "rows"}, readTree(t, d, ref))
}

func TestFileDriverSnapshotHoldsTheTreeAsItWas(t *testing.T) {
	src := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa"}
	writeTree(t, src, ref, map[string]string{"app.db": "before"})

	snap, err := src.Snapshot(context.Background(), ref)
	require.NoError(t, err)
	writeTree(t, src, ref, map[string]string{"app.db": "after", "later.db": "new"})

	dst := NewFileDriver(t.TempDir())
	rc, err := src.Export(context.Background(), ref, snap.ID, "")
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	require.NoError(t, dst.Import(context.Background(), ref, snap.ID, "", rc))

	require.Equal(t, map[string]string{"app.db": "before"}, readTree(t, dst, ref))
}

func TestFileDriverArchivesAnUnchangedTreeIdentically(t *testing.T) {
	ctx := context.Background()

	d := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa"}
	writeTree(t, d, ref, map[string]string{"a": "1", "b/c": "2", "b/d": "3"})

	first, err := d.Snapshot(ctx, ref)
	require.NoError(t, err)
	second, err := d.Snapshot(ctx, ref)
	require.NoError(t, err)

	firstDigest, err := d.Digest(ctx, ref, first.ID)
	require.NoError(t, err)
	secondDigest, err := d.Digest(ctx, ref, second.ID)
	require.NoError(t, err)

	require.Equal(t, firstDigest, secondDigest)
}

func TestFileDriverImportReplacesWhatTheVolumeHeld(t *testing.T) {
	src := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa"}
	writeTree(t, src, ref, map[string]string{"fresh/app.db": "current"})

	dst := NewFileDriver(t.TempDir())
	dstRef := Ref{Name: ref.Name, Path: t.TempDir()}
	writeTree(t, dst, dstRef, map[string]string{"stale.db": "old", "fresh/leftover": "old"})

	moveVolume(t, src, ref, dst, dstRef)

	require.Equal(t, map[string]string{"fresh/app.db": "current"}, readTree(t, dst, dstRef))
}

func TestFileDriverKeepsModesAndTimes(t *testing.T) {
	src := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa"}
	writeTree(t, src, ref, map[string]string{"conf/app.conf": "x"})

	mtime := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	dir := filepath.Join(src.DataPath(ref), "conf")
	file := filepath.Join(dir, "app.conf")
	require.NoError(t, os.Chmod(file, 0o604))
	require.NoError(t, os.Chtimes(file, mtime, mtime))
	require.NoError(t, os.Chmod(dir, 0o705))
	require.NoError(t, os.Chtimes(dir, mtime, mtime))

	dst := NewFileDriver(t.TempDir())
	dstRef := Ref{Name: ref.Name, Path: t.TempDir()}
	moveVolume(t, src, ref, dst, dstRef)

	for _, name := range []string{"conf", "conf/app.conf"} {
		want, err := os.Stat(filepath.Join(src.DataPath(ref), name))
		require.NoError(t, err)
		got, err := os.Stat(filepath.Join(dstRef.Path, name))
		require.NoError(t, err)

		require.Equal(t, want.Mode(), got.Mode(), name)
		require.True(t, want.ModTime().Equal(got.ModTime()), name)
	}
}

func TestFileDriverDropsSpecialPermissionBits(t *testing.T) {
	dst := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa", Path: t.TempDir()}

	entry := regularEntry("tool", "#!/bin/sh")
	entry.hdr.Mode = 0o4755
	archive := archiveOf(t, entry)

	require.NoError(t, dst.Import(context.Background(), ref, "snap-00000001", "", bytes.NewReader(archive)))

	fi, err := os.Stat(filepath.Join(ref.Path, "tool"))
	require.NoError(t, err)
	require.Equal(t, fs.FileMode(0o755), fi.Mode())
}

func TestFileDriverNeverArchivesThroughATenantSymlink(t *testing.T) {
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "host-secret"), []byte("provider credentials"), 0o600))

	src := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa"}
	writeTree(t, src, ref, map[string]string{"app.db": "rows"})
	require.NoError(t, os.Symlink(filepath.Join(outside, "host-secret"), filepath.Join(src.DataPath(ref), "secret")))
	require.NoError(t, os.Symlink(outside, filepath.Join(src.DataPath(ref), "hostdir")))

	dst := NewFileDriver(t.TempDir())
	dstRef := Ref{Name: ref.Name, Path: t.TempDir()}
	moveVolume(t, src, ref, dst, dstRef)

	require.Equal(t, map[string]string{
		"app.db":  "rows",
		"secret":  "-> " + filepath.Join(outside, "host-secret"),
		"hostdir": "-> " + outside,
	}, readTree(t, dst, dstRef))
}

func TestFileDriverRefusesArchivesThatLeaveTheVolume(t *testing.T) {
	outside := t.TempDir()

	for _, tc := range []struct {
		name    string
		entries []archiveEntryFixture
	}{
		{name: "parent path", entries: []archiveEntryFixture{regularEntry("../escaped", "x")}},
		{name: "nested parent path", entries: []archiveEntryFixture{regularEntry("a/../../escaped", "x")}},
		{name: "absolute path", entries: []archiveEntryFixture{regularEntry(filepath.Join(outside, "escaped"), "x")}},
		{name: "write through a symlink", entries: []archiveEntryFixture{
			{hdr: tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: outside}},
			regularEntry("link/escaped", "x"),
		}},
		{name: "overwrite through a symlink", entries: []archiveEntryFixture{
			{hdr: tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: filepath.Join(outside, "escaped")}},
			regularEntry("link", "x"),
		}},
		{name: "hard link", entries: []archiveEntryFixture{
			{hdr: tar.Header{Name: "link", Typeflag: tar.TypeLink, Linkname: filepath.Join(outside, "escaped")}},
		}},
		{name: "device", entries: []archiveEntryFixture{
			{hdr: tar.Header{Name: "dev", Typeflag: tar.TypeChar, Devmajor: 1, Devminor: 3}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := NewFileDriver(t.TempDir())
			ref := Ref{Name: "volume-aaaaaaaaaaaa", Path: t.TempDir()}
			writeTree(t, dst, ref, map[string]string{"app.db": "kept"})

			err := dst.Import(context.Background(), ref, "snap-00000001", "", bytes.NewReader(archiveOf(t, tc.entries...)))

			require.Error(t, err)
			require.NoFileExists(t, filepath.Join(outside, "escaped"))
			require.Equal(t, map[string]string{"app.db": "kept"}, readTree(t, dst, ref))
		})
	}
}

func TestFileDriverRefusesASparseFile(t *testing.T) {
	dst := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa", Path: t.TempDir()}
	writeTree(t, dst, ref, map[string]string{"app.db": "kept"})

	err := dst.Import(context.Background(), ref, "snap-00000001", "", bytes.NewReader(sparseArchive(t)))

	require.ErrorContains(t, err, "is sparse")
	require.Equal(t, map[string]string{"app.db": "kept"}, readTree(t, dst, ref))
}

// sparseArchive is an archive whose one file uses the GNU 1.0 sparse
// format: a block of map and one byte on the wire that read back as a
// terabyte. Go's writer cannot produce one, so it is built by hand.
func sparseArchive(t *testing.T) []byte {
	t.Helper()

	records := paxRecord("GNU.sparse.major", "1") +
		paxRecord("GNU.sparse.minor", "0") +
		paxRecord("GNU.sparse.name", "big") +
		paxRecord("GNU.sparse.realsize", "1099511627776")
	data := padBlock("1\n0\n1\n") + padBlock("x")

	archive := ustarHeader(t, "PaxHeaders/big", 'x', len(records)) + padBlock(records) +
		ustarHeader(t, "GNUSparseFile.0/big", '0', len(data)) + data +
		string(make([]byte, 1024))

	return []byte(archive)
}

func paxRecord(key, value string) string {
	body := " " + key + "=" + value + "\n"

	size := len(body) + 1
	for len(fmt.Sprint(size))+len(body) != size {
		size = len(fmt.Sprint(size)) + len(body)
	}

	return fmt.Sprint(size) + body
}

func padBlock(s string) string {
	if rem := len(s) % 512; rem != 0 {
		s += string(make([]byte, 512-rem))
	}

	return s
}

func ustarHeader(t *testing.T, name string, typeflag byte, size int) string {
	t.Helper()

	hdr := make([]byte, 512)
	copy(hdr[0:], name)
	copy(hdr[100:], "0000644\x00")
	copy(hdr[108:], "0000000\x00")
	copy(hdr[116:], "0000000\x00")
	copy(hdr[124:], fmt.Sprintf("%011o\x00", size))
	copy(hdr[136:], "00000000000\x00")
	copy(hdr[148:], "        ")
	hdr[156] = typeflag
	copy(hdr[257:], "ustar\x0000")

	sum := 0
	for _, b := range hdr {
		sum += int(b)
	}

	copy(hdr[148:], fmt.Sprintf("%06o\x00 ", sum))

	return string(hdr)
}

func TestFileDriverClearsAnInterruptedImport(t *testing.T) {
	src := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa"}
	writeTree(t, src, ref, map[string]string{"app.db": "rows"})

	dst := NewFileDriver(t.TempDir())
	dstRef := Ref{Name: ref.Name, Path: t.TempDir()}
	writeTree(t, dst, dstRef, map[string]string{fileStagingPrefix + "0011223344556677/partial": "half"})

	moveVolume(t, src, ref, dst, dstRef)

	require.Equal(t, map[string]string{"app.db": "rows"}, readTree(t, dst, dstRef))
}

func TestFileDriverLeavesAHalfLandedImportOutOfSnapshots(t *testing.T) {
	src := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa"}
	writeTree(t, src, ref, map[string]string{
		"app.db": "rows",
		fileStagingPrefix + "0011223344556677/partial": "half",
	})

	dst := NewFileDriver(t.TempDir())
	dstRef := Ref{Name: ref.Name, Path: t.TempDir()}
	moveVolume(t, src, ref, dst, dstRef)

	require.Equal(t, map[string]string{"app.db": "rows"}, readTree(t, dst, dstRef))
}

func TestFileDriverDigestsTheImportedStream(t *testing.T) {
	ctx := context.Background()

	dst := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-bbbbbbbbbbbb", Path: t.TempDir()}
	archive := archiveOf(t, regularEntry("app.db", "payload shipped from the source provider"))

	require.NoError(t, dst.Import(ctx, ref, "snap-00000004", "", bytes.NewReader(archive)))

	digest, err := dst.Digest(ctx, ref, "snap-00000004")
	require.NoError(t, err)

	sum := sha256.Sum256(archive)
	require.Equal(t, Digest{Size: uint64(len(archive)), SHA256: sum[:]}, digest)

	snaps, err := dst.Snapshots(ctx, ref)
	require.NoError(t, err)
	require.Empty(t, snaps, "an import keeps no second copy of the volume")
}

func TestFileDriverAppliesADiffOnlyOnTheImportedBase(t *testing.T) {
	ctx := context.Background()

	dst := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-bbbbbbbbbbbb", Path: t.TempDir()}

	require.NoError(t, dst.Import(ctx, ref, "snap-00000004", "", bytes.NewReader(archiveOf(t, regularEntry("app.db", "v4")))))

	err := dst.Import(ctx, ref, "snap-00000009", "snap-nonexistent", bytes.NewReader(archiveOf(t, regularEntry("app.db", "v9"))))
	require.ErrorIs(t, err, ErrUnknownSnapshot)

	require.NoError(t, dst.Import(ctx, ref, "snap-00000005", "snap-00000004", bytes.NewReader(archiveOf(t, regularEntry("app.db", "v5")))))
	require.Equal(t, map[string]string{"app.db": "v5"}, readTree(t, dst, ref))
}

func TestFileDriverExportUnknownSnapshots(t *testing.T) {
	ctx := context.Background()

	d := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-cccccccccccc"}

	writeTree(t, d, ref, map[string]string{"app.db": "data"})

	snap, err := d.Snapshot(ctx, ref)
	require.NoError(t, err)

	_, err = d.Export(ctx, ref, "snap-99999999", "")
	require.ErrorIs(t, err, ErrUnknownSnapshot)

	_, err = d.Export(ctx, ref, snap.ID, "snap-99999999")
	require.ErrorIs(t, err, ErrUnknownSnapshot)
}

func TestFileDriverHoldsNoSnapshotNamedOutsideItsStore(t *testing.T) {
	ctx := context.Background()

	d := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-cccccccccccc"}
	writeTree(t, d, ref, map[string]string{"app.db": "data"})
	require.NoError(t, os.WriteFile(filepath.Join(d.base, "elsewhere.tar"), []byte("not a snapshot"), 0o600))

	_, err := d.Export(ctx, ref, "../../elsewhere", "")
	require.ErrorIs(t, err, ErrUnknownSnapshot)

	_, err = d.Digest(ctx, ref, "../../elsewhere")
	require.ErrorIs(t, err, ErrUnknownSnapshot)
}

func TestFileDriverPruneKeepsNewest(t *testing.T) {
	ctx := context.Background()

	d := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-dddddddddddd"}

	writeTree(t, d, ref, map[string]string{"app.db": "data"})

	for i := 0; i < 4; i++ {
		_, err := d.Snapshot(ctx, ref)
		require.NoError(t, err)
	}

	require.NoError(t, d.Prune(ctx, ref, 2))

	snaps, err := d.Snapshots(ctx, ref)
	require.NoError(t, err)
	require.Len(t, snaps, 2)
	require.Regexp(t, `^snap-00000003-`, snaps[0].ID)
	require.Regexp(t, `^snap-00000004-`, snaps[1].ID)
}

func TestFileDriverNeverReusesASnapshotID(t *testing.T) {
	ctx := context.Background()

	d := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-dddddddddddd"}
	writeTree(t, d, ref, map[string]string{"app.db": "first"})

	first, err := d.Snapshot(ctx, ref)
	require.NoError(t, err)

	file, _ := d.snapPath(ref, first.ID)
	require.NoError(t, os.Remove(file))
	writeTree(t, d, ref, map[string]string{"app.db": "second"})

	second, err := d.Snapshot(ctx, ref)
	require.NoError(t, err)

	require.NotEqual(t, first.ID, second.ID)
}

func TestFileDriverOrdersSnapshotsOldestFirst(t *testing.T) {
	ctx := context.Background()

	d := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-dddddddddddd"}
	writeTree(t, d, ref, map[string]string{"app.db": "data"})

	var taken []string

	for i := 0; i < 12; i++ {
		snap, err := d.Snapshot(ctx, ref)
		require.NoError(t, err)

		taken = append(taken, snap.ID)
	}

	snaps, err := d.Snapshots(ctx, ref)
	require.NoError(t, err)

	listed := make([]string, 0, len(snaps))
	for _, snap := range snaps {
		listed = append(listed, snap.ID)
	}

	require.Equal(t, taken, listed)
}

func TestFileDriverRemovesTheStateOfVolumesThatAreGone(t *testing.T) {
	ctx := context.Background()

	d := NewFileDriver(t.TempDir())
	kept := Ref{Name: "volume-aaaaaaaaaaaa"}
	gone := Ref{Name: "volume-bbbbbbbbbbbb"}

	for _, ref := range []Ref{kept, gone} {
		writeTree(t, d, ref, map[string]string{"app.db": fmt.Sprint(ref.Name)})
		_, err := d.Snapshot(ctx, ref)
		require.NoError(t, err)
	}

	require.NoError(t, d.RemoveStale(map[string]bool{kept.Name: true}))

	keptSnaps, err := d.Snapshots(ctx, kept)
	require.NoError(t, err)
	require.Len(t, keptSnaps, 1)
	require.NoDirExists(t, filepath.Join(d.base, gone.Name))
}

func TestFileDriverRemovesNothingElseInItsBaseDirectory(t *testing.T) {
	d := NewFileDriver(t.TempDir())
	require.NoError(t, os.MkdirAll(filepath.Join(d.base, "postgres-backups"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(d.base, "volume-not-a-hash"), 0o750))

	require.NoError(t, d.RemoveStale(nil))

	require.DirExists(t, filepath.Join(d.base, "postgres-backups"))
	require.DirExists(t, filepath.Join(d.base, "volume-not-a-hash"))
}

func TestFileDriverRemoveStaleWithoutAnyState(t *testing.T) {
	d := NewFileDriver(filepath.Join(t.TempDir(), "never-created"))

	require.NoError(t, d.RemoveStale(nil))
}

// chunkCollector records the framed stream a server-side export produced.
type chunkCollector struct {
	chunks []chunkFrame
}

type chunkFrame struct {
	data   []byte
	sha256 []byte
	offset uint64
}

func (c *chunkCollector) send(data, sum []byte, offset uint64) {
	c.chunks = append(c.chunks, chunkFrame{data: data, sha256: sum, offset: offset})
}

func TestChunkFramingGolden(t *testing.T) {
	// 10 bytes at chunk size 4: chunks of 4, 4, 2 at offsets 0, 4, 8
	payload := []byte("0123456789")

	collected := &chunkCollector{}

	reached, err := SendChunks(chunkSenderFunc(func(data, sum []byte, offset uint64) error {
		collected.send(data, sum, offset)
		return nil
	}), bytes.NewReader(payload), 0, 4)
	require.NoError(t, err)
	require.EqualValues(t, 10, reached)

	require.Len(t, collected.chunks, 3)

	golden := []struct {
		data   string
		offset uint64
	}{
		{"0123", 0},
		{"4567", 4},
		{"89", 8},
	}

	for i, want := range golden {
		require.Equal(t, []byte(want.data), collected.chunks[i].data, "chunk %d data", i)
		require.Equal(t, want.offset, collected.chunks[i].offset, "chunk %d offset", i)

		sum := sha256.Sum256([]byte(want.data))
		require.Equal(t, sum[:], collected.chunks[i].sha256, "chunk %d sha256", i)
	}
}

func TestChunkFramingResume(t *testing.T) {
	payload := []byte("0123456789")

	collected := &chunkCollector{}

	// resume at offset 6: only bytes 6.. ship, offsets stay absolute
	reached, err := SendChunks(chunkSenderFunc(func(data, sum []byte, offset uint64) error {
		collected.send(data, sum, offset)
		return nil
	}), bytes.NewReader(payload), 6, 4)
	require.NoError(t, err)
	require.EqualValues(t, 10, reached)

	require.Len(t, collected.chunks, 1)
	require.Equal(t, []byte("6789"), collected.chunks[0].data)
	require.EqualValues(t, 6, collected.chunks[0].offset)
}

func TestChunkFramingResumeBeyondStream(t *testing.T) {
	_, err := SendChunks(chunkSenderFunc(func(_, _ []byte, _ uint64) error {
		t.Fatal("no chunk should ship")
		return nil
	}), bytes.NewReader([]byte("0123")), 10, 4)
	require.ErrorIs(t, err, ErrReplication)
}

func TestReceiveChunksVerifies(t *testing.T) {
	payload := []byte("abcdefghij")

	spool, err := os.CreateTemp(t.TempDir(), "spool-*")
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	src := framesFor(t, payload, 3)

	reached, err := ReceiveChunks(&queueReceiver{frames: src}, spool, 0)
	require.NoError(t, err)
	require.EqualValues(t, len(payload), reached)

	got, err := os.ReadFile(spool.Name())
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestReceiveChunksChecksumFailure(t *testing.T) {
	payload := []byte("abcdefghij")

	frames := framesFor(t, payload, 3)
	// corrupt the second chunk's bytes; its declared sha256 no longer matches
	frames[1].data[0] ^= 0xff

	spool, err := os.CreateTemp(t.TempDir(), "spool-*")
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	_, err = ReceiveChunks(&queueReceiver{frames: frames}, spool, 0)
	require.ErrorIs(t, err, ErrChecksumMismatch)
}

func TestReceiveChunksContinuityFailure(t *testing.T) {
	payload := []byte("abcdefghij")

	frames := framesFor(t, payload, 3)
	// drop a chunk: the offset gap must be refused, not silently zero-filled
	frames = append(frames[:1], frames[2:]...)

	spool, err := os.CreateTemp(t.TempDir(), "spool-*")
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	_, err = ReceiveChunks(&queueReceiver{frames: frames}, spool, 0)
	require.ErrorIs(t, err, ErrChecksumMismatch)
}

func framesFor(t *testing.T, payload []byte, chunkSize int) []chunkFrame {
	t.Helper()

	var frames []chunkFrame

	_, err := SendChunks(chunkSenderFunc(func(data, sum []byte, offset uint64) error {
		frames = append(frames, chunkFrame{
			data:   append([]byte(nil), data...),
			sha256: append([]byte(nil), sum...),
			offset: offset,
		})
		return nil
	}), bytes.NewReader(payload), 0, chunkSize)
	require.NoError(t, err)

	return frames
}

func TestSendChunksSenderError(t *testing.T) {
	sent := 0

	_, err := SendChunks(chunkSenderFunc(func(_, _ []byte, _ uint64) error {
		sent++
		if sent > 1 {
			return fmt.Errorf("stream broke")
		}
		return nil
	}), bytes.NewReader(bytes.Repeat([]byte("x"), 10)), 0, 4)
	require.Error(t, err)
	require.Equal(t, 2, sent)
}
