package replication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeImage(t *testing.T, d *FileDriver, ref Ref, content []byte) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(d.ImagePath(ref)), 0o750))
	require.NoError(t, os.WriteFile(d.ImagePath(ref), content, 0o600))
}

func TestFileDriverSnapshotExportRoundTrip(t *testing.T) {
	ctx := context.Background()

	src := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-aaaaaaaaaaaa"}

	content := []byte("the quick brown fox jumps over the lazy dog")
	writeImage(t, src, ref, content)

	snap, err := src.Snapshot(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, "snap-00000001", snap.ID)

	// the snapshot is immutable: later image writes don't leak into it
	writeImage(t, src, ref, []byte("mutated after snapshot"))

	rc, err := src.Export(ctx, ref, snap.ID, "")
	require.NoError(t, err)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, content, got)

	digest, err := src.Digest(ctx, ref, snap.ID)
	require.NoError(t, err)
	require.EqualValues(t, len(content), digest.Size)

	sum := sha256.Sum256(content)
	require.Equal(t, sum[:], digest.SHA256)
}

func TestFileDriverImportAppliesAndRecordsSnapshot(t *testing.T) {
	ctx := context.Background()

	dst := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-bbbbbbbbbbbb"}

	content := []byte("payload shipped from the source provider")

	require.NoError(t, dst.Import(ctx, ref, "snap-00000004", "", bytes.NewReader(content)))

	// the image converged
	got, err := os.ReadFile(dst.ImagePath(ref))
	require.NoError(t, err)
	require.Equal(t, content, got)

	// the imported state is held as the next diff base
	snaps, err := dst.Snapshots(ctx, ref)
	require.NoError(t, err)
	require.Len(t, snaps, 1)
	require.Equal(t, "snap-00000004", snaps[0].ID)

	// a diff on an unknown base is refused
	err = dst.Import(ctx, ref, "snap-00000009", "snap-nonexistent", bytes.NewReader(content))
	require.ErrorIs(t, err, ErrUnknownSnapshot)

	// a diff on the held base applies
	newer := []byte("newer payload after the source volume changed")
	require.NoError(t, dst.Import(ctx, ref, "snap-00000005", "snap-00000004", bytes.NewReader(newer)))

	got, err = os.ReadFile(dst.ImagePath(ref))
	require.NoError(t, err)
	require.Equal(t, newer, got)
}

func TestFileDriverExportUnknownSnapshots(t *testing.T) {
	ctx := context.Background()

	d := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-cccccccccccc"}

	writeImage(t, d, ref, []byte("data"))

	snap, err := d.Snapshot(ctx, ref)
	require.NoError(t, err)

	_, err = d.Export(ctx, ref, "snap-99999999", "")
	require.ErrorIs(t, err, ErrUnknownSnapshot)

	_, err = d.Export(ctx, ref, snap.ID, "snap-99999999")
	require.ErrorIs(t, err, ErrUnknownSnapshot)
}

func TestFileDriverPruneKeepsNewest(t *testing.T) {
	ctx := context.Background()

	d := NewFileDriver(t.TempDir())
	ref := Ref{Name: "volume-dddddddddddd"}

	writeImage(t, d, ref, []byte("data"))

	for i := 0; i < 4; i++ {
		_, err := d.Snapshot(ctx, ref)
		require.NoError(t, err)
	}

	require.NoError(t, d.Prune(ctx, ref, 2))

	snaps, err := d.Snapshots(ctx, ref)
	require.NoError(t, err)
	require.Len(t, snaps, 2)
	require.Equal(t, "snap-00000003", snaps[0].ID)
	require.Equal(t, "snap-00000004", snaps[1].ID)
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
