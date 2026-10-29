package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	mv1 "pkg.akt.dev/go/node/market/v1"
	"pkg.akt.dev/go/testutil"
	vtv1 "pkg.akt.dev/go/volume/v1"
)

const (
	rbdTestPool  = "replicapool"
	rbdTestImage = "csi-vol-0001"
	rbdTestImg   = rbdTestPool + "/" + rbdTestImage

	// fakeRBDWrite is how much the fake rbd writes or reads at a time
	fakeRBDWrite = 64 << 10
)

var rbdTestRef = Ref{Name: "volume-eeeeeeeeeeee", Pool: rbdTestPool, Image: rbdTestImage}

// fakeContent is an image's bytes: size bytes of a pattern drawn from seed,
// generated as they stream, never held. An imported image is known by the
// digest of what was read.
type fakeContent struct {
	size   int64
	seed   int64
	digest []byte
}

func (c fakeContent) sum() []byte {
	if c.digest != nil {
		return c.digest
	}

	h := sha256.New()
	_, _ = io.Copy(h, newPattern(c.seed, c.size))

	return h.Sum(nil)
}

// pattern generates size bytes from seed, a block at a time.
type pattern struct {
	block []byte
	left  int64
	off   int
}

func newPattern(seed, size int64) *pattern {
	block := make([]byte, fakeRBDWrite)
	_, _ = rand.New(rand.NewSource(seed)).Read(block) // nolint: gosec

	return &pattern{block: block, left: size}
}

func (p *pattern) Read(b []byte) (int, error) {
	if p.left == 0 {
		return 0, io.EOF
	}

	n := copy(b, p.block[p.off:])
	if int64(n) > p.left {
		n = int(p.left)
	}

	p.off = (p.off + n) % len(p.block)
	p.left -= int64(n)

	return n, nil
}

type fakeImage struct {
	content fakeContent
	snaps   map[string]fakeContent
	// infoSize, when set, is the size rbd info reports in place of the
	// image's own
	infoSize int64
}

// fakeRBD is a Ceph cluster as the rbd commands in the tools pod see it.
type fakeRBD struct {
	mu     sync.Mutex
	images map[string]*fakeImage
	calls  [][]string

	// consumed reports how much of an export its reader took; ahead is
	// the most the export ever wrote past that
	consumed func() int64
	ahead    int64

	// generated reports how much of an import's input was produced;
	// importAhead is the most ever produced past what rbd read
	generated   func() int64
	importAhead int64

	// failExports fails the next exports after failAfter bytes
	failExports int
	failAfter   int64

	exports int
}

var _ PodExec = (*fakeRBD)(nil)

func newFakeRBD() *fakeRBD {
	return &fakeRBD{images: make(map[string]*fakeImage)}
}

func (f *fakeRBD) addImage(img string, content fakeContent, snaps ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	image := &fakeImage{content: content, snaps: make(map[string]fakeContent)}
	for _, snap := range snaps {
		image.snaps[snap] = content
	}

	f.images[img] = image
}

func (f *fakeRBD) image(img string) *fakeImage {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.images[img]
}

// snapshot reads what an image held at snap.
func (f *fakeRBD) snapshot(img, snap string) (*fakeImage, fakeContent, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	image := f.images[img]
	if image == nil {
		return nil, fakeContent{}, false
	}

	content, held := image.snaps[snap]

	return image, content, held
}

func (f *fakeRBD) commands() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([][]string(nil), f.calls...)
}

func notFoundErr(cmd PodCommand, what string) error {
	_, _ = fmt.Fprintf(cmd.Stderr, "rbd: error opening %s: (2) No such file or directory\n", what)
	return errors.New("command terminated with exit code 2")
}

func (f *fakeRBD) Exec(ctx context.Context, cmd PodCommand) error {
	f.mu.Lock()
	f.calls = append(f.calls, cmd.Command)
	f.mu.Unlock()

	args := cmd.Command

	switch {
	case len(args) == 6 && args[1] == "snap" && args[2] == "ls":
		f.mu.Lock()
		image := f.images[args[3]]

		var entries []rbdSnapEntry
		if image != nil {
			for name := range image.snaps {
				entries = append(entries, rbdSnapEntry{Name: name, Timestamp: time.Now().UTC().Format(time.ANSIC)})
			}
		}
		f.mu.Unlock()

		if image == nil {
			return notFoundErr(cmd, args[3])
		}

		return json.NewEncoder(cmd.Stdout).Encode(entries)
	case len(args) == 4 && args[1] == "snap" && args[2] == "create":
		img, snap, _ := strings.Cut(args[3], "@")

		f.mu.Lock()
		defer f.mu.Unlock()

		image := f.images[img]
		if image == nil {
			return notFoundErr(cmd, img)
		}

		image.snaps[snap] = image.content

		return nil
	case len(args) == 4 && args[1] == "snap" && args[2] == "rm":
		img, snap, _ := strings.Cut(args[3], "@")

		f.mu.Lock()
		defer f.mu.Unlock()

		delete(f.images[img].snaps, snap)

		return nil
	case len(args) == 5 && args[1] == "snap" && args[2] == "purge":
		f.mu.Lock()
		defer f.mu.Unlock()

		image := f.images[args[4]]
		if image == nil {
			return notFoundErr(cmd, args[4])
		}

		image.snaps = make(map[string]fakeContent)

		return nil
	case len(args) == 4 && args[1] == "rm":
		f.mu.Lock()
		defer f.mu.Unlock()

		image := f.images[args[3]]
		if image == nil {
			return notFoundErr(cmd, args[3])
		}

		if len(image.snaps) > 0 {
			_, _ = fmt.Fprintln(cmd.Stderr, "rbd: image has snapshots - these must be deleted with 'rbd snap purge' before the image can be removed.")
			return errors.New("command terminated with exit code 39")
		}

		delete(f.images, args[3])

		return nil
	case len(args) == 5 && args[1] == "export":
		img, snap, _ := strings.Cut(args[3], "@")

		return f.export(ctx, cmd, img, snap)
	case len(args) == 7 && args[1] == "export-diff":
		img, snap, _ := strings.Cut(args[5], "@")

		return f.export(ctx, cmd, img, snap)
	case len(args) == 7 && args[1] == "import":
		if f.image(args[6]) != nil {
			_, _ = fmt.Fprintln(cmd.Stderr, "rbd: image creation failed: (17) File exists")
			return errors.New("command terminated with exit code 17")
		}

		content, err := f.read(cmd.Stdin)
		if err != nil {
			return err
		}

		f.addImage(args[6], content)

		return nil
	case len(args) == 5 && args[1] == "import-diff":
		if _, err := f.read(cmd.Stdin); err != nil {
			return err
		}

		return nil
	case len(args) == 5 && args[1] == "info":
		img, snap, _ := strings.Cut(args[2], "@")

		image, content, held := f.snapshot(img, snap)
		if !held {
			return notFoundErr(cmd, args[2])
		}

		size := content.size

		f.mu.Lock()
		if image.infoSize != 0 {
			size = image.infoSize
		}
		f.mu.Unlock()

		return json.NewEncoder(cmd.Stdout).Encode(map[string]int64{"size": size})
	case len(args) == 5 && args[0] == "sh" && args[2] == rbdDigestScript:
		img, snap, _ := strings.Cut(args[4], "@")

		_, content, held := f.snapshot(img, snap)
		if !held {
			return notFoundErr(cmd, args[4])
		}

		_, err := fmt.Fprintf(cmd.Stdout, "%s  -\n", hex.EncodeToString(content.sum()))

		return err
	}

	return fmt.Errorf("fake rbd: unexpected command %q", args)
}

// export streams a snapshot's bytes, a write at a time, noting how far
// ahead of its reader it ever got.
func (f *fakeRBD) export(ctx context.Context, cmd PodCommand, img, snap string) error {
	_, content, held := f.snapshot(img, snap)
	if !held {
		return notFoundErr(cmd, img+"@"+snap)
	}

	f.mu.Lock()
	f.exports++
	fail := f.failExports > 0
	if fail {
		f.failExports--
	}
	failAfter := f.failAfter
	f.mu.Unlock()

	src := newPattern(content.seed, content.size)
	buf := make([]byte, fakeRBDWrite)

	var written int64

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if fail && written >= failAfter {
			_, _ = fmt.Fprintln(cmd.Stderr, "rbd: export error: (5) Input/output error")
			return errors.New("command terminated with exit code 5")
		}

		n, err := src.Read(buf)
		if err == io.EOF {
			return nil
		}

		if _, err := cmd.Stdout.Write(buf[:n]); err != nil {
			return err
		}

		written += int64(n)

		if f.consumed != nil {
			f.mu.Lock()
			f.ahead = max(f.ahead, written-f.consumed())
			f.mu.Unlock()
		}
	}
}

// read takes an import's input as rbd would, a read at a time.
func (f *fakeRBD) read(stdin io.Reader) (fakeContent, error) {
	h := sha256.New()
	buf := make([]byte, fakeRBDWrite)

	var read int64

	for {
		n, err := stdin.Read(buf)
		h.Write(buf[:n])
		read += int64(n)

		if f.generated != nil {
			f.mu.Lock()
			f.importAhead = max(f.importAhead, f.generated()-read)
			f.mu.Unlock()
		}

		if err == io.EOF {
			return fakeContent{size: read, digest: h.Sum(nil)}, nil
		}

		if err != nil {
			return fakeContent{}, err
		}
	}
}

func rbdTestDriver(f *fakeRBD) *RBDDriver {
	return NewRBDDriver(f, func(context.Context, string, string) (string, error) { return "rook-ceph-tools-0", nil }, "rook-ceph")
}

// countingSender takes an export's chunks as a destination would, hashing
// them.
type countingSender struct {
	h      hash.Hash
	sent   atomic.Int64
	chunks int
}

func (s *countingSender) Send(chunk *vtv1.ExportChunk) error {
	s.h.Write(chunk.Data)
	s.sent.Add(int64(len(chunk.Data)))
	s.chunks++

	return nil
}

const rbdTestSnap = "akash-xfer-20261029T120000Z"

func TestRBDExportStreamsAnImageOfAnySize(t *testing.T) {
	const size = 256 << 20

	f := newFakeRBD()
	content := fakeContent{size: size, seed: 1}
	f.addImage(rbdTestImg, content, rbdTestSnap)

	sender := &countingSender{h: sha256.New()}
	f.consumed = sender.sent.Load

	rc, err := rbdTestDriver(f).Export(context.Background(), rbdTestRef, rbdTestSnap, "")
	require.NoError(t, err)

	sent, err := SendChunks(sender, rc, 0, DefaultChunkSize)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	require.Equal(t, uint64(size), sent)
	require.Equal(t, size/DefaultChunkSize, sender.chunks)
	require.Equal(t, content.sum(), sender.h.Sum(nil))

	// rbd never got more than a chunk and a write ahead of the transfer
	require.LessOrEqual(t, f.ahead, int64(DefaultChunkSize+fakeRBDWrite))
}

// generator is an import's input, produced as it is read.
type generator struct {
	io.Reader
	produced atomic.Int64
}

func (g *generator) Read(b []byte) (int, error) {
	n, err := g.Reader.Read(b)
	g.produced.Add(int64(n))

	return n, err
}

func TestRBDImportStreamsAnImageOfAnySize(t *testing.T) {
	const size = 256 << 20

	f := newFakeRBD()
	f.addImage(rbdTestImg, fakeContent{size: 1 << 30, seed: 9})

	input := &generator{Reader: newPattern(2, size)}
	f.generated = input.produced.Load

	require.NoError(t, rbdTestDriver(f).Import(context.Background(), rbdTestRef, rbdTestSnap, "", input))

	image := f.image(rbdTestImg)
	require.Equal(t, fakeContent{size: size, seed: 2}.sum(), image.content.digest)
	require.Equal(t, int64(size), image.content.size)
	require.Contains(t, image.snaps, rbdTestSnap, "the diff base of the next round")

	// the input was read as rbd took it, never drained ahead of it
	require.LessOrEqual(t, f.importAhead, int64(fakeRBDWrite))
}

func TestRBDImportReplacesAnImageAndItsSnapshots(t *testing.T) {
	f := newFakeRBD()
	f.addImage(rbdTestImg, fakeContent{size: 4 << 20, seed: 3}, "akash-xfer-20261028T000000Z", "akash-xfer-20261028T001500Z")

	require.NoError(t, rbdTestDriver(f).Import(context.Background(), rbdTestRef, rbdTestSnap, "", newPattern(4, 2<<20)))

	require.Equal(t, [][]string{
		{"rbd", "snap", "purge", "--no-progress", rbdTestImg},
		{"rbd", "rm", "--no-progress", rbdTestImg},
		{"rbd", "import", "--no-progress", "--export-format", "1", "-", rbdTestImg},
		{"rbd", "snap", "create", rbdTestImg + "@" + rbdTestSnap},
	}, f.commands())

	image := f.image(rbdTestImg)
	require.Len(t, image.snaps, 1)
	require.Equal(t, fakeContent{size: 2 << 20, seed: 4}.sum(), image.content.digest)
}

func TestRBDImportCreatesAnImageThatIsGone(t *testing.T) {
	f := newFakeRBD()

	require.NoError(t, rbdTestDriver(f).Import(context.Background(), rbdTestRef, rbdTestSnap, "", newPattern(4, 1<<20)))
	require.NotNil(t, f.image(rbdTestImg))
}

func TestRBDImportOfADiffLeavesItsSnapshotToRBD(t *testing.T) {
	f := newFakeRBD()
	f.addImage(rbdTestImg, fakeContent{size: 4 << 20, seed: 3}, "akash-xfer-20261028T000000Z")

	require.NoError(t, rbdTestDriver(f).Import(context.Background(), rbdTestRef, rbdTestSnap, "akash-xfer-20261028T000000Z", newPattern(5, 1<<20)))

	// import-diff creates the snapshot the diff ends at: creating it again
	// would fail the round
	require.Equal(t, [][]string{{"rbd", "import-diff", "--no-progress", "-", rbdTestImg}}, f.commands())
}

func TestRBDExportRefusesASnapshotItDoesNotHold(t *testing.T) {
	f := newFakeRBD()
	f.addImage(rbdTestImg, fakeContent{size: 1 << 20, seed: 1}, rbdTestSnap)

	_, err := rbdTestDriver(f).Export(context.Background(), rbdTestRef, rbdTestSnap, "akash-xfer-20261028T000000Z")
	require.ErrorIs(t, err, ErrUnknownSnapshot)

	_, err = rbdTestDriver(f).Export(context.Background(), rbdTestRef, "akash-xfer-20261028T000000Z", "")
	require.ErrorIs(t, err, ErrUnknownSnapshot)

	for _, cmd := range f.commands() {
		require.Equal(t, "ls", cmd[2], "nothing but the snapshot list was read")
	}
}

// TestRBDNeverHandsAPeersSnapshotNameToRBD: snapshot names arrive from
// other providers. One the driver did not take is refused before anything
// runs in the tools pod, which holds the cluster's admin keys.
func TestRBDNeverHandsAPeersSnapshotNameToRBD(t *testing.T) {
	ctx := context.Background()

	f := newFakeRBD()
	f.addImage(rbdTestImg, fakeContent{size: 1 << 20, seed: 1}, rbdTestSnap)
	d := rbdTestDriver(f)

	for _, name := range []string{
		"akash-xfer-1; ceph auth ls",
		"$(ceph auth get client.admin)",
		"akash-xfer-20261029T120000Z -",
		"../other@snap",
		"--pool=other",
	} {
		_, err := d.Export(ctx, rbdTestRef, rbdTestSnap, name)
		require.ErrorIs(t, err, ErrUnknownSnapshot, name)

		_, err = d.Export(ctx, rbdTestRef, name, "")
		require.ErrorIs(t, err, ErrUnknownSnapshot, name)

		require.Error(t, d.Import(ctx, rbdTestRef, name, "", newPattern(1, 1)), name)
		require.Error(t, d.Import(ctx, rbdTestRef, rbdTestSnap, name, newPattern(1, 1)), name)

		_, err = d.Digest(ctx, rbdTestRef, name)
		require.ErrorIs(t, err, ErrUnknownSnapshot, name)
	}

	require.Empty(t, f.commands())

	digest, err := d.Digest(ctx, rbdTestRef, rbdTestSnap)
	require.NoError(t, err)
	require.Equal(t, fakeContent{size: 1 << 20, seed: 1}.sum(), digest.SHA256)
	require.Equal(t, uint64(1<<20), digest.Size)

	// the one shell runs a fixed script, and takes the image as an argument
	sh := f.commands()[1]
	require.Equal(t, []string{"sh", "-c", rbdDigestScript, "sh", rbdTestImg + "@" + rbdTestSnap}, sh)
}

func TestRBDExportFailureReachesTheReader(t *testing.T) {
	f := newFakeRBD()
	f.addImage(rbdTestImg, fakeContent{size: 8 << 20, seed: 1}, rbdTestSnap)
	f.failExports, f.failAfter = 1, 3<<20

	rc, err := rbdTestDriver(f).Export(context.Background(), rbdTestRef, rbdTestSnap, "")
	require.NoError(t, err)

	defer func() { _ = rc.Close() }()

	n, err := io.Copy(io.Discard, rc)
	require.ErrorIs(t, err, ErrReplication)
	require.ErrorContains(t, err, "Input/output error")
	require.GreaterOrEqual(t, n, int64(3<<20))
}

func TestRBDExportEndsWhenItsReaderCloses(t *testing.T) {
	f := newFakeRBD()
	f.addImage(rbdTestImg, fakeContent{size: 1 << 30, seed: 1}, rbdTestSnap)

	rc, err := rbdTestDriver(f).Export(context.Background(), rbdTestRef, rbdTestSnap, "")
	require.NoError(t, err)

	_, err = io.ReadFull(rc, make([]byte, 1<<20))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- rc.Close() }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("closing the export left rbd running")
	}
}

// rbdTransfer is a move between two RBD-backed providers: the source
// serves its image through the transfer server, the destination pulls it
// with a Syncer.
type rbdTransfer struct {
	src    *fakeRBD
	dst    *fakeRBD
	client *inprocClient
	syncer *Syncer
}

func newRBDTransfer(t *testing.T, content fakeContent) *rbdTransfer {
	t.Helper()

	w := &rbdTransfer{src: newFakeRBD(), dst: newFakeRBD()}
	w.src.addImage(rbdTestImg, content, rbdTestSnap)

	srv := NewServer(
		rbdTestDriver(w.src),
		&fakeChainState{leases: []VolumeLease{
			migrationLease(authzExporter, mv1.LeaseReclaiming),
			migrationLease(authzRequester, mv1.LeaseActive),
		}},
		&staticVolumeSource{info: VolumeInfo{Ref: rbdTestRef, DSeq: authzDSeq, Exportable: true}},
		authzExporter,
		testutil.Logger(t),
		WithPeerID(func(context.Context) (string, error) { return authzRequester, nil }),
	)
	w.client = &inprocClient{srv: srv}

	w.syncer = &Syncer{
		Client:    w.client,
		Driver:    rbdTestDriver(w.dst),
		Ref:       rbdTestRef,
		Owner:     authzOwner,
		Vid:       authzVID,
		DSeq:      authzDSeq,
		Requester: authzRequester,
		SpoolDir:  t.TempDir(),
	}

	return w
}

func TestRBDTransferResumesAStreamThatBroke(t *testing.T) {
	content := fakeContent{size: 24 << 20, seed: 7}

	w := newRBDTransfer(t, content)
	w.src.failExports, w.src.failAfter = 1, 10<<20

	applied, err := w.syncer.SyncOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, rbdTestSnap, applied)

	require.Equal(t, 2, w.src.exports, "the broken stream resumed from where it broke")
	require.Equal(t, content.sum(), w.dst.image(rbdTestImg).content.digest)
	require.Contains(t, w.dst.image(rbdTestImg).snaps, rbdTestSnap)
}

func TestRBDTransferRefusesAnImagePastTheVolume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	w := newRBDTransfer(t, fakeContent{size: 8 << 20, seed: 7})
	w.syncer.MaxBytes = 4 << 20

	_, err := w.syncer.SyncOnce(ctx)
	require.ErrorIs(t, err, ErrTooLarge)
	require.Nil(t, w.dst.image(rbdTestImg))

	// a source that understates its image is cut off as it streams
	w = newRBDTransfer(t, fakeContent{size: 8 << 20, seed: 7})
	w.src.mu.Lock()
	w.src.images[rbdTestImg].infoSize = 1 << 20
	w.src.mu.Unlock()
	w.syncer.MaxBytes = 4 << 20

	_, err = w.syncer.SyncOnce(ctx)
	require.ErrorIs(t, err, ErrTooLarge)
	require.Nil(t, w.dst.image(rbdTestImg))
}
