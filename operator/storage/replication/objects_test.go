package replication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mv1 "pkg.akt.dev/go/node/market/v1"
	"pkg.akt.dev/go/testutil"
	vtv1 "pkg.akt.dev/go/volume/v1"

	"github.com/akash-network/provider/bucket"
	"github.com/akash-network/provider/bucket/buckettest"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

func (c *inprocClient) ExportObjects(ctx context.Context, in *vtv1.ExportObjectsRequest, _ ...grpc.CallOption) (vtv1.VolumeTransfer_ExportObjectsClient, error) {
	pipe := &objectPipe{ctx: ctx, ch: make(chan *vtv1.ObjectChunk, 16)}

	go func() {
		err := c.srv.ExportObjects(in, &objectSendStream{pipe: pipe})

		pipe.mu.Lock()
		pipe.err = err
		pipe.mu.Unlock()

		close(pipe.ch)
	}()

	return &objectRecvStream{pipe: pipe}, nil
}

// objectPipe carries object chunks from the server half to the client half.
type objectPipe struct {
	ctx context.Context
	ch  chan *vtv1.ObjectChunk
	mu  sync.Mutex
	err error
}

type objectSendStream struct {
	grpc.ServerStream

	pipe *objectPipe
}

func (s *objectSendStream) Context() context.Context { return s.pipe.ctx }

func (s *objectSendStream) Send(chunk *vtv1.ObjectChunk) error {
	select {
	case s.pipe.ch <- chunk:
		return nil
	case <-s.pipe.ctx.Done():
		return s.pipe.ctx.Err()
	}
}

type objectRecvStream struct {
	grpc.ClientStream

	pipe *objectPipe
}

func (s *objectRecvStream) Context() context.Context { return s.pipe.ctx }

func (s *objectRecvStream) Recv() (*vtv1.ObjectChunk, error) {
	chunk, ok := <-s.pipe.ch
	if !ok {
		s.pipe.mu.Lock()
		defer s.pipe.mu.Unlock()

		if s.pipe.err != nil {
			return nil, s.pipe.err
		}

		return nil, io.EOF
	}

	return chunk, nil
}

const bucketVolumeName = "volume-bbbbbbbbbbbb"

// bucketWorld is a bucket moving from the exporter, whose bucket holds the
// objects, to the requester, whose bucket they land in.
type bucketWorld struct {
	client *inprocClient
	vols   *staticVolumeSource
	chain  *fakeChainState
	src    *buckettest.Fake
	dst    *buckettest.Fake
	ref    bucket.Ref
}

func bucketScaffold(t *testing.T) *bucketWorld {
	t.Helper()

	return bucketScaffoldNamed(t, bucketVolumeName)
}

// bucketScaffoldNamed is bucketScaffold for the bucket of the Volume CRD
// named name.
func bucketScaffoldNamed(t *testing.T, name string, opts ...ServerOpt) *bucketWorld {
	t.Helper()

	w := &bucketWorld{
		vols: &staticVolumeSource{info: VolumeInfo{Ref: Ref{Name: name}, DSeq: authzDSeq, Exportable: true, Bucket: true}},
		chain: &fakeChainState{leases: []VolumeLease{
			migrationLease(authzExporter, mv1.LeaseReclaiming),
			migrationLease(authzRequester, mv1.LeaseActive),
		}},
		src: buckettest.NewFake(),
		dst: buckettest.NewFake(),
		ref: bucket.RefFor(name, authzOwner, authzVID),
	}

	for _, fake := range []*buckettest.Fake{w.src, w.dst} {
		require.NoError(t, fake.Ensure(context.Background(), bucket.Spec{Ref: w.ref, SizeBytes: 1 << 20}))
	}

	srv := NewServer(
		NewFileDriver(t.TempDir()),
		w.chain,
		w.vols,
		authzExporter,
		testutil.Logger(t),
		append([]ServerOpt{
			WithPeerID(func(context.Context) (string, error) { return authzRequester, nil }),
			WithChunkSize(5), // tiny chunks exercise the framing
			WithBuckets(w.src),
		}, opts...)...,
	)
	w.client = &inprocClient{srv: srv}

	return w
}

// goFinal closes the exporter's lease and revokes the bucket's last key.
func (w *bucketWorld) goFinal() {
	w.chain.update(func(chain *fakeChainState) { chain.leases[0].State = mv1.LeaseClosed })
	w.vols.update(func(info *VolumeInfo) { info.Quiesced = true })
}

func (w *bucketWorld) syncer(t *testing.T, client vtv1.VolumeTransferClient) *BucketSyncer {
	t.Helper()

	return &BucketSyncer{
		Client: client,
		Open: func(ctx context.Context) (bucket.Objects, error) {
			return w.dst.Objects(ctx, w.ref)
		},
		Owner:     authzOwner,
		Vid:       authzVID,
		DSeq:      authzDSeq,
		Requester: authzRequester,
		SpoolDir:  t.TempDir(),
	}
}

func putTestObject(t *testing.T, fake *buckettest.Fake, ref bucket.Ref, info bucket.ObjectInfo, body []byte) {
	t.Helper()

	objects, err := fake.Objects(context.Background(), ref)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, objects.Close())
	}()

	info.Size = int64(len(body))
	sum := sha256.Sum256(body)
	require.NoError(t, objects.Put(context.Background(), info, bytes.NewReader(body), sum[:]))
}

// listTestObjects reads every object of a bucket as stored.
func listTestObjects(t *testing.T, fake *buckettest.Fake, ref bucket.Ref) map[string][]byte {
	t.Helper()

	objects, err := fake.Objects(context.Background(), ref)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, objects.Close())
	}()

	listed, _, err := objects.List(context.Background(), "", 0)
	require.NoError(t, err)

	out := make(map[string][]byte, len(listed))
	for _, object := range listed {
		_, body, ok := fake.Object(ref, object.Key)
		require.True(t, ok)
		out[object.Key] = append([]byte{}, body...)
	}

	return out
}

func TestBucketMoveCopiesEveryObjectWhole(t *testing.T) {
	w := bucketScaffold(t)

	want := map[string][]byte{
		"empty":            {},
		"short.txt":        []byte("abc"),
		"one-chunk.bin":    []byte("12345"),
		"dir/several.bin":  []byte("a dozen byte"),
		"dir/z/longer.txt": bytes.Repeat([]byte("move me "), 13),
	}
	for key, body := range want {
		putTestObject(t, w.src, w.ref, bucket.ObjectInfo{
			Key:             key,
			ContentType:     "text/plain",
			ContentEncoding: "gzip",
			CacheControl:    "no-cache",
			Expires:         "Thu, 01 Dec 2050 16:00:00 GMT",
			Metadata:        map[string]string{"origin": key},
		}, body)
	}
	w.goFinal()

	s := w.syncer(t, w.client)
	complete, err := s.SyncOnce(context.Background())
	require.NoError(t, err)
	require.True(t, complete)

	require.Equal(t, want, listTestObjects(t, w.dst, w.ref))
	for key := range want {
		srcInfo, _, _ := w.src.Object(w.ref, key)
		dstInfo, _, _ := w.dst.Object(w.ref, key)
		require.Equal(t, srcInfo, dstInfo, key)
	}

	require.Equal(t, len(want), s.Objects())
	require.Zero(t, w.src.Sessions(w.ref), "the exporter kept a session open")
	require.Zero(t, w.dst.Sessions(w.ref), "the importer kept a session open")

	spooled, err := os.ReadDir(s.SpoolDir)
	require.NoError(t, err)
	require.Empty(t, spooled, "objects stayed spooled")
}

func TestBucketMoveWaitsUntilTheBucketIsFinal(t *testing.T) {
	w := bucketScaffold(t)
	putTestObject(t, w.src, w.ref, bucket.ObjectInfo{Key: "data"}, []byte("written while keys still work"))
	putTestObject(t, w.dst, w.ref, bucket.ObjectInfo{Key: "here-before"}, []byte("kept until the copy starts"))

	s := w.syncer(t, w.client)

	for name, set := range map[string]func(){
		"lease still in its window": func() {
			w.chain.update(func(chain *fakeChainState) { chain.leases[0].State = mv1.LeaseReclaiming })
			w.vols.update(func(info *VolumeInfo) { info.Quiesced = true })
		},
		"a key still writes": func() {
			w.chain.update(func(chain *fakeChainState) { chain.leases[0].State = mv1.LeaseClosed })
			w.vols.update(func(info *VolumeInfo) { info.Quiesced = false })
		},
	} {
		set()

		complete, err := s.SyncOnce(context.Background())
		require.NoError(t, err, name)
		require.False(t, complete, name)
		require.Equal(t, map[string][]byte{"here-before": []byte("kept until the copy starts")}, listTestObjects(t, w.dst, w.ref), name)
	}

	w.goFinal()

	complete, err := s.SyncOnce(context.Background())
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, map[string][]byte{"data": []byte("written while keys still work")}, listTestObjects(t, w.dst, w.ref))
}

func TestBucketMoveReplacesWhatTheBucketHeldBefore(t *testing.T) {
	w := bucketScaffold(t)
	putTestObject(t, w.src, w.ref, bucket.ObjectInfo{Key: "kept.txt"}, []byte("the newer copy"))
	putTestObject(t, w.dst, w.ref, bucket.ObjectInfo{Key: "kept.txt"}, []byte("an older copy"))
	putTestObject(t, w.dst, w.ref, bucket.ObjectInfo{Key: "deleted-since.txt"}, []byte("gone at the source"))
	w.goFinal()

	complete, err := w.syncer(t, w.client).SyncOnce(context.Background())
	require.NoError(t, err)
	require.True(t, complete)

	require.Equal(t, map[string][]byte{"kept.txt": []byte("the newer copy")}, listTestObjects(t, w.dst, w.ref))
}

func TestBucketMoveFromAnEmptyBucketEmptiesThisOne(t *testing.T) {
	w := bucketScaffold(t)
	putTestObject(t, w.dst, w.ref, bucket.ObjectInfo{Key: "stale"}, []byte("from an earlier lease"))
	w.goFinal()

	complete, err := w.syncer(t, w.client).SyncOnce(context.Background())
	require.NoError(t, err)
	require.True(t, complete)

	require.Empty(t, listTestObjects(t, w.dst, w.ref))
}

// breakingClient cuts the first export after a number of chunks, as a
// dropped connection would.
type breakingClient struct {
	vtv1.VolumeTransferClient

	after int
	calls []string
}

func (c *breakingClient) ExportObjects(ctx context.Context, in *vtv1.ExportObjectsRequest, opts ...grpc.CallOption) (vtv1.VolumeTransfer_ExportObjectsClient, error) {
	c.calls = append(c.calls, in.StartAfter)

	stream, err := c.VolumeTransferClient.ExportObjects(ctx, in, opts...)
	if err != nil || len(c.calls) > 1 {
		return stream, err
	}

	return &breakingStream{VolumeTransfer_ExportObjectsClient: stream, left: c.after}, nil
}

type breakingStream struct {
	vtv1.VolumeTransfer_ExportObjectsClient

	left int
}

func (s *breakingStream) Recv() (*vtv1.ObjectChunk, error) {
	if s.left == 0 {
		return nil, status.Error(codes.Unavailable, "connection reset")
	}

	s.left--

	return s.VolumeTransfer_ExportObjectsClient.Recv()
}

func TestBucketMoveResumesAfterTheLastObjectThatLanded(t *testing.T) {
	w := bucketScaffold(t)
	want := map[string][]byte{
		"a": []byte("first"),
		"b": []byte("second object, two chunks"),
		"c": []byte("third"),
	}
	for key, body := range want {
		putTestObject(t, w.src, w.ref, bucket.ObjectInfo{Key: key}, body)
	}
	w.goFinal()

	// "a" lands in one chunk; the stream breaks inside "b"
	client := &breakingClient{VolumeTransferClient: w.client, after: 3}

	complete, err := w.syncer(t, client).SyncOnce(context.Background())
	require.NoError(t, err)
	require.True(t, complete)

	require.Equal(t, []string{"", "a"}, client.calls)
	require.Equal(t, want, listTestObjects(t, w.dst, w.ref))
}

// scriptedClient plays a fixed ExportObjects stream.
type scriptedClient struct {
	vtv1.VolumeTransferClient

	chunks []*vtv1.ObjectChunk
	end    error
}

func (c *scriptedClient) ExportObjects(context.Context, *vtv1.ExportObjectsRequest, ...grpc.CallOption) (vtv1.VolumeTransfer_ExportObjectsClient, error) {
	return &scriptedStream{chunks: append([]*vtv1.ObjectChunk(nil), c.chunks...), end: c.end}, nil
}

type scriptedStream struct {
	vtv1.VolumeTransfer_ExportObjectsClient

	chunks []*vtv1.ObjectChunk
	end    error
}

func (s *scriptedStream) Recv() (*vtv1.ObjectChunk, error) {
	if len(s.chunks) == 0 {
		if s.end != nil {
			return nil, s.end
		}

		return nil, io.EOF
	}

	chunk := s.chunks[0]
	s.chunks = s.chunks[1:]

	return chunk, nil
}

// objectChunks frames body as the exporter would, in pieces of size bytes.
func objectChunks(key string, body []byte, size int) []*vtv1.ObjectChunk {
	whole := sha256.Sum256(body)

	var chunks []*vtv1.ObjectChunk
	for offset := 0; offset == 0 || offset < len(body); offset += size {
		data := body[offset:min(offset+size, len(body))]
		sum := sha256.Sum256(data)

		chunk := &vtv1.ObjectChunk{Key: key, Offset: uint64(offset), Data: data, SHA256: sum[:]}
		if offset == 0 {
			chunk.Attributes = &vtv1.ObjectAttributes{Size_: uint64(len(body))}
		}
		if offset+size >= len(body) {
			chunk.ObjectSHA256 = whole[:]
		}

		chunks = append(chunks, chunk)
	}

	return chunks
}

func TestBucketMoveRefusesWhatDoesNotAddUp(t *testing.T) {
	good := objectChunks("good", []byte("lands before the fault"), 8)

	for name, tc := range map[string]struct {
		chunks   func() []*vtv1.ObjectChunk
		maxBytes uint64
		want     error
	}{
		"a chunk that does not match its digest": {
			chunks: func() []*vtv1.ObjectChunk {
				chunks := objectChunks("zz-bad", []byte("tampered with on the way"), 8)
				chunks[1].Data = []byte("TAMPERED")
				return chunks
			},
			want: ErrChecksumMismatch,
		},
		"an object that does not match its digest": {
			chunks: func() []*vtv1.ObjectChunk {
				chunks := objectChunks("zz-bad", []byte("the digest at the end lies"), 8)
				lie := sha256.Sum256([]byte("something else"))
				chunks[len(chunks)-1].ObjectSHA256 = lie[:]
				return chunks
			},
			want: ErrChecksumMismatch,
		},
		"a gap in an object": {
			chunks: func() []*vtv1.ObjectChunk {
				chunks := objectChunks("zz-bad", []byte("a chunk goes missing here"), 8)
				return append(chunks[:1:1], chunks[2:]...)
			},
			want: ErrChecksumMismatch,
		},
		"an object shorter than declared": {
			chunks: func() []*vtv1.ObjectChunk {
				chunks := objectChunks("zz-bad", []byte("ends early"), 64)
				chunks[0].Attributes.Size_ = 64
				return chunks
			},
			want: ErrChecksumMismatch,
		},
		"an object longer than declared": {
			chunks: func() []*vtv1.ObjectChunk {
				chunks := objectChunks("zz-bad", []byte("runs past its declared size"), 64)
				chunks[0].Attributes.Size_ = 4
				return chunks
			},
			want: ErrTooLarge,
		},
		"an object without its attributes": {
			chunks: func() []*vtv1.ObjectChunk {
				chunks := objectChunks("zz-bad", []byte("who knows how big"), 64)
				chunks[0].Attributes = nil
				return chunks
			},
			want: ErrChecksumMismatch,
		},
		"keys out of order": {
			chunks: func() []*vtv1.ObjectChunk {
				return objectChunks("a-before-good", []byte("sorts before the last"), 64)
			},
			want: ErrChecksumMismatch,
		},
		"another object before one ends": {
			chunks: func() []*vtv1.ObjectChunk {
				first := objectChunks("h", []byte("never finishes, over two chunks"), 8)
				return append(first[:1:1], objectChunks("i", []byte("x"), 8)...)
			},
			want: ErrChecksumMismatch,
		},
		"more than the bucket allows": {
			chunks: func() []*vtv1.ObjectChunk {
				return objectChunks("huge", bytes.Repeat([]byte("x"), 64), 64)
			},
			maxBytes: 64,
			want:     ErrTooLarge,
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := bucketScaffold(t)

			s := w.syncer(t, &scriptedClient{chunks: append(append([]*vtv1.ObjectChunk(nil), good...), tc.chunks()...)})
			s.MaxBytes = tc.maxBytes

			complete, err := s.SyncOnce(context.Background())
			require.ErrorIs(t, err, tc.want)
			require.False(t, complete)

			landed := listTestObjects(t, w.dst, w.ref)
			require.Equal(t, map[string][]byte{"good": []byte("lands before the fault")}, landed)

			spooled, err := os.ReadDir(s.SpoolDir)
			require.NoError(t, err)
			require.Empty(t, spooled, "a refused object stayed spooled")
		})
	}
}

func TestBucketMoveKeepsNothingOfAnObjectCutShort(t *testing.T) {
	w := bucketScaffold(t)

	chunks := objectChunks("cut", []byte("the connection drops in here"), 8)
	s := w.syncer(t, &scriptedClient{chunks: chunks[:2], end: status.Error(codes.Unavailable, "connection reset")})

	complete, err := s.SyncOnce(context.Background())
	require.Error(t, err)
	require.False(t, complete)
	require.Empty(t, listTestObjects(t, w.dst, w.ref))
}

func TestBucketMoveTriesTheProviderBeforeAnIncompleteOne(t *testing.T) {
	w := bucketScaffold(t)
	w.vols.update(func(info *VolumeInfo) { info.Incomplete = true })
	w.goFinal()

	_, err := w.syncer(t, w.client).SyncOnce(context.Background())
	require.True(t, IsImportIncomplete(err), "%v", err)
}

func TestExportObjectsServesOnlyBuckets(t *testing.T) {
	w := bucketScaffold(t)
	w.vols.update(func(info *VolumeInfo) { info.Bucket = false })
	w.goFinal()

	_, err := w.syncer(t, w.client).SyncOnce(context.Background())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.False(t, IsNotFinal(err))
}

func TestExportObjectsServesOnlyBucketsMovingAway(t *testing.T) {
	w := bucketScaffold(t)
	w.vols.update(func(info *VolumeInfo) { info.Exportable = false })
	w.goFinal()

	_, err := w.syncer(t, w.client).SyncOnce(context.Background())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestExportObjectsRefusesAnotherProvider(t *testing.T) {
	w := bucketScaffold(t)
	w.goFinal()

	s := w.syncer(t, w.client)
	s.Requester = authzStranger

	_, err := s.SyncOnce(context.Background())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestExportOfABucketImageIsRefused(t *testing.T) {
	w := bucketScaffold(t)
	w.goFinal()

	_, err := w.client.Status(context.Background(), statusReq())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	stream, err := w.client.Export(context.Background(), exportReq())
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestSendObjectsRefusesAnObjectShorterThanServed(t *testing.T) {
	objects := &shortObjects{}

	_, _, err := SendObjects(context.Background(), &discardSender{}, objects, "", 4)
	require.ErrorIs(t, err, ErrReplication)
}

// shortObjects serves one object whose body ends before its length.
type shortObjects struct {
	bucket.Objects
}

func (shortObjects) List(context.Context, string, int) ([]bucket.ObjectInfo, bool, error) {
	return []bucket.ObjectInfo{{Key: "short", Size: 10}}, false, nil
}

func (shortObjects) Get(context.Context, string) (bucket.ObjectInfo, io.ReadCloser, error) {
	return bucket.ObjectInfo{Key: "short", Size: 10}, io.NopCloser(bytes.NewReader([]byte("abc"))), nil
}

type discardSender struct{}

func (discardSender) Send(*vtv1.ObjectChunk) error { return nil }

func TestMigrationsCopyABucketInAndParkIt(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)
	dst := buckettest.NewFake()
	w.m.cfg.Buckets = dst

	vol := w.volume(t)
	vol.Spec.Access = "bucket"
	vol.Status.PVName = ""
	_, err := w.vols.AkashV2beta2().Volumes(migrationsNS).Update(context.Background(), vol, metav1.UpdateOptions{})
	require.NoError(t, err)

	ref := bucket.RefFor(vol.Name, authzOwner, authzVID)
	require.NoError(t, dst.Ensure(context.Background(), bucket.Spec{Ref: ref, SizeBytes: 1 << 20}))

	source := bucketScaffoldNamed(t, vol.Name)
	putTestObject(t, source.src, source.ref, bucket.ObjectInfo{Key: "photo.jpg", ContentType: "image/jpeg"}, []byte("pixels"))
	source.goFinal()
	w.source.client = source.client

	require.NoError(t, w.m.reconcile(w.ctx))

	require.Eventually(t, func() bool { return w.volume(t).Status.Phase == crd.VolumePhaseProvisioned }, 5*time.Second, time.Millisecond)
	require.Empty(t, w.volume(t).Spec.ImportFrom)
	require.Equal(t, map[string][]byte{"photo.jpg": []byte("pixels")}, listTestObjects(t, dst, ref))
	require.Equal(t, []string{authzExporter}, w.dials())
	require.Empty(t, w.evictions())
}

func TestMigrationsGiveBackABucketThisProviderCannotServe(t *testing.T) {
	w := newMigrationsWorld(t, crd.VolumePhaseImporting)
	w.m.cfg.Buckets = nil

	vol := w.volume(t)
	vol.Spec.Access = "bucket"
	_, err := w.vols.AkashV2beta2().Volumes(migrationsNS).Update(context.Background(), vol, metav1.UpdateOptions{})
	require.NoError(t, err)

	err = w.m.migrate(w.ctx, w.volume(t))
	require.True(t, errors.Is(err, ErrReplication), "%v", err)
	require.Empty(t, w.dials())
}
