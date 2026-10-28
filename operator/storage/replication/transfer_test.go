package replication

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mv1 "pkg.akt.dev/go/node/market/v1"
	"pkg.akt.dev/go/testutil"
	vtv1 "pkg.akt.dev/go/volume/v1"
)

// inprocClient drives a Server through in-memory streams, standing in for
// the gateway's mTLS gRPC plumbing.
type inprocClient struct {
	srv *Server
}

var _ vtv1.VolumeTransferClient = (*inprocClient)(nil)

func (c *inprocClient) Export(ctx context.Context, in *vtv1.ExportRequest, _ ...grpc.CallOption) (vtv1.VolumeTransfer_ExportClient, error) {
	pipe := &inprocPipe{
		ctx: ctx,
		ch:  make(chan *vtv1.ExportChunk, 16),
	}

	go func() {
		err := c.srv.Export(in, &inprocSendStream{pipe: pipe})

		pipe.mu.Lock()
		pipe.err = err
		pipe.mu.Unlock()

		close(pipe.ch)
	}()

	return &inprocRecvStream{pipe: pipe}, nil
}

func (c *inprocClient) Status(ctx context.Context, in *vtv1.StatusRequest, _ ...grpc.CallOption) (*vtv1.StatusResponse, error) {
	return c.srv.Status(ctx, in)
}

// inprocPipe carries chunks from the server half to the client half.
type inprocPipe struct {
	ctx context.Context
	ch  chan *vtv1.ExportChunk
	mu  sync.Mutex
	err error
}

// inprocSendStream is the server's send half.
type inprocSendStream struct {
	grpc.ServerStream

	pipe *inprocPipe
}

func (s *inprocSendStream) Context() context.Context { return s.pipe.ctx }

func (s *inprocSendStream) Send(chunk *vtv1.ExportChunk) error {
	select {
	case s.pipe.ch <- chunk:
		return nil
	case <-s.pipe.ctx.Done():
		return s.pipe.ctx.Err()
	}
}

// inprocRecvStream is the client's recv half.
type inprocRecvStream struct {
	grpc.ClientStream

	pipe *inprocPipe
}

func (s *inprocRecvStream) Context() context.Context { return s.pipe.ctx }

func (s *inprocRecvStream) Recv() (*vtv1.ExportChunk, error) {
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

// staticVolumeSource serves one volume, as the test currently has it.
type staticVolumeSource struct {
	mu   sync.Mutex
	info VolumeInfo
}

func (s *staticVolumeSource) Resolve(_ context.Context, _, _ string) (VolumeInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.info, nil
}

func (s *staticVolumeSource) update(fn func(*VolumeInfo)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fn(&s.info)
}

type transferWorld struct {
	client *inprocClient
	src    *FileDriver
	ref    Ref
	vols   *staticVolumeSource
	chain  *fakeChainState
}

func transferScaffold(t *testing.T) *transferWorld {
	t.Helper()

	w := &transferWorld{
		src: NewFileDriver(t.TempDir()),
		ref: Ref{Name: "volume-eeeeeeeeeeee"},
		chain: &fakeChainState{leases: []VolumeLease{
			migrationLease(authzExporter, mv1.LeaseReclaiming),
			migrationLease(authzRequester, mv1.LeaseActive),
		}},
	}
	w.vols = &staticVolumeSource{info: VolumeInfo{Ref: w.ref, DSeq: authzDSeq, Exportable: true}}

	srv := NewServer(
		w.src,
		w.chain,
		w.vols,
		authzExporter,
		testutil.Logger(t),
		WithPeerID(func(context.Context) (string, error) { return authzRequester, nil }),
		WithChunkSize(7), // tiny chunks exercise the framing
	)
	w.client = &inprocClient{srv: srv}

	return w
}

// goFinal closes the exporter's lease and stops the workload using the
// volume.
func (w *transferWorld) goFinal() {
	w.chain.update(func(chain *fakeChainState) { chain.leases[0].State = mv1.LeaseClosed })
	w.vols.update(func(info *VolumeInfo) { info.Quiesced = true })
}

func (w *transferWorld) syncer(t *testing.T, dst *FileDriver, dstRef Ref) *Syncer {
	return &Syncer{
		Client:    w.client,
		Driver:    dst,
		Ref:       dstRef,
		Owner:     authzOwner,
		Vid:       authzVID,
		DSeq:      authzDSeq,
		Requester: authzRequester,
		SpoolDir:  t.TempDir(),
	}
}

func statusReq() *vtv1.StatusRequest {
	return &vtv1.StatusRequest{
		Owner:     authzOwner,
		Vid:       authzVID,
		DSeq:      authzDSeq,
		Requester: authzRequester,
	}
}

func TestTransferEndToEndFullThenDiff(t *testing.T) {
	ctx := context.Background()

	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "volume state before the migration window opened"})

	dst := NewFileDriver(t.TempDir())
	dstRef := Ref{Name: "volume-eeeeeeeeeeee", Path: t.TempDir()}
	syncer := w.syncer(t, dst, dstRef)

	// round 1: full export (the server takes the first snapshot itself)
	applied, err := syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, applied)
	require.Equal(t, map[string]string{"app.db": "volume state before the migration window opened"}, readTree(t, dst, dstRef))

	// in sync: nothing to pull
	applied2, err := syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, applied2)

	// the source volume changes and a new snapshot lands
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "volume state after more writes on the source side"})

	_, err = w.src.Snapshot(ctx, w.ref)
	require.NoError(t, err)

	// round 2: diff from the held base
	applied3, err := syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, applied3)
	require.NotEqual(t, applied, applied3)
	require.Equal(t, map[string]string{"app.db": "volume state after more writes on the source side"}, readTree(t, dst, dstRef))
}

func TestTransferDeniedForStranger(t *testing.T) {
	ctx := context.Background()

	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "data"})

	// the peer identity does not match the request's requester
	req := statusReq()
	req.Requester = authzStranger

	_, err := w.client.Status(ctx, req)
	require.Error(t, err)
}

func TestTransferStatusReportsDigest(t *testing.T) {
	ctx := context.Background()

	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "digest me"})

	st, err := w.client.Status(ctx, statusReq())
	require.NoError(t, err)

	digest, err := w.src.Digest(ctx, w.ref, st.LatestSnapshot)
	require.NoError(t, err)

	require.Equal(t, digest.Size, st.Size_)
	require.Equal(t, digest.SHA256, st.SHA256)
	require.Contains(t, st.Snapshots, st.LatestSnapshot)
	require.False(t, st.Final)
}

func TestTransferIsNotFinalWhileTheSourceLeaseRuns(t *testing.T) {
	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "rows"})
	w.vols.update(func(info *VolumeInfo) { info.Quiesced = true })

	st, err := w.client.Status(context.Background(), statusReq())
	require.NoError(t, err)

	require.False(t, st.Final)
}

func TestTransferIsNotFinalWhileAWorkloadStillHasTheVolume(t *testing.T) {
	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "rows"})
	w.chain.update(func(chain *fakeChainState) { chain.leases[0].State = mv1.LeaseClosed })

	st, err := w.client.Status(context.Background(), statusReq())
	require.NoError(t, err)

	require.False(t, st.Final)
}

func TestTransferFinalSnapshotHoldsTheLastWrite(t *testing.T) {
	ctx := context.Background()

	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "first"})

	live, err := w.client.Status(ctx, statusReq())
	require.NoError(t, err)

	writeTree(t, w.src, w.ref, map[string]string{"app.db": "last write"})
	w.goFinal()

	final, err := w.client.Status(ctx, statusReq())
	require.NoError(t, err)
	require.True(t, final.Final)
	require.NotEqual(t, live.LatestSnapshot, final.LatestSnapshot)

	again, err := w.client.Status(ctx, statusReq())
	require.NoError(t, err)
	require.Equal(t, final.LatestSnapshot, again.LatestSnapshot, "the final snapshot is taken once")

	dst := NewFileDriver(t.TempDir())
	dstRef := Ref{Name: w.ref.Name, Path: t.TempDir()}
	syncer := w.syncer(t, dst, dstRef)

	_, err = syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.True(t, syncer.Final())
	require.Equal(t, map[string]string{"app.db": "last write"}, readTree(t, dst, dstRef))
}

func TestTransferTakesANewFinalSnapshotForEachClose(t *testing.T) {
	ctx := context.Background()

	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "before the first move"})
	w.goFinal()

	first, err := w.client.Status(ctx, statusReq())
	require.NoError(t, err)

	writeTree(t, w.src, w.ref, map[string]string{"app.db": "after the volume came back"})
	w.chain.update(func(chain *fakeChainState) {
		back := migrationLease(authzExporter, mv1.LeaseClosed)
		back.OSeq = 3
		chain.leases = append(chain.leases, back)
	})

	second, err := w.client.Status(ctx, statusReq())
	require.NoError(t, err)
	require.True(t, second.Final)
	require.NotEqual(t, first.LatestSnapshot, second.LatestSnapshot)
}

func TestTransferTakesAnotherFinalSnapshotWhenItsOwnIsGone(t *testing.T) {
	ctx := context.Background()

	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "rows"})
	w.goFinal()

	first, err := w.client.Status(ctx, statusReq())
	require.NoError(t, err)

	file, _ := w.src.snapPath(w.ref, first.LatestSnapshot)
	require.NoError(t, os.Remove(file))

	second, err := w.client.Status(ctx, statusReq())
	require.NoError(t, err)
	require.True(t, second.Final)
	require.NotEqual(t, first.LatestSnapshot, second.LatestSnapshot)

	again, err := w.client.Status(ctx, statusReq())
	require.NoError(t, err)
	require.Equal(t, second.LatestSnapshot, again.LatestSnapshot)
}

func TestSyncerIsFinalOnlyOnceItHoldsTheFinalSnapshot(t *testing.T) {
	ctx := context.Background()

	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "rows"})

	dst := NewFileDriver(t.TempDir())
	syncer := w.syncer(t, dst, Ref{Name: w.ref.Name, Path: t.TempDir()})

	_, err := syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.False(t, syncer.Final())

	_, err = syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.False(t, syncer.Final())

	w.goFinal()

	applied, err := syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, applied)
	require.True(t, syncer.Final())

	_, err = syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.True(t, syncer.Final())
}

func TestSyncerRefusesAnImageLargerThanTheVolume(t *testing.T) {
	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "more than the destination leased"})

	dst := NewFileDriver(t.TempDir())
	dstRef := Ref{Name: w.ref.Name, Path: t.TempDir()}
	syncer := w.syncer(t, dst, dstRef)
	syncer.MaxBytes = 16

	_, err := syncer.SyncOnce(context.Background())
	require.ErrorIs(t, err, ErrTooLarge)
	require.Empty(t, readTree(t, dst, dstRef))
}

func TestPullStopsAStreamPastTheVolume(t *testing.T) {
	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "a stream that keeps going"})

	spool, err := os.CreateTemp(t.TempDir(), "spool-*")
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	req := exportReq()
	_, err = Pull(context.Background(), w.client, req, &boundedWriterAt{w: spool, limit: 16}, 3)
	require.ErrorIs(t, err, ErrTooLarge)
}

func TestTransferRefusesAVolumeThatNeverFinishedArriving(t *testing.T) {
	w := transferScaffold(t)
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "half a copy"})
	w.vols.update(func(info *VolumeInfo) { info.Incomplete = true })

	_, err := w.client.Status(context.Background(), statusReq())
	require.True(t, IsImportIncomplete(err))

	err = w.client.srv.Export(exportReq(), &inprocSendStream{pipe: &inprocPipe{ctx: context.Background(), ch: make(chan *vtv1.ExportChunk, 1)}})
	require.True(t, IsImportIncomplete(err))
}

func TestTransferRefusesAVolumeItCannotExport(t *testing.T) {
	w := transferScaffold(t)
	w.vols.update(func(info *VolumeInfo) { info.Exportable = false })

	_, err := w.client.Status(context.Background(), statusReq())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.False(t, IsImportIncomplete(err))
}

func TestIsImportIncompleteIgnoresOtherErrors(t *testing.T) {
	require.False(t, IsImportIncomplete(errors.New("plain")))
	require.False(t, IsImportIncomplete(status.Error(codes.FailedPrecondition, "not exportable")))
	require.False(t, IsImportIncomplete(nil))
}
