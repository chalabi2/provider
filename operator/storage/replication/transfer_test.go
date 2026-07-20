package replication

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

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

// staticVolumeSource serves one volume.
type staticVolumeSource struct {
	info VolumeInfo
}

func (s *staticVolumeSource) Resolve(_ context.Context, _, _ string) (VolumeInfo, error) {
	return s.info, nil
}

func transferScaffold(t *testing.T) (*inprocClient, *FileDriver, Ref) {
	t.Helper()

	srcDriver := NewFileDriver(t.TempDir())
	srcRef := Ref{Name: "volume-eeeeeeeeeeee"}

	cs := &fakeChainState{leases: []VolumeLease{
		migrationLease(authzExporter, mv1.LeaseReclaiming),
		migrationLease(authzRequester, mv1.LeaseActive),
	}}

	srv := NewServer(
		srcDriver,
		cs,
		&staticVolumeSource{info: VolumeInfo{Ref: srcRef, DSeq: authzDSeq, Exportable: true}},
		authzExporter,
		testutil.Logger(t),
		WithPeerID(func(context.Context) (string, error) { return authzRequester, nil }),
		WithChunkSize(7), // tiny chunks exercise the framing
	)

	return &inprocClient{srv: srv}, srcDriver, srcRef
}

func TestTransferEndToEndFullThenDiff(t *testing.T) {
	ctx := context.Background()

	client, srcDriver, srcRef := transferScaffold(t)

	original := []byte("volume state before the migration window opened")
	writeImage(t, srcDriver, srcRef, original)

	dstDriver := NewFileDriver(t.TempDir())
	dstRef := Ref{Name: "volume-eeeeeeeeeeee"}

	syncer := &Syncer{
		Client:    client,
		Driver:    dstDriver,
		Ref:       dstRef,
		Owner:     authzOwner,
		Vid:       authzVID,
		DSeq:      authzDSeq,
		Requester: authzRequester,
		SpoolDir:  t.TempDir(),
	}

	// round 1: full export (the server takes the first snapshot itself)
	applied, err := syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, applied)

	got, err := io.ReadAll(mustExport(t, dstDriver, dstRef, applied))
	require.NoError(t, err)
	require.Equal(t, original, got)

	// in sync: nothing to pull
	applied2, err := syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, applied2)

	// the source volume changes and a new snapshot lands
	changed := []byte("volume state after more writes on the source side")
	writeImage(t, srcDriver, srcRef, changed)

	_, err = srcDriver.Snapshot(ctx, srcRef)
	require.NoError(t, err)

	// round 2: diff from the held base
	applied3, err := syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, applied3)
	require.NotEqual(t, applied, applied3)

	got, err = io.ReadAll(mustExport(t, dstDriver, dstRef, applied3))
	require.NoError(t, err)
	require.Equal(t, changed, got)
}

func TestTransferDeniedForStranger(t *testing.T) {
	ctx := context.Background()

	client, srcDriver, srcRef := transferScaffold(t)
	writeImage(t, srcDriver, srcRef, []byte("data"))

	// the peer identity does not match the request's requester
	req := &vtv1.StatusRequest{
		Owner:     authzOwner,
		Vid:       authzVID,
		DSeq:      authzDSeq,
		Requester: authzStranger,
	}

	_, err := client.Status(ctx, req)
	require.Error(t, err)
}

func TestTransferStatusReportsDigest(t *testing.T) {
	ctx := context.Background()

	client, srcDriver, srcRef := transferScaffold(t)

	content := []byte("digest me")
	writeImage(t, srcDriver, srcRef, content)

	st, err := client.Status(ctx, &vtv1.StatusRequest{
		Owner:     authzOwner,
		Vid:       authzVID,
		DSeq:      authzDSeq,
		Requester: authzRequester,
	})
	require.NoError(t, err)

	require.NotEmpty(t, st.LatestSnapshot)
	require.EqualValues(t, len(content), st.Size_)
	require.Len(t, st.SHA256, 32)
	require.Contains(t, st.Snapshots, st.LatestSnapshot)
}

func mustExport(t *testing.T, d Driver, ref Ref, snapshot string) io.Reader {
	t.Helper()

	rc, err := d.Export(context.Background(), ref, snapshot, "")
	require.NoError(t, err)

	t.Cleanup(func() { _ = rc.Close() })

	return rc
}
