package grpc

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "pkg.akt.dev/go/inventory/v1"

	"github.com/akash-network/provider/verification/inventory"
)

type testInventorySnapshotter struct {
	snapshot *inventory.Snapshot
	err      error
	req      inventory.SnapshotRequest
	called   bool
}

func (s *testInventorySnapshotter) Build(_ context.Context, req inventory.SnapshotRequest) (*inventory.Snapshot, error) {
	s.called = true
	s.req = req

	return s.snapshot, s.err
}

func TestGetInventorySnapshotBuildsResponse(t *testing.T) {
	nonce := bytes.Repeat([]byte{1}, inventory.NonceSize)
	snapshotter := &testInventorySnapshotter{
		snapshot: &inventory.Snapshot{
			Payload:   []byte("payload"),
			Hash:      inventory.HashPayload([]byte("payload")),
			Signature: []byte("signature"),
			Provider:  "akash1provider",
		},
	}
	server := &grpcInventoryV1{snapshotter: snapshotter}

	resp, err := server.GetInventorySnapshot(context.Background(), &inventoryv1.GetInventorySnapshotRequest{
		Nonce: nonce,
	})
	require.NoError(t, err)

	require.True(t, snapshotter.called)
	require.Equal(t, nonce, snapshotter.req.Nonce)
	require.Equal(t, []byte("payload"), resp.SnapshotPayload)
	require.Equal(t, []byte("signature"), resp.Signature)
	require.Equal(t, "akash1provider", resp.Provider)
}

func TestGetInventorySnapshotRejectsEmptyRequest(t *testing.T) {
	server := &grpcInventoryV1{snapshotter: &testInventorySnapshotter{}}

	resp, err := server.GetInventorySnapshot(context.Background(), nil)
	require.Nil(t, resp)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetInventorySnapshotRejectsInvalidNonce(t *testing.T) {
	snapshotter := &testInventorySnapshotter{}
	server := &grpcInventoryV1{snapshotter: snapshotter}

	resp, err := server.GetInventorySnapshot(context.Background(), &inventoryv1.GetInventorySnapshotRequest{
		Nonce: bytes.Repeat([]byte{1}, inventory.NonceSize-1),
	})
	require.Nil(t, resp)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.False(t, snapshotter.called)
}

func TestGetInventorySnapshotAcceptsMissingNonce(t *testing.T) {
	snapshotter := &testInventorySnapshotter{
		snapshot: &inventory.Snapshot{
			Payload:   []byte("payload"),
			Hash:      inventory.HashPayload([]byte("payload")),
			Signature: []byte("signature"),
			Provider:  "akash1provider",
		},
	}
	server := &grpcInventoryV1{snapshotter: snapshotter}

	resp, err := server.GetInventorySnapshot(context.Background(), &inventoryv1.GetInventorySnapshotRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, snapshotter.called)
	require.Empty(t, snapshotter.req.Nonce)
}

func TestGetInventorySnapshotReturnsUnavailableWithoutSnapshotter(t *testing.T) {
	server := &grpcInventoryV1{}

	resp, err := server.GetInventorySnapshot(context.Background(), &inventoryv1.GetInventorySnapshotRequest{
		Nonce: bytes.Repeat([]byte{1}, inventory.NonceSize),
	})
	require.Nil(t, resp)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestGetInventorySnapshotReturnsSnapshotterError(t *testing.T) {
	expected := errors.New("snapshot failed")
	server := &grpcInventoryV1{
		snapshotter: &testInventorySnapshotter{err: expected},
	}

	resp, err := server.GetInventorySnapshot(context.Background(), &inventoryv1.GetInventorySnapshotRequest{
		Nonce: bytes.Repeat([]byte{1}, inventory.NonceSize),
	})
	require.Nil(t, resp)
	require.ErrorIs(t, err, expected)
}

func TestGetInventorySnapshotRejectsNilSnapshot(t *testing.T) {
	server := &grpcInventoryV1{
		snapshotter: &testInventorySnapshotter{},
	}

	resp, err := server.GetInventorySnapshot(context.Background(), &inventoryv1.GetInventorySnapshotRequest{
		Nonce: bytes.Repeat([]byte{1}, inventory.NonceSize),
	})
	require.Nil(t, resp)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestGetInventorySnapshotRejectsInvalidSnapshot(t *testing.T) {
	server := &grpcInventoryV1{
		snapshotter: &testInventorySnapshotter{snapshot: &inventory.Snapshot{
			Hash:      []byte("hash"),
			Signature: []byte("signature"),
			Provider:  "akash1provider",
		}},
	}

	resp, err := server.GetInventorySnapshot(context.Background(), &inventoryv1.GetInventorySnapshotRequest{
		Nonce: bytes.Repeat([]byte{1}, inventory.NonceSize),
	})
	require.Nil(t, resp)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, err.Error(), "missing inventory snapshot payload")
}

type testCommittedSnapshotStore struct {
	record          inventory.CommittedSnapshot
	err             error
	getHash         []byte
	getCalls        int
	latestCalls     int
	stageCalls      int
	markPostedCalls int
	pendingCalls    int
}

func (s *testCommittedSnapshotStore) Stage(context.Context, inventory.Snapshot) error {
	s.stageCalls++
	return nil
}

func (s *testCommittedSnapshotStore) MarkPosted(context.Context, []byte, time.Time) error {
	s.markPostedCalls++
	return nil
}

func (s *testCommittedSnapshotStore) Pending(context.Context) ([]inventory.CommittedSnapshot, error) {
	s.pendingCalls++
	return nil, nil
}

func (s *testCommittedSnapshotStore) Get(_ context.Context, hash []byte) (inventory.CommittedSnapshot, error) {
	s.getCalls++
	s.getHash = append([]byte(nil), hash...)
	return inventory.CloneCommittedSnapshot(s.record), s.err
}

func (s *testCommittedSnapshotStore) Latest(context.Context) (inventory.CommittedSnapshot, error) {
	s.latestCalls++
	return inventory.CloneCommittedSnapshot(s.record), s.err
}

func newPostedInventorySnapshot() inventory.CommittedSnapshot {
	payload := []byte("committed payload")

	return inventory.CommittedSnapshot{
		Snapshot: inventory.Snapshot{
			Payload:   payload,
			Hash:      inventory.HashPayload(payload),
			Signature: []byte("committed signature"),
			Provider:  "akash1provider",
		},
		State:    inventory.CommittedSnapshotStatePosted,
		PostedAt: time.Date(2026, time.July, 27, 19, 34, 56, 789, time.UTC),
	}
}

func TestGetInventorySnapshotNeverMutatesCommittedSnapshots(t *testing.T) {
	for _, nonce := range [][]byte{nil, bytes.Repeat([]byte{1}, inventory.NonceSize)} {
		store := &testCommittedSnapshotStore{}
		snapshotter := &testInventorySnapshotter{
			snapshot: &inventory.Snapshot{
				Payload:   []byte("payload"),
				Hash:      inventory.HashPayload([]byte("payload")),
				Signature: []byte("signature"),
				Provider:  "akash1provider",
			},
		}
		server := &grpcInventoryV1{
			snapshotter:        snapshotter,
			committedSnapshots: store,
		}

		resp, err := server.GetInventorySnapshot(
			context.Background(),
			&inventoryv1.GetInventorySnapshotRequest{Nonce: nonce},
		)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Zero(t, store.getCalls)
		require.Zero(t, store.latestCalls)
		require.Zero(t, store.stageCalls)
		require.Zero(t, store.markPostedCalls)
		require.Zero(t, store.pendingCalls)
	}
}

func TestGetCommittedInventorySnapshotByHash(t *testing.T) {
	record := newPostedInventorySnapshot()
	store := &testCommittedSnapshotStore{record: record}
	server := &grpcInventoryV1{committedSnapshots: store}

	resp, err := server.GetCommittedInventorySnapshot(
		context.Background(),
		&inventoryv1.GetCommittedInventorySnapshotRequest{SnapshotHash: record.Snapshot.Hash},
	)
	require.NoError(t, err)
	require.Equal(t, record.Snapshot.Payload, resp.SnapshotPayload)
	require.Equal(t, record.Snapshot.Signature, resp.Signature)
	require.Equal(t, record.Snapshot.Provider, resp.Provider)
	require.Equal(t, record.Snapshot.Hash, resp.SnapshotHash)
	require.Equal(t, record.PostedAt, resp.PostedAt)
	require.Equal(t, 1, store.getCalls)
	require.Equal(t, record.Snapshot.Hash, store.getHash)
	require.Zero(t, store.latestCalls)
}

func TestGetCommittedInventorySnapshotLatest(t *testing.T) {
	record := newPostedInventorySnapshot()
	store := &testCommittedSnapshotStore{record: record}
	server := &grpcInventoryV1{committedSnapshots: store}

	resp, err := server.GetCommittedInventorySnapshot(
		context.Background(),
		&inventoryv1.GetCommittedInventorySnapshotRequest{},
	)
	require.NoError(t, err)
	require.Equal(t, record.Snapshot.Hash, resp.SnapshotHash)
	require.Zero(t, store.getCalls)
	require.Equal(t, 1, store.latestCalls)
}

func TestGetCommittedInventorySnapshotRejectsInvalidRequest(t *testing.T) {
	store := &testCommittedSnapshotStore{}
	server := &grpcInventoryV1{committedSnapshots: store}

	resp, err := server.GetCommittedInventorySnapshot(context.Background(), nil)
	require.Nil(t, resp)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	resp, err = server.GetCommittedInventorySnapshot(
		context.Background(),
		&inventoryv1.GetCommittedInventorySnapshotRequest{SnapshotHash: []byte("short")},
	)
	require.Nil(t, resp)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Zero(t, store.getCalls)
	require.Zero(t, store.latestCalls)
}

func TestGetCommittedInventorySnapshotReturnsUnavailableWithoutStore(t *testing.T) {
	server := &grpcInventoryV1{}

	resp, err := server.GetCommittedInventorySnapshot(
		context.Background(),
		&inventoryv1.GetCommittedInventorySnapshotRequest{},
	)
	require.Nil(t, resp)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestGetCommittedInventorySnapshotReturnsNotFound(t *testing.T) {
	store := &testCommittedSnapshotStore{err: inventory.ErrCommittedSnapshotNotFound}
	server := &grpcInventoryV1{committedSnapshots: store}

	resp, err := server.GetCommittedInventorySnapshot(
		context.Background(),
		&inventoryv1.GetCommittedInventorySnapshotRequest{},
	)
	require.Nil(t, resp)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestGetCommittedInventorySnapshotRejectsPendingRecord(t *testing.T) {
	record := newPostedInventorySnapshot()
	record.State = inventory.CommittedSnapshotStatePending
	record.PostedAt = time.Time{}
	server := &grpcInventoryV1{
		committedSnapshots: &testCommittedSnapshotStore{record: record},
	}

	resp, err := server.GetCommittedInventorySnapshot(
		context.Background(),
		&inventoryv1.GetCommittedInventorySnapshotRequest{},
	)
	require.Nil(t, resp)
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestGetCommittedInventorySnapshotRejectsMismatchedHash(t *testing.T) {
	record := newPostedInventorySnapshot()
	server := &grpcInventoryV1{
		committedSnapshots: &testCommittedSnapshotStore{record: record},
	}

	resp, err := server.GetCommittedInventorySnapshot(
		context.Background(),
		&inventoryv1.GetCommittedInventorySnapshotRequest{
			SnapshotHash: inventory.HashPayload([]byte("different payload")),
		},
	)
	require.Nil(t, resp)
	require.Equal(t, codes.Internal, status.Code(err))
}
