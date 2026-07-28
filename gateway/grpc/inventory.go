package grpc

import (
	"bytes"
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "pkg.akt.dev/go/inventory/v1"

	"github.com/akash-network/provider/verification/inventory"
)

type InventorySnapshotter interface {
	Build(context.Context, inventory.SnapshotRequest) (*inventory.Snapshot, error)
}

type grpcInventoryV1 struct {
	inventoryv1.UnimplementedInventoryServiceServer
	snapshotter        InventorySnapshotter
	committedSnapshots inventory.CommittedSnapshotReader
}

var _ inventoryv1.InventoryServiceServer = (*grpcInventoryV1)(nil)

func (gm *grpcInventoryV1) GetInventorySnapshot(ctx context.Context, req *inventoryv1.GetInventorySnapshotRequest) (*inventoryv1.GetInventorySnapshotResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "empty request")
	}

	if gm.snapshotter == nil {
		return nil, status.Error(codes.Unavailable, "inventory snapshot service unavailable")
	}

	if err := inventory.ValidateNonce(req.GetNonce()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	snapshot, err := gm.snapshotter.Build(ctx, inventory.SnapshotRequest{
		Nonce: req.GetNonce(),
	})
	if err != nil {
		return nil, err
	}
	if err := inventory.ValidateSnapshot(snapshot); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &inventoryv1.GetInventorySnapshotResponse{
		SnapshotPayload: snapshot.Payload,
		Signature:       snapshot.Signature,
		Provider:        snapshot.Provider,
	}, nil
}

func (gm *grpcInventoryV1) GetCommittedInventorySnapshot(ctx context.Context, req *inventoryv1.GetCommittedInventorySnapshotRequest) (*inventoryv1.GetCommittedInventorySnapshotResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "empty request")
	}

	if gm.committedSnapshots == nil {
		return nil, status.Error(codes.Unavailable, "committed inventory snapshot service unavailable")
	}

	var (
		record inventory.CommittedSnapshot
		err    error
	)

	hash := req.GetSnapshotHash()
	if len(hash) == 0 {
		record, err = gm.committedSnapshots.Latest(ctx)
	} else {
		if err := inventory.ValidateCommittedSnapshotHash(hash); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}

		record, err = gm.committedSnapshots.Get(ctx, hash)
	}
	if err != nil {
		switch {
		case errors.Is(err, inventory.ErrCommittedSnapshotNotFound):
			return nil, status.Error(codes.NotFound, err.Error())
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return nil, status.FromContextError(err).Err()
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if err := inventory.ValidateCommittedSnapshot(record); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if record.State != inventory.CommittedSnapshotStatePosted {
		return nil, status.Error(codes.Internal, "committed inventory snapshot is not posted")
	}
	if len(hash) != 0 && !bytes.Equal(record.Snapshot.Hash, hash) {
		return nil, status.Error(codes.Internal, "committed inventory snapshot hash does not match request")
	}

	return &inventoryv1.GetCommittedInventorySnapshotResponse{
		SnapshotPayload: append([]byte(nil), record.Snapshot.Payload...),
		Signature:       append([]byte(nil), record.Snapshot.Signature...),
		Provider:        record.Snapshot.Provider,
		SnapshotHash:    append([]byte(nil), record.Snapshot.Hash...),
		PostedAt:        record.PostedAt,
	}, nil
}
