package bbolt

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	boltdb "go.etcd.io/bbolt"

	"github.com/akash-network/provider/verification/inventory"
)

func TestInventorySnapshotsRejectCorruptRecord(t *testing.T) {
	ctx := context.Background()
	payload := []byte("payload")
	snapshot := inventory.Snapshot{
		Payload:   payload,
		Hash:      inventory.HashPayload(payload),
		Signature: []byte("signature"),
		Provider:  "akash1provider",
	}

	storage, err := NewBBolt(filepath.Join(t.TempDir(), "pconfig.db"))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, storage.Close())
	})

	store := storage.InventorySnapshots()
	require.NoError(t, store.Stage(ctx, snapshot))
	require.NoError(t, store.MarkPosted(ctx, snapshot.Hash, time.Now().UTC()))

	db := storage.(*impl).db
	err = db.Update(func(tx *boltdb.Tx) error {
		records, err := inventorySnapshotRecordsBucket(tx)
		if err != nil {
			return err
		}

		return records.Bucket(snapshot.Hash).Put(keyInventorySnapshotPayload, []byte("corrupt"))
	})
	require.NoError(t, err)

	_, err = store.Get(ctx, snapshot.Hash)
	require.ErrorIs(t, err, errInvalidSnapshotRecord)

	_, err = store.Latest(ctx)
	require.ErrorIs(t, err, errInvalidSnapshotRecord)
}
