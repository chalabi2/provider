package pconfig_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/akash-network/provider/tools/pconfig/bbolt"
	"github.com/akash-network/provider/verification/inventory"
)

func newInventorySnapshot(payload string) inventory.Snapshot {
	data := []byte(payload)

	return inventory.Snapshot{
		Payload:   data,
		Hash:      inventory.HashPayload(data),
		Signature: []byte("signature-" + payload),
		Provider:  "akash1provider",
	}
}

func TestInventorySnapshotsLifecycle(t *testing.T) {
	dbs := initTestBackends(t)
	defer func() {
		for _, db := range dbs {
			db.cleanup()
		}
	}()

	for _, db := range dbs {
		t.Run(db.name, func(t *testing.T) {
			ctx := context.Background()
			store := db.InventorySnapshots()
			snapshot := newInventorySnapshot("first")
			original := inventory.CloneSnapshot(snapshot)

			err := store.Stage(ctx, snapshot)
			require.NoError(t, err)

			snapshot.Payload[0] = 'x'
			snapshot.Hash[0] = 0
			snapshot.Signature[0] = 'x'

			_, err = store.Get(ctx, original.Hash)
			require.ErrorIs(t, err, inventory.ErrCommittedSnapshotNotFound)

			_, err = store.Latest(ctx)
			require.ErrorIs(t, err, inventory.ErrCommittedSnapshotNotFound)

			pending, err := store.Pending(ctx)
			require.NoError(t, err)
			require.Equal(t, []inventory.CommittedSnapshot{{
				Snapshot: original,
				State:    inventory.CommittedSnapshotStatePending,
			}}, pending)

			pending[0].Snapshot.Payload[0] = 'x'
			pending, err = store.Pending(ctx)
			require.NoError(t, err)
			require.Equal(t, []byte("first"), pending[0].Snapshot.Payload)

			postedAt := time.Date(2026, time.July, 27, 19, 34, 56, 789, time.UTC)
			err = store.MarkPosted(ctx, original.Hash, postedAt)
			require.NoError(t, err)

			expected := inventory.CommittedSnapshot{
				Snapshot: original,
				State:    inventory.CommittedSnapshotStatePosted,
				PostedAt: postedAt,
			}

			record, err := store.Get(ctx, original.Hash)
			require.NoError(t, err)
			require.Equal(t, expected, record)

			latest, err := store.Latest(ctx)
			require.NoError(t, err)
			require.Equal(t, expected, latest)

			pending, err = store.Pending(ctx)
			require.NoError(t, err)
			require.Empty(t, pending)

			record.Snapshot.Signature[0] = 'x'
			record, err = store.Get(ctx, original.Hash)
			require.NoError(t, err)
			require.Equal(t, expected, record)

			err = store.Stage(ctx, original)
			require.NoError(t, err)

			record, err = store.Get(ctx, original.Hash)
			require.NoError(t, err)
			require.Equal(t, expected, record)
		})
	}
}

func TestInventorySnapshotsLatestUsesSuccessfulPostTime(t *testing.T) {
	dbs := initTestBackends(t)
	defer func() {
		for _, db := range dbs {
			db.cleanup()
		}
	}()

	for _, db := range dbs {
		t.Run(db.name, func(t *testing.T) {
			ctx := context.Background()
			store := db.InventorySnapshots()
			older := newInventorySnapshot("older")
			newer := newInventorySnapshot("newer")
			olderPostedAt := time.Date(2026, time.July, 27, 10, 0, 0, 0, time.UTC)
			newerPostedAt := olderPostedAt.Add(time.Minute)

			require.NoError(t, store.Stage(ctx, older))
			require.NoError(t, store.Stage(ctx, newer))
			require.NoError(t, store.MarkPosted(ctx, newer.Hash, newerPostedAt))
			require.NoError(t, store.MarkPosted(ctx, older.Hash, olderPostedAt))

			latest, err := store.Latest(ctx)
			require.NoError(t, err)
			require.Equal(t, newer.Hash, latest.Snapshot.Hash)
			require.Equal(t, newerPostedAt, latest.PostedAt)

			olderRecord, err := store.Get(ctx, older.Hash)
			require.NoError(t, err)
			require.Equal(t, olderPostedAt, olderRecord.PostedAt)
		})
	}
}

func TestInventorySnapshotsRejectInvalidLifecycle(t *testing.T) {
	dbs := initTestBackends(t)
	defer func() {
		for _, db := range dbs {
			db.cleanup()
		}
	}()

	for _, db := range dbs {
		t.Run(db.name, func(t *testing.T) {
			ctx := context.Background()
			store := db.InventorySnapshots()
			snapshot := newInventorySnapshot("payload")
			invalid := inventory.CloneSnapshot(snapshot)
			invalid.Hash = inventory.HashPayload([]byte("different"))

			require.Error(t, store.Stage(ctx, invalid))
			require.Error(t, store.MarkPosted(ctx, snapshot.Hash, time.Time{}))
			require.ErrorIs(
				t,
				store.MarkPosted(ctx, snapshot.Hash, time.Now().UTC()),
				inventory.ErrCommittedSnapshotNotFound,
			)

			require.NoError(t, store.Stage(ctx, snapshot))
			postedAt := time.Now().UTC()
			require.NoError(t, store.MarkPosted(ctx, snapshot.Hash, postedAt))
			require.NoError(t, store.MarkPosted(ctx, snapshot.Hash, postedAt))
			require.ErrorIs(
				t,
				store.MarkPosted(ctx, snapshot.Hash, postedAt.Add(time.Second)),
				inventory.ErrCommittedSnapshotConflict,
			)

			conflicting := inventory.CloneSnapshot(snapshot)
			conflicting.Signature = []byte("different signature")
			require.ErrorIs(t, store.Stage(ctx, conflicting), inventory.ErrCommittedSnapshotConflict)

			_, err := store.Get(ctx, []byte("short"))
			require.Error(t, err)
			require.False(t, errors.Is(err, inventory.ErrCommittedSnapshotNotFound))
		})
	}
}

func TestBBoltInventorySnapshotsPersistAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "pconfig.db")
	postedSnapshot := newInventorySnapshot("posted")
	pendingSnapshot := newInventorySnapshot("pending")
	postedAt := time.Date(2026, time.July, 27, 19, 34, 56, 123456789, time.UTC)

	db, err := bbolt.NewBBolt(dbPath)
	require.NoError(t, err)
	store := db.InventorySnapshots()
	require.NoError(t, store.Stage(ctx, postedSnapshot))
	require.NoError(t, store.Stage(ctx, pendingSnapshot))
	require.NoError(t, store.MarkPosted(ctx, postedSnapshot.Hash, postedAt))
	require.NoError(t, db.Close())

	db, err = bbolt.NewBBolt(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	store = db.InventorySnapshots()

	record, err := store.Get(ctx, postedSnapshot.Hash)
	require.NoError(t, err)
	require.Equal(t, postedSnapshot.Payload, record.Snapshot.Payload)
	require.Equal(t, postedSnapshot.Signature, record.Snapshot.Signature)
	require.Equal(t, postedSnapshot.Hash, record.Snapshot.Hash)
	require.Equal(t, postedSnapshot.Provider, record.Snapshot.Provider)
	require.Equal(t, postedAt, record.PostedAt)
	require.Equal(t, inventory.CommittedSnapshotStatePosted, record.State)

	latest, err := store.Latest(ctx)
	require.NoError(t, err)
	require.Equal(t, record, latest)

	_, err = store.Get(ctx, pendingSnapshot.Hash)
	require.ErrorIs(t, err, inventory.ErrCommittedSnapshotNotFound)

	pending, err := store.Pending(ctx)
	require.NoError(t, err)
	require.Equal(t, pendingSnapshot, pending[0].Snapshot)

	pendingPostedAt := postedAt.Add(time.Minute)
	require.NoError(t, store.MarkPosted(ctx, pendingSnapshot.Hash, pendingPostedAt))
	latest, err = store.Latest(ctx)
	require.NoError(t, err)
	require.Equal(t, pendingSnapshot, latest.Snapshot)
	require.Equal(t, pendingPostedAt, latest.PostedAt)
}
