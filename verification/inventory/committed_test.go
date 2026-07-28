package inventory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testCommittedSnapshot() Snapshot {
	payload := "payload"
	data := []byte(payload)

	return Snapshot{
		Payload:   data,
		Hash:      HashPayload(data),
		Signature: []byte("signature-" + payload),
		Provider:  "akash1provider",
	}
}

func TestNewPendingCommittedSnapshot(t *testing.T) {
	snapshot := testCommittedSnapshot()

	record, err := NewPendingCommittedSnapshot(snapshot)
	require.NoError(t, err)
	require.Equal(t, CommittedSnapshotStatePending, record.State)
	require.True(t, record.PostedAt.IsZero())
	require.Equal(t, snapshot, record.Snapshot)

	snapshot.Payload[0] = 'x'
	snapshot.Hash[0] = 0
	snapshot.Signature[0] = 'x'

	require.Equal(t, []byte("payload"), record.Snapshot.Payload)
	require.Equal(t, HashPayload([]byte("payload")), record.Snapshot.Hash)
	require.Equal(t, []byte("signature-payload"), record.Snapshot.Signature)
}

func TestNewPendingCommittedSnapshotRejectsInvalidHash(t *testing.T) {
	snapshot := testCommittedSnapshot()
	snapshot.Hash = HashPayload([]byte("different payload"))

	record, err := NewPendingCommittedSnapshot(snapshot)
	require.Error(t, err)
	require.Empty(t, record)
}

func TestMarkCommittedSnapshotPosted(t *testing.T) {
	record, err := NewPendingCommittedSnapshot(testCommittedSnapshot())
	require.NoError(t, err)

	postedAt := time.Date(2026, time.July, 27, 12, 34, 56, 789, time.FixedZone("test", -7*60*60))
	posted, err := MarkCommittedSnapshotPosted(record, postedAt)
	require.NoError(t, err)

	require.Equal(t, CommittedSnapshotStatePosted, posted.State)
	require.Equal(t, postedAt.UTC(), posted.PostedAt)
	require.Equal(t, record.Snapshot, posted.Snapshot)
	require.True(t, record.PostedAt.IsZero())
}

func TestMarkCommittedSnapshotPostedRejectsInvalidTransition(t *testing.T) {
	record, err := NewPendingCommittedSnapshot(testCommittedSnapshot())
	require.NoError(t, err)

	posted, err := MarkCommittedSnapshotPosted(record, time.Time{})
	require.Error(t, err)
	require.Empty(t, posted)

	record.State = CommittedSnapshotStatePosted
	record.PostedAt = time.Now().UTC()

	posted, err = MarkCommittedSnapshotPosted(record, time.Now())
	require.ErrorIs(t, err, ErrCommittedSnapshotConflict)
	require.Empty(t, posted)
}

func TestValidateCommittedSnapshotState(t *testing.T) {
	snapshot := testCommittedSnapshot()
	postedAt := time.Now().UTC()

	tests := []struct {
		name    string
		record  CommittedSnapshot
		wantErr bool
	}{
		{
			name: "pending",
			record: CommittedSnapshot{
				Snapshot: snapshot,
				State:    CommittedSnapshotStatePending,
			},
		},
		{
			name: "posted",
			record: CommittedSnapshot{
				Snapshot: snapshot,
				State:    CommittedSnapshotStatePosted,
				PostedAt: postedAt,
			},
		},
		{
			name: "unknown state",
			record: CommittedSnapshot{
				Snapshot: snapshot,
			},
			wantErr: true,
		},
		{
			name: "pending with post time",
			record: CommittedSnapshot{
				Snapshot: snapshot,
				State:    CommittedSnapshotStatePending,
				PostedAt: postedAt,
			},
			wantErr: true,
		},
		{
			name: "posted without post time",
			record: CommittedSnapshot{
				Snapshot: snapshot,
				State:    CommittedSnapshotStatePosted,
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateCommittedSnapshot(test.record)
			if test.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}
