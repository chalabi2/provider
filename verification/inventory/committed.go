package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"
)

const SnapshotHashSize = sha256.Size

var (
	ErrCommittedSnapshotNotFound = errors.New("inventory: committed snapshot not found")
	ErrCommittedSnapshotConflict = errors.New("inventory: committed snapshot conflict")

	errInvalidCommittedSnapshotHash     = errors.New("invalid committed inventory snapshot hash")
	errInvalidCommittedSnapshotState    = errors.New("invalid committed inventory snapshot state")
	errInvalidCommittedSnapshotPostedAt = errors.New("invalid committed inventory snapshot post time")
)

type CommittedSnapshotState uint8

const (
	CommittedSnapshotStatePending CommittedSnapshotState = iota + 1
	CommittedSnapshotStatePosted
)

type CommittedSnapshot struct {
	Snapshot Snapshot
	State    CommittedSnapshotState
	PostedAt time.Time
}

type CommittedSnapshotReader interface {
	Get(context.Context, []byte) (CommittedSnapshot, error)
	Latest(context.Context) (CommittedSnapshot, error)
}

type CommittedSnapshotStore interface {
	CommittedSnapshotReader
	Stage(context.Context, Snapshot) error
	Pending(context.Context) ([]CommittedSnapshot, error)
	MarkPosted(context.Context, []byte, time.Time) error
}

func ValidateCommittedSnapshotHash(hash []byte) error {
	if len(hash) != SnapshotHashSize {
		return fmt.Errorf("%w: expected %d bytes, got %d", errInvalidCommittedSnapshotHash, SnapshotHashSize, len(hash))
	}

	return nil
}

func NewPendingCommittedSnapshot(snapshot Snapshot) (CommittedSnapshot, error) {
	record := CommittedSnapshot{
		Snapshot: CloneSnapshot(snapshot),
		State:    CommittedSnapshotStatePending,
	}
	if err := ValidateCommittedSnapshot(record); err != nil {
		return CommittedSnapshot{}, err
	}

	return record, nil
}

func MarkCommittedSnapshotPosted(record CommittedSnapshot, postedAt time.Time) (CommittedSnapshot, error) {
	if err := ValidateCommittedSnapshot(record); err != nil {
		return CommittedSnapshot{}, err
	}
	if record.State != CommittedSnapshotStatePending {
		return CommittedSnapshot{}, ErrCommittedSnapshotConflict
	}
	if postedAt.IsZero() {
		return CommittedSnapshot{}, errInvalidCommittedSnapshotPostedAt
	}

	record = CloneCommittedSnapshot(record)
	record.State = CommittedSnapshotStatePosted
	record.PostedAt = postedAt.Round(0).UTC()

	return record, nil
}

func ValidateCommittedSnapshot(record CommittedSnapshot) error {
	if err := ValidateSnapshot(&record.Snapshot); err != nil {
		return err
	}
	if err := ValidateCommittedSnapshotHash(record.Snapshot.Hash); err != nil {
		return err
	}
	if !bytes.Equal(record.Snapshot.Hash, HashPayload(record.Snapshot.Payload)) {
		return errInvalidCommittedSnapshotHash
	}

	switch record.State {
	case CommittedSnapshotStatePending:
		if !record.PostedAt.IsZero() {
			return errInvalidCommittedSnapshotPostedAt
		}
	case CommittedSnapshotStatePosted:
		if record.PostedAt.IsZero() {
			return errInvalidCommittedSnapshotPostedAt
		}
	default:
		return errInvalidCommittedSnapshotState
	}

	return nil
}

func CloneSnapshot(snapshot Snapshot) Snapshot {
	return Snapshot{
		Payload:   append([]byte(nil), snapshot.Payload...),
		Hash:      append([]byte(nil), snapshot.Hash...),
		Signature: append([]byte(nil), snapshot.Signature...),
		Provider:  snapshot.Provider,
	}
}

func CloneCommittedSnapshot(record CommittedSnapshot) CommittedSnapshot {
	return CommittedSnapshot{
		Snapshot: CloneSnapshot(record.Snapshot),
		State:    record.State,
		PostedAt: record.PostedAt,
	}
}
