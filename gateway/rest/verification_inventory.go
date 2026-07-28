package rest

import (
	"context"
	"encoding/base64"
	"errors"
	"time"

	inventoryv1 "pkg.akt.dev/go/inventory/v1"

	"github.com/akash-network/provider/verification/inventory"
)

type verificationInventoryStatusSource interface {
	Latest(context.Context) (inventory.CommittedSnapshot, error)
}

type verificationInventoryStatus struct {
	Provider      string                          `json:"provider"`
	Hash          string                          `json:"hash"`
	Signature     string                          `json:"signature"`
	SchemaVersion uint32                          `json:"schema_version"`
	CreatedAt     time.Time                       `json:"created_at"`
	PostedAt      time.Time                       `json:"posted_at"`
	Validation    verificationInventoryValidation `json:"validation"`
}

type verificationInventoryValidation struct {
	Status      string    `json:"status"`
	ValidatedAt time.Time `json:"validated_at"`
}

type verificationInventoryStatusKey struct{}

func SetVerificationInventoryStatusSource(
	cfg map[interface{}]interface{},
	source verificationInventoryStatusSource,
) {
	if cfg == nil || source == nil {
		return
	}

	cfg[verificationInventoryStatusKey{}] = source
}

func verificationInventoryStatusSourceFromConfig(
	cfg map[interface{}]interface{},
) verificationInventoryStatusSource {
	if cfg == nil {
		return nil
	}

	source, _ := cfg[verificationInventoryStatusKey{}].(verificationInventoryStatusSource)
	return source
}

func latestVerificationInventoryStatus(
	ctx context.Context,
	source verificationInventoryStatusSource,
) (*verificationInventoryStatus, error) {
	if source == nil {
		return nil, nil
	}

	record, err := source.Latest(ctx)
	if errors.Is(err, inventory.ErrCommittedSnapshotNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := inventory.ValidateCommittedSnapshot(record); err != nil {
		return nil, err
	}

	var payload inventoryv1.SnapshotPayload
	if err := payload.Unmarshal(record.Snapshot.Payload); err != nil {
		return nil, err
	}

	return &verificationInventoryStatus{
		Provider:      record.Snapshot.Provider,
		Hash:          base64.StdEncoding.EncodeToString(record.Snapshot.Hash),
		Signature:     base64.StdEncoding.EncodeToString(record.Snapshot.Signature),
		SchemaVersion: payload.SchemaVersion,
		CreatedAt:     payload.Timestamp,
		PostedAt:      record.PostedAt,
		Validation: verificationInventoryValidation{
			Status:      "valid",
			ValidatedAt: record.PostedAt,
		},
	}, nil
}
