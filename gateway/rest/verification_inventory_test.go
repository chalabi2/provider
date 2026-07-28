package rest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	sdk "github.com/cosmos/cosmos-sdk/types"

	inventoryv1 "pkg.akt.dev/go/inventory/v1"
	apclient "pkg.akt.dev/go/provider/client"
	providerv1 "pkg.akt.dev/go/provider/v1"
	"pkg.akt.dev/go/testutil"

	pmock "github.com/akash-network/provider/mocks/client"
	"github.com/akash-network/provider/verification/inventory"
)

type testVerificationInventoryStatusSource struct {
	record inventory.CommittedSnapshot
	err    error
}

func (s testVerificationInventoryStatusSource) Latest(context.Context) (inventory.CommittedSnapshot, error) {
	return inventory.CloneCommittedSnapshot(s.record), s.err
}

func TestStatusIncludesCommittedInventorySnapshot(t *testing.T) {
	providerAddr := sdk.AccAddress(testutil.Key(t).PubKey().Address())
	createdAt := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	postedAt := createdAt.Add(time.Minute)
	payload, err := (&inventoryv1.SnapshotPayload{
		SchemaVersion: 1,
		Provider:      providerAddr.String(),
		ChainID:       "sandbox-2",
		Timestamp:     createdAt,
	}).Marshal()
	require.NoError(t, err)

	snapshot := inventory.Snapshot{
		Payload:   payload,
		Hash:      inventory.HashPayload(payload),
		Signature: []byte("provider-signature"),
		Provider:  providerAddr.String(),
	}
	record, err := inventory.NewPendingCommittedSnapshot(snapshot)
	require.NoError(t, err)
	record, err = inventory.MarkCommittedSnapshotPosted(record, postedAt)
	require.NoError(t, err)

	pclient := pmock.NewClient(t)
	pclient.On("Status", mock.Anything).Return(&apclient.ProviderStatus{}, nil)
	pclient.On("StatusV1", mock.Anything).Return(&providerv1.Status{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	resp := httptest.NewRecorder()
	createStatusHandler(
		testutil.Logger(t),
		pclient,
		providerAddr,
		testVerificationInventoryStatusSource{record: record},
	).ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var body struct {
		VerificationInventory *verificationInventoryStatus `json:"verification_inventory"`
	}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &body))
	require.NotNil(t, body.VerificationInventory)
	require.Equal(t, providerAddr.String(), body.VerificationInventory.Provider)
	require.Equal(t, base64.StdEncoding.EncodeToString(snapshot.Hash), body.VerificationInventory.Hash)
	require.Equal(t, base64.StdEncoding.EncodeToString(snapshot.Signature), body.VerificationInventory.Signature)
	require.Equal(t, uint32(1), body.VerificationInventory.SchemaVersion)
	require.Equal(t, createdAt, body.VerificationInventory.CreatedAt)
	require.Equal(t, postedAt, body.VerificationInventory.PostedAt)
	require.Equal(t, "valid", body.VerificationInventory.Validation.Status)
	require.Equal(t, postedAt, body.VerificationInventory.Validation.ValidatedAt)
}

func TestLatestVerificationInventoryStatusIgnoresMissingSnapshot(t *testing.T) {
	status, err := latestVerificationInventoryStatus(
		context.Background(),
		testVerificationInventoryStatusSource{err: inventory.ErrCommittedSnapshotNotFound},
	)

	require.NoError(t, err)
	require.Nil(t, status)
}
