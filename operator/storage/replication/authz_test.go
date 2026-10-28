package replication

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	sdk "github.com/cosmos/cosmos-sdk/types"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mv1 "pkg.akt.dev/go/node/market/v1"
	vtv1 "pkg.akt.dev/go/volume/v1"
)

var (
	authzOwner     = testAddress(1)
	authzExporter  = testAddress(2)
	authzRequester = testAddress(3)
	authzStranger  = testAddress(4)
)

const (
	authzVID  = "myapp-pgdata"
	authzDSeq = uint64(100)
)

func testAddress(seed byte) string {
	return sdk.AccAddress(bytes.Repeat([]byte{seed}, 20)).String()
}

// fakeChainState serves canned VolumeLeases keyed on owner/vid.
type fakeChainState struct {
	mu     sync.Mutex
	leases []VolumeLease
	err    error
}

func (f *fakeChainState) VolumeLeases(_ context.Context, _, _ string) ([]VolumeLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]VolumeLease(nil), f.leases...), f.err
}

// update changes the chain while transfers may be reading it.
func (f *fakeChainState) update(fn func(*fakeChainState)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fn(f)
}

func exportReq() *vtv1.ExportRequest {
	return &vtv1.ExportRequest{
		Owner:     authzOwner,
		Vid:       authzVID,
		DSeq:      authzDSeq,
		Requester: authzRequester,
	}
}

func migrationLease(provider string, state mv1.Lease_State) VolumeLease {
	return VolumeLease{
		Provider: provider,
		DSeq:     authzDSeq,
		State:    state,
		Policy:   dv1.VolumePolicy{Vid: authzVID},
	}
}

func replicaLease(provider string, state mv1.Lease_State) VolumeLease {
	return VolumeLease{
		Provider: provider,
		DSeq:     authzDSeq + 50,
		State:    state,
		Policy: dv1.VolumePolicy{
			Vid: authzVID + "-replica",
			ReplicaOf: &dv1.VolumeRef{
				Owner: authzOwner,
				DSeq:  authzDSeq,
				Name:  authzVID,
			},
		},
	}
}

func TestAuthorizeExportMigration(t *testing.T) {
	// the requester won the re-ordered volume order: an active lease on
	// the same deployment, held by the requester
	cs := &fakeChainState{leases: []VolumeLease{
		migrationLease(authzExporter, mv1.LeaseReclaiming),
		migrationLease(authzRequester, mv1.LeaseActive),
	}}

	require.NoError(t, AuthorizeExport(context.Background(), cs, exportReq(), authzRequester, authzExporter))
}

func TestAuthorizeExportReplication(t *testing.T) {
	cs := &fakeChainState{leases: []VolumeLease{
		migrationLease(authzExporter, mv1.LeaseActive),
		replicaLease(authzRequester, mv1.LeaseActive),
	}}

	require.NoError(t, AuthorizeExport(context.Background(), cs, exportReq(), authzRequester, authzExporter))
}

func TestAuthorizeExportDeniesPeerMismatch(t *testing.T) {
	cs := &fakeChainState{leases: []VolumeLease{
		migrationLease(authzRequester, mv1.LeaseActive),
	}}

	// the mTLS peer is not who the request claims to be
	err := AuthorizeExport(context.Background(), cs, exportReq(), authzStranger, authzExporter)
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestAuthorizeExportDeniesEmptyRequester(t *testing.T) {
	req := exportReq()
	req.Requester = ""

	err := AuthorizeExport(context.Background(), &fakeChainState{}, req, "", authzExporter)
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestAuthorizeExportDeniesSelf(t *testing.T) {
	req := exportReq()
	req.Requester = authzExporter

	cs := &fakeChainState{leases: []VolumeLease{
		migrationLease(authzExporter, mv1.LeaseActive),
	}}

	err := AuthorizeExport(context.Background(), cs, req, authzExporter, authzExporter)
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestAuthorizeExportDeniesInactiveLease(t *testing.T) {
	cs := &fakeChainState{leases: []VolumeLease{
		migrationLease(authzRequester, mv1.LeaseClosed),
	}}

	err := AuthorizeExport(context.Background(), cs, exportReq(), authzRequester, authzExporter)
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestAuthorizeExportDeniesWrongDeployment(t *testing.T) {
	other := migrationLease(authzRequester, mv1.LeaseActive)
	other.DSeq = authzDSeq + 1

	cs := &fakeChainState{leases: []VolumeLease{other}}

	err := AuthorizeExport(context.Background(), cs, exportReq(), authzRequester, authzExporter)
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestAuthorizeExportDeniesUnrelatedReplica(t *testing.T) {
	// a replica of some other volume entitles nothing here
	lease := replicaLease(authzRequester, mv1.LeaseActive)
	lease.Policy.ReplicaOf.Name = "other-volume"

	cs := &fakeChainState{leases: []VolumeLease{lease}}

	err := AuthorizeExport(context.Background(), cs, exportReq(), authzRequester, authzExporter)
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestAuthorizeExportDeniesNoRecord(t *testing.T) {
	err := AuthorizeExport(context.Background(), &fakeChainState{}, exportReq(), authzRequester, authzExporter)
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestAuthorizeImportMigration(t *testing.T) {
	for _, state := range []mv1.Lease_State{mv1.LeaseActive, mv1.LeaseReclaiming, mv1.LeaseClosed, mv1.LeaseInsufficientFunds} {
		cs := &fakeChainState{leases: []VolumeLease{
			migrationLease(authzExporter, state),
		}}

		require.NoError(t,
			AuthorizeImport(context.Background(), cs, authzOwner, authzVID, authzExporter, false),
			"state %s", state)
	}
}

func TestAuthorizeImportReplicaRequiresActive(t *testing.T) {
	cs := &fakeChainState{leases: []VolumeLease{
		migrationLease(authzExporter, mv1.LeaseActive),
	}}

	require.NoError(t, AuthorizeImport(context.Background(), cs, authzOwner, authzVID, authzExporter, true))

	cs = &fakeChainState{leases: []VolumeLease{
		migrationLease(authzExporter, mv1.LeaseClosed),
	}}

	err := AuthorizeImport(context.Background(), cs, authzOwner, authzVID, authzExporter, true)
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestAuthorizeImportDeniesUnknownSource(t *testing.T) {
	cs := &fakeChainState{leases: []VolumeLease{
		migrationLease(authzExporter, mv1.LeaseActive),
	}}

	err := AuthorizeImport(context.Background(), cs, authzOwner, authzVID, authzStranger, false)
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func closedLease(provider string, oseq uint32, reason mv1.LeaseClosedReason) VolumeLease {
	lease := migrationLease(provider, mv1.LeaseClosed)
	lease.OSeq = oseq
	lease.Reason = reason

	return lease
}

func TestMigrationSourcesNewestFirst(t *testing.T) {
	leases := []VolumeLease{
		closedLease("akash1first", 1, mv1.LeaseClosedReasonVolumeMigrate),
		closedLease("akash1second", 2, mv1.LeaseClosedReasonVolumeEvict),
		migrationLease(authzRequester, mv1.LeaseActive),
	}

	require.Equal(t, []string{"akash1second", "akash1first"}, MigrationSources(leases, authzVID, authzDSeq, authzRequester, false))
}

func TestMigrationSourcesStopAtTheDestinationsOwnCopy(t *testing.T) {
	leases := []VolumeLease{
		closedLease("akash1first", 1, mv1.LeaseClosedReasonVolumeMigrate),
		closedLease(authzRequester, 2, mv1.LeaseClosedReasonVolumeMigrate),
		closedLease("akash1third", 3, mv1.LeaseClosedReasonVolumeMigrate),
	}

	require.Equal(t, []string{"akash1third"}, MigrationSources(leases, authzVID, authzDSeq, authzRequester, true))
}

func TestMigrationSourcesPassOverTheDestinationsLeasesWithoutACopy(t *testing.T) {
	leases := []VolumeLease{
		closedLease("akash1first", 1, mv1.LeaseClosedReasonVolumeMigrate),
		closedLease(authzRequester, 2, mv1.LeaseClosedReasonVolumeEvict),
		closedLease("akash1third", 3, mv1.LeaseClosedReasonVolumeMigrate),
	}

	require.Equal(t, []string{"akash1third", "akash1first"}, MigrationSources(leases, authzVID, authzDSeq, authzRequester, false))
}

func TestMigrationSourcesNoneWhenTheDestinationHeldItLast(t *testing.T) {
	leases := []VolumeLease{
		closedLease("akash1first", 1, mv1.LeaseClosedReasonVolumeMigrate),
		closedLease(authzRequester, 2, mv1.LeaseClosedReasonVolumeMigrate),
	}

	require.Empty(t, MigrationSources(leases, authzVID, authzDSeq, authzRequester, true))
}

func TestMigrationSourcesListAProviderOnce(t *testing.T) {
	leases := []VolumeLease{
		closedLease("akash1first", 1, mv1.LeaseClosedReasonVolumeMigrate),
		closedLease("akash1first", 2, mv1.LeaseClosedReasonVolumeEvict),
	}

	require.Equal(t, []string{"akash1first"}, MigrationSources(leases, authzVID, authzDSeq, authzRequester, false))
}

func TestMigrationSourcesOnlyMovesAndEvictions(t *testing.T) {
	unfunded := closedLease("akash1unfunded", 3, mv1.LeaseClosedReasonVolumeUnfunded)
	reclaiming := migrationLease("akash1reclaiming", mv1.LeaseReclaiming)
	reclaiming.OSeq = 4
	otherDeployment := closedLease("akash1other", 5, mv1.LeaseClosedReasonVolumeMigrate)
	otherDeployment.DSeq = authzDSeq + 1
	otherVolume := closedLease("akash1othervolume", 6, mv1.LeaseClosedReasonVolumeMigrate)
	otherVolume.Policy.Vid = authzVID + "-other"

	leases := []VolumeLease{
		closedLease("akash1moved", 1, mv1.LeaseClosedReasonVolumeMigrate),
		closedLease("akash1closed", 2, mv1.LeaseClosedReasonVolumeClosedRetain),
		unfunded,
		reclaiming,
		otherDeployment,
		otherVolume,
	}

	require.Equal(t, []string{"akash1moved"}, MigrationSources(leases, authzVID, authzDSeq, authzRequester, false))
}

func TestLatestLeaseIsTheProvidersHighestOrder(t *testing.T) {
	reopened := migrationLease(authzExporter, mv1.LeaseActive)
	reopened.OSeq = 3

	leases := []VolumeLease{
		closedLease(authzExporter, 1, mv1.LeaseClosedReasonVolumeMigrate),
		reopened,
		closedLease(authzStranger, 2, mv1.LeaseClosedReasonVolumeMigrate),
	}

	latest, found := latestLease(leases, authzVID, authzDSeq, authzExporter)
	require.True(t, found)
	require.Equal(t, reopened, latest)

	_, found = latestLease(leases, authzVID+"-other", authzDSeq, authzExporter)
	require.False(t, found)

	_, found = latestLease(leases, authzVID, authzDSeq+1, authzExporter)
	require.False(t, found)
}
