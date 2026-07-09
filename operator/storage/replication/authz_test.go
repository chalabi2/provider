package replication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mv1 "pkg.akt.dev/go/node/market/v1"
	vtv1 "pkg.akt.dev/go/volume/v1"
)

const (
	authzOwner     = "akash1owner"
	authzExporter  = "akash1exporter"
	authzRequester = "akash1requester"
	authzStranger  = "akash1stranger"
	authzVID       = "myapp-pgdata"
	authzDSeq      = uint64(100)
)

// fakeChainState serves canned VolumeLeases keyed on owner/vid.
type fakeChainState struct {
	leases []VolumeLease
	err    error
}

func (f *fakeChainState) VolumeLeases(_ context.Context, _, _ string) ([]VolumeLease, error) {
	return f.leases, f.err
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
