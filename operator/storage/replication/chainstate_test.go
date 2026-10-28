package replication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clientmocks "pkg.akt.dev/go/mocks/node/client"
	deploymentmocks "pkg.akt.dev/go/mocks/node/client/deployment"
	marketmocks "pkg.akt.dev/go/mocks/node/client/market"
	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dtypes "pkg.akt.dev/go/node/deployment/v1beta4"
	mv1 "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v1beta5"
)

// A lease in its reclamation window has no close reason yet: the chain
// keeps why it is being reclaimed with the reclamation.
func TestChainStateReportsWhyALeaseIsBeingReclaimed(t *testing.T) {
	leaseOf := func(oseq uint32, state mv1.Lease_State, reason, reclaim mv1.LeaseClosedReason) mvbeta.QueryLeaseResponse {
		return mvbeta.QueryLeaseResponse{Lease: mv1.Lease{
			ID:          mv1.LeaseID{Owner: authzOwner, DSeq: authzDSeq, GSeq: 1, OSeq: oseq, Provider: authzExporter},
			State:       state,
			Reason:      reason,
			Reclamation: &mv1.Reclamation{Reason: reclaim},
		}}
	}

	market := &marketmocks.QueryClient{}
	market.On("Leases", mock.Anything, mock.Anything).Return(&mvbeta.QueryLeasesResponse{Leases: []mvbeta.QueryLeaseResponse{
		leaseOf(1, mv1.LeaseClosed, mv1.LeaseClosedReasonVolumeEvict, mv1.LeaseClosedReasonVolumeEvict),
		leaseOf(2, mv1.LeaseReclaiming, mv1.LeaseClosedReasonInvalid, mv1.LeaseClosedReasonVolumeMigrate),
	}}, nil)

	deployment := &deploymentmocks.QueryClient{}
	deployment.On("Group", mock.Anything, mock.Anything).Return(&dtypes.QueryGroupResponse{
		Group: dtypes.Group{GroupSpec: dtypes.GroupSpec{Volume: &dv1.VolumePolicy{Vid: authzVID}}},
	}, nil)

	query := &clientmocks.QueryClient{}
	query.On("Market").Return(market)
	query.On("Deployment").Return(deployment)

	leases, err := NewChainState(query).VolumeLeases(context.Background(), authzOwner, authzVID)
	require.NoError(t, err)
	require.Len(t, leases, 2)

	require.Equal(t, mv1.LeaseClosed, leases[0].State)
	require.Equal(t, mv1.LeaseClosedReasonVolumeEvict, leases[0].Reason)
	require.Equal(t, mv1.LeaseReclaiming, leases[1].State)
	require.Equal(t, mv1.LeaseClosedReasonVolumeMigrate, leases[1].Reason)
}
