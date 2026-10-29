package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clientmocks "pkg.akt.dev/go/mocks/node/client"
	marketmocks "pkg.akt.dev/go/mocks/node/client/market"
	mv1 "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v1beta5"
	"pkg.akt.dev/go/testutil"
)

func TestChainClientReadsWhenALeaseClosed(t *testing.T) {
	closed := testutil.LeaseID(t)
	open := testutil.LeaseID(t)

	market := &marketmocks.QueryClient{}
	market.On("Lease", mock.Anything, &mvbeta.QueryLeaseRequest{ID: closed}).
		Return(&mvbeta.QueryLeaseResponse{Lease: mv1.Lease{ID: closed, State: mv1.LeaseClosed, ClosedAt: 1792488600}}, nil)
	market.On("Lease", mock.Anything, &mvbeta.QueryLeaseRequest{ID: open}).
		Return(&mvbeta.QueryLeaseResponse{Lease: mv1.Lease{ID: open, State: mv1.LeaseActive}}, nil)

	query := &clientmocks.QueryClient{}
	query.On("Market").Return(market)

	chain := newChainClient(query, nil, closed.Provider)

	at, err := chain.LeaseClosedAt(context.Background(), closed)
	require.NoError(t, err)
	require.True(t, time.Unix(1792488600, 0).Equal(at))

	at, err = chain.LeaseClosedAt(context.Background(), open)
	require.NoError(t, err)
	require.True(t, at.IsZero(), "an open lease has not closed")
}
