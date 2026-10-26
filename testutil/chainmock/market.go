// Package chainmock adapts chain-sdk's generated query mocks to the query
// interfaces of the chain-sdk the provider builds against.
package chainmock

import (
	"context"

	"google.golang.org/grpc"

	marketmocks "pkg.akt.dev/go/mocks/node/client/market"
	mvbeta "pkg.akt.dev/go/node/market/v2beta1"
)

// Market is chain-sdk's market query mock with LeaseResizes, which the
// generated mock predates. Without it the mock no longer satisfies
// mvbeta.QueryClient and the client mock's Market() panics on the type
// assertion. Expectations, LeaseResizes included, are set on the embedded
// mock as usual.
type Market struct {
	*marketmocks.QueryClient
}

var _ mvbeta.QueryClient = Market{}

func (m Market) LeaseResizes(ctx context.Context, in *mvbeta.QueryLeaseResizesRequest, opts ...grpc.CallOption) (*mvbeta.QueryLeaseResizesResponse, error) {
	args := make([]interface{}, 0, 2+len(opts))
	args = append(args, ctx, in)
	for _, opt := range opts {
		args = append(args, opt)
	}

	ret := m.MethodCalled("LeaseResizes", args...)

	var resp *mvbeta.QueryLeaseResizesResponse
	if val := ret.Get(0); val != nil {
		resp = val.(*mvbeta.QueryLeaseResizesResponse)
	}

	return resp, ret.Error(1)
}
