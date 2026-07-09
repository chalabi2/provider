package storage

import (
	"context"
	"fmt"

	abci "github.com/cometbft/cometbft/abci/types"
	cmclient "github.com/cometbft/cometbft/rpc/client"
	cmtypes "github.com/cometbft/cometbft/types"

	sdkclient "github.com/cosmos/cosmos-sdk/client"
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkquery "github.com/cosmos/cosmos-sdk/types/query"

	aclient "pkg.akt.dev/go/node/client"
	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dtypes "pkg.akt.dev/go/node/deployment/v1beta5"
	mv1 "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v2beta1"
)

// ChainClient is the operator's chain surface: adoption verification for
// the reconciler, the active-lease re-list for restart recovery, and the
// live event stream. The operator is stateless towards the chain - every
// answer is re-derivable by asking again.
type ChainClient interface {
	ChainQuery

	// ActiveLeases returns the IDs (String() form) of the provider's active
	// leases. The pagination cursor is the operator's own, held locally for
	// the duration of the walk - it never touches the bid engine's persisted
	// orders checkpoint (pconfig GetOrdersNextKey).
	ActiveLeases(ctx context.Context) (map[string]bool, error)

	// Events streams the AEP-87 relevant chain events: EventLeaseClosed,
	// EventVolumeAttached, EventVolumeDetached, EventVolumeAdopted.
	Events(ctx context.Context, name string) (<-chan interface{}, error)
}

type chainClient struct {
	qc       aclient.QueryClient
	node     sdkclient.CometRPC
	provider string
}

var _ ChainClient = (*chainClient)(nil)

// newChainClient wraps an akash query client and a comet RPC connection.
func newChainClient(qc aclient.QueryClient, node sdkclient.CometRPC, provider string) *chainClient {
	return &chainClient{
		qc:       qc,
		node:     node,
		provider: provider,
	}
}

func (c *chainClient) GroupVolumePolicy(ctx context.Context, id dv1.GroupID) (*dv1.VolumePolicy, error) {
	resp, err := c.qc.Deployment().Group(ctx, &dtypes.QueryGroupRequest{ID: id})
	if err != nil {
		return nil, err
	}

	return resp.Group.GroupSpec.Volume, nil
}

func (c *chainClient) ActiveLeases(ctx context.Context) (map[string]bool, error) {
	active := make(map[string]bool)

	// the operator's own pagination cursor; local to this walk
	var nextKey []byte

	for {
		req := &mvbeta.QueryLeasesRequest{
			Filters: mv1.LeaseFilters{
				Provider: c.provider,
				State:    mv1.LeaseActive.String(),
			},
		}

		if nextKey != nil {
			req.Pagination = &sdkquery.PageRequest{Key: nextKey}
		}

		resp, err := c.qc.Market().Leases(ctx, req)
		if err != nil {
			return nil, err
		}

		for _, lease := range resp.Leases {
			active[lease.Lease.ID.String()] = true
		}

		if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
			break
		}

		nextKey = resp.Pagination.NextKey
	}

	return active, nil
}

// Events subscribes to new-block headers and decodes the volume-relevant
// typed events out of each block's results. chain-sdk's util/events service
// does not forward the AEP-87 volume events yet, so the operator runs its
// own narrow pump off the same primitives.
func (c *chainClient) Events(ctx context.Context, name string) (<-chan interface{}, error) {
	ebus, valid := c.node.(cmclient.EventsClient)
	if !valid {
		return nil, fmt.Errorf("%w: rpc client does not support event subscription", ErrVolumeOperator)
	}

	query := fmt.Sprintf("%s='%s'", cmtypes.EventTypeKey, cmtypes.EventNewBlockHeader)

	blkch, err := ebus.Subscribe(ctx, name, query, 100)
	if err != nil {
		return nil, err
	}

	out := make(chan interface{}, 100)

	go func() {
		defer close(out)
		defer func() {
			_ = ebus.Unsubscribe(context.Background(), name, query)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-blkch:
				if !ok {
					return
				}

				hdr, valid := ev.Data.(cmtypes.EventDataNewBlockHeader)
				if !valid {
					continue
				}

				height := hdr.Header.Height

				blkResults, err := c.node.BlockResults(ctx, &height)
				if err != nil {
					continue
				}

				emit := func(evts []abci.Event) bool {
					for _, bev := range evts {
						mev, matched := processEvent(bev)
						if !matched {
							continue
						}

						select {
						case out <- mev:
						case <-ctx.Done():
							return false
						}
					}

					return true
				}

				for _, tx := range blkResults.TxsResults {
					if tx == nil {
						continue
					}

					if !emit(tx.Events) {
						return
					}
				}

				// escrow-exhaustion cascades settle outside tx context
				if !emit(blkResults.FinalizeBlockEvents) {
					return
				}
			}
		}
	}()

	return out, nil
}

// processEvent decodes a typed event and keeps only the ones the storage
// operator acts on.
func processEvent(bev abci.Event) (interface{}, bool) {
	pev, err := sdk.ParseTypedEvent(bev)
	if err != nil {
		return nil, false
	}

	switch pev.(type) {
	case *mv1.EventLeaseClosed:
	case *mv1.EventVolumeAttached:
	case *mv1.EventVolumeDetached:
	case *dv1.EventVolumeAdopted:
	default:
		return nil, false
	}

	return pev, true
}
