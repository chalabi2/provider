package provider

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/boz/go-lifecycle"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkquery "github.com/cosmos/cosmos-sdk/types/query"

	clientmocks "pkg.akt.dev/go/mocks/node/client"
	marketmocks "pkg.akt.dev/go/mocks/node/client/market"
	aclient "pkg.akt.dev/go/node/client/v1beta3"
	mv1 "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v1beta5"
	ptypes "pkg.akt.dev/go/node/provider/v1beta4"
	"pkg.akt.dev/go/testutil"

	clustermocks "github.com/akash-network/provider/mocks/cluster"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	"github.com/akash-network/provider/session"
)

type reclaimerWorld struct {
	vr         *volumeReclaimer
	provider   string
	now        time.Time
	pages      [][]mv1.Lease
	volumes    map[string]bool
	catchingUp bool
	closeErrs  map[string]error
	closed     []mvbeta.MsgCloseBid
	queried    []*mvbeta.QueryLeasesRequest
	synced     int
}

func newReclaimerWorld(t *testing.T) *reclaimerWorld {
	t.Helper()

	w := &reclaimerWorld{
		provider:  testutil.AccAddress(t).String(),
		now:       time.Unix(1_800_000_000, 0),
		volumes:   make(map[string]bool),
		closeErrs: make(map[string]error),
	}

	market := &marketmocks.QueryClient{}
	market.On("Leases", mock.Anything, mock.Anything).Return(func(_ context.Context, req *mvbeta.QueryLeasesRequest, _ ...grpc.CallOption) (*mvbeta.QueryLeasesResponse, error) {
		w.queried = append(w.queried, req)

		page := len(w.queried) - 1
		resp := &mvbeta.QueryLeasesResponse{Pagination: &sdkquery.PageResponse{}}

		if page < len(w.pages) {
			for _, lease := range w.pages[page] {
				resp.Leases = append(resp.Leases, mvbeta.QueryLeaseResponse{Lease: lease})
			}
		}

		if page+1 < len(w.pages) {
			resp.Pagination.NextKey = []byte(strconv.Itoa(page + 1))
		}

		return resp, nil
	}).Maybe()

	query := &clientmocks.QueryClient{}
	query.On("Market").Return(market).Maybe()

	node := &clientmocks.NodeClient{}
	node.On("SyncInfo", mock.Anything).Return(func(context.Context) (*coretypes.SyncInfo, error) {
		w.synced++
		return &coretypes.SyncInfo{LatestBlockTime: w.now, CatchingUp: w.catchingUp}, nil
	}).Maybe()

	tx := &clientmocks.TxClient{}
	tx.On("BroadcastMsgs", mock.Anything, mock.Anything, mock.Anything).Return(func(_ context.Context, msgs []sdk.Msg, _ ...aclient.BroadcastOption) (interface{}, error) {
		msg := msgs[0].(*mvbeta.MsgCloseBid)
		if err := w.closeErrs[msg.ID.LeaseID().String()]; err != nil {
			return nil, err
		}

		w.closed = append(w.closed, *msg)

		return &sdk.Result{}, nil
	}).Maybe()

	client := &clientmocks.Client{}
	client.On("Query").Return(query).Maybe()
	client.On("Node").Return(node).Maybe()
	client.On("Tx").Return(tx).Maybe()

	cclient := clustermocks.NewClient(t)
	cclient.On("LeaseVolume", mock.Anything, mock.Anything).Return(func(_ context.Context, lid mv1.LeaseID) (*crd.Volume, error) {
		if w.volumes[lid.String()] {
			return &crd.Volume{}, nil
		}

		return nil, errors.New("no volume for this lease")
	}).Maybe()

	sess := session.New(testutil.Logger(t), client, &ptypes.Provider{Owner: w.provider}, 1)

	w.vr = &volumeReclaimer{session: sess, cclient: cclient, log: testutil.Logger(t)}

	return w
}

func (w *reclaimerWorld) reclaiming(t *testing.T, deadline time.Time, volume bool) mv1.Lease {
	t.Helper()

	provider, err := sdk.AccAddressFromBech32(w.provider)
	require.NoError(t, err)

	lease := mv1.Lease{
		ID:          mv1.MakeLeaseID(mv1.MakeBidID(testutil.OrderID(t), provider)),
		State:       mv1.LeaseReclaiming,
		Reclamation: &mv1.Reclamation{Deadline: deadline.Unix(), Reason: mv1.LeaseClosedReasonVolumeMigrate},
	}

	w.volumes[lease.ID.String()] = volume

	return lease
}

func TestVolumeReclaimerClosesAVolumeLeasePastItsDeadline(t *testing.T) {
	w := newReclaimerWorld(t)
	lease := w.reclaiming(t, w.now.Add(-time.Second), true)
	w.pages = [][]mv1.Lease{{lease}}

	require.NoError(t, w.vr.sweep(context.Background()))

	require.Equal(t, []mvbeta.MsgCloseBid{{ID: lease.ID.BidID(), Reason: mv1.LeaseClosedReasonDecommissioned}}, w.closed)
}

func TestVolumeReclaimerClosesALeaseAtItsDeadline(t *testing.T) {
	w := newReclaimerWorld(t)
	lease := w.reclaiming(t, w.now, true)
	w.pages = [][]mv1.Lease{{lease}}

	require.NoError(t, w.vr.sweep(context.Background()))

	require.Len(t, w.closed, 1)
}

func TestVolumeReclaimerAsksOnlyForItsOwnReclaimingLeases(t *testing.T) {
	w := newReclaimerWorld(t)

	require.NoError(t, w.vr.sweep(context.Background()))

	require.Equal(t, mv1.LeaseFilters{Provider: w.provider, State: mv1.LeaseReclaiming.String()}, w.queried[0].Filters)
	require.Zero(t, w.synced, "nothing reclaiming, nothing to time")
}

func TestVolumeReclaimerWaitsForTheDeadline(t *testing.T) {
	w := newReclaimerWorld(t)
	w.pages = [][]mv1.Lease{{w.reclaiming(t, w.now.Add(time.Second), true)}}

	require.NoError(t, w.vr.sweep(context.Background()))

	require.Empty(t, w.closed)
}

func TestVolumeReclaimerLeavesComputeLeasesToTheirManagers(t *testing.T) {
	w := newReclaimerWorld(t)
	w.pages = [][]mv1.Lease{{w.reclaiming(t, w.now.Add(-time.Hour), false)}}

	require.NoError(t, w.vr.sweep(context.Background()))

	require.Empty(t, w.closed)
}

func TestVolumeReclaimerWaitsForANodeCatchingUp(t *testing.T) {
	w := newReclaimerWorld(t)
	w.catchingUp = true
	w.pages = [][]mv1.Lease{{w.reclaiming(t, w.now.Add(-time.Hour), true)}}

	require.NoError(t, w.vr.sweep(context.Background()))

	require.Empty(t, w.closed)
}

func TestVolumeReclaimerSkipsALeaseWithoutReclamation(t *testing.T) {
	w := newReclaimerWorld(t)
	lease := w.reclaiming(t, w.now.Add(-time.Hour), true)
	lease.Reclamation = nil
	w.pages = [][]mv1.Lease{{lease}}

	require.NoError(t, w.vr.sweep(context.Background()))

	require.Empty(t, w.closed)
}

func TestVolumeReclaimerKeepsClosingAfterAFailure(t *testing.T) {
	w := newReclaimerWorld(t)
	refused := w.reclaiming(t, w.now.Add(-time.Hour), true)
	due := w.reclaiming(t, w.now.Add(-time.Hour), true)
	w.closeErrs[refused.ID.String()] = errors.New("sequence mismatch")
	w.pages = [][]mv1.Lease{{refused, due}}

	require.NoError(t, w.vr.sweep(context.Background()))

	require.Len(t, w.closed, 1)
	require.Equal(t, due.ID.BidID(), w.closed[0].ID)
}

func TestVolumeReclaimerReadsEveryPage(t *testing.T) {
	w := newReclaimerWorld(t)
	first := w.reclaiming(t, w.now.Add(-time.Hour), true)
	second := w.reclaiming(t, w.now.Add(-time.Hour), true)
	w.pages = [][]mv1.Lease{{first}, {second}}

	require.NoError(t, w.vr.sweep(context.Background()))

	require.Len(t, w.queried, 2)
	require.Equal(t, []byte("1"), w.queried[1].Pagination.Key)
	require.Len(t, w.closed, 2)
}

func TestVolumeReclaimerSweepsUntilClosed(t *testing.T) {
	w := newReclaimerWorld(t)
	w.pages = [][]mv1.Lease{{w.reclaiming(t, w.now.Add(-time.Hour), true)}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vr := w.vr
	vr.interval = time.Millisecond
	vr.lc = lifecycle.New()

	go vr.lc.WatchContext(ctx)
	go vr.run(ctx)

	require.Eventually(t, func() bool { return len(w.queried) > 0 }, 5*time.Second, time.Millisecond)
	require.NoError(t, vr.Close())
}
