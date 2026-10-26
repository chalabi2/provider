package bidengine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	tpubsub "github.com/troian/pubsub"

	sdk "github.com/cosmos/cosmos-sdk/types"

	clientmocks "pkg.akt.dev/go/mocks/node/client"
	deploymentmocks "pkg.akt.dev/go/mocks/node/client/deployment"
	marketmocks "pkg.akt.dev/go/mocks/node/client/market"
	providermocks "pkg.akt.dev/go/mocks/node/client/provider"
	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dvbeta "pkg.akt.dev/go/node/deployment/v1beta5"
	mtypes "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v2beta1"
	ptypes "pkg.akt.dev/go/node/provider/v1beta4"
	"pkg.akt.dev/go/node/types/constants"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"
	"pkg.akt.dev/go/node/types/unit"
	"pkg.akt.dev/go/sdkutil"
	"pkg.akt.dev/go/testutil"
	"pkg.akt.dev/go/util/pubsub"

	"github.com/akash-network/provider/cluster"
	ctypes "github.com/akash-network/provider/cluster/types/v1beta3"
	"github.com/akash-network/provider/event"
	cmocks "github.com/akash-network/provider/mocks/cluster"
	"github.com/akash-network/provider/operator/waiter"
	"github.com/akash-network/provider/session"
	"github.com/akash-network/provider/tools/fromctx"
)

// computeGroupForResizeTest is defaultGroupResult's group: one unit, two
// replicas at 23uact, so a ceiling of 46uact for an unchanged price.
func computeGroupForResizeTest(t *testing.T) dvbeta.Group {
	t.Helper()

	group := defaultGroupResult().Group
	group.ID = dv1.MakeGroupID(testutil.DeploymentID(t), 1)

	return group
}

// resizedMemory is the memory per unit the compute resizes ask for, up from
// defaultGroupResult's minimum.
const resizedMemory = 2 * unit.Mi

// withResizedMemory is units with every unit's memory set to resizedMemory.
func withResizedMemory(units dvbeta.ResourceUnits) dvbeta.ResourceUnits {
	next := units.Dup()
	for i := range next {
		next[i].Memory.Quantity = rtypes.NewResourceValue(resizedMemory)
	}

	return next
}

// withVolumeSize is a volume group's units with its storage set to size.
func withVolumeSize(units dvbeta.ResourceUnits, size uint64) dvbeta.ResourceUnits {
	next := units.Dup()
	next[0].Storage[0].Quantity = rtypes.NewResourceValue(size)

	return next
}

func resizeRequestForTest(lid mtypes.LeaseID, resources dvbeta.ResourceUnits, expiresAt time.Time) mvbeta.LeaseResize {
	return mvbeta.LeaseResize{
		LeaseID:   lid,
		Resources: resources,
		MaxPrice:  dvbeta.GroupSpec{Resources: resources}.Price(),
		ExpiresAt: expiresAt.Unix(),
	}
}

// recordingPricing prices every group at a fixed amount and keeps the last
// request it priced.
type recordingPricing struct {
	price int64
	req   Request
}

func (p *recordingPricing) CalculatePrice(_ context.Context, req Request) (sdk.DecCoin, error) {
	p.req = req
	return sdk.NewInt64DecCoin(sdkutil.DenomUact, p.price), nil
}

type resizeTestScaffold struct {
	lid         mtypes.LeaseID
	req         mvbeta.LeaseResize
	deployments *deploymentmocks.QueryClient
	market      *marketmocks.QueryClient
	cluster     *cmocks.Cluster
	r           *leaseResize
}

// makeResizeForTest builds an evaluation of the pending resize of a lease on
// group to resources. The chain mocks hold the group and the request.
func makeResizeForTest(t *testing.T, group dvbeta.Group, resources dvbeta.ResourceUnits, cfg Config) *resizeTestScaffold {
	t.Helper()

	provider := testutil.AccAddress(t)

	s := &resizeTestScaffold{
		lid:         mtypes.MakeLeaseID(mtypes.MakeBidID(mtypes.MakeOrderID(group.ID, 1), provider)),
		deployments: &deploymentmocks.QueryClient{},
		market:      &marketmocks.QueryClient{},
		cluster:     &cmocks.Cluster{},
	}

	s.req = resizeRequestForTest(s.lid, resources, time.Now().Add(10*time.Minute))

	s.deployments.On("Group", mock.Anything, &dvbeta.QueryGroupRequest{ID: group.ID}).
		Return(&dvbeta.QueryGroupResponse{Group: group}, nil)

	s.market.On("LeaseResizes", mock.Anything, mock.MatchedBy(func(req *mvbeta.QueryLeaseResizesRequest) bool {
		return req.Owner == s.lid.Owner && req.Provider == s.lid.Provider
	})).Return(&mvbeta.QueryLeaseResizesResponse{LeaseResizes: []mvbeta.LeaseResize{s.req}}, nil)

	query := &clientmocks.QueryClient{}
	query.On("Deployment").Return(s.deployments)
	query.On("Market").Return(s.market)

	client := &clientmocks.Client{}
	client.On("Query").Return(query)

	s.r = &leaseResize{
		lid:       s.lid,
		expiresAt: s.req.ExpiresAt,
		cfg:       cfg,
		session:   session.New(testutil.Logger(t), client, &ptypes.Provider{Owner: provider.String()}, 0),
		cluster:   s.cluster,
		pass:      nullProviderAttrSignatureService{},
		log:       testutil.Logger(t),
		now:       time.Now,
	}

	return s
}

func requireReject(t *testing.T, msg sdk.Msg, lid mtypes.LeaseID, reason mtypes.LeaseResizeCancelReason) {
	t.Helper()

	reject := requireMsgType[*mvbeta.MsgRejectLeaseResize](t, []sdk.Msg{msg})
	require.Equal(t, lid, reject.ID)
	require.Equal(t, reason, reject.Reason)
}

func Test_LeaseResizeAcceptsWithinMaxPrice(t *testing.T) {
	group := computeGroupForResizeTest(t)
	resources := withResizedMemory(group.GroupSpec.Resources)

	pricing := &recordingPricing{price: 37}
	s := makeResizeForTest(t, group, resources, Config{
		PricingStrategy: pricing,
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
	})

	allocated := resources.Dup()
	s.cluster.On("CheckResize", s.lid.OrderID(), mock.Anything).Return(allocated, nil)

	msg, err := s.r.evaluate(context.Background())
	require.NoError(t, err)

	accept := requireMsgType[*mvbeta.MsgAcceptLeaseResize](t, []sdk.Msg{msg})
	require.Equal(t, s.lid, accept.ID)
	require.Equal(t, sdk.NewInt64DecCoin(sdkutil.DenomUact, 37), accept.Price)

	// the capacity check and the price see the resized group, priced on the
	// resources the reservation would be allocated, as a bid is
	resized := s.cluster.Calls[0].Arguments.Get(1).(*dvbeta.Group)
	require.Equal(t, uint64(resizedMemory), resized.GroupSpec.Resources[0].Memory.Quantity.Value())
	require.Equal(t, uint64(resizedMemory), pricing.req.GSpec.Resources[0].Memory.Quantity.Value())
	require.Equal(t, allocated, pricing.req.AllocatedResources)
}

func Test_LeaseResizeRejectsPriceAboveMax(t *testing.T) {
	group := computeGroupForResizeTest(t)

	s := makeResizeForTest(t, group, withResizedMemory(group.GroupSpec.Resources), Config{
		PricingStrategy: testBidPricingStrategy(47),
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
	})
	s.cluster.On("CheckResize", s.lid.OrderID(), mock.Anything).Return(dvbeta.ResourceUnits{}, nil)

	msg, err := s.r.evaluate(context.Background())
	require.NoError(t, err)
	requireReject(t, msg, s.lid, mtypes.LeaseResizeRejectPrice)
}

func Test_LeaseResizeRejectsWithoutCapacity(t *testing.T) {
	group := computeGroupForResizeTest(t)

	s := makeResizeForTest(t, group, withResizedMemory(group.GroupSpec.Resources), Config{
		PricingStrategy: testBidPricingStrategy(37),
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
	})
	s.cluster.On("CheckResize", s.lid.OrderID(), mock.Anything).Return(nil, ctypes.ErrInsufficientCapacity)

	msg, err := s.r.evaluate(context.Background())
	require.NoError(t, err)
	requireReject(t, msg, s.lid, mtypes.LeaseResizeRejectCapacity)
}

func Test_LeaseResizeRejectsUnsupportedChange(t *testing.T) {
	group := computeGroupForResizeTest(t)

	// GPUs cannot be added to a unit that has none
	resources := group.GroupSpec.Resources.Dup()
	resources[0].GPU.Units = rtypes.NewResourceValue(1)

	s := makeResizeForTest(t, group, resources, Config{
		PricingStrategy: testBidPricingStrategy(37),
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
	})

	msg, err := s.r.evaluate(context.Background())
	require.NoError(t, err)
	requireReject(t, msg, s.lid, mtypes.LeaseResizeRejectUnsupported)

	s.cluster.AssertNotCalled(t, "CheckResize", mock.Anything, mock.Anything)
}

func Test_LeaseResizeRejectsWithoutReservation(t *testing.T) {
	group := computeGroupForResizeTest(t)

	s := makeResizeForTest(t, group, withResizedMemory(group.GroupSpec.Resources), Config{
		PricingStrategy: testBidPricingStrategy(37),
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
	})
	s.cluster.On("CheckResize", s.lid.OrderID(), mock.Anything).Return(nil, cluster.ErrReservationNotFound)

	msg, err := s.r.evaluate(context.Background())
	require.NoError(t, err)
	requireReject(t, msg, s.lid, mtypes.LeaseResizeRejectOther)
}

func Test_LeaseResizeVolumeGrowChecksGrowthHeadroom(t *testing.T) {
	group := *volumeGroupForTest(t, volumeGroupSpec())
	resources := withVolumeSize(group.GroupSpec.Resources, 25*unit.Gi)

	cfg := Config{
		PricingStrategy: testBidPricingStrategy(50),
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
		Volumes:         defaultVolumeConfig(),
	}

	evaluate := func(t *testing.T, headroom uint64) (*resizeTestScaffold, sdk.Msg) {
		t.Helper()

		s := makeResizeForTest(t, group, resources, cfg)
		s.r.pass = fixedAttrSignatureService{attrs: volumeCapabilities()}
		s.r.storage = fakeStorageInventory{testVolumeClass: headroom}
		s.cluster.On("CheckResize", s.lid.OrderID(), mock.Anything).Return(resources.Dup(), nil)

		msg, err := s.r.evaluate(context.Background())
		require.NoError(t, err)

		return s, msg
	}

	// 10Gi grows to 25Gi: the 15Gi growth fits in 20Gi, the full size would not
	t.Run("growth fits", func(t *testing.T) {
		s, msg := evaluate(t, 20*unit.Gi)

		accept := requireMsgType[*mvbeta.MsgAcceptLeaseResize](t, []sdk.Msg{msg})
		require.Equal(t, s.lid, accept.ID)
	})

	t.Run("growth does not fit", func(t *testing.T) {
		s, msg := evaluate(t, 10*unit.Gi)

		requireReject(t, msg, s.lid, mtypes.LeaseResizeRejectCapacity)
		s.cluster.AssertNotCalled(t, "CheckResize", mock.Anything, mock.Anything)
	})
}

func Test_LeaseResizeIgnoresExpiredRequest(t *testing.T) {
	group := computeGroupForResizeTest(t)

	s := makeResizeForTest(t, group, withResizedMemory(group.GroupSpec.Resources), Config{
		PricingStrategy: testBidPricingStrategy(37),
	})
	s.r.now = func() time.Time { return time.Unix(s.req.ExpiresAt+1, 0) }

	msg, err := s.r.evaluate(context.Background())
	require.NoError(t, err)
	require.Nil(t, msg)

	s.deployments.AssertNotCalled(t, "Group", mock.Anything, mock.Anything)
	s.cluster.AssertNotCalled(t, "CheckResize", mock.Anything, mock.Anything)
}

func Test_LeaseResizeIgnoresResolvedRequest(t *testing.T) {
	group := computeGroupForResizeTest(t)

	s := makeResizeForTest(t, group, withResizedMemory(group.GroupSpec.Resources), Config{
		PricingStrategy: testBidPricingStrategy(37),
	})

	// a request this evaluation was not started for replaced it
	s.r.expiresAt = s.req.ExpiresAt - 60

	msg, err := s.r.evaluate(context.Background())
	require.NoError(t, err)
	require.Nil(t, msg)

	// and once the chain has dropped it, there is none at all
	s.market.ExpectedCalls = nil
	s.market.On("LeaseResizes", mock.Anything, mock.Anything).Return(&mvbeta.QueryLeaseResizesResponse{}, nil)

	msg, err = s.r.evaluate(context.Background())
	require.NoError(t, err)
	require.Nil(t, msg)

	s.cluster.AssertNotCalled(t, "CheckResize", mock.Anything, mock.Anything)
}

// resizeServiceScaffold runs the bid engine service against mocks holding
// one group, leased to the provider under as many order sequences as a test
// needs.
type resizeServiceScaffold struct {
	provider   sdk.AccAddress
	group      dvbeta.Group
	cluster    *cmocks.Cluster
	bus        pubsub.Bus
	svc        *service
	broadcasts chan []sdk.Msg
}

func newResizeServiceScaffold(t *testing.T) *resizeServiceScaffold {
	t.Helper()

	return &resizeServiceScaffold{
		provider:   testutil.AccAddress(t),
		group:      computeGroupForResizeTest(t),
		cluster:    &cmocks.Cluster{},
		broadcasts: make(chan []sdk.Msg, 10),
	}
}

func (s *resizeServiceScaffold) lease(oseq uint32) mtypes.LeaseID {
	return mtypes.MakeLeaseID(mtypes.MakeBidID(mtypes.MakeOrderID(s.group.ID, oseq), s.provider))
}

func (s *resizeServiceScaffold) request(lid mtypes.LeaseID, expiresAt time.Time) mvbeta.LeaseResize {
	return resizeRequestForTest(lid, withResizedMemory(s.group.GroupSpec.Resources), expiresAt)
}

// start runs the service. catchUp is what the startup catch-up lists for
// the provider, pending what an evaluation finds for a tenant.
func (s *resizeServiceScaffold) start(t *testing.T, catchUp, pending []mvbeta.LeaseResize) {
	t.Helper()

	deployments := &deploymentmocks.QueryClient{}
	deployments.On("Group", mock.Anything, mock.Anything).Return(&dvbeta.QueryGroupResponse{Group: s.group}, nil)

	market := &marketmocks.QueryClient{}
	market.On("Orders", mock.Anything, mock.Anything).Return(&mvbeta.QueryOrdersResponse{}, nil)
	market.On("LeaseResizes", mock.Anything, mock.MatchedBy(func(req *mvbeta.QueryLeaseResizesRequest) bool {
		return req.Owner == ""
	})).Return(&mvbeta.QueryLeaseResizesResponse{LeaseResizes: catchUp}, nil)
	market.On("LeaseResizes", mock.Anything, mock.MatchedBy(func(req *mvbeta.QueryLeaseResizesRequest) bool {
		return req.Owner != ""
	})).Return(&mvbeta.QueryLeaseResizesResponse{LeaseResizes: pending}, nil)

	providers := &providermocks.QueryClient{}
	providers.On("Provider", mock.Anything, mock.Anything).Return(&ptypes.QueryProviderResponse{}, nil)

	query := &clientmocks.QueryClient{}
	query.On("Deployment").Return(deployments)
	query.On("Market").Return(market)
	query.On("Provider").Return(providers)

	tx := &clientmocks.TxClient{}
	tx.On("BroadcastMsgs", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		s.broadcasts <- args.Get(1).([]sdk.Msg)
	}).Return(&sdk.Result{}, nil)

	client := &clientmocks.Client{}
	client.On("Query").Return(query)
	client.On("Tx").Return(tx)

	s.bus = pubsub.NewBus()

	ctx := context.WithValue(context.Background(), fromctx.CtxKeyPubSub, tpubsub.New(context.Background(), 1000))

	svc, err := NewService(ctx, query, session.New(testutil.Logger(t), client, &ptypes.Provider{Owner: s.provider.String()}, 0),
		s.cluster, s.bus, waiter.NewNullWaiter(), Config{
			PricingStrategy: testBidPricingStrategy(37),
			Deposit:         mvbeta.DefaultBidMinDeposit,
			MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
		})
	require.NoError(t, err)

	s.svc = svc.(*service)

	t.Cleanup(func() {
		_ = svc.Close()
		s.bus.Close()
	})
}

func (s *resizeServiceScaffold) requested(t *testing.T, req mvbeta.LeaseResize) {
	t.Helper()

	require.NoError(t, s.bus.Publish(&mtypes.EventLeaseResizeRequested{
		ID:        req.LeaseID,
		MaxPrice:  req.MaxPrice,
		ExpiresAt: req.ExpiresAt,
	}))
}

func (s *resizeServiceScaffold) requireAccepted(t *testing.T, lid mtypes.LeaseID) {
	t.Helper()

	accept := requireMsgType[*mvbeta.MsgAcceptLeaseResize](t, testutil.ChannelWaitForValue(t, s.broadcasts))
	require.Equal(t, lid, accept.ID)
}

func Test_LeaseResizeAnsweredOnceOnRequestEvent(t *testing.T) {
	s := newResizeServiceScaffold(t)
	lidA, lidB := s.lease(1), s.lease(2)
	reqA, reqB := s.request(lidA, time.Now().Add(10*time.Minute)), s.request(lidB, time.Now().Add(10*time.Minute))

	s.cluster.On("CheckResize", mock.Anything, mock.Anything).Return(dvbeta.ResourceUnits{}, nil)
	s.start(t, nil, []mvbeta.LeaseResize{reqA, reqB})

	s.requested(t, reqA)
	s.requireAccepted(t, lidA)

	// another provider's lease is not this provider's to answer
	other := reqB
	other.LeaseID.Provider = testutil.AccAddress(t).String()
	s.requested(t, other)

	// the same request seen again is not evaluated again; the service handles
	// events in order, so once B is answered both have been passed over
	s.requested(t, reqA)
	s.requested(t, reqB)
	s.requireAccepted(t, lidB)

	s.cluster.AssertNumberOfCalls(t, "CheckResize", 2)
}

func Test_LeaseResizeCaughtUpAtStartup(t *testing.T) {
	s := newResizeServiceScaffold(t)
	lidA, lidB := s.lease(1), s.lease(2)

	expired := s.request(lidB, time.Now().Add(-time.Minute))
	pending := s.request(lidA, time.Now().Add(10*time.Minute))

	s.cluster.On("CheckResize", mock.Anything, mock.Anything).Return(dvbeta.ResourceUnits{}, nil)

	// no event: the requests were made while the provider was down
	s.start(t, []mvbeta.LeaseResize{expired, pending}, nil)

	s.requireAccepted(t, lidA)
	s.cluster.AssertNotCalled(t, "CheckResize", lidB.OrderID(), mock.Anything)
}

func Test_LeaseResizeCanceledWhileEvaluating(t *testing.T) {
	s := newResizeServiceScaffold(t)
	lidA, lidB := s.lease(1), s.lease(2)
	reqA, reqB := s.request(lidA, time.Now().Add(10*time.Minute)), s.request(lidB, time.Now().Add(10*time.Minute))

	checking := make(chan struct{}, 1)
	release := make(chan struct{})

	s.cluster.On("CheckResize", lidA.OrderID(), mock.Anything).Run(func(mock.Arguments) {
		checking <- struct{}{}
		<-release
	}).Return(dvbeta.ResourceUnits{}, nil)
	s.cluster.On("CheckResize", lidB.OrderID(), mock.Anything).Return(dvbeta.ResourceUnits{}, nil)

	s.start(t, nil, []mvbeta.LeaseResize{reqA, reqB})

	s.requested(t, reqA)
	testutil.ChannelWaitForValue(t, checking)

	require.NoError(t, s.bus.Publish(&mtypes.EventLeaseResizeCanceled{
		ID:     lidA,
		Reason: mtypes.LeaseResizeCancelReasonOwner,
	}))

	// answered after the cancel was handled
	s.requested(t, reqB)
	s.requireAccepted(t, lidB)

	close(release)

	// shutdown waits for A's evaluation to finish
	require.NoError(t, s.svc.Close())

	select {
	case msgs := <-s.broadcasts:
		t.Fatalf("canceled resize answered: %v", msgs)
	default:
	}
}

func Test_LeaseResizedSwapsReservation(t *testing.T) {
	s := newResizeServiceScaffold(t)
	lid := s.lease(1)

	// the group as the chain holds it once the resize is applied
	s.group.GroupSpec.Resources = withResizedMemory(s.group.GroupSpec.Resources)

	swapped := make(chan *dvbeta.Group, 1)
	s.cluster.On("Resize", lid.OrderID(), mock.Anything).Run(func(args mock.Arguments) {
		swapped <- args.Get(1).(*dvbeta.Group)
	}).Return(nil)

	s.start(t, nil, nil)

	other := lid
	other.Provider = testutil.AccAddress(t).String()

	require.NoError(t, s.bus.Publish(&mtypes.EventLeaseResized{ID: other}))
	require.NoError(t, s.bus.Publish(&mtypes.EventLeaseResized{ID: lid}))

	group := testutil.ChannelWaitForValue(t, swapped).(*dvbeta.Group)
	require.Equal(t, s.group.ID, group.ID)
	require.Equal(t, uint64(resizedMemory), group.GroupSpec.Resources[0].Memory.Quantity.Value())

	s.cluster.AssertNumberOfCalls(t, "Resize", 1)
}

func Test_LeaseResizedHandsVolumeToProvisioner(t *testing.T) {
	s := newResizeServiceScaffold(t)
	s.group = *volumeGroupForTest(t, volumeGroupSpec())
	s.group.GroupSpec.Resources = withVolumeSize(s.group.GroupSpec.Resources, 25*unit.Gi)
	lid := s.lease(1)

	s.cluster.On("Resize", lid.OrderID(), mock.Anything).Return(nil)
	s.start(t, nil, nil)

	sub, err := s.bus.Subscribe()
	require.NoError(t, err)
	defer sub.Close()

	require.NoError(t, s.bus.Publish(&mtypes.EventLeaseResized{ID: lid}))

	for {
		select {
		case ev := <-sub.Events():
			resized, ok := ev.(event.VolumeLeaseResized)
			if !ok {
				continue
			}

			require.Equal(t, lid, resized.LeaseID)
			require.Equal(t, uint64(25*unit.Gi), resized.Group.GroupSpec.Resources[0].Storage[0].Quantity.Value())

			// the reservation is swapped before the volume is handed over
			s.cluster.AssertCalled(t, "Resize", lid.OrderID(), mock.Anything)

			return
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for VolumeLeaseResized")
		}
	}
}
