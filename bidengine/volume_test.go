package bidengine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	tpubsub "github.com/troian/pubsub"
	"k8s.io/apimachinery/pkg/api/resource"

	sdk "github.com/cosmos/cosmos-sdk/types"

	inventoryV1 "pkg.akt.dev/go/inventory/v1"
	clientmocks "pkg.akt.dev/go/mocks/node/client"
	marketmocks "pkg.akt.dev/go/mocks/node/client/market"
	audittypes "pkg.akt.dev/go/node/audit/v1"
	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dvbeta "pkg.akt.dev/go/node/deployment/v1beta4"
	mtypes "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v1beta5"
	ptypes "pkg.akt.dev/go/node/provider/v1beta4"
	attrtypes "pkg.akt.dev/go/node/types/attributes/v1"
	"pkg.akt.dev/go/node/types/constants"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"
	"pkg.akt.dev/go/node/types/unit"
	provider "pkg.akt.dev/go/provider/v1"
	"pkg.akt.dev/go/sdkutil"
	"pkg.akt.dev/go/sdl"
	"pkg.akt.dev/go/testutil"
	"pkg.akt.dev/go/util/pubsub"

	"github.com/akash-network/provider/event"
	"github.com/akash-network/provider/operator/waiter"
	"github.com/akash-network/provider/session"
	"github.com/akash-network/provider/tools/fromctx"
)

const (
	testVolumeClass = "beta2"
	testVolumeSize  = 10 * unit.Gi
	testVolumeVid   = "pgdata"
)

// fakeStorageInventory is a static per-class headroom map.
type fakeStorageInventory map[string]uint64

func (f fakeStorageInventory) AvailableStorage(class string) (uint64, bool) {
	size, exists := f[class]
	return size, exists
}

// fakeVolumeLookup answers local Volume CRD pre-checks from fixed values.
type fakeVolumeLookup struct {
	retained bool
	parked   bool
	err      error

	retainedCalls int
	parkedCalls   int
}

func (f *fakeVolumeLookup) RetainedVolume(_ context.Context, _ string, _ string, _ uint64) (bool, error) {
	f.retainedCalls++
	return f.retained, f.err
}

func (f *fakeVolumeLookup) ParkedVolume(_ context.Context, _ dv1.VolumeRef) (bool, error) {
	f.parkedCalls++
	return f.parked, f.err
}

// fixedAttrSignatureService returns the configured provider attributes and
// no audited signatures.
type fixedAttrSignatureService struct {
	attrs attrtypes.Attributes
}

func (s fixedAttrSignatureService) GetAuditorAttributeSignatures(_ string) (audittypes.AuditedProviders, error) {
	return nil, nil
}

func (s fixedAttrSignatureService) GetAttributes() (attrtypes.Attributes, error) {
	return s.attrs, nil
}

// volumeCapabilities is the advertised on-chain shape matching
// volumeGroupSpec's storage attributes.
func volumeCapabilities() attrtypes.Attributes {
	return attrtypes.Attributes{
		{Key: "capabilities/storage/1/class", Value: testVolumeClass},
		{Key: "capabilities/storage/1/persistent", Value: "true"},
	}
}

// volumeGroupSpec builds a valid storage-only (volume) group spec.
func volumeGroupSpec() dvbeta.GroupSpec {
	return dvbeta.GroupSpec{
		Name:         "volume-group",
		Requirements: attrtypes.PlacementRequirements{},
		Resources: dvbeta.ResourceUnits{
			{
				Resources: rtypes.Resources{
					ID:     1,
					CPU:    &rtypes.CPU{Units: rtypes.NewResourceValue(0)},
					GPU:    &rtypes.GPU{Units: rtypes.NewResourceValue(0)},
					Memory: &rtypes.Memory{Quantity: rtypes.NewResourceValue(0)},
					Storage: rtypes.Volumes{
						rtypes.Storage{
							Name:     "data",
							Quantity: rtypes.NewResourceValue(testVolumeSize),
							Attributes: attrtypes.Attributes{
								{Key: sdl.StorageAttributeClass, Value: testVolumeClass},
								{Key: sdl.StorageAttributePersistent, Value: "true"},
							},
						},
					},
				},
				Count: 1,
				Price: sdk.NewInt64DecCoin(sdkutil.DenomUact, 100),
			},
		},
		Volume: &dv1.VolumePolicy{
			Vid:            testVolumeVid,
			Reclaim:        dv1.VolumeReclaimRetain,
			Retention:      24 * time.Hour,
			MaxAttachments: 1,
		},
	}
}

func defaultVolumeConfig() VolumeConfig {
	return VolumeConfig{
		Classes:         []string{testVolumeClass},
		MaxRetention:    7 * 24 * time.Hour,
		GrowableClasses: []string{testVolumeClass},
	}
}

func volumeGroupForTest(t *testing.T, gspec dvbeta.GroupSpec) *dvbeta.Group {
	t.Helper()

	return &dvbeta.Group{
		ID:        dv1.MakeGroupID(testutil.DeploymentID(t), 1),
		GroupSpec: gspec,
	}
}

// volumeChain answers the chain reads of volume orders: the leases on the
// volume group a replica names as its primary.
type volumeChain struct {
	client *clientmocks.Client
	market *marketmocks.QueryClient
}

func newVolumeChain(leases []mvbeta.QueryLeaseResponse, err error) *volumeChain {
	market := &marketmocks.QueryClient{}
	if err != nil {
		market.On("Leases", mock.Anything, mock.Anything).Return(nil, err)
	} else {
		market.On("Leases", mock.Anything, mock.Anything).Return(&mvbeta.QueryLeasesResponse{Leases: leases}, nil)
	}

	query := &clientmocks.QueryClient{}
	query.On("Market").Return(market)

	client := &clientmocks.Client{}
	client.On("Query").Return(query)

	return &volumeChain{client: client, market: market}
}

// makeShouldBidOrder builds a bare order sufficient for exercising the
// shouldBid/shouldBidVolume gates directly, on a chain where no replica's
// primary is leased here.
func makeShouldBidOrder(t *testing.T, cfg Config, pass ProviderAttrSignatureService, storage StorageInventory, volumes VolumeLookup) *order {
	t.Helper()

	return makeShouldBidOrderOnChain(t, cfg, pass, storage, volumes, newVolumeChain(nil, nil))
}

func makeShouldBidOrderOnChain(t *testing.T, cfg Config, pass ProviderAttrSignatureService, storage StorageInventory, volumes VolumeLookup, chain *volumeChain) *order {
	t.Helper()

	myLog := testutil.Logger(t)

	myProvider := &ptypes.Provider{
		Owner:      testutil.AccAddress(t).String(),
		HostURI:    "",
		Attributes: nil,
	}

	return &order{
		cfg:     cfg,
		session: session.New(myLog, chain.client, myProvider, 0).ForModule("bidengine-order"),
		log:     myLog,
		pass:    pass,
		storage: storage,
		volumes: volumes,
	}
}

func Test_VolumeConfigClassEnabled(t *testing.T) {
	cfg := VolumeConfig{}
	require.False(t, cfg.ClassEnabled(testVolumeClass))

	cfg = defaultVolumeConfig()
	require.True(t, cfg.ClassEnabled(testVolumeClass))
	require.False(t, cfg.ClassEnabled("beta3"))
}

func Test_StorageInventoryFromRetainedSnapshot(t *testing.T) {
	si := newStorageInventory()

	// no snapshot yet
	_, found := si.AvailableStorage(testVolumeClass)
	require.False(t, found)

	si.update(&provider.Inventory{
		Cluster: inventoryV1.Cluster{
			Storage: inventoryV1.ClusterStorage{
				{
					Quantity: inventoryV1.NewResourcePair(100*unit.Gi, 100*unit.Gi, 40*unit.Gi, resource.DecimalSI),
					Info:     inventoryV1.StorageInfo{Class: testVolumeClass},
				},
			},
		},
	})

	avail, found := si.AvailableStorage(testVolumeClass)
	require.True(t, found)
	require.Equal(t, uint64(60*unit.Gi), avail)

	_, found = si.AvailableStorage("unknown-class")
	require.False(t, found)
}

func volumeShouldBid(t *testing.T, mutate func(*dvbeta.GroupSpec, *Config, *fakeVolumeLookup)) (bool, error) {
	t.Helper()

	gspec := volumeGroupSpec()
	cfg := Config{
		Attributes:      nil,
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
		Volumes:         defaultVolumeConfig(),
	}
	lookup := &fakeVolumeLookup{retained: true, parked: true}

	if mutate != nil {
		mutate(&gspec, &cfg, lookup)
	}

	storage := fakeStorageInventory{testVolumeClass: 100 * unit.Gi}
	pass := fixedAttrSignatureService{attrs: volumeCapabilities()}
	order := makeShouldBidOrder(t, cfg, pass, storage, lookup)

	return order.shouldBidVolume(context.Background(), volumeGroupForTest(t, gspec))
}

func Test_ShouldBidVolume(t *testing.T) {
	shouldBid, err := volumeShouldBid(t, nil)
	require.NoError(t, err)
	require.True(t, shouldBid)
}

func Test_ShouldNotBidVolumeWhenClassNotConfigured(t *testing.T) {
	shouldBid, err := volumeShouldBid(t, func(_ *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		cfg.Volumes.Classes = nil // volume bidding disabled
	})
	require.NoError(t, err)
	require.False(t, shouldBid)

	shouldBid, err = volumeShouldBid(t, func(_ *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		cfg.Volumes.Classes = []string{"beta3"} // different class configured
	})
	require.NoError(t, err)
	require.False(t, shouldBid)
}

func bucketOrder(gspec *dvbeta.GroupSpec) {
	gspec.Volume.Access = dv1.VolumeAccessBucket
	gspec.Volume.MaxAttachments = 0
}

func Test_ShouldBidBucket(t *testing.T) {
	shouldBid, err := volumeShouldBid(t, func(gspec *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		bucketOrder(gspec)
		cfg.Volumes.Classes = nil // no -retain class needed for buckets
		cfg.Volumes.BucketClasses = []string{testVolumeClass}
	})
	require.NoError(t, err)
	require.True(t, shouldBid)
}

func Test_ShouldNotBidBucketWithoutBuckets(t *testing.T) {
	for name, classes := range map[string][]string{
		"buckets not served":       nil,
		"class not behind buckets": {"beta3"},
	} {
		shouldBid, err := volumeShouldBid(t, func(gspec *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
			bucketOrder(gspec)
			cfg.Volumes.BucketClasses = classes // the class stays configured for mounted volumes
		})
		require.NoError(t, err, name)
		require.False(t, shouldBid, name)
	}
}

func Test_ShouldNotBidVolumeOnBucketClassAlone(t *testing.T) {
	shouldBid, err := volumeShouldBid(t, func(_ *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		cfg.Volumes.Classes = nil
		cfg.Volumes.BucketClasses = []string{testVolumeClass}
	})
	require.NoError(t, err)
	require.False(t, shouldBid)
}

func Test_ShouldNotBidBucketWithoutHeadroom(t *testing.T) {
	gspec := volumeGroupSpec()
	bucketOrder(&gspec)

	cfg := Config{
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
		Volumes:         VolumeConfig{BucketClasses: []string{testVolumeClass}, MaxRetention: 7 * 24 * time.Hour},
	}

	storage := fakeStorageInventory{testVolumeClass: testVolumeSize - 1}
	order := makeShouldBidOrder(t, cfg, fixedAttrSignatureService{attrs: volumeCapabilities()}, storage, nil)

	shouldBid, err := order.shouldBidVolume(context.Background(), volumeGroupForTest(t, gspec))
	require.NoError(t, err)
	require.False(t, shouldBid)
}

func Test_ShouldNotBidVolumeWhenClassNotAdvertised(t *testing.T) {
	gspec := volumeGroupSpec()
	cfg := Config{
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
		Volumes:         defaultVolumeConfig(),
	}

	storage := fakeStorageInventory{testVolumeClass: 100 * unit.Gi}
	// advertised capabilities lack the class entirely
	order := makeShouldBidOrder(t, cfg, nullProviderAttrSignatureService{}, storage, nil)

	shouldBid, err := order.shouldBidVolume(context.Background(), volumeGroupForTest(t, gspec))
	require.NoError(t, err)
	require.False(t, shouldBid)
}

// Test_ShouldBidVolumeOnceInventoryArrives: an order evaluated before the
// first inventory snapshot, as every order open at startup is, waits for it.
func Test_ShouldBidVolumeOnceInventoryArrives(t *testing.T) {
	cfg := Config{MaxGroupVolumes: constants.DefaultMaxGroupVolumes, Volumes: defaultVolumeConfig()}
	storage := newStorageInventory()
	order := makeShouldBidOrder(t, cfg, fixedAttrSignatureService{attrs: volumeCapabilities()}, storage, nil)

	type result struct {
		bid bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		bid, err := order.shouldBidVolume(context.Background(), volumeGroupForTest(t, volumeGroupSpec()))
		done <- result{bid, err}
	}()

	select {
	case <-done:
		t.Fatal("decided before the inventory arrived")
	case <-time.After(100 * time.Millisecond):
	}

	storage.update(&provider.Inventory{
		Cluster: inventoryV1.Cluster{
			Storage: inventoryV1.ClusterStorage{
				{
					Quantity: inventoryV1.NewResourcePair(100*unit.Gi, 100*unit.Gi, 0, resource.DecimalSI),
					Info:     inventoryV1.StorageInfo{Class: testVolumeClass},
				},
			},
		},
	})

	select {
	case res := <-done:
		require.NoError(t, res.err)
		require.True(t, res.bid)
	case <-time.After(5 * time.Second):
		t.Fatal("still waiting after the inventory arrived")
	}
}

func Test_WaitForInventoryEndsWithTheOrder(t *testing.T) {
	order := makeShouldBidOrder(t, Config{}, fixedAttrSignatureService{}, newStorageInventory(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, order.waitForInventory(ctx), context.Canceled)
}

func Test_ShouldNotBidVolumeOverSizeCap(t *testing.T) {
	shouldBid, err := volumeShouldBid(t, func(_ *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		cfg.Volumes.MaxSize = testVolumeSize - 1
	})
	require.NoError(t, err)
	require.False(t, shouldBid)

	// zero cap means no provider-side limit
	shouldBid, err = volumeShouldBid(t, func(_ *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		cfg.Volumes.MaxSize = 0
	})
	require.NoError(t, err)
	require.True(t, shouldBid)
}

func Test_ShouldNotBidVolumeOverRetentionCap(t *testing.T) {
	shouldBid, err := volumeShouldBid(t, func(gspec *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		gspec.Volume.Retention = cfg.Volumes.MaxRetention + time.Hour
	})
	require.NoError(t, err)
	require.False(t, shouldBid)
}

func Test_ShouldNotBidVolumeOverReplicaCap(t *testing.T) {
	shouldBid, err := volumeShouldBid(t, func(gspec *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		gspec.Volume.MaxReplicas = 3
		cfg.Volumes.MaxReplicas = 2
	})
	require.NoError(t, err)
	require.False(t, shouldBid)

	shouldBid, err = volumeShouldBid(t, func(gspec *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		gspec.Volume.MaxReplicas = 2
		cfg.Volumes.MaxReplicas = 2
	})
	require.NoError(t, err)
	require.True(t, shouldBid)
}

func Test_ShouldNotBidReplicaVolumeWithoutReplicationSupport(t *testing.T) {
	owner := testutil.AccAddress(t).String()

	replicaOf := func(gspec *dvbeta.GroupSpec) {
		gspec.Volume.ReplicaOf = &dv1.VolumeRef{
			Owner: owner,
			DSeq:  5,
			GSeq:  1,
			Name:  "data",
		}
	}

	shouldBid, err := volumeShouldBid(t, func(gspec *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		replicaOf(gspec)
		cfg.Volumes.MaxReplicas = 0
	})
	require.NoError(t, err)
	require.False(t, shouldBid)

	shouldBid, err = volumeShouldBid(t, func(gspec *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		replicaOf(gspec)
		cfg.Volumes.MaxReplicas = 2
	})
	require.NoError(t, err)
	require.True(t, shouldBid)
}

func Test_ShouldNotBidVolumeWithoutHeadroom(t *testing.T) {
	gspec := volumeGroupSpec()
	cfg := Config{
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
		Volumes:         defaultVolumeConfig(),
	}
	pass := fixedAttrSignatureService{attrs: volumeCapabilities()}

	// insufficient per-class headroom
	order := makeShouldBidOrder(t, cfg, pass, fakeStorageInventory{testVolumeClass: testVolumeSize - 1}, nil)
	shouldBid, err := order.shouldBidVolume(context.Background(), volumeGroupForTest(t, gspec))
	require.NoError(t, err)
	require.False(t, shouldBid)

	// class missing from the live inventory
	order = makeShouldBidOrder(t, cfg, pass, fakeStorageInventory{}, nil)
	shouldBid, err = order.shouldBidVolume(context.Background(), volumeGroupForTest(t, gspec))
	require.NoError(t, err)
	require.False(t, shouldBid)
}

func Test_ShouldBidVolumeAdoption(t *testing.T) {
	adopt := func(gspec *dvbeta.GroupSpec, owner string) {
		gspec.Volume.Adopt = &dv1.VolumeRef{
			Owner: owner,
			DSeq:  5,
			GSeq:  1,
			Name:  "data",
		}
	}

	owner := testutil.AccAddress(t).String()

	// retained volume present locally (the helper default)
	shouldBid, err := volumeShouldBid(t, func(gspec *dvbeta.GroupSpec, _ *Config, _ *fakeVolumeLookup) {
		adopt(gspec, owner)
	})
	require.NoError(t, err)
	require.True(t, shouldBid)

	// no retained volume locally
	shouldBid, err = volumeShouldBid(t, func(gspec *dvbeta.GroupSpec, _ *Config, l *fakeVolumeLookup) {
		adopt(gspec, owner)
		l.retained = false
	})
	require.NoError(t, err)
	require.False(t, shouldBid)

	// lookup errors bubble up
	_, err = volumeShouldBid(t, func(gspec *dvbeta.GroupSpec, _ *Config, l *fakeVolumeLookup) {
		adopt(gspec, owner)
		l.err = errors.New("crd lookup failed in test")
	})
	require.Error(t, err)
}

func Test_ShouldBidVolumeAdoptionWithoutLookup(t *testing.T) {
	// nil lookup skips the pre-check; the chain gate is the authority
	gspec := volumeGroupSpec()
	gspec.Volume.Adopt = &dv1.VolumeRef{
		Owner: testutil.AccAddress(t).String(),
		DSeq:  5,
		GSeq:  1,
		Name:  "data",
	}

	cfg := Config{
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
		Volumes:         defaultVolumeConfig(),
	}
	pass := fixedAttrSignatureService{attrs: volumeCapabilities()}
	order := makeShouldBidOrder(t, cfg, pass, fakeStorageInventory{testVolumeClass: 100 * unit.Gi}, nil)

	shouldBid, err := order.shouldBidVolume(context.Background(), volumeGroupForTest(t, gspec))
	require.NoError(t, err)
	require.True(t, shouldBid)
}

func Test_ShouldNotBidVolumeWithIncompatibleOrderAttributes(t *testing.T) {
	shouldBid, err := volumeShouldBid(t, func(_ *dvbeta.GroupSpec, cfg *Config, _ *fakeVolumeLookup) {
		cfg.Attributes = attrtypes.Attributes{
			{Key: "owner", Value: "me"},
		}
	})
	require.NoError(t, err)
	require.False(t, shouldBid)
}

func attachGroupForTest(t *testing.T, refs []dv1.VolumeRef) *dvbeta.Group {
	t.Helper()

	groupResult := defaultGroupResult()
	groupResult.Group.GroupSpec.Resources[0].Volumes = refs
	group := groupResult.Group
	group.ID = dv1.MakeGroupID(testutil.DeploymentID(t), 1)

	return &group
}

func Test_ShouldBidAttachChecksParkedVolumes(t *testing.T) {
	refs := []dv1.VolumeRef{
		{
			Owner: testutil.AccAddress(t).String(),
			DSeq:  5,
			GSeq:  1,
			Name:  "data",
		},
	}

	cfg := Config{MaxGroupVolumes: constants.DefaultMaxGroupVolumes}

	// parked locally: ordinary compute path proceeds
	lookup := &fakeVolumeLookup{parked: true}
	order := makeShouldBidOrder(t, cfg, nullProviderAttrSignatureService{}, nil, lookup)
	shouldBid, err := order.shouldBid(context.Background(), attachGroupForTest(t, refs))
	require.NoError(t, err)
	require.True(t, shouldBid)
	require.Equal(t, 1, lookup.parkedCalls)

	// not parked locally: decline
	lookup = &fakeVolumeLookup{parked: false}
	order = makeShouldBidOrder(t, cfg, nullProviderAttrSignatureService{}, nil, lookup)
	shouldBid, err = order.shouldBid(context.Background(), attachGroupForTest(t, refs))
	require.NoError(t, err)
	require.False(t, shouldBid)

	// nil lookup skips the pre-check; the chain colocation gate is the
	// authority
	order = makeShouldBidOrder(t, cfg, nullProviderAttrSignatureService{}, nil, nil)
	shouldBid, err = order.shouldBid(context.Background(), attachGroupForTest(t, refs))
	require.NoError(t, err)
	require.True(t, shouldBid)
}

// makeVolumeOrderForTest mirrors makeOrderForTest with a volume group, the
// matching advertised capabilities, and seeded live storage inventory.
func makeVolumeOrderForTest(
	t *testing.T,
	checkForExistingBid bool,
	bidState mvbeta.Bid_State,
) (*order, orderTestScaffold, <-chan int) {
	t.Helper()

	return makeVolumeOrderForTestWith(t, checkForExistingBid, bidState, 1356326, nil)
}

// makeVolumeOrderForTestWith builds a volume order of sequence oseq, with
// configure adjusting the bid engine config.
func makeVolumeOrderForTestWith(
	t *testing.T,
	checkForExistingBid bool,
	bidState mvbeta.Bid_State,
	oseq uint32,
	configure func(*Config),
) (*order, orderTestScaffold, <-chan int) {
	t.Helper()

	var scaffold orderTestScaffold
	scaffold.deploymentID = testutil.DeploymentID(t)
	scaffold.groupID = dv1.MakeGroupID(scaffold.deploymentID, 1)
	scaffold.orderID = mtypes.MakeOrderID(scaffold.groupID, oseq)

	myLog := testutil.Logger(t)

	groupResult := &dvbeta.QueryGroupResponse{}
	groupResult.Group.ID = scaffold.groupID
	groupResult.Group.GroupSpec = volumeGroupSpec()

	makeMocks(&scaffold, groupResult)

	scaffold.testAddr = testutil.AccAddress(t)

	myProvider := &ptypes.Provider{
		Owner:      scaffold.testAddr.String(),
		HostURI:    "",
		Attributes: nil,
	}
	mySession := session.New(myLog, scaffold.client, myProvider, testBidCreatedAt)

	scaffold.testBus = pubsub.NewBus()

	cfg := Config{
		PricingStrategy: testBidPricingStrategy(1),
		Deposit:         mvbeta.DefaultBidMinDeposit,
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
		Volumes:         defaultVolumeConfig(),
	}

	if configure != nil {
		configure(&cfg)
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, fromctx.CtxKeyPubSub, tpubsub.New(ctx, 1000))
	myService, err := NewService(ctx, scaffold.queryClient, mySession, scaffold.cluster, scaffold.testBus, waiter.NewNullWaiter(), cfg)
	require.NoError(t, err)
	require.NotNil(t, myService)

	serviceCast := myService.(*service)

	// seed live per-class headroom as the retained snapshot would
	serviceCast.storageInv.update(&provider.Inventory{
		Cluster: inventoryV1.Cluster{
			Storage: inventoryV1.ClusterStorage{
				{
					Quantity: inventoryV1.NewResourcePair(100*unit.Gi, 100*unit.Gi, 0, resource.DecimalSI),
					Info:     inventoryV1.StorageInfo{Class: testVolumeClass},
				},
			},
		},
	})

	if checkForExistingBid {
		bidID := mtypes.MakeBidID(scaffold.orderID, mySession.Provider().Address())
		scaffold.bidID = &bidID
		queryBidRequest := &mvbeta.QueryBidRequest{
			ID: bidID,
		}
		response := &mvbeta.QueryBidResponse{
			Bid: mvbeta.Bid{
				ID:        bidID,
				State:     bidState,
				Price:     sdk.NewInt64DecCoin(sdkutil.DenomUact, int64(testutil.RandRangeInt(100, 1000))),
				CreatedAt: testBidCreatedAt,
			},
		}
		scaffold.marketMocks.On("Bid", mock.Anything, queryBidRequest).Return(response, nil)
	}

	reservationFulfilledNotify := make(chan int, 1)
	order, err := newOrderInternal(serviceCast, scaffold.orderID, cfg, fixedAttrSignatureService{attrs: volumeCapabilities()}, checkForExistingBid, reservationFulfilledNotify)

	require.NoError(t, err)
	require.NotNil(t, order)

	return order, scaffold, reservationFulfilledNotify
}

func Test_BidVolumeOrderAndUnreserve(t *testing.T) {
	order, scaffold, _ := makeVolumeOrderForTest(t, false, mvbeta.BidStateInvalid)

	broadcast := testutil.ChannelWaitForValue(t, scaffold.broadcasts)

	// Should have called reserve once
	scaffold.cluster.AssertCalled(t, "Reserve", scaffold.orderID, mock.Anything)

	createBidMsg := requireMsgType[*mvbeta.MsgCreateBid](t, broadcast)
	require.Equal(t, createBidMsg.ID.OrderID(), scaffold.orderID)

	// the bid states the volume policy it honors, which the chain requires
	require.Equal(t, volumeGroupSpec().Volume, createBidMsg.VolumePolicy)

	order.lc.Shutdown(nil)

	// Should have called unreserve once, nothing happened after the bid
	scaffold.cluster.AssertCalled(t, "Unreserve", scaffold.orderID, mock.Anything)
}

func Test_BidVolumeOrderAndThenLeaseCreated(t *testing.T) {
	order, scaffold, _ := makeVolumeOrderForTest(t, false, mvbeta.BidStateInvalid)

	// Wait for the bid
	broadcast := testutil.ChannelWaitForValue(t, scaffold.broadcasts)
	createBidMsg := requireMsgType[*mvbeta.MsgCreateBid](t, broadcast)
	require.Equal(t, createBidMsg.ID.OrderID(), scaffold.orderID)

	subscriber, err := scaffold.testBus.Subscribe()
	require.NoError(t, err)

	leaseID := mtypes.MakeLeaseID(mtypes.MakeBidID(scaffold.orderID, scaffold.testAddr))

	ev := &mtypes.EventLeaseCreated{
		ID:    leaseID,
		Price: testutil.AkashDecCoin(t, 1),
	}

	err = scaffold.testBus.Publish(ev)
	require.NoError(t, err)

	// The win handoff must be VolumeLeaseWon — a volume lease has no
	// manifest — and never LeaseWon.
	var wonEv event.VolumeLeaseWon
waitLoop:
	for {
		select {
		case rawEv := <-subscriber.Events():
			switch rawEv := rawEv.(type) {
			case event.VolumeLeaseWon:
				wonEv = rawEv
				break waitLoop
			case event.LeaseWon:
				t.Fatal("volume lease published as LeaseWon")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for VolumeLeaseWon")
		}
	}

	require.Equal(t, leaseID, wonEv.LeaseID)
	require.NotNil(t, wonEv.Group)
	require.NotNil(t, wonEv.Group.GroupSpec.Volume)

	// Wait for the order to stop on its own
	<-order.lc.Done()

	// Reservation is deliberately kept on win
	scaffold.cluster.AssertNotCalled(t, "Unreserve", mock.Anything, mock.Anything)
}

func Test_ShouldNotBidVolumeOrderWhenAlreadySet(t *testing.T) {
	// checkForExistingBid recovery covers volume bids: the existing open
	// bid is found, resources re-reserved, and no new bid is broadcast.
	order, scaffold, reservationFulfilledNotify := makeVolumeOrderForTest(t, true, mvbeta.BidOpen)

	// Wait for a reserve call
	testutil.ChannelWaitForValue(t, scaffold.reserveCallNotify)

	// Should have queried for the bid
	scaffold.marketMocks.AssertCalled(t, "Bid", mock.Anything, &mvbeta.QueryBidRequest{ID: *scaffold.bidID})

	// Should have called reserve once
	scaffold.cluster.AssertCalled(t, "Reserve", scaffold.orderID, mock.Anything)

	// Wait for the reservation to be processed
	testutil.ChannelWaitForValue(t, reservationFulfilledNotify)

	// Close the order
	ev := &mtypes.EventOrderClosed{
		ID: scaffold.orderID,
	}

	err := scaffold.testBus.Publish(ev)
	require.NoError(t, err)

	// Wait for it to stop
	<-order.lc.Done()

	// Should have called unreserve during shutdown
	scaffold.cluster.AssertCalled(t, "Unreserve", scaffold.orderID, mock.Anything)

	var broadcast []sdk.Msg
	select {
	case broadcast = <-scaffold.broadcasts:
	default:
	}

	// The only broadcast is the close of the recovered bid
	closeBid := requireMsgType[*mvbeta.MsgCloseBid](t, broadcast)
	require.Equal(t, closeBid.ID, *scaffold.bidID)
}

func Test_RelistedVolumeBidOutlivesTheBidTimeout(t *testing.T) {
	order, scaffold, _ := makeVolumeOrderForTestWith(t, false, mvbeta.BidStateInvalid, 2, func(cfg *Config) {
		cfg.BidTimeout = 100 * time.Millisecond
		cfg.VolumeMoveBidTimeout = time.Hour
	})

	_ = requireMsgType[*mvbeta.MsgCreateBid](t, testutil.ChannelWaitForValue(t, scaffold.broadcasts))

	select {
	case broadcast := <-scaffold.broadcasts:
		t.Fatalf("the bid on a moved volume closed after the compute bid timeout: %v", broadcast)
	case <-time.After(time.Second):
	}

	order.lc.Shutdown(nil)
	<-order.lc.Done()
}

func Test_RelistedVolumeBidClosesAfterTheMoveBidTimeout(t *testing.T) {
	order, scaffold, _ := makeVolumeOrderForTestWith(t, false, mvbeta.BidStateInvalid, 2, func(cfg *Config) {
		cfg.BidTimeout = time.Hour
		cfg.VolumeMoveBidTimeout = 100 * time.Millisecond
	})

	_ = requireMsgType[*mvbeta.MsgCreateBid](t, testutil.ChannelWaitForValue(t, scaffold.broadcasts))
	_ = requireMsgType[*mvbeta.MsgCloseBid](t, testutil.ChannelWaitForValue(t, scaffold.broadcasts))

	<-order.lc.Done()
}

func Test_FirstVolumeOrderBidTakesTheBidTimeout(t *testing.T) {
	order, scaffold, _ := makeVolumeOrderForTestWith(t, false, mvbeta.BidStateInvalid, 1, func(cfg *Config) {
		cfg.BidTimeout = 100 * time.Millisecond
		cfg.VolumeMoveBidTimeout = time.Hour
	})

	_ = requireMsgType[*mvbeta.MsgCreateBid](t, testutil.ChannelWaitForValue(t, scaffold.broadcasts))
	_ = requireMsgType[*mvbeta.MsgCloseBid](t, testutil.ChannelWaitForValue(t, scaffold.broadcasts))

	<-order.lc.Done()
}

func Test_RelistedVolumeBidFoundAfterRestartKeepsTheMoveBidTimeout(t *testing.T) {
	order, scaffold, reservationFulfilledNotify := makeVolumeOrderForTestWith(t, true, mvbeta.BidOpen, 2, func(cfg *Config) {
		cfg.BidTimeout = time.Nanosecond
		cfg.VolumeMoveBidTimeout = time.Hour
	})

	testutil.ChannelWaitForValue(t, reservationFulfilledNotify)

	select {
	case broadcast := <-scaffold.broadcasts:
		t.Fatalf("an open bid on a moved volume took the compute bid timeout: %v", broadcast)
	case <-time.After(500 * time.Millisecond):
	}

	order.lc.Shutdown(nil)
	<-order.lc.Done()
}

// replicaShouldBid runs the volume gates on a replica order of the volume
// group primary, on chain.
func replicaShouldBid(t *testing.T, primary dv1.VolumeRef, chain *volumeChain) (*order, bool, error) {
	t.Helper()

	gspec := volumeGroupSpec()
	gspec.Volume.ReplicaOf = &primary

	cfg := Config{
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
		Volumes:         defaultVolumeConfig(),
	}
	cfg.Volumes.MaxReplicas = 4

	order := makeShouldBidOrderOnChain(t, cfg, fixedAttrSignatureService{attrs: volumeCapabilities()},
		fakeStorageInventory{testVolumeClass: 100 * unit.Gi}, &fakeVolumeLookup{}, chain)

	shouldBid, err := order.shouldBidVolume(context.Background(), volumeGroupForTest(t, gspec))

	return order, shouldBid, err
}

// Test_ShouldNotBidReplicaBesideItsPrimary: a replica on the provider that
// holds its primary adds no redundancy and could never sync.
func Test_ShouldNotBidReplicaBesideItsPrimary(t *testing.T) {
	primary := dv1.VolumeRef{Owner: testutil.AccAddress(t).String(), DSeq: 5, GSeq: 1, Name: "data"}

	chain := newVolumeChain([]mvbeta.QueryLeaseResponse{{Lease: mtypes.Lease{State: mtypes.LeaseActive}}}, nil)

	order, shouldBid, err := replicaShouldBid(t, primary, chain)
	require.NoError(t, err)
	require.False(t, shouldBid)

	// asked for this provider's active lease on the very group named
	chain.market.AssertCalled(t, "Leases", mock.Anything, &mvbeta.QueryLeasesRequest{
		Filters: mtypes.LeaseFilters{
			Owner:    primary.Owner,
			DSeq:     primary.DSeq,
			GSeq:     primary.GSeq,
			Provider: order.session.Provider().Address().String(),
			State:    mtypes.LeaseActive.String(),
		},
	})
}

func Test_ShouldBidReplicaOfAPrimaryLeasedElsewhere(t *testing.T) {
	primary := dv1.VolumeRef{Owner: testutil.AccAddress(t).String(), DSeq: 5, GSeq: 1, Name: "data"}

	_, shouldBid, err := replicaShouldBid(t, primary, newVolumeChain(nil, nil))
	require.NoError(t, err)
	require.True(t, shouldBid)
}

func Test_ShouldNotBidReplicaWhileTheChainCannotSayWhereItsPrimaryIs(t *testing.T) {
	primary := dv1.VolumeRef{Owner: testutil.AccAddress(t).String(), DSeq: 5, GSeq: 1, Name: "data"}

	_, shouldBid, err := replicaShouldBid(t, primary, newVolumeChain(nil, errors.New("node unreachable")))
	require.Error(t, err)
	require.False(t, shouldBid)
}

func Test_ShouldBidVolumeWithoutAskingWhereAPrimaryIs(t *testing.T) {
	chain := newVolumeChain(nil, nil)

	cfg := Config{
		MaxGroupVolumes: constants.DefaultMaxGroupVolumes,
		Volumes:         defaultVolumeConfig(),
	}

	order := makeShouldBidOrderOnChain(t, cfg, fixedAttrSignatureService{attrs: volumeCapabilities()},
		fakeStorageInventory{testVolumeClass: 100 * unit.Gi}, &fakeVolumeLookup{}, chain)

	shouldBid, err := order.shouldBidVolume(context.Background(), volumeGroupForTest(t, volumeGroupSpec()))
	require.NoError(t, err)
	require.True(t, shouldBid)

	chain.market.AssertNotCalled(t, "Leases", mock.Anything, mock.Anything)
}
