package manifest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	tpubsub "github.com/troian/pubsub"

	maniv2beta2 "pkg.akt.dev/go/manifest/v2beta4"
	clientmocks "pkg.akt.dev/go/mocks/node/client"
	deploymentmocks "pkg.akt.dev/go/mocks/node/client/deployment"
	marketmocks "pkg.akt.dev/go/mocks/node/client/market"
	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dvbeta "pkg.akt.dev/go/node/deployment/v1beta5"
	mtypes "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v2beta1"
	"pkg.akt.dev/go/testutil"
	"pkg.akt.dev/go/util/pubsub"

	"github.com/akash-network/provider/event"
	"github.com/akash-network/provider/session"
	"github.com/akash-network/provider/tools/fromctx"
	"github.com/akash-network/provider/tools/logging/logtest"
)

// managerLogged returns the lease IDs the deployment manager logged msg for.
func managerLogged(rec *logtest.Recorder, msg string) []mtypes.LeaseID {
	var lids []mtypes.LeaseID

	for _, e := range rec.Entries() {
		if e.Msg != msg || e.KV["module"] != "manifest-manager" {
			continue
		}

		if lid, ok := e.KV["lid"].(mtypes.LeaseID); ok {
			lids = append(lids, lid)
		}
	}

	return lids
}

// A resize rewrites the group under the deployment data the manager caches,
// so the manifest that follows it must be checked against freshly fetched
// groups, not the cache.
func TestManagerRefetchesAfterLeaseResize(t *testing.T) {
	provider := testutil.Provider(t)
	lid := testutil.LeaseID(t)
	lid.Provider = provider.Owner
	did := lid.DeploymentID()

	group := dvbeta.Group{
		ID:        lid.GroupID(),
		GroupSpec: dvbeta.GroupSpec{Name: "group"},
	}

	deployments := &deploymentmocks.QueryClient{}
	deployments.On("Deployment", mock.Anything, &dvbeta.QueryDeploymentRequest{ID: did}).Return(&dvbeta.QueryDeploymentResponse{
		Deployment: dv1.Deployment{ID: did},
		Groups:     []dvbeta.Group{group},
	}, nil)

	market := &marketmocks.QueryClient{}
	market.On("Leases", mock.Anything, mock.Anything).Return(&mvbeta.QueryLeasesResponse{
		Leases: []mvbeta.QueryLeaseResponse{{Lease: mtypes.Lease{ID: lid, State: mtypes.LeaseActive}}},
	}, nil)

	query := &clientmocks.QueryClient{}
	query.On("Deployment").Return(deployments)
	query.On("Market").Return(market)

	client := &clientmocks.Client{}
	client.On("Query").Return(query)

	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, fromctx.CtxKeyPubSub, tpubsub.New(ctx, 1000))

	bus := pubsub.NewBus()
	defer bus.Close()

	rec := logtest.New()

	svc, err := NewService(ctx, session.New(rec, client, &provider, 0), bus, nil, ServiceConfig{
		RPCQueryTimeout:    time.Minute,
		CachedResultMaxAge: time.Hour,
	})
	require.NoError(t, err)

	defer func() {
		cancel()
		<-svc.Done()
	}()

	// invalid, so Submit returns as soon as the manager has its data
	mani := maniv2beta2.Manifest{{
		Name:     group.GroupSpec.Name,
		Services: []maniv2beta2.Service{{Name: "svc", Image: "image", Count: 1}},
	}}

	require.NoError(t, bus.Publish(event.LeaseWon{LeaseID: lid, Group: &group}))
	require.Eventually(t, func() bool {
		return len(managerLogged(rec, "new lease")) == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Error(t, svc.Submit(ctx, did, mani))
	require.Error(t, svc.Submit(ctx, did, mani))
	deployments.AssertNumberOfCalls(t, "Deployment", 1)

	other := lid
	other.Provider = testutil.AccAddress(t).String()

	require.NoError(t, bus.Publish(&mtypes.EventLeaseResized{ID: other}))
	require.NoError(t, bus.Publish(&mtypes.EventLeaseResized{ID: lid}))

	// the manager drops its data while handling the resize, before it takes
	// the next manifest
	require.Eventually(t, func() bool {
		return len(managerLogged(rec, "lease resized")) != 0
	}, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, []mtypes.LeaseID{lid}, managerLogged(rec, "lease resized"))

	require.Error(t, svc.Submit(ctx, did, mani))
	deployments.AssertNumberOfCalls(t, "Deployment", 2)
}
