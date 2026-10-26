package provider

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	dtypes "pkg.akt.dev/go/node/deployment/v1beta4"
	"pkg.akt.dev/go/testutil"
	"pkg.akt.dev/go/util/pubsub"

	"github.com/akash-network/provider/event"
	clustermocks "github.com/akash-network/provider/mocks/cluster"
	"github.com/akash-network/provider/session"
)

func TestVolumeProvisionerRecordsResize(t *testing.T) {
	lid := testutil.LeaseID(t)
	group := &dtypes.Group{ID: lid.GroupID()}

	bus := pubsub.NewBus()
	defer bus.Close()

	recorded := make(chan *dtypes.Group, 1)

	cclient := clustermocks.NewClient(t)
	cclient.On("DeployVolume", mock.Anything, lid, group).Run(func(args mock.Arguments) {
		recorded <- args.Get(2).(*dtypes.Group)
	}).Return(nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vp, err := newVolumeProvisioner(ctx, session.New(testutil.Logger(t), nil, nil, 0), bus, cclient)
	require.NoError(t, err)

	require.NoError(t, bus.Publish(event.VolumeLeaseResized{LeaseID: lid, Group: group}))
	require.Equal(t, group, testutil.ChannelWaitForValue(t, recorded))

	require.NoError(t, vp.Close())
}
