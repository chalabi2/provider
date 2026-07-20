package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tpubsub "github.com/troian/pubsub"
	"k8s.io/client-go/kubernetes"
	kfake "k8s.io/client-go/kubernetes/fake"

	manifest "pkg.akt.dev/go/manifest/v2beta4"
	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dvbeta "pkg.akt.dev/go/node/deployment/v1beta5"
	attrtypes "pkg.akt.dev/go/node/types/attributes/v1"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"
	"pkg.akt.dev/go/node/types/unit"
	"pkg.akt.dev/go/testutil"
	"pkg.akt.dev/go/util/pubsub"

	ctypes "github.com/akash-network/provider/cluster/types/v1beta3"
	cinventory "github.com/akash-network/provider/cluster/types/v1beta3/clients/inventory"
	cfromctx "github.com/akash-network/provider/cluster/types/v1beta3/fromctx"
	"github.com/akash-network/provider/event"
	cmocks "github.com/akash-network/provider/mocks/cluster"
	"github.com/akash-network/provider/operator/waiter"
	aclient "github.com/akash-network/provider/pkg/client/clientset/versioned"
	afake "github.com/akash-network/provider/pkg/client/clientset/versioned/fake"
	"github.com/akash-network/provider/tools/fromctx"
)

const volumeGroupNameForTest = "volume-group"

// makeVolumeGroupForTest builds an AEP-87 storage-only GroupSpec: typed
// VolumePolicy, present-but-zero compute legs, exactly one storage entry.
func makeVolumeGroupForTest(size uint64) dvbeta.GroupSpec {
	return dvbeta.GroupSpec{
		Name: volumeGroupNameForTest,
		Volume: &dv1.VolumePolicy{
			Vid:            "myapp-pgdata",
			Reclaim:        dv1.VolumeReclaimRetain,
			Retention:      168 * time.Hour,
			MaxAttachments: 1,
		},
		Resources: dvbeta.ResourceUnits{
			{
				Resources: rtypes.Resources{
					ID: 1,
					CPU: &rtypes.CPU{
						Units: rtypes.NewResourceValue(0),
					},
					GPU: &rtypes.GPU{
						Units: rtypes.NewResourceValue(0),
					},
					Memory: &rtypes.Memory{
						Quantity: rtypes.NewResourceValue(0),
					},
					Storage: rtypes.Volumes{
						{
							Name:     "myapp-pgdata",
							Quantity: rtypes.NewResourceValue(size),
							Attributes: attrtypes.Attributes{
								{Key: "class", Value: "beta2"},
								{Key: "persistent", Value: "true"},
							},
						},
					},
					Endpoints: rtypes.Endpoints{},
				},
				Count: 1,
			},
		},
	}
}

func TestReservationKinds(t *testing.T) {
	lid := testutil.LeaseID(t)

	volume := newReservation(lid.OrderID(), makeVolumeGroupForTest(1*unit.Gi))
	require.Equal(t, ctypes.ReservationKindDurable, volume.Kind())

	compute := newReservation(lid.OrderID(), makeGroupForInventoryTest(false, false, false))
	require.Equal(t, ctypes.ReservationKindLease, compute.Kind())
}

func volumeInventoryServiceForTest(t *testing.T, volumes []ctypes.VolumeDeployment) (*inventoryService, pubsub.Bus, context.CancelFunc) {
	t.Helper()

	config := Config{
		InventoryResourcePollPeriod:     time.Second,
		InventoryResourceDebugFrequency: 1,
		InventoryExternalPortQuantity:   1000,
	}

	myLog := testutil.Logger(t)
	bus := pubsub.NewBus()
	subscriber, err := bus.Subscribe()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, fromctx.CtxKeyPubSub, tpubsub.New(ctx, 1000))
	ctx = context.WithValue(ctx, fromctx.CtxKeyKubeClientSet, kubernetes.Interface(kfake.NewClientset()))
	ctx = context.WithValue(ctx, fromctx.CtxKeyAkashClientSet, aclient.Interface(afake.NewClientset()))
	ctx = context.WithValue(ctx, cfromctx.CtxKeyClientInventory, cinventory.NewNull(ctx, "nodeA"))

	inv, err := newInventoryService(
		ctx,
		config,
		myLog,
		subscriber,
		&cmocks.Client{},
		waiter.NewNullWaiter(), // Do not need to wait in test
		make([]ctypes.IDeployment, 0),
		volumes)
	require.NoError(t, err)
	require.NotNil(t, inv)

	return inv, bus, cancel
}

func waitForActiveReservations(t *testing.T, inv *inventoryService, count int) {
	t.Helper()

	for {
		status, err := inv.status(context.Background())
		require.NoError(t, err)

		if len(status.Active) >= count {
			return
		}

		time.Sleep(time.Second / 2)
	}
}

// TestInventory_DurableReservationLifecycle covers the AEP-87 accounting
// rules end to end: exact (uncommitted) storage quantities, the bid-loss
// unreserve path, and the exemption from unreserve-on-lease-close once the
// volume is provisioned.
func TestInventory_DurableReservationLifecycle(t *testing.T) {
	lid := testutil.LeaseID(t)

	inv, bus, cancel := volumeInventoryServiceForTest(t, nil)
	defer func() {
		cancel()
		<-inv.lc.Done()
	}()
	defer bus.Close()

	group := makeVolumeGroupForTest(1 * unit.Gi)

	reservation, err := inv.reserve(lid.OrderID(), group)
	require.NoError(t, err)
	require.NotNil(t, reservation)
	require.Equal(t, ctypes.ReservationKindDurable, reservation.Kind())

	// durable leased bytes are never thin-sold: the committed quantity is
	// EXACTLY the ordered quantity, no commit-level scaling
	runits := reservation.Resources().GetResourceUnits()
	require.Len(t, runits, 1)
	require.Equal(t, uint64(1*unit.Gi), runits[0].Storage[0].Quantity.Value())

	// bid lost: a pending (not yet provisioned) durable reservation
	// unreserves like any other
	require.NoError(t, inv.unreserve(lid.OrderID()))

	// re-reserve and provision (the volume provisioner announces the
	// deployment after recording the Volume CRD)
	_, err = inv.reserve(lid.OrderID(), group)
	require.NoError(t, err)

	err = bus.Publish(event.ClusterDeployment{
		LeaseID: lid,
		Group: &manifest.Group{
			Name: volumeGroupNameForTest,
		},
		Status: event.ClusterDeploymentDeployed,
	})
	require.NoError(t, err)

	waitForActiveReservations(t, inv, 1)

	// the volume lease closes: the durable reservation is exempt from
	// unreserve-on-lease-close and stays allocated
	err = inv.unreserve(lid.OrderID())
	require.ErrorIs(t, err, errReservationDurable)

	status, err := inv.status(context.Background())
	require.NoError(t, err)
	require.Len(t, status.Active, 1)
}

// TestInventory_DurableReservationRebuild pins the restart path: durable
// reservations are rebuilt from Volume CRDs (via the cluster client's
// DeployedVolumes) as already-allocated entries and keep their exemption.
func TestInventory_DurableReservationRebuild(t *testing.T) {
	lid := testutil.LeaseID(t)

	volumes := []ctypes.VolumeDeployment{
		{
			LeaseID: lid,
			Group:   makeVolumeGroupForTest(1 * unit.Gi),
		},
	}

	inv, bus, cancel := volumeInventoryServiceForTest(t, volumes)
	defer func() {
		cancel()
		<-inv.lc.Done()
	}()
	defer bus.Close()

	waitForActiveReservations(t, inv, 1)

	// still exempt from unreserve-on-lease-close after the rebuild
	err := inv.unreserve(lid.OrderID())
	require.ErrorIs(t, err, errReservationDurable)

	res, err := inv.lookup(lid.OrderID(), makeVolumeGroupForTest(1*unit.Gi))
	require.NoError(t, err)
	require.Equal(t, ctypes.ReservationKindDurable, res.Kind())
	require.True(t, res.Allocated())
}
