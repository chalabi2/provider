package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	manifest "pkg.akt.dev/go/manifest/v2beta4"
	dvbeta "pkg.akt.dev/go/node/deployment/v1beta5"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"
	"pkg.akt.dev/go/node/types/unit"
	"pkg.akt.dev/go/testutil"

	ctypes "github.com/akash-network/provider/cluster/types/v1beta3"
	"github.com/akash-network/provider/event"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

const resizeGroupNameForTest = "compute-group"

// resizeGroupForTest is a single-unit compute group; randomPorts endpoints
// take external ports while the reservation is allocated.
func resizeGroupForTest(cpu, gpu uint64, randomPorts int) dvbeta.GroupSpec {
	endpoints := make(rtypes.Endpoints, randomPorts)
	for i := range endpoints {
		endpoints[i].Kind = rtypes.Endpoint_RANDOM_PORT
	}

	return dvbeta.GroupSpec{
		Name: resizeGroupNameForTest,
		Resources: dvbeta.ResourceUnits{
			{
				Resources: rtypes.Resources{
					ID:     1,
					CPU:    &rtypes.CPU{Units: rtypes.NewResourceValue(cpu)},
					GPU:    &rtypes.GPU{Units: rtypes.NewResourceValue(gpu)},
					Memory: &rtypes.Memory{Quantity: rtypes.NewResourceValue(unit.Gi)},
					Storage: rtypes.Volumes{
						{Name: "default", Quantity: rtypes.NewResourceValue(unit.Gi)},
					},
					Endpoints: endpoints,
				},
				Count: 1,
			},
		},
	}
}

func TestInventory_ResizeCheckLeavesReservation(t *testing.T) {
	lid := testutil.LeaseID(t)

	inv, bus, cancel := volumeInventoryServiceForTest(t, nil)
	defer func() {
		cancel()
		<-inv.lc.Done()
	}()
	defer bus.Close()

	group := resizeGroupForTest(1000, 0, 0)

	_, err := inv.reserve(lid.OrderID(), group)
	require.NoError(t, err)

	allocated, err := inv.checkResize(lid.OrderID(), resizeGroupForTest(2000, 0, 0))
	require.NoError(t, err)
	require.Len(t, allocated, 1)
	require.Equal(t, uint64(2000), allocated[0].CPU.Units.Value())

	res, err := inv.lookup(lid.OrderID(), group)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), res.Resources().GetResourceUnits()[0].CPU.Units.Value())
	require.Equal(t, uint64(1000), res.GetAllocatedResources()[0].CPU.Units.Value())

	// the node has 4900m free and the reservation holds 1000m of it. 4000m
	// would fit in place of the old size, but the old size is not credited
	// back, so it has to fit next to it.
	_, err = inv.checkResize(lid.OrderID(), resizeGroupForTest(4000, 0, 0))
	require.ErrorIs(t, err, ctypes.ErrInsufficientCapacity)

	_, err = inv.checkResize(testutil.LeaseID(t).OrderID(), resizeGroupForTest(2000, 0, 0))
	require.ErrorIs(t, err, ErrReservationNotFound)
}

func TestInventory_ResizeSwapsLeaseReservation(t *testing.T) {
	lid := testutil.LeaseID(t)

	inv, bus, cancel := volumeInventoryServiceForTest(t, nil)
	defer func() {
		cancel()
		<-inv.lc.Done()
	}()
	defer bus.Close()

	group := resizeGroupForTest(1000, 0, 1)

	_, err := inv.reserve(lid.OrderID(), group)
	require.NoError(t, err)

	deployed := event.ClusterDeployment{
		LeaseID: lid,
		Group:   &manifest.Group{Name: resizeGroupNameForTest},
		Status:  event.ClusterDeploymentDeployed,
	}

	require.NoError(t, bus.Publish(deployed))
	waitForActiveReservations(t, inv, 1)
	require.Equal(t, uint(999), inv.availableExternalPorts)

	require.NoError(t, inv.resize(lid.OrderID(), resizeGroupForTest(2000, 0, 1)))

	res, err := inv.lookup(lid.OrderID(), group)
	require.NoError(t, err)
	require.Equal(t, ctypes.ReservationKindLease, res.Kind())
	require.Equal(t, uint64(2000), res.Resources().GetResourceUnits()[0].CPU.Units.Value())
	require.Equal(t, uint64(2000), res.GetAllocatedResources()[0].CPU.Units.Value())

	// pending until the new pods are up, with its port back in the pool
	require.False(t, res.Allocated())
	require.Equal(t, uint(1000), inv.availableExternalPorts)

	require.NoError(t, bus.Publish(deployed))
	waitForActiveReservations(t, inv, 1)
	require.Equal(t, uint(999), inv.availableExternalPorts)

	require.ErrorIs(t, inv.resize(testutil.LeaseID(t).OrderID(), group), ErrReservationNotFound)
}

// interconnectInventory stamps one interconnect unit per GPU into the
// scheduler params, as the inventory operator's Adjust does for units that
// opt into interconnect.
type interconnectInventory struct {
	statusTestInventory
	err error
}

func (inv interconnectInventory) Adjust(rg ctypes.ReservationGroup, _ ...ctypes.InventoryOption) error {
	if inv.err != nil {
		return inv.err
	}

	units := rg.Resources().GetResourceUnits()
	cparams := make(crd.ReservationClusterSettings, len(units))

	for _, ru := range units {
		cparams[ru.ID] = &crd.SchedulerParams{
			Resources: &crd.SchedulerResources{
				Interconnect: &crd.SchedulerResourceInterconnect{
					Enabled: true,
					Units:   ru.GPU.Units.Value(),
				},
			},
		}
	}

	rg.SetAllocatedResources(units)
	rg.SetClusterParams(cparams)

	return nil
}

func interconnectUnits(t *testing.T, res *reservation) uint64 {
	t.Helper()

	cparams, ok := res.ClusterParams().(crd.ReservationClusterSettings)
	require.True(t, ok)

	return cparams[1].Resources.Interconnect.Units
}

func TestInventory_ResizeRestampsClusterParams(t *testing.T) {
	lid := testutil.LeaseID(t)

	reserve := func(t *testing.T, state *inventoryServiceState) *reservation {
		t.Helper()

		res := newReservation(lid.OrderID(), resizeGroupForTest(1000, 1, 0))
		require.NoError(t, interconnectInventory{}.Adjust(res))
		res.allocated = true

		state.reservations = []*reservation{res}

		return res
	}

	resize := func(t *testing.T, state *inventoryServiceState) {
		t.Helper()

		is := &inventoryService{log: testutil.Logger(t)}

		ch := make(chan inventoryResponse, 1)
		is.handleResize(resizeRequest{
			order:     lid.OrderID(),
			resources: resizeGroupForTest(1000, 2, 0),
			ch:        ch,
		}, state)
		require.NoError(t, (<-ch).err)
	}

	t.Run("fits", func(t *testing.T) {
		state := &inventoryServiceState{inventory: interconnectInventory{}}
		res := reserve(t, state)
		require.Equal(t, uint64(1), interconnectUnits(t, res))

		resize(t, state)

		require.Equal(t, uint64(2), interconnectUnits(t, res))
		require.Equal(t, uint64(2), res.GetAllocatedResources()[0].GPU.Units.Value())
	})

	// the chain applied the resize regardless: the reservation takes the new
	// size and keeps the old params until an inventory update re-adjusts it
	t.Run("no longer fits", func(t *testing.T) {
		state := &inventoryServiceState{inventory: interconnectInventory{err: ctypes.ErrInsufficientCapacity}}
		res := reserve(t, state)

		resize(t, state)

		require.Equal(t, uint64(2), res.Resources().GetResourceUnits()[0].GPU.Units.Value())
		require.Equal(t, uint64(1), interconnectUnits(t, res))
		require.False(t, res.Allocated())
	})
}

func TestInventory_ResizeDurableReservation(t *testing.T) {
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

	// the class has 10Gi free and the volume already holds 1Gi of 11Gi:
	// only the 10Gi growth has to fit, the full size would not
	allocated, err := inv.checkResize(lid.OrderID(), makeVolumeGroupForTest(11*unit.Gi))
	require.NoError(t, err)
	require.Equal(t, uint64(11*unit.Gi), allocated[0].Storage[0].Quantity.Value())

	_, err = inv.checkResize(lid.OrderID(), makeVolumeGroupForTest(12*unit.Gi))
	require.ErrorIs(t, err, ctypes.ErrInsufficientCapacity)

	require.NoError(t, inv.resize(lid.OrderID(), makeVolumeGroupForTest(11*unit.Gi)))

	res, err := inv.lookup(lid.OrderID(), makeVolumeGroupForTest(11*unit.Gi))
	require.NoError(t, err)
	require.Equal(t, ctypes.ReservationKindDurable, res.Kind())
	require.True(t, res.Allocated())
	require.Equal(t, uint64(11*unit.Gi), res.Resources().GetResourceUnits()[0].Storage[0].Quantity.Value())

	// still released only on volume GC, not on lease close
	require.ErrorIs(t, inv.unreserve(lid.OrderID()), errReservationDurable)
}

func TestVolumeGrowth(t *testing.T) {
	held := makeVolumeGroupForTest(4 * unit.Gi)
	// a reservation rebuilt from its Volume CRD names the entry after the vid
	held.Resources[0].Storage[0].Name = "vid"

	growth := volumeGrowth(held, makeVolumeGroupForTest(10*unit.Gi))
	require.Equal(t, uint64(6*unit.Gi), growth.Resources[0].Storage[0].Quantity.Value())
	require.NotNil(t, growth.Volume)

	growth = volumeGrowth(held, makeVolumeGroupForTest(4*unit.Gi))
	require.Equal(t, uint64(0), growth.Resources[0].Storage[0].Quantity.Value())

	// the resized group itself is left alone
	next := makeVolumeGroupForTest(10 * unit.Gi)
	_ = volumeGrowth(held, next)
	require.Equal(t, uint64(10*unit.Gi), next.Resources[0].Storage[0].Quantity.Value())
}
