package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sdk "github.com/cosmos/cosmos-sdk/types"

	manifest "pkg.akt.dev/go/manifest/v2beta3"
	dvbeta "pkg.akt.dev/go/node/deployment/v1beta4"
	mtypes "pkg.akt.dev/go/node/market/v1"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"
	"pkg.akt.dev/go/node/types/unit"
	"pkg.akt.dev/go/sdkutil"
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

// manifestGroupForTest is the manifest group a tenant deploys for gspec: a
// service per resource unit, at the unit's resources and count.
func manifestGroupForTest(gspec dvbeta.GroupSpec) *manifest.Group {
	mgroup := &manifest.Group{Name: gspec.Name}

	for _, ru := range gspec.Resources {
		mgroup.Services = append(mgroup.Services, manifest.Service{
			Name:      fmt.Sprintf("svc-%d", ru.ID),
			Resources: ru.Resources,
			Count:     ru.Count,
		})
	}

	return mgroup
}

// deploymentReport is the cluster's report on a lease's deployment of the
// manifest for gspec.
func deploymentReport(lid mtypes.LeaseID, gspec dvbeta.GroupSpec, status event.ClusterDeploymentStatus) event.ClusterDeployment {
	return event.ClusterDeployment{
		LeaseID: lid,
		Group:   manifestGroupForTest(gspec),
		Status:  status,
	}
}

// withPrice is gspec with every unit priced at amount.
func withPrice(gspec dvbeta.GroupSpec, amount int64) dvbeta.GroupSpec {
	gspec = gspec.Dup()
	for i := range gspec.Resources {
		gspec.Resources[i].Price = sdk.NewInt64DecCoin(sdkutil.DenomUact, amount)
	}

	return gspec
}

// resizeHandlersForTest drives the inventory's resize and deployment
// handlers directly, on a snapshot that fits anything.
type resizeHandlersForTest struct {
	is    *inventoryService
	state *inventoryServiceState
}

func newResizeHandlersForTest(t *testing.T, config Config) *resizeHandlersForTest {
	t.Helper()

	return &resizeHandlersForTest{
		is: &inventoryService{
			config:                 config,
			log:                    testutil.Logger(t),
			availableExternalPorts: 10,
		},
		state: &inventoryServiceState{inventory: statusTestInventory{}},
	}
}

// reserve reserves gspec as a bid on its order does.
func (h *resizeHandlersForTest) reserve(t *testing.T, lid mtypes.LeaseID, gspec dvbeta.GroupSpec) *reservation {
	t.Helper()

	ch := make(chan inventoryResponse, 1)
	h.is.handleRequest(inventoryRequest{order: lid.OrderID(), resources: gspec, ch: ch}, h.state)

	resp := <-ch
	require.NoError(t, resp.err)

	return resp.value.(*reservation)
}

// resize applies a resize of the lease to gspec and reports whether the
// reservation changed.
func (h *resizeHandlersForTest) resize(t *testing.T, lid mtypes.LeaseID, gspec dvbeta.GroupSpec) bool {
	t.Helper()

	ch := make(chan inventoryResponse, 1)
	changed := h.is.handleResize(resizeRequest{order: lid.OrderID(), resources: gspec, ch: ch}, h.state)
	require.NoError(t, (<-ch).err)

	return changed
}

func (h *resizeHandlersForTest) report(res *reservation, lid mtypes.LeaseID, gspec dvbeta.GroupSpec, status event.ClusterDeploymentStatus) {
	h.is.handleDeployment(res, deploymentReport(lid, gspec, status))
}

func TestInventory_ResizeOfThePriceAloneKeepsReservation(t *testing.T) {
	lid := testutil.LeaseID(t)
	group := withPrice(resizeGroupForTest(1000, 0, 1), 10)
	repriced := withPrice(group, 20)

	requireUnchanged := func(t *testing.T, h *resizeHandlersForTest, res *reservation) {
		t.Helper()

		resources := res.Resources()

		h.report(res, lid, group, event.ClusterDeploymentDeployed)
		require.True(t, res.Allocated())
		require.Equal(t, uint(9), h.is.availableExternalPorts)

		require.False(t, h.resize(t, lid, repriced))

		require.True(t, res.Allocated(), "a resize of the price alone rolls no pods")
		require.False(t, res.awaitingManifest)
		require.Equal(t, uint(9), h.is.availableExternalPorts)
		require.Equal(t, resources, res.Resources())
	}

	// the reservation holds committed resources; the comparison is on the
	// resources as ordered
	t.Run("reserved by a bid", func(t *testing.T) {
		config := inventoryConfigForTest()
		config.CPUCommitLevel = 2
		h := newResizeHandlersForTest(t, config)

		res := h.reserve(t, lid, group)
		require.Equal(t, uint64(500), res.Resources().GetResourceUnits()[0].CPU.Units.Value())

		requireUnchanged(t, h, res)
	})

	t.Run("rebuilt from its manifest at startup", func(t *testing.T) {
		h := newResizeHandlersForTest(t, inventoryConfigForTest())

		res := newReservation(lid.OrderID(), manifestGroupForTest(group))
		h.state.reservations = append(h.state.reservations, res)

		requireUnchanged(t, h, res)
	})
}

func TestSameResources(t *testing.T) {
	twoUnits := func(cpu1, cpu2 uint64) dvbeta.ResourceUnits {
		units := resizeGroupForTest(cpu1, 0, 0).Resources
		units = append(units, resizeGroupForTest(cpu2, 0, 0).Resources...)
		units[1].ID = 2

		return units
	}

	reordered := twoUnits(1000, 2000)
	reordered[0], reordered[1] = reordered[1], reordered[0]

	recounted := twoUnits(1000, 2000)
	recounted[1].Count = 3

	renumbered := twoUnits(1000, 1000)
	renumbered[1].ID = 3

	noGPU := resizeGroupForTest(1000, 0, 0).Resources
	noGPU[0].GPU = nil

	volume := makeVolumeGroupForTest(4 * unit.Gi).Resources

	// as rebuilt from its Volume CRD, the storage is named after the vid
	rebuilt := volume.Dup()
	rebuilt[0].Storage[0].Name = "vid"

	tests := map[string]struct {
		a, b dvbeta.ResourceUnits
		same bool
	}{
		"identical":         {twoUnits(1000, 2000), twoUnits(1000, 2000), true},
		"price differs":     {withPrice(resizeGroupForTest(1000, 0, 0), 1).Resources, resizeGroupForTest(1000, 0, 0).Resources, true},
		"units reordered":   {twoUnits(1000, 2000), reordered, true},
		"unset gpu is none": {resizeGroupForTest(1000, 0, 0).Resources, noGPU, true},
		"volume renamed":    {volume, rebuilt, true},
		"cpu differs":       {twoUnits(1000, 2000), twoUnits(1000, 3000), false},
		"count differs":     {twoUnits(1000, 2000), recounted, false},
		"ids differ":        {twoUnits(1000, 1000), renumbered, false},
		"unit missing":      {twoUnits(1000, 2000), twoUnits(1000, 2000)[:1], false},
		"volume grown":      {volume, makeVolumeGroupForTest(5 * unit.Gi).Resources, false},
		"none and some":     {nil, volume, false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.same, sameResources(tc.a, tc.b))
			require.Equal(t, tc.same, sameResources(tc.b, tc.a))
		})
	}
}

// A reservation rebuilt at startup describes the manifest deployed before
// the restart. When the chain resized the lease while the provider was down,
// or while it missed the event, that manifest has the old size.
func TestInventory_ResizeOfAReservationRebuiltAtTheOldSize(t *testing.T) {
	lid := testutil.LeaseID(t)
	group := resizeGroupForTest(1000, 0, 1)
	resized := resizeGroupForTest(2000, 0, 1)

	h := newResizeHandlersForTest(t, inventoryConfigForTest())

	res := newReservation(lid.OrderID(), manifestGroupForTest(group))
	h.state.reservations = append(h.state.reservations, res)
	h.report(res, lid, group, event.ClusterDeploymentDeployed)

	require.True(t, h.resize(t, lid, resized))
	require.Equal(t, uint64(2000), res.Resources().GetResourceUnits()[0].CPU.Units.Value())
	require.False(t, res.Allocated())
	require.True(t, res.awaitingManifest)

	// a later resync finds it in step and leaves its wait alone
	require.False(t, h.resize(t, lid, resized))
	require.False(t, res.Allocated())
	require.True(t, res.awaitingManifest)
}

func TestInventory_ResizeWaitsForTheManifestAtTheNewSize(t *testing.T) {
	lid := testutil.LeaseID(t)
	group := resizeGroupForTest(1000, 0, 1)
	resized := resizeGroupForTest(2000, 0, 1)

	h := newResizeHandlersForTest(t, inventoryConfigForTest())
	res := h.reserve(t, lid, group)
	h.report(res, lid, group, event.ClusterDeploymentDeployed)

	require.True(t, h.resize(t, lid, resized))
	require.False(t, res.Allocated())
	require.True(t, res.awaitingManifest)
	require.Equal(t, uint(10), h.is.availableExternalPorts)

	// the pods from before the resize reporting in keep it pending
	h.report(res, lid, group, event.ClusterDeploymentPending)
	h.report(res, lid, group, event.ClusterDeploymentDeployed)
	require.False(t, res.Allocated())
	require.True(t, res.awaitingManifest)

	// once the manifest at the new size reaches the cluster, the reservation
	// follows its deployment as during any redeploy
	h.report(res, lid, resized, event.ClusterDeploymentUpdated)
	require.False(t, res.Allocated())
	require.False(t, res.awaitingManifest)

	h.report(res, lid, resized, event.ClusterDeploymentDeployed)
	require.True(t, res.Allocated())
	require.Equal(t, uint(9), h.is.availableExternalPorts)
}

// The tenant's manifest can roll the pods before the chain's resize reaches
// the inventory, and the deployment monitor reports only changes in status.
func TestInventory_ResizeAfterThePodsAtTheNewSizeKeepsThemAllocated(t *testing.T) {
	lid := testutil.LeaseID(t)
	group := resizeGroupForTest(1000, 0, 1)
	resized := resizeGroupForTest(2000, 0, 1)

	h := newResizeHandlersForTest(t, inventoryConfigForTest())
	res := h.reserve(t, lid, group)
	h.report(res, lid, group, event.ClusterDeploymentDeployed)

	h.report(res, lid, resized, event.ClusterDeploymentUpdated)
	h.report(res, lid, resized, event.ClusterDeploymentDeployed)
	require.True(t, res.Allocated())

	require.True(t, h.resize(t, lid, resized))
	require.Equal(t, uint64(2000), res.Resources().GetResourceUnits()[0].CPU.Units.Value())
	require.True(t, res.Allocated())
	require.False(t, res.awaitingManifest)
	require.Equal(t, uint(9), h.is.availableExternalPorts)

	// while those pods are still rolling, the reservation stays pending
	// until they report deployed, without a wait to run out
	t.Run("still rolling", func(t *testing.T) {
		h := newResizeHandlersForTest(t, inventoryConfigForTest())
		res := h.reserve(t, lid, group)
		h.report(res, lid, group, event.ClusterDeploymentDeployed)
		h.report(res, lid, resized, event.ClusterDeploymentUpdated)

		require.True(t, h.resize(t, lid, resized))
		require.False(t, res.Allocated())
		require.False(t, res.awaitingManifest)

		h.report(res, lid, resized, event.ClusterDeploymentDeployed)
		require.True(t, res.Allocated())
	})
}

func TestInventory_ResizeWaitForTheManifestRunsOut(t *testing.T) {
	const timeout = 10 * time.Minute

	lid := testutil.LeaseID(t)
	group := resizeGroupForTest(1000, 0, 1)
	resized := resizeGroupForTest(2000, 0, 1)

	config := inventoryConfigForTest()
	config.LeaseResizeManifestTimeout = timeout

	h := newResizeHandlersForTest(t, config)
	res := h.reserve(t, lid, group)
	h.report(res, lid, group, event.ClusterDeploymentDeployed)

	before := time.Now()
	require.True(t, h.resize(t, lid, resized))
	require.WithinRange(t, res.awaitingUntil, before.Add(timeout), time.Now().Add(timeout))
	require.Equal(t, res.awaitingUntil, nextManifestWaitEnd(h.state.reservations))

	require.False(t, h.is.expireManifestWaits(h.state, res.awaitingUntil.Add(-time.Second)))
	require.False(t, res.Allocated())

	// the tenant never sent the manifest: the reservation is allocated again
	// at the new size, next to the pods from before the resize
	require.True(t, h.is.expireManifestWaits(h.state, res.awaitingUntil))
	require.True(t, res.Allocated())
	require.False(t, res.awaitingManifest)
	require.True(t, nextManifestWaitEnd(h.state.reservations).IsZero())
	require.Equal(t, uint(9), h.is.availableExternalPorts)

	// a roll that comes later is followed as any redeploy is
	h.report(res, lid, resized, event.ClusterDeploymentUpdated)
	require.False(t, res.Allocated())

	h.report(res, lid, resized, event.ClusterDeploymentDeployed)
	require.True(t, res.Allocated())

	// a lease whose pods never came up has no old pods to fall back on
	t.Run("never deployed", func(t *testing.T) {
		h := newResizeHandlersForTest(t, config)
		res := h.reserve(t, lid, group)

		require.True(t, h.resize(t, lid, resized))
		require.False(t, h.is.expireManifestWaits(h.state, res.awaitingUntil))
		require.False(t, res.Allocated())
		require.False(t, res.awaitingManifest)
	})
}

func TestInventory_ResizeWaitForTheManifestTimesOut(t *testing.T) {
	lid := testutil.LeaseID(t)
	group := resizeGroupForTest(1000, 0, 1)

	config := inventoryConfigForTest()
	config.LeaseResizeManifestTimeout = 200 * time.Millisecond

	inv, bus, cancel := inventoryServiceForTest(t, config, nil)
	defer func() {
		cancel()
		<-inv.lc.Done()
	}()
	defer bus.Close()

	_, err := inv.reserve(lid.OrderID(), group)
	require.NoError(t, err)

	require.NoError(t, bus.Publish(deploymentReport(lid, group, event.ClusterDeploymentDeployed)))
	waitForActiveReservations(t, inv, 1)

	_, err = inv.resize(lid.OrderID(), resizeGroupForTest(2000, 0, 1))
	require.NoError(t, err)

	status, err := inv.status(context.Background())
	require.NoError(t, err)
	require.Empty(t, status.Active)

	require.Eventually(t, func() bool {
		status, err := inv.status(context.Background())
		return err == nil && len(status.Active) == 1
	}, 10*time.Second, 20*time.Millisecond)
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
	resized := resizeGroupForTest(2000, 0, 1)

	_, err := inv.reserve(lid.OrderID(), group)
	require.NoError(t, err)

	require.NoError(t, bus.Publish(deploymentReport(lid, group, event.ClusterDeploymentDeployed)))
	waitForActiveReservations(t, inv, 1)
	require.Equal(t, uint(999), inv.availableExternalPorts)

	changed, err := inv.resize(lid.OrderID(), resized)
	require.NoError(t, err)
	require.True(t, changed)

	res, err := inv.lookup(lid.OrderID(), group)
	require.NoError(t, err)
	require.Equal(t, ctypes.ReservationKindLease, res.Kind())
	require.Equal(t, uint64(2000), res.Resources().GetResourceUnits()[0].CPU.Units.Value())
	require.Equal(t, uint64(2000), res.GetAllocatedResources()[0].CPU.Units.Value())

	// pending until the new pods are up, with its port back in the pool
	require.False(t, res.Allocated())
	require.Equal(t, uint(1000), inv.availableExternalPorts)

	require.NoError(t, bus.Publish(deploymentReport(lid, resized, event.ClusterDeploymentDeployed)))
	waitForActiveReservations(t, inv, 1)
	require.Equal(t, uint(999), inv.availableExternalPorts)

	// a reservation already in step with the group is left as it is
	changed, err = inv.resize(lid.OrderID(), resized)
	require.NoError(t, err)
	require.False(t, changed)

	_, err = inv.resize(testutil.LeaseID(t).OrderID(), group)
	require.ErrorIs(t, err, ErrReservationNotFound)
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

	changed, err := inv.resize(lid.OrderID(), makeVolumeGroupForTest(11*unit.Gi))
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = inv.resize(lid.OrderID(), makeVolumeGroupForTest(11*unit.Gi))
	require.NoError(t, err)
	require.False(t, changed)

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
