package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/boz/go-lifecycle"
	"github.com/desertbit/timer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	tpubsub "github.com/troian/pubsub"
	"k8s.io/apimachinery/pkg/api/resource"

	"cosmossdk.io/log"

	sdk "github.com/cosmos/cosmos-sdk/types"

	inventoryV1 "pkg.akt.dev/go/inventory/v1"
	dtypes "pkg.akt.dev/go/node/deployment/v1beta4"
	mtypes "pkg.akt.dev/go/node/market/v1"
	atypes "pkg.akt.dev/go/node/types/attributes/v1"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"
	provider "pkg.akt.dev/go/provider/v1"

	sdlutil "pkg.akt.dev/go/sdl/util"
	"pkg.akt.dev/go/util/pubsub"
	"pkg.akt.dev/node/v2/util/runner"

	ctypes "github.com/akash-network/provider/cluster/types/v1beta3"
	cinventory "github.com/akash-network/provider/cluster/types/v1beta3/clients/inventory"
	cip "github.com/akash-network/provider/cluster/types/v1beta3/clients/ip"
	cfromctx "github.com/akash-network/provider/cluster/types/v1beta3/fromctx"
	"github.com/akash-network/provider/event"
	"github.com/akash-network/provider/operator/waiter"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	"github.com/akash-network/provider/tools/fromctx"
	ptypes "github.com/akash-network/provider/types"
)

var (
	// ErrReservationNotFound reports that no reservation is held for the order.
	ErrReservationNotFound = errors.New("reservation not found")
	// errReservationDurable reports an unreserve that was declined because
	// the reservation is durable (storage-market volume) and allocated: the leased
	// bytes outlive the lease and are released only on volume GC.
	errReservationDurable       = errors.New("reservation is durable; released only on volume GC")
	errInventoryNotAvailableYet = errors.New("inventory status not available yet")
	errInventoryReservation     = errors.New("inventory error")
	errNoLeasedIPsAvailable     = fmt.Errorf("%w: no leased IPs available", errInventoryReservation)
	errInsufficientIPs          = fmt.Errorf("%w: insufficient number of IPs", errInventoryReservation)

	inventoryStatusQuantityZero = resource.NewQuantity(0, resource.DecimalSI)
)

var (
	inventoryRequestsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "provider_inventory_requests",
		Help:        "",
		ConstLabels: nil,
	}, []string{"action", "result"})

	inventoryReservations = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "provider_inventory_reservations_total",
		Help: "",
	}, []string{"classification", "quantity"})

	clusterInventoryAllocatable = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "provider_inventory_allocatable_total",
		Help: "",
	}, []string{"quantity"})

	clusterInventoryAvailable = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "provider_inventory_available_total",
		Help: "",
	}, []string{"quantity"})
)

type invSnapshotResp struct {
	res *provider.Inventory
	err error
}

type inventoryRequest struct {
	order     mtypes.OrderID
	resources dtypes.ResourceGroup
	ch        chan<- inventoryResponse
}

// resizeRequest moves the reservation held for an order to the resources of
// a lease resize. A dry run only checks the new size against the current
// snapshot and leaves the reservation untouched.
type resizeRequest struct {
	order     mtypes.OrderID
	resources dtypes.ResourceGroup
	dryRun    bool
	ch        chan<- inventoryResponse
}

type inventoryResponse struct {
	value ctypes.Reservation
	// changed reports, for a resize, whether the reservation changed
	changed bool
	err     error
}

type inventoryService struct {
	config                 Config
	client                 Client
	sub                    pubsub.Subscriber
	statusch               chan chan<- inventoryV1.InventoryMetrics
	statusV1ch             chan chan<- invSnapshotResp
	lookupch               chan inventoryRequest
	reservech              chan inventoryRequest
	unreservech            chan inventoryRequest
	resizech               chan resizeRequest
	reservationCount       int64
	readych                chan struct{}
	log                    log.Logger
	lc                     lifecycle.Lifecycle
	waiter                 waiter.OperatorWaiter
	availableExternalPorts uint
	teePlatform            ctypes.TEEPlatform

	clients struct {
		ip        cip.Client
		inventory cinventory.Client
	}
}

func newInventoryService(
	ctx context.Context,
	config Config,
	log log.Logger,
	sub pubsub.Subscriber,
	client Client,
	waiter waiter.OperatorWaiter,
	deployments []ctypes.IDeployment,
	volumes []ctypes.VolumeDeployment,
) (*inventoryService, error) {
	sub, err := sub.Clone()
	if err != nil {
		return nil, err
	}

	is := &inventoryService{
		config:                 config,
		client:                 client,
		sub:                    sub,
		statusch:               make(chan chan<- inventoryV1.InventoryMetrics),
		statusV1ch:             make(chan chan<- invSnapshotResp),
		lookupch:               make(chan inventoryRequest),
		reservech:              make(chan inventoryRequest),
		unreservech:            make(chan inventoryRequest),
		resizech:               make(chan resizeRequest),
		readych:                make(chan struct{}),
		log:                    log.With("module", "inventory-service"),
		lc:                     lifecycle.New(),
		availableExternalPorts: config.InventoryExternalPortQuantity,
		waiter:                 waiter,
	}

	is.clients.inventory = cfromctx.ClientInventoryFromContext(ctx)
	is.clients.ip = cfromctx.ClientIPFromContext(ctx)

	is.teePlatform = client.DetectTEEPlatform(ctx)
	if is.teePlatform != ctypes.TEEPlatformNone {
		is.log.Info("detected TEE platform", "platform", is.teePlatform)
	}

	reservations := make([]*reservation, 0, len(deployments)+len(volumes))
	for _, d := range deployments {
		res := newReservation(d.LeaseID().OrderID(), d.ManifestGroup())
		res.SetClusterParams(d.ClusterParams())
		res.teeType = teeTypeFromClusterParams(d.ClusterParams())

		reservations = append(reservations, res)
	}

	// Storage market: durable (volume) reservations are rebuilt from Volume CRDs.
	// They are marked allocated: their PVs already exist and are counted as
	// allocated by the inventory operator's cluster state, so they must not
	// be re-adjusted against reported capacity like pending reservations.
	for _, v := range volumes {
		res := newReservation(v.LeaseID.OrderID(), v.Group)
		res.allocated = true

		reservations = append(reservations, res)
	}

	go is.lc.WatchChannel(ctx.Done())
	go is.run(ctx, reservations)

	return is, nil
}

func (is *inventoryService) done() <-chan struct{} {
	return is.lc.Done()
}

func (is *inventoryService) ready() <-chan struct{} {
	return is.readych
}

func (is *inventoryService) lookup(order mtypes.OrderID, resources dtypes.ResourceGroup) (ctypes.Reservation, error) {
	ch := make(chan inventoryResponse, 1)
	req := inventoryRequest{
		order:     order,
		resources: resources,
		ch:        ch,
	}

	select {
	case is.lookupch <- req:
		response := <-ch
		return response.value, response.err
	case <-is.lc.ShuttingDown():
		return nil, ErrNotRunning
	}
}

// checkResourceUnits rejects units with a nil CPU, GPU or memory, which
// Adjust dereferences.
func checkResourceUnits(resources dtypes.ResourceGroup) error {
	for idx, res := range resources.GetResourceUnits() {
		if res.CPU == nil {
			return fmt.Errorf("%w: CPU resource at idx %d is nil", ErrInvalidResource, idx)
		}
		if res.GPU == nil {
			return fmt.Errorf("%w: GPU resource at idx %d is nil", ErrInvalidResource, idx)
		}
		if res.Memory == nil {
			return fmt.Errorf("%w: Memory resource at idx %d is nil", ErrInvalidResource, idx)
		}
	}

	return nil
}

func (is *inventoryService) reserve(order mtypes.OrderID, resources dtypes.ResourceGroup) (ctypes.Reservation, error) {
	if err := checkResourceUnits(resources); err != nil {
		return nil, err
	}

	ch := make(chan inventoryResponse, 1)
	req := inventoryRequest{
		order:     order,
		resources: resources,
		ch:        ch,
	}

	select {
	case is.reservech <- req:
		response := <-ch
		if response.err == nil {
			cnt := atomic.AddInt64(&is.reservationCount, 1)
			is.log.Debug("reservation count", "oid", order, "count", cnt)
		}
		return response.value, response.err
	case <-is.lc.ShuttingDown():
		return nil, ErrNotRunning
	}
}

func (is *inventoryService) unreserve(order mtypes.OrderID) error { // nolint: unparam
	ch := make(chan inventoryResponse, 1)
	req := inventoryRequest{
		order: order,
		ch:    ch,
	}

	select {
	case is.unreservech <- req:
		response := <-ch
		if response.err == nil {
			cnt := atomic.AddInt64(&is.reservationCount, -1)
			is.log.Debug("reservation count", "oid", order, "count", cnt)
		}
		return response.err
	case <-is.lc.ShuttingDown():
		return ErrNotRunning
	}
}

// checkResize returns the resources the reservation held for order would be
// allocated at the new size, or the error that keeps the size from fitting.
// The reservation itself is left as it is.
func (is *inventoryService) checkResize(order mtypes.OrderID, resources dtypes.ResourceGroup) (dtypes.ResourceUnits, error) {
	resp, err := is.resizeReservation(order, resources, true)
	if err != nil {
		return nil, err
	}

	return resp.value.GetAllocatedResources(), nil
}

// resize swaps the reservation held for order to the new size, and reports
// whether the reservation changed.
func (is *inventoryService) resize(order mtypes.OrderID, resources dtypes.ResourceGroup) (bool, error) {
	resp, err := is.resizeReservation(order, resources, false)
	return resp.changed, err
}

func (is *inventoryService) resizeReservation(order mtypes.OrderID, resources dtypes.ResourceGroup, dryRun bool) (inventoryResponse, error) {
	if err := checkResourceUnits(resources); err != nil {
		return inventoryResponse{}, err
	}

	ch := make(chan inventoryResponse, 1)
	req := resizeRequest{
		order:     order,
		resources: resources,
		dryRun:    dryRun,
		ch:        ch,
	}

	select {
	case is.resizech <- req:
		response := <-ch
		return response, response.err
	case <-is.lc.ShuttingDown():
		return inventoryResponse{}, ErrNotRunning
	}
}

func (is *inventoryService) status(ctx context.Context) (inventoryV1.InventoryMetrics, error) {
	ch := make(chan inventoryV1.InventoryMetrics, 1)

	select {
	case <-is.lc.Done():
		return inventoryV1.InventoryMetrics{}, ErrNotRunning
	case <-ctx.Done():
		return inventoryV1.InventoryMetrics{}, ctx.Err()
	case is.statusch <- ch:
	}

	select {
	case <-is.lc.Done():
		return inventoryV1.InventoryMetrics{}, ErrNotRunning
	case <-ctx.Done():
		return inventoryV1.InventoryMetrics{}, ctx.Err()
	case result := <-ch:
		return result, nil
	}
}

func (is *inventoryService) statusV1(ctx context.Context) (*provider.Inventory, error) {
	ch := make(chan invSnapshotResp, 1)

	select {
	case <-is.lc.Done():
		return nil, ErrNotRunning
	case <-ctx.Done():
		return nil, ctx.Err()
	case is.statusV1ch <- ch:
	}

	select {
	case <-is.lc.Done():
		return nil, ErrNotRunning
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-ch:
		return result.res, result.err
	}
}

func (is *inventoryService) resourcesToCommit(rgroup dtypes.ResourceGroup) dtypes.ResourceGroup {
	// Storage market: a volume group's bid offer must EQUAL the order's storage
	// quantity (the chain rejects anything else) and its zero compute legs
	// must stay zero (ComputeCommittedResources clamps zero to one under
	// overcommit), so commit levels do not apply — durable leased bytes are
	// never thin-sold (StorageCommitLevel effectively pinned to 1.0). The
	// resulting reservation carries ReservationKindDurable via
	// newReservation.
	if groupVolumePolicy(rgroup) != nil {
		return rgroup
	}

	replacedResources := make(dtypes.ResourceUnits, 0)

	for _, resource := range rgroup.GetResourceUnits() {
		runits := rtypes.Resources{
			ID: resource.ID,
			CPU: &rtypes.CPU{
				Units:      sdlutil.ComputeCommittedResources(is.config.CPUCommitLevel, resource.GetCPU().GetUnits()),
				Attributes: resource.GetCPU().GetAttributes(),
			},
			GPU: &rtypes.GPU{
				Units:      sdlutil.ComputeCommittedResources(is.config.GPUCommitLevel, resource.GetGPU().GetUnits()),
				Attributes: resource.GetGPU().GetAttributes(),
			},
			Memory: &rtypes.Memory{
				Quantity:   sdlutil.ComputeCommittedResources(is.config.MemoryCommitLevel, resource.GetMemory().GetQuantity()),
				Attributes: resource.GetMemory().GetAttributes(),
			},
			Endpoints: resource.GetEndpoints(),
		}

		storage := make(rtypes.Volumes, 0, len(resource.GetStorage()))

		for _, volume := range resource.GetStorage() {
			storage = append(storage, rtypes.Storage{
				Name:       volume.Name,
				Quantity:   sdlutil.ComputeCommittedResources(is.config.StorageCommitLevel, volume.GetQuantity()),
				Attributes: volume.GetAttributes(),
			})
		}

		runits.Storage = storage

		v := dtypes.ResourceUnit{
			Resources: runits,
			Count:     resource.Count,
			Price:     sdk.DecCoin{},
		}

		replacedResources = append(replacedResources, v)
	}

	result := dtypes.GroupSpec{
		Name:         rgroup.GetName(),
		Requirements: placementRequirements(rgroup),
		Resources:    replacedResources,
	}

	return result
}

// placementRequirements extracts the on-chain placement requirements from
// the concrete ResourceGroup shapes the bid path produces. The committed
// copy must carry them (CS-6 / AKT-406): the inventory client's Adjust
// reads the `capabilities/gpu-interconnect/fabric/...` pin off
// reservation.Resources() to gate interconnect node selection — an empty
// Requirements here silently disabled the tenant's fabric pin.
// Restart-recovered reservations are built from the off-chain manifest
// group instead, which carries no requirements; those stay permissive
// (their pods are already placed, only capacity re-accounting happens).
func placementRequirements(rgroup dtypes.ResourceGroup) atypes.PlacementRequirements {
	switch rg := rgroup.(type) {
	case *dtypes.Group:
		return rg.GroupSpec.Requirements
	case dtypes.Group:
		return rg.GroupSpec.Requirements
	case *dtypes.GroupSpec:
		return rg.Requirements
	case dtypes.GroupSpec:
		return rg.Requirements
	default:
		return atypes.PlacementRequirements{}
	}
}

func (is *inventoryService) updateInventoryMetrics(metrics inventoryV1.Metrics) {
	clusterInventoryAllocatable.WithLabelValues("nodes").Set(float64(len(metrics.Nodes)))
	clusterInventoryAllocatable.WithLabelValues("cpu").Set(float64(metrics.TotalAllocatable.CPU) / 1000)
	clusterInventoryAllocatable.WithLabelValues("gpu").Set(float64(metrics.TotalAllocatable.GPU) / 1000)
	clusterInventoryAllocatable.WithLabelValues("memory").Set(float64(metrics.TotalAllocatable.Memory))
	clusterInventoryAllocatable.WithLabelValues("storage-ephemeral").Set(float64(metrics.TotalAllocatable.StorageEphemeral))
	for class, val := range metrics.TotalAllocatable.Storage {
		clusterInventoryAllocatable.WithLabelValues(fmt.Sprintf("storage-%s", class)).Set(float64(val))
	}

	clusterInventoryAllocatable.WithLabelValues("endpoints").Set(float64(is.config.InventoryExternalPortQuantity))

	clusterInventoryAvailable.WithLabelValues("cpu").Set(float64(metrics.TotalAvailable.CPU) / 1000)
	clusterInventoryAvailable.WithLabelValues("memory").Set(float64(metrics.TotalAvailable.Memory))
	clusterInventoryAvailable.WithLabelValues("storage-ephemeral").Set(float64(metrics.TotalAvailable.StorageEphemeral))
	for class, val := range metrics.TotalAvailable.Storage {
		clusterInventoryAvailable.WithLabelValues(fmt.Sprintf("storage-%s", class)).Set(float64(val))
	}

	clusterInventoryAvailable.WithLabelValues("endpoints").Set(float64(is.availableExternalPorts))
}

func updateReservationMetrics(reservations []*reservation) {
	inventoryReservations.WithLabelValues("none", "quantity").Set(float64(len(reservations)))

	activeCPUTotal := 0.0
	activeGPUTotal := 0.0
	activeMemoryTotal := 0.0
	activeStorageEphemeralTotal := 0.0
	activeEndpointsTotal := 0.0

	pendingCPUTotal := 0.0
	pendingGPUTotal := 0.0
	pendingMemoryTotal := 0.0
	pendingStorageEphemeralTotal := 0.0
	pendingEndpointsTotal := 0.0

	allocated := 0.0
	for _, reservation := range reservations {
		cpuTotal := &pendingCPUTotal
		gpuTotal := &pendingGPUTotal
		memoryTotal := &pendingMemoryTotal
		endpointsTotal := &pendingEndpointsTotal

		if reservation.allocated {
			allocated++
			cpuTotal = &activeCPUTotal
			gpuTotal = &activeGPUTotal
			memoryTotal = &activeMemoryTotal
			endpointsTotal = &activeEndpointsTotal
		}
		for _, resource := range reservation.Resources().GetResourceUnits() {
			*cpuTotal += float64(resource.GetCPU().GetUnits().Value() * uint64(resource.Count))
			*gpuTotal += float64(resource.GetGPU().GetUnits().Value() * uint64(resource.Count))
			*memoryTotal += float64(resource.GetMemory().Quantity.Value() * uint64(resource.Count))
			*endpointsTotal += float64(len(resource.GetEndpoints()))
		}
	}

	inventoryReservations.WithLabelValues("none", "allocated").Set(allocated)

	inventoryReservations.WithLabelValues("active", "cpu").Set(activeCPUTotal)
	inventoryReservations.WithLabelValues("active", "gpu").Set(activeGPUTotal)
	inventoryReservations.WithLabelValues("active", "memory").Set(activeMemoryTotal)
	inventoryReservations.WithLabelValues("active", "storage-ephemeral").Set(activeStorageEphemeralTotal)
	inventoryReservations.WithLabelValues("active", "endpoints").Set(activeEndpointsTotal)

	inventoryReservations.WithLabelValues("pending", "cpu").Set(pendingCPUTotal)
	inventoryReservations.WithLabelValues("pending", "gpu").Set(pendingGPUTotal)
	inventoryReservations.WithLabelValues("pending", "memory").Set(pendingMemoryTotal)
	inventoryReservations.WithLabelValues("pending", "storage-ephemeral").Set(pendingStorageEphemeralTotal)
	inventoryReservations.WithLabelValues("pending", "endpoints").Set(pendingEndpointsTotal)
}

type inventoryServiceState struct {
	inventory    ctypes.Inventory
	ipAddrUsage  cip.AddressUsage
	reservations []*reservation
}

func countReservedIPs(state *inventoryServiceState) uint {
	reserved := uint(0)
	for _, entry := range state.reservations {
		if !entry.ipsConfirmed {
			reserved += entry.endpointQuantity
		}
	}

	return reserved
}

func availableLeasedIPs(total, inUse, reserved uint) uint {
	if inUse >= total {
		return 0
	}

	available := total - inUse
	if reserved >= available {
		return 0
	}

	return available - reserved
}

func leasedIPStatus(state *inventoryServiceState) inventoryV1.ResourcePair {
	reserved := countReservedIPs(state)
	allocated := state.ipAddrUsage.InUse + reserved

	return inventoryV1.NewResourcePair(
		int64(state.ipAddrUsage.Available), // nolint: gosec
		int64(state.ipAddrUsage.Available), // nolint: gosec
		int64(allocated),                   // nolint: gosec
		resource.DecimalSI)
}

// teeTypeFromResourceGroup extracts the TEE type from the placement requirement
// attributes of a resource group (e.g. tee/type=cpu-gpu).
func teeTypeFromResourceGroup(rg dtypes.ResourceGroup) ctypes.TEEType {
	var attrs atypes.Attributes
	switch v := rg.(type) {
	case dtypes.GroupSpec:
		attrs = v.Requirements.Attributes
	case *dtypes.GroupSpec:
		attrs = v.Requirements.Attributes
	case dtypes.Group:
		attrs = v.GroupSpec.Requirements.Attributes
	case *dtypes.Group:
		attrs = v.GroupSpec.Requirements.Attributes
	default:
		return ctypes.TEETypeNone
	}
	for _, attr := range attrs {
		if attr.Key == "tee/type" {
			t, err := ctypes.ParseTEEType(attr.Value)
			if err == nil {
				return t
			}
		}
	}
	return ctypes.TEETypeNone
}

// teeTypeFromClusterParams extracts the TEE type from stored cluster params
// (used for existing reservations loaded at startup).
func teeTypeFromClusterParams(cp interface{}) ctypes.TEEType {
	var sparams []*crd.SchedulerParams
	switch v := cp.(type) {
	case *crd.ClusterSettings:
		if v == nil {
			return ctypes.TEETypeNone
		}
		sparams = v.SchedulerParams
	case crd.ClusterSettings:
		sparams = v.SchedulerParams
	default:
		return ctypes.TEETypeNone
	}
	for _, sp := range sparams {
		if sp != nil && sp.TEEType != "" && !sp.AttestationDisabled {
			t, err := ctypes.ParseTEEType(sp.TEEType)
			if err == nil {
				return t
			}
		}
	}
	return ctypes.TEETypeNone
}

func (is *inventoryService) adjustOpts(teeType ctypes.TEEType) []ctypes.InventoryOption {
	opts := []ctypes.InventoryOption{ctypes.WithTEEPlatform(is.teePlatform)}
	if teeType != ctypes.TEETypeNone {
		opts = append(opts, ctypes.WithTEEType(teeType))
	}
	return opts
}

func (is *inventoryService) handleRequest(req inventoryRequest, state *inventoryServiceState) {
	log := is.log.With("oid", req.order)

	// convert the resources to the committed amount
	resourcesToCommit := is.resourcesToCommit(req.resources)
	// create new registration if capacity available
	reservation := newReservation(req.order, resourcesToCommit)
	reservation.requested = req.resources.GetResourceUnits().Dup()

	{
		jReservation, _ := json.Marshal(req.resources.GetResourceUnits())
		log.Debug("reservation requested", "resources", string(jReservation))
	}

	if reservation.endpointQuantity != 0 {
		if is.clients.ip == nil {
			req.ch <- inventoryResponse{err: errNoLeasedIPsAvailable}
			return
		}
		reserved := countReservedIPs(state)
		available := availableLeasedIPs(state.ipAddrUsage.Available, state.ipAddrUsage.InUse, reserved)
		if reservation.endpointQuantity > available {
			log.Info("insufficient number of IP addresses available")
			req.ch <- inventoryResponse{err: fmt.Errorf("%w: unable to reserve %d", errInsufficientIPs, reservation.endpointQuantity)}
			return
		}

		log.Info("reservation used leased IPs", "used", reservation.endpointQuantity, "available", state.ipAddrUsage.Available, "in_use", state.ipAddrUsage.InUse, "reserved", reserved, "remaining", available)
	} else {
		reservation.ipsConfirmed = true // No IPs, just mark it as confirmed implicitly
	}

	reservation.teeType = teeTypeFromResourceGroup(req.resources)
	err := state.inventory.Adjust(reservation, is.adjustOpts(reservation.teeType)...)
	if err != nil {
		log.Info("insufficient capacity for reservation")
		inventoryRequestsCounter.WithLabelValues("reserve", "insufficient-capacity").Inc()
		req.ch <- inventoryResponse{err: err}
		return
	}

	// Add the reservation to the list
	state.reservations = append(state.reservations, reservation)
	req.ch <- inventoryResponse{value: reservation}
	inventoryRequestsCounter.WithLabelValues("reserve", "create").Inc()
}

func findReservation(reservations []*reservation, order mtypes.OrderID) *reservation {
	for _, res := range reservations {
		if res.OrderID().Equals(order) {
			return res
		}
	}

	return nil
}

// resizedReservation is the reservation the order would hold at the resized
// group, committed the same way as a new reservation.
func (is *inventoryService) resizedReservation(req resizeRequest) *reservation {
	next := newReservation(req.order, is.resourcesToCommit(req.resources))
	next.requested = req.resources.GetResourceUnits().Dup()
	next.teeType = teeTypeFromResourceGroup(req.resources)

	return next
}

// handleResizeCheck dry-runs a lease resize against the current snapshot.
//
// The check is conservative. The snapshot still counts the lease's running
// pods, and a reservation does not record the nodes its pods landed on, so
// the old size cannot be credited back: the new size has to fit next to the
// old one. That never over-commits, but it declines a resize that would only
// fit by reusing the old pods' node. A durable (volume) reservation is the
// exception: its bytes sit in a cluster-wide storage class where they are
// already counted, so only the growth has to fit.
func (is *inventoryService) handleResizeCheck(req resizeRequest, state *inventoryServiceState) {
	log := is.log.With("oid", req.order)

	res := findReservation(state.reservations, req.order)
	if res == nil {
		inventoryRequestsCounter.WithLabelValues("resize-check", "not-found").Inc()
		req.ch <- inventoryResponse{err: ErrReservationNotFound}
		return
	}

	if state.inventory == nil {
		req.ch <- inventoryResponse{err: errInventoryNotAvailableYet}
		return
	}

	next := is.resizedReservation(req)

	check := next
	if res.kind == ctypes.ReservationKindDurable {
		check = newReservation(req.order, volumeGrowth(res.Resources(), next.Resources()))
		next.SetAllocatedResources(next.Resources().GetResourceUnits())
	}

	opts := append(is.adjustOpts(next.teeType), ctypes.WithDryRun())
	if err := state.inventory.Adjust(check, opts...); err != nil {
		log.Info("insufficient capacity for resize", "err", err)
		inventoryRequestsCounter.WithLabelValues("resize-check", "insufficient-capacity").Inc()
		req.ch <- inventoryResponse{err: err}
		return
	}

	inventoryRequestsCounter.WithLabelValues("resize-check", "fits").Inc()
	req.ch <- inventoryResponse{value: next}
}

// handleResize swaps the reservation held for the order to the resized group
// once the chain has applied the resize, and reports whether the reservation
// changed. The reservation is changed in place so its kind and leased IP
// confirmation carry over. One that already holds the group's resources, as
// after a resize of the price alone, is left as it is.
func (is *inventoryService) handleResize(req resizeRequest, state *inventoryServiceState) bool {
	log := is.log.With("oid", req.order)

	res := findReservation(state.reservations, req.order)
	if res == nil {
		inventoryRequestsCounter.WithLabelValues("resize", "not-found").Inc()
		req.ch <- inventoryResponse{err: ErrReservationNotFound}
		return false
	}

	next := is.resizedReservation(req)

	if sameResources(res.requested, next.requested) {
		log.Debug("reservation already holds the resized resources")
		inventoryRequestsCounter.WithLabelValues("resize", "unchanged").Inc()
		req.ch <- inventoryResponse{}
		return false
	}

	res.requested = next.requested

	if res.kind == ctypes.ReservationKindDurable {
		// The volume's bytes are counted in its storage class rather than by
		// re-adjusting the reservation, and a volume has no scheduler params,
		// so only the record changes. The reservation stays allocated: that
		// is what keeps it from being released on lease close.
		res.resources = next.resources
		res.adjustedResources = next.Resources().GetResourceUnits()

		log.Info("durable reservation resized")
		inventoryRequestsCounter.WithLabelValues("resize", "applied").Inc()
		req.ch <- inventoryResponse{changed: true}
		return true
	}

	// Adjust stamps the scheduler params per resource unit, and the GPU
	// params and interconnect units follow the GPU count. If the new size no
	// longer fits, the chain has applied the resize anyway: the old params
	// stay until an inventory update re-adjusts the pending reservation.
	if state.inventory != nil {
		if err := state.inventory.Adjust(next, is.adjustOpts(next.teeType)...); err != nil {
			log.Error("adjusting resized reservation", "err", err)
		} else {
			res.adjustedResources = next.adjustedResources
			res.clusterParams = next.clusterParams
		}
	}

	res.resources = next.resources
	res.endpointQuantity = next.endpointQuantity
	res.teeType = next.teeType

	if sameResources(res.deployment, res.requested) {
		// The tenant's manifest at the new size reached the cluster before
		// the chain's resize did: the reservation keeps following its
		// deployment, allocated if those pods already reported deployed.
		res.awaitingManifest = false
		res.awaitingUntil = time.Time{}
		is.setAllocated(res, res.deployed)
	} else {
		// Until the tenant's new manifest rolls the pods, the snapshot counts
		// the old ones. Back to pending, the reservation is re-adjusted at
		// its new size on every inventory update, on top of them, as during
		// any redeploy, until the manifest at the new size reaches the
		// cluster or the wait for it runs out (see expireManifestWaits).
		is.setAllocated(res, false)
		res.awaitingManifest = true
		res.awaitingUntil = time.Time{}

		if timeout := is.config.LeaseResizeManifestTimeout; timeout > 0 {
			res.awaitingUntil = time.Now().Add(timeout)
		}
	}

	log.Info("reservation resized", "allocated", res.allocated)
	inventoryRequestsCounter.WithLabelValues("resize", "applied").Inc()
	req.ch <- inventoryResponse{changed: true}

	return true
}

// setAllocated marks the reservation allocated or pending, taking its
// external ports from the pool while it is allocated, and reports whether
// that changed.
func (is *inventoryService) setAllocated(res *reservation, allocated bool) bool {
	if res.allocated == allocated {
		return false
	}

	res.allocated = allocated

	if allocated {
		is.availableExternalPorts -= reservationCountEndpoints(res)
	} else {
		is.availableExternalPorts += reservationCountEndpoints(res)
	}

	return true
}

// handleDeployment records the cluster's report on a lease's deployment on
// its reservation, and reports whether the reservation's allocation changed.
// A reservation is allocated while its deployment's pods are up and pending
// while they are not, except while a resize waits for the tenant's manifest
// at the new size: the pods from before the resize keep it pending.
func (is *inventoryService) handleDeployment(res *reservation, ev event.ClusterDeployment) bool {
	deployed := ev.Status == event.ClusterDeploymentDeployed

	// a volume lease's deployment carries no manifest (see volumeProvisioner)
	if res.kind == ctypes.ReservationKindDurable {
		return is.setAllocated(res, deployed)
	}

	res.deployment = ev.Group.GetResourceUnits().Dup()
	res.deployed = deployed

	if res.awaitingManifest {
		if !sameResources(res.deployment, res.requested) {
			return is.setAllocated(res, false)
		}

		// the manifest at the new size reached the cluster: from here the
		// reservation follows its deployment, as during any redeploy
		res.awaitingManifest = false
		res.awaitingUntil = time.Time{}
	}

	return is.setAllocated(res, deployed)
}

// expireManifestWaits ends the wait of each resized reservation whose tenant
// has not sent the manifest at the new size within
// LeaseResizeManifestTimeout, and reports whether any reservation's
// allocation changed. The reservation then follows its deployment again: it
// is allocated while the pods from before the resize are up.
//
// While a resize waits, the provider counts both the lease's running pods,
// in the inventory snapshot, and its pending reservation at the new size, so
// the tenant's roll is sure to fit. Once the wait ends it no longer holds the
// difference: the snapshot counts only the old pods, and other leases may
// take the room the new size needs. A roll that comes later may then not
// fit: its pods wait for room, and if none frees up the deployment monitor
// closes the lease as unstable, as after any failed redeploy.
func (is *inventoryService) expireManifestWaits(state *inventoryServiceState, now time.Time) bool {
	changed := false

	for _, res := range state.reservations {
		if !res.awaitingManifest || res.awaitingUntil.IsZero() || now.Before(res.awaitingUntil) {
			continue
		}

		res.awaitingManifest = false
		res.awaitingUntil = time.Time{}

		is.log.Info("resized lease's manifest not received in time; reservation follows its deployment again",
			"oid", res.OrderID(), "allocated", res.deployed)

		if is.setAllocated(res, res.deployed) {
			changed = true
		}
	}

	return changed
}

// nextManifestWaitEnd is the earliest end of a resize's wait for its
// manifest, zero when none waits with a bound.
func nextManifestWaitEnd(reservations []*reservation) time.Time {
	var next time.Time

	for _, res := range reservations {
		if !res.awaitingManifest || res.awaitingUntil.IsZero() {
			continue
		}

		if next.IsZero() || res.awaitingUntil.Before(next) {
			next = res.awaitingUntil
		}
	}

	return next
}

// volumeGrowth is the part of a resized volume group a durable reservation
// does not hold yet: the group with each storage quantity reduced by the
// held one. Volume groups carry a single unit with a single storage entry,
// and a reservation rebuilt from its Volume CRD names that entry after the
// vid, so entries are matched by position rather than by name.
func volumeGrowth(held dtypes.ResourceGroup, next dtypes.ResourceGroup) dtypes.GroupSpec {
	growth := dtypes.GroupSpec{
		Name:         next.GetName(),
		Requirements: placementRequirements(next),
		Resources:    next.GetResourceUnits().Dup(),
		Volume:       groupVolumePolicy(next),
	}

	cur := held.GetResourceUnits()

	for i := 0; i < len(growth.Resources) && i < len(cur); i++ {
		storage := growth.Resources[i].Storage

		for j := 0; j < len(storage) && j < len(cur[i].Storage); j++ {
			size, heldSize := storage[j].Quantity.Value(), cur[i].Storage[j].Quantity.Value()

			if size > heldSize {
				size -= heldSize
			} else {
				size = 0
			}

			storage[j].Quantity = rtypes.NewResourceValue(size)
		}
	}

	return growth
}

func (is *inventoryService) run(ctx context.Context, reservationsArg []*reservation) {
	defer is.lc.ShutdownCompleted()
	defer is.sub.Close()

	rctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	state := &inventoryServiceState{
		inventory:    nil,
		reservations: reservationsArg,
	}
	is.log.Info("starting with existing reservations", "count", len(state.reservations))

	// wait on the operators to be ready
	err := is.waiter.WaitForAll(ctx)
	if err != nil {
		is.lc.ShutdownInitiated(err)
		return
	}

	var runch <-chan runner.Result
	var currinv ctypes.Inventory

	invupch := make(chan ctypes.Inventory, 1)

	invch := is.clients.inventory.ResultChan()
	var reservech <-chan inventoryRequest
	// resizes wait for an inventory the same way reservations do: a resize
	// check before the first snapshot could only fail
	var resizech <-chan resizeRequest

	resumeProcessingReservations := func() {
		reservech = is.reservech
		resizech = is.resizech
	}

	t := timer.NewStoppedTimer()

	// readjust has the inventory taken from the last snapshot again, with
	// the pending reservations adjusted on top of it
	readjust := func() {
		if currinv == nil {
			return
		}

		select {
		case invupch <- currinv:
		default:
		}
	}

	// waitTimer fires when the first wait of a resize for its manifest ends
	waitTimer := timer.NewStoppedTimer()
	defer waitTimer.Stop()

	armWaits := func() {
		if next := nextManifestWaitEnd(state.reservations); !next.IsZero() {
			waitTimer.Reset(time.Until(next))
		} else {
			waitTimer.Stop()
		}
	}

	updateIPs := func() {
		if is.clients.ip != nil {
			reservech = nil
			resizech = nil
			if runch == nil {
				t.Stop()
				runch = is.runCheck(rctx, state)
			}
		} else if reservech == nil && state.inventory != nil {
			resumeProcessingReservations()
		}
	}

	bus := fromctx.MustPubSubFromCtx(ctx)

	signalch := make(chan struct{}, 1)
	trySignal := func() {
		select {
		case signalch <- struct{}{}:
		case <-is.lc.ShutdownRequest():
		default:
		}
	}
loop:
	for {
		select {
		case err := <-is.lc.ShutdownRequest():
			is.log.Debug("received shutdown request", "err", err)
			is.lc.ShutdownInitiated(err)
			break loop
		case ev := <-is.sub.Events():
			switch ev := ev.(type) { // nolint: gocritic
			case event.ClusterDeployment:
				// mark reservation allocated if deployment successful
				for _, res := range state.reservations {
					if !res.OrderID().Equals(ev.LeaseID.OrderID()) {
						continue
					}
					if res.Resources().GetName() != ev.Group.Name {
						continue
					}

					if is.handleDeployment(res, ev) {
						is.log.Debug("reservation status update",
							"oid", res.OrderID(),
							"group_name", res.Resources().GetName(),
							"allocated", res.allocated)

						readjust()
					}

					break
				}

				armWaits()
				updateIPs()
			}
		case <-t.C:
			updateIPs()
		case <-waitTimer.C:
			if is.expireManifestWaits(state, time.Now()) {
				readjust()
			}

			armWaits()
		case req := <-reservech:
			is.handleRequest(req, state)
		case req := <-resizech:
			if req.dryRun {
				is.handleResizeCheck(req, state)
			} else if is.handleResize(req, state) {
				readjust()
				armWaits()
			}
		case req := <-is.lookupch:
			// lookup registration
			for _, res := range state.reservations {
				if !res.OrderID().Equals(req.order) {
					continue
				}
				if res.Resources().GetName() != req.resources.GetName() {
					continue
				}
				req.ch <- inventoryResponse{value: res}
				inventoryRequestsCounter.WithLabelValues("lookup", "found").Inc()
				continue loop
			}

			inventoryRequestsCounter.WithLabelValues("lookup", "not-found").Inc()
			req.ch <- inventoryResponse{err: ErrReservationNotFound}
		case req := <-is.unreservech:
			is.log.Debug("unreserving capacity", "oid", req.order)
			// remove reservation

			is.log.Info("attempting to removing reservation", "oid", req.order)

			for idx, res := range state.reservations {
				if !res.OrderID().Equals(req.order) {
					continue
				}

				// Storage market: an allocated durable (volume) reservation is
				// exempt from unreserve-on-lease-close - the provider
				// holds the bytes through retention/adoption and the
				// capacity is released only on volume GC. A pending
				// durable reservation (bid lost, never provisioned)
				// unreserves normally.
				if res.kind == ctypes.ReservationKindDurable && res.allocated {
					is.log.Info("retaining durable reservation", "oid", res.OrderID())
					req.ch <- inventoryResponse{err: errReservationDurable}
					inventoryRequestsCounter.WithLabelValues("unreserve", "retained-durable").Inc()
					continue loop
				}

				is.log.Info("removing reservation", "oid", res.OrderID())

				state.reservations = append(state.reservations[:idx], state.reservations[idx+1:]...)
				// reclaim availableExternalPorts if unreserving allocated resources
				if res.allocated {
					is.availableExternalPorts += reservationCountEndpoints(res)
				}

				req.ch <- inventoryResponse{value: res}
				is.log.Info("unreserve capacity complete", "oid", req.order)
				inventoryRequestsCounter.WithLabelValues("unreserve", "destroyed").Inc()
				continue loop
			}

			inventoryRequestsCounter.WithLabelValues("unreserve", "not-found").Inc()
			req.ch <- inventoryResponse{err: ErrReservationNotFound}
		case responseCh := <-is.statusch:
			select {
			case responseCh <- is.getStatus(state):
			default:
			}
			inventoryRequestsCounter.WithLabelValues("status", "success").Inc()
		case responseCh := <-is.statusV1ch:
			resp, err := is.getStatusV1(state)
			select {
			case responseCh <- invSnapshotResp{
				res: resp,
				err: err,
			}:
			default:
			}

			if err == nil {
				inventoryRequestsCounter.WithLabelValues("status", "success").Inc()
			} else {
				inventoryRequestsCounter.WithLabelValues("status", "error").Inc()
			}
		case inv := <-invch:
			if inv == nil {
				continue
			}

			select {
			case <-invupch:
			default:
			}

			invupch <- inv
		case inv := <-invupch:
			currinv = inv.Dup()
			state.inventory = inv

			updateIPs()

			metrics := state.inventory.Metrics()

			is.updateInventoryMetrics(metrics)

			data, err := json.Marshal(&metrics)
			if err == nil {
				is.log.Debug(fmt.Sprintf("cluster resources dump=%s", string(data)))
			} else {
				is.log.Error("unable to dump cluster inventory", "err", err)
			}

			// readjust inventory accordingly with pending leases
			for _, r := range state.reservations {
				if !r.allocated {
					if err := state.inventory.Adjust(r, is.adjustOpts(r.teeType)...); err != nil {
						is.log.Error("adjust inventory for pending reservation", "oid", r.OrderID(), "err", err)
					}
				}
			}

			trySignal()
		case run := <-runch:
			runch = nil
			t.Reset(5 * time.Second)
			if err := run.Error(); err == nil {
				res := run.Value().(runCheckResult)
				state.ipAddrUsage = res.ipResult

				// Process confirmed IP addresses usage
				for _, confirmedOrderID := range res.confirmedResult {
					for i, entry := range state.reservations {
						if entry.order.Equals(confirmedOrderID) {
							state.reservations[i].ipsConfirmed = true
							is.log.Info("confirmed IP allocation", "oid", confirmedOrderID)
							break
						}
					}
				}
			} else {
				is.log.Error("checking IP addresses", "err", err)
			}

			resumeProcessingReservations()

			trySignal()
		case <-signalch:
			inv, err := is.getStatusV1(state)
			if err != nil {
				continue
			}

			bus.Pub(inv, []string{ptypes.PubSubTopicInventoryStatus}, tpubsub.WithRetain())
		}

		updateReservationMetrics(state.reservations)
	}

	is.log.Debug("shutting down")
	if runch != nil {
		<-runch
	}

	if is.clients.ip != nil {
		is.clients.ip.Stop()
	}

	is.log.Debug("shutdown complete")
}

type confirmationItem struct {
	orderID          mtypes.OrderID
	expectedQuantity uint
}

type runCheckResult struct {
	ipResult        cip.AddressUsage
	confirmedResult []mtypes.OrderID
}

func (is *inventoryService) runCheck(ctx context.Context, state *inventoryServiceState) <-chan runner.Result {
	// Look for unconfirmed IPs, these are IPs that have a deployment created
	// event and are marked allocated. But until the IP address operator has reported
	// that it has actually created the associated resources, we need to consider the total number of end
	// points as pending

	confirm := make([]confirmationItem, 0, len(state.reservations))

	for _, entry := range state.reservations {
		// Skip anything already confirmed or not allocated
		if entry.ipsConfirmed || !entry.allocated {
			continue
		}

		confirm = append(confirm, confirmationItem{
			orderID:          entry.OrderID(),
			expectedQuantity: entry.endpointQuantity,
		})
	}

	return runner.Do(func() runner.Result {
		retval := runCheckResult{}
		var err error

		retval.ipResult, err = is.clients.ip.GetIPAddressUsage(ctx)
		if err != nil {
			return runner.NewResult(nil, err)
		}

		for _, confirmItem := range confirm {
			status, err := is.clients.ip.GetIPAddressStatus(ctx, confirmItem.orderID)
			if err != nil {
				// This error is not really fatal, so don't bail on this entirely. The other results
				// retrieved in this code are still valid
				is.log.Error("failed checking IP address usage", "oid", confirmItem.orderID, "err", err)
				continue
			}

			numConfirmed := uint(len(status))
			if numConfirmed == confirmItem.expectedQuantity {
				retval.confirmedResult = append(retval.confirmedResult, confirmItem.orderID)
			}
		}

		return runner.NewResult(retval, nil)
	})
}

func (is *inventoryService) getStatus(state *inventoryServiceState) inventoryV1.InventoryMetrics {
	status := inventoryV1.InventoryMetrics{}

	if state.inventory == nil {
		status.Error = errInventoryNotAvailableYet
		return status
	}

	for _, reservation := range state.reservations {
		total := inventoryV1.MetricTotal{
			Storage: make(map[string]uint64),
		}

		for _, resources := range reservation.Resources().GetResourceUnits() {
			total.AddResources(resources)
		}

		if reservation.allocated {
			status.Active = append(status.Active, total)
		} else {
			status.Pending = append(status.Pending, total)
		}
	}

	status.Available.Nodes = append(status.Available.Nodes, state.inventory.Metrics().Nodes...)

	for class, size := range state.inventory.Metrics().TotalAvailable.Storage {
		status.Available.Storage = append(status.Available.Storage, inventoryV1.StorageStatus{Class: class, Size: int64(size)}) //nolint: gosec
	}

	return status
}

func (is *inventoryService) getStatusV1(state *inventoryServiceState) (*provider.Inventory, error) {
	if state.inventory == nil {
		return nil, errInventoryNotAvailableYet
	}

	cluster := state.inventory.Snapshot()
	sanitizeStatusCluster(&cluster)

	status := &provider.Inventory{
		Cluster:  cluster,
		LeasedIP: leasedIPStatus(state),
		Reservations: provider.Reservations{
			Pending: provider.ReservationsMetric{
				Count:     0,
				Resources: provider.NewResourcesMetric(),
			},
			Active: provider.ReservationsMetric{
				Count:     0,
				Resources: provider.NewResourcesMetric(),
			},
		},
	}

	for _, reservation := range state.reservations {
		runits := reservation.Resources().GetResourceUnits()
		if reservation.allocated {
			status.Reservations.Active.Resources.AddResourceUnits(runits)
			status.Reservations.Active.Count++
		} else {
			status.Reservations.Pending.Resources.AddResourceUnits(runits)
			status.Reservations.Pending.Count++
		}
	}

	return status, nil
}

func sanitizeStatusCluster(cluster *inventoryV1.Cluster) {
	for idx := range cluster.Nodes {
		sanitizeStatusNodeResources(&cluster.Nodes[idx].Resources)
	}

	for idx := range cluster.Storage {
		sanitizeStatusResourcePair(&cluster.Storage[idx].Quantity)
	}
}

func sanitizeStatusNodeResources(resources *inventoryV1.NodeResources) {
	sanitizeStatusResourcePair(&resources.CPU.Quantity)
	sanitizeStatusResourcePair(&resources.Memory.Quantity)
	sanitizeStatusResourcePair(&resources.GPU.Quantity)
	sanitizeStatusResourcePair(&resources.EphemeralStorage)
	sanitizeStatusResourcePair(&resources.VolumesAttached)
	sanitizeStatusResourcePair(&resources.VolumesMounted)
}

func sanitizeStatusResourcePair(pair *inventoryV1.ResourcePair) {
	sanitizeStatusQuantity(pair.Capacity)
	sanitizeStatusQuantity(pair.Allocatable)
	sanitizeStatusQuantity(pair.Allocated)
}

func sanitizeStatusQuantity(quantity *resource.Quantity) {
	if quantity.Cmp(*inventoryStatusQuantityZero) < 0 {
		quantity.Set(0)
	}
}

func reservationCountEndpoints(reservation *reservation) uint {
	var externalPortCount uint

	resources := reservation.Resources().GetResourceUnits()
	// Count the number of endpoints per resource. The number of instances does not affect
	// the number of ports
	for _, resource := range resources {
		for _, endpoint := range resource.Endpoints {
			if endpoint.Kind == rtypes.Endpoint_RANDOM_PORT {
				externalPortCount++
			}
		}
	}

	return externalPortCount
}
