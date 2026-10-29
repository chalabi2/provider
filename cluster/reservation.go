package cluster

import (
	"time"

	sdkmath "cosmossdk.io/math"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dtypes "pkg.akt.dev/go/node/deployment/v1beta4"
	mtypes "pkg.akt.dev/go/node/market/v1"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"

	ctypes "github.com/akash-network/provider/cluster/types/v1beta3"
	"github.com/akash-network/provider/cluster/util"
)

// groupVolumePolicy extracts the storage-market volume policy when the resource
// group is a storage-only (volume) group; nil for compute groups and shapes
// that carry no GroupSpec.
func groupVolumePolicy(rgroup dtypes.ResourceGroup) *dv1.VolumePolicy {
	switch group := rgroup.(type) {
	case *dtypes.Group:
		return group.GroupSpec.Volume
	case dtypes.Group:
		return group.GroupSpec.Volume
	case *dtypes.GroupSpec:
		return group.Volume
	case dtypes.GroupSpec:
		return group.Volume
	default:
		return nil
	}
}

func newReservation(order mtypes.OrderID, resources dtypes.ResourceGroup) *reservation {
	kind := ctypes.ReservationKindLease
	if groupVolumePolicy(resources) != nil {
		// a volume group's leased bytes outlive the lease
		kind = ctypes.ReservationKindDurable
	}

	return &reservation{
		order:            order,
		resources:        resources,
		requested:        resources.GetResourceUnits().Dup(),
		kind:             kind,
		endpointQuantity: util.GetEndpointQuantityOfResourceGroup(resources, rtypes.Endpoint_LEASED_IP)}
}

type reservation struct {
	order     mtypes.OrderID
	resources dtypes.ResourceGroup
	// requested is the resource units the reservation is for, before commit
	// levels apply: the order's group for a bid, the deployed manifest's or
	// the Volume CRD's units for a reservation rebuilt at startup, and the
	// resized group after a lease resize.
	requested         dtypes.ResourceUnits
	adjustedResources dtypes.ResourceUnits
	clusterParams     interface{}
	endpointQuantity  uint
	kind              ctypes.ReservationKind
	allocated         bool
	ipsConfirmed      bool
	teeType           ctypes.TEEType

	// deployment is the resource units of the manifest group the cluster
	// last reported the lease's deployment for, nil before the first
	// report, and deployed whether that report found its pods up.
	deployment dtypes.ResourceUnits
	deployed   bool

	// awaitingManifest is set while a lease resize waits for the tenant's
	// manifest at the new size to reach the cluster, until awaitingUntil
	// (no bound when zero).
	awaitingManifest bool
	awaitingUntil    time.Time
}

var _ ctypes.Reservation = (*reservation)(nil)

func (r *reservation) OrderID() mtypes.OrderID {
	return r.order
}

func (r *reservation) Resources() dtypes.ResourceGroup {
	return r.resources
}

func (r *reservation) SetAllocatedResources(val dtypes.ResourceUnits) {
	r.adjustedResources = val
}

func (r *reservation) GetAllocatedResources() dtypes.ResourceUnits {
	return r.adjustedResources
}

func (r *reservation) SetClusterParams(val interface{}) {
	r.clusterParams = val
}

func (r *reservation) ClusterParams() interface{} {
	return r.clusterParams
}

func (r *reservation) Allocated() bool {
	return r.allocated
}

func (r *reservation) Kind() ctypes.ReservationKind {
	return r.kind
}

// sameResources reports whether two lists of resource units hold the same
// resources: the same units, each with the same count and the same CPU, GPU,
// memory and storage quantities. Those and the price are what a lease
// resize can change, and the price holds no resources. Units are matched by
// ID, or directly when each list has one, and names, order and attributes
// are left out: a reservation rebuilt from a manifest lists its units in any
// order, and one rebuilt from a Volume CRD numbers its unit 1 and names its
// storage after the volume.
func sameResources(a, b dtypes.ResourceUnits) bool {
	if len(a) != len(b) {
		return false
	}

	if len(a) == 1 {
		return sameUnit(a[0], b[0])
	}

	byIDa, uniqueA := unitsByID(a)
	byIDb, uniqueB := unitsByID(b)

	if !uniqueA || !uniqueB {
		return false
	}

	for id, unit := range byIDa {
		other, found := byIDb[id]
		if !found || !sameUnit(unit, other) {
			return false
		}
	}

	return true
}

// unitsByID indexes units by ID, and reports whether their IDs are unique.
func unitsByID(units dtypes.ResourceUnits) (map[uint32]dtypes.ResourceUnit, bool) {
	byID := make(map[uint32]dtypes.ResourceUnit, len(units))

	for _, unit := range units {
		byID[unit.ID] = unit
	}

	return byID, len(byID) == len(units)
}

func sameUnit(a, b dtypes.ResourceUnit) bool {
	if a.Count != b.Count ||
		!sameQuantity(a.GetCPU().GetUnits(), b.GetCPU().GetUnits()) ||
		!sameQuantity(a.GetGPU().GetUnits(), b.GetGPU().GetUnits()) ||
		!sameQuantity(a.GetMemory().GetQuantity(), b.GetMemory().GetQuantity()) ||
		len(a.Storage) != len(b.Storage) {
		return false
	}

	for i := range a.Storage {
		if !sameQuantity(a.Storage[i].Quantity, b.Storage[i].Quantity) {
			return false
		}
	}

	return true
}

// sameQuantity compares two resource quantities, taking an unset one as
// zero.
func sameQuantity(a, b rtypes.ResourceValue) bool {
	return quantityOf(a).Equal(quantityOf(b))
}

func quantityOf(v rtypes.ResourceValue) sdkmath.Int {
	if v.Val.IsNil() {
		return sdkmath.ZeroInt()
	}

	return v.Val
}
