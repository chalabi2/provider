package cluster

import (
	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dtypes "pkg.akt.dev/go/node/deployment/v1beta5"
	mtypes "pkg.akt.dev/go/node/market/v1"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"

	ctypes "github.com/akash-network/provider/cluster/types/v1beta3"
	"github.com/akash-network/provider/cluster/util"
)

// groupVolumePolicy extracts the AEP-87 volume policy when the resource
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
		kind:             kind,
		endpointQuantity: util.GetEndpointQuantityOfResourceGroup(resources, rtypes.Endpoint_LEASED_IP)}
}

type reservation struct {
	order             mtypes.OrderID
	resources         dtypes.ResourceGroup
	adjustedResources dtypes.ResourceUnits
	clusterParams     interface{}
	endpointQuantity  uint
	kind              ctypes.ReservationKind
	allocated         bool
	ipsConfirmed      bool
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
