package v1beta3

import (
	dtypes "pkg.akt.dev/go/node/deployment/v1beta5"
	mtypes "pkg.akt.dev/go/node/market/v1"
)

type ReservationGroup interface {
	Resources() dtypes.ResourceGroup
	SetAllocatedResources(dtypes.ResourceUnits)
	GetAllocatedResources() dtypes.ResourceUnits
	SetClusterParams(interface{})
	ClusterParams() interface{}
}

// ReservationKind discriminates how a reservation's lifetime is bound to
// its lease.
type ReservationKind uint8

const (
	// ReservationKindLease is the ordinary compute reservation: released
	// when its lease closes.
	ReservationKindLease ReservationKind = iota
	// ReservationKindDurable is an AEP-87 volume reservation: the leased
	// bytes outlive the lease (retention window, adoption), so it is exempt
	// from unreserve-on-lease-close and released only when the volume is
	// garbage-collected. Rebuilt from Volume CRDs on restart.
	ReservationKindDurable
)

// Reservation interface implements orders and resources
type Reservation interface {
	OrderID() mtypes.OrderID
	Allocated() bool
	Kind() ReservationKind
	ReservationGroup
}
