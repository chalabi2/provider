package v1beta3

import (
	dtypes "pkg.akt.dev/go/node/deployment/v1beta5"
	mtypes "pkg.akt.dev/go/node/market/v1"
)

// VolumeDeployment is a deployed AEP-87 first-class volume as reported by
// the cluster: the volume's own (storage) lease plus the storage-only
// GroupSpec reconstructed from the Volume CRD. It is the restart rebuild
// source for durable reservations.
type VolumeDeployment struct {
	LeaseID mtypes.LeaseID
	Group   dtypes.GroupSpec
}
