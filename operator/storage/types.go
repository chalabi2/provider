package storage

import (
	"errors"
	"strings"

	mtypes "pkg.akt.dev/go/node/market/v1"
)

const (
	// FlagVolumesNS is the namespace holder PVCs are parked in. Every parked
	// or retained volume keeps a claim here so its PV is never an unclaimed
	// Available Retain PV the kube binder could hand to an arbitrary PVC.
	FlagVolumesNS = "volumes-namespace"
	// FlagResyncInterval is the period of the full CRD re-reconcile sweep.
	FlagResyncInterval = "resync-interval"

	// FlagProvisionNodeHint annotates provisioning claims with
	// volume.kubernetes.io/selected-node for node-constrained provisioners
	// (local-path); leave off for network-attached storage (Ceph RBD).
	FlagProvisionNodeHint = "provision-node-hint"

	defaultVolumesNS = "akash-volumes"
)

const (
	// RetainClassSuffix is appended to the volume's storage class to select
	// the operator-installed reclaimPolicy=Retain twin (beta3 -> beta3-retain).
	RetainClassSuffix = "-retain"

	// LabelVolumeOwner is the PV label carrying the volume owner address.
	LabelVolumeOwner = "akash.network/volume-owner"
	// LabelVolumeVID is the PV label carrying the tenant-chosen vid.
	LabelVolumeVID = "akash.network/volume-vid"
	// LabelVolumeDSeq is the PV label carrying the dseq of the volume
	// deployment the PV data belongs to. On adoption it is rewritten to the
	// adopting deployment only after the chain state is verified; until then
	// it is the durable record of the dead volume's identity.
	LabelVolumeDSeq = "akash.network/volume-dseq"
)

var (
	ErrVolumeOperator = errors.New("volume operator error")
	// ErrAdoptionMismatch flags an adoption whose on-chain policy does not
	// reference the identity recorded on the PV. The volume stays Retained.
	ErrAdoptionMismatch = errors.New("volume adoption mismatch")
)

// RetainClass maps a storage class to its Retain-policy twin.
func RetainClass(class string) string {
	return class + RetainClassSuffix
}

// parseLeaseID parses the LeaseID string form stored in the Volume CRD
// status (owner/dseq/gseq/oseq/provider).
func parseLeaseID(val string) (mtypes.LeaseID, error) {
	return mtypes.ParseLeasePath(strings.Split(val, "/"))
}
