package v2beta2

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dtypes "pkg.akt.dev/go/node/deployment/v1beta5"
	mtypes "pkg.akt.dev/go/node/market/v1"
	attrtypes "pkg.akt.dev/go/node/types/attributes/v1"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"
	"pkg.akt.dev/go/sdl"
)

// VolumePhase is the storage-operator reconciliation state of a Volume
// (AEP-87, DESIGN §3.3). All state is re-derivable from CRDs + PVs + chain
// queries; the phase is the operator's record of where in the lifecycle the
// backing PV currently is.
type VolumePhase string

const (
	// VolumePhasePending - CRD created, PV not yet provisioned.
	VolumePhasePending VolumePhase = "Pending"
	// VolumePhaseProvisioned - PV exists (Retain), parked on the holder PVC.
	VolumePhaseProvisioned VolumePhase = "Provisioned"
	// VolumePhaseAttached - PV bound to a compute lease's PVC.
	VolumePhaseAttached VolumePhase = "Attached"
	// VolumePhaseRetained - volume lease closed; data held until retainedUntil.
	VolumePhaseRetained VolumePhase = "Retained"
	// VolumePhaseAdopting - adoption lease won; PV being re-bound to the new
	// volume deployment.
	VolumePhaseAdopting VolumePhase = "Adopting"
	// VolumePhaseExporting - migration/replication export in flight; GC frozen.
	VolumePhaseExporting VolumePhase = "Exporting"
	// VolumePhaseReleasing - past retainedUntil; PV + image being destroyed.
	VolumePhaseReleasing VolumePhase = "Releasing"
)

const volumeNameHashLen = 12

// Volume is the provider's durable record of an AEP-87 first-class volume:
// the restart-survival record and the GC ledger. The cluster client writes
// the spec (desired state from chain events); the storage operator owns the
// PV choreography and the status.
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Volume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   VolumeSpec   `json:"spec,omitempty"`
	Status VolumeStatus `json:"status,omitempty"`
}

// VolumeList is a list of Volume objects.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type VolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []Volume `json:"items"`
}

// VolumeGroupID identifies the on-chain volume group (owner is in the spec).
type VolumeGroupID struct {
	DSeq string `json:"dseq"`
	GSeq uint32 `json:"gseq"`
}

// VolumeSpec mirrors the on-chain volume group contract. GroupID/LeaseID
// track the volume's *current* deployment/lease and are rewritten on
// adoption; owner and vid are the data-continuity identity and never change.
type VolumeSpec struct {
	Owner string `json:"owner"`
	// VID is the tenant-chosen data-continuity label (VolumePolicy.vid).
	VID     string        `json:"vid"`
	GroupID VolumeGroupID `json:"groupId"`
	Class   string        `json:"class"`
	// Size is the storage quantity in bytes, decimal-encoded.
	Size string `json:"size"`
	// Reclaim is the on-chain reclaim policy: "retain" or "delete".
	Reclaim string `json:"reclaim"`
	// Retention is the post-close adoption window (Go duration string).
	Retention string `json:"retention"`
	// LeaseID is the volume's own (storage) lease.
	LeaseID LeaseID `json:"leaseId"`
}

// VolumeStatus is the storage operator's reconciliation record.
type VolumeStatus struct {
	Phase VolumePhase `json:"phase,omitempty"`
	// PVName is the cluster-scoped PersistentVolume backing this volume.
	PVName string `json:"pvName,omitempty"`
	// AttachedLease is the compute LeaseID string while Attached.
	AttachedLease string `json:"attachedLease,omitempty"`
	// RetainedUntil = closedAt + retention; the GC deadline. The single
	// destruction path triggers only past this positive deadline.
	RetainedUntil *metav1.Time `json:"retainedUntil,omitempty"`
}

// VolumeName derives the deterministic CRD object name for a volume:
// volume-<sha256(owner/vid)[:12]>. The same name is used for the holder PVC
// (namespace akash-volumes) and the attach PVC (compute lease namespace), so
// the workload builder and the storage operator agree without coordination.
func VolumeName(owner, vid string) string {
	sum := sha256.Sum256([]byte(owner + "/" + vid))
	return "volume-" + hex.EncodeToString(sum[:])[:volumeNameHashLen]
}

// NewVolume builds the Volume CRD object for a won volume lease from its
// on-chain GroupSpec. Labels (lease, component) are appended by the caller.
func NewVolume(ns string, lid mtypes.LeaseID, gspec *dtypes.GroupSpec) (*Volume, error) {
	if gspec == nil || gspec.Volume == nil {
		return nil, fmt.Errorf("%w: group spec is not a volume group", ErrInvalidArgs)
	}

	if len(gspec.Resources) != 1 || len(gspec.Resources[0].Storage) != 1 {
		return nil, fmt.Errorf("%w: volume group must carry exactly one storage entry", ErrInvalidArgs)
	}

	storage := gspec.Resources[0].Storage[0]

	class := sdl.StorageClassDefault
	if val, set := storage.Attributes.Find(sdl.StorageAttributeClass).AsString(); set {
		class = val
	}

	return &Volume{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Volume",
			APIVersion: "akash.network/v2beta2",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      VolumeName(lid.Owner, gspec.Volume.Vid),
			Namespace: ns,
		},
		Spec: VolumeSpec{
			Owner: lid.Owner,
			VID:   gspec.Volume.Vid,
			GroupID: VolumeGroupID{
				DSeq: strconv.FormatUint(lid.DSeq, 10),
				GSeq: lid.GSeq,
			},
			Class:     class,
			Size:      strconv.FormatUint(storage.Quantity.Value(), 10),
			Reclaim:   gspec.Volume.Reclaim.String(),
			Retention: gspec.Volume.Retention.String(),
			LeaseID:   LeaseIDFromAkash(lid),
		},
		Status: VolumeStatus{
			Phase: VolumePhasePending,
		},
	}, nil
}

// FromCRD reconstructs the volume's lease and a storage-only GroupSpec from
// the CRD - the restart rebuild source for durable reservations. The group
// name is not persisted on-chain state the provider keeps, so the vid stands
// in; capacity accounting keys on the OrderID, not the name.
func (v *Volume) FromCRD() (mtypes.LeaseID, dtypes.GroupSpec, error) {
	lid, err := v.Spec.LeaseID.FromCRD()
	if err != nil {
		return mtypes.LeaseID{}, dtypes.GroupSpec{}, err
	}

	size, err := strconv.ParseUint(v.Spec.Size, 10, 64)
	if err != nil {
		return mtypes.LeaseID{}, dtypes.GroupSpec{}, fmt.Errorf("%w: invalid volume size %q: %s", ErrInvalidArgs, v.Spec.Size, err.Error())
	}

	retention, err := time.ParseDuration(v.Spec.Retention)
	if err != nil {
		return mtypes.LeaseID{}, dtypes.GroupSpec{}, fmt.Errorf("%w: invalid volume retention %q: %s", ErrInvalidArgs, v.Spec.Retention, err.Error())
	}

	reclaim := dv1.VolumePolicy_ReclaimPolicy(dv1.VolumePolicy_ReclaimPolicy_value[v.Spec.Reclaim])

	gspec := dtypes.GroupSpec{
		Name: v.Spec.VID,
		Volume: &dv1.VolumePolicy{
			Vid:            v.Spec.VID,
			Reclaim:        reclaim,
			Retention:      retention,
			MaxAttachments: 1,
		},
		Resources: dtypes.ResourceUnits{
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
							Name:     v.Spec.VID,
							Quantity: rtypes.NewResourceValue(size),
							Attributes: attrtypes.Attributes{
								{Key: sdl.StorageAttributeClass, Value: v.Spec.Class},
								{Key: sdl.StorageAttributePersistent, Value: "true"},
							},
						},
					},
					Endpoints: rtypes.Endpoints{},
				},
				Count: 1,
			},
		},
	}

	return lid, gspec, nil
}
