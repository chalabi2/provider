package rest

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"cosmossdk.io/log"

	kubeclienterrors "github.com/akash-network/provider/cluster/kube/errors"
)

// VolumeStatus is where a volume lease's volume stands on this provider.
type VolumeStatus struct {
	// Phase is the storage operator's phase: Pending, Importing,
	// Provisioned, Attached, Exporting, Adopting, Retained or Releasing.
	Phase string `json:"phase"`
	// SizeBytes is the size the volume is leased at.
	SizeBytes uint64 `json:"size_bytes"`
	// ImportFrom is the provider a moved volume's data is still being
	// copied from; absent once it all arrived.
	ImportFrom string `json:"import_from,omitempty"`
	// AttachedLease is the compute lease the volume is attached to.
	AttachedLease string `json:"attached_lease,omitempty"`
	// RetainedUntil is the deadline to adopt a volume whose lease closed;
	// the volume is destroyed once it passes.
	RetainedUntil *time.Time `json:"retained_until,omitempty"`
	// ReplicaOf names the primary a replica follows (owner/dseq/gseq/name).
	ReplicaOf string `json:"replica_of,omitempty"`
	// SyncedAt is when the data a replica holds was taken at its primary;
	// absent until its first copy lands.
	SyncedAt *time.Time `json:"synced_at,omitempty"`
	// SyncLagSeconds is how far a replica still following its primary is
	// behind it: the time since SyncedAt.
	SyncLagSeconds *int64 `json:"sync_lag_seconds,omitempty"`
	// Promoted reports a replica that compute attached, so it no longer
	// follows its primary.
	Promoted bool `json:"promoted,omitempty"`
}

// volumeStatusHandler serves a volume lease's owner where its volume
// stands: a tenant moving a volume learns here when the copy landed.
func volumeStatusHandler(log log.Logger, volumes leaseVolumes) http.HandlerFunc {
	log = log.With("module", "gateway-volume")

	return func(w http.ResponseWriter, r *http.Request) {
		lid := requestLeaseID(r)

		vol, err := volumes.LeaseVolume(r.Context(), lid)
		if err != nil {
			if errors.Is(err, kubeclienterrors.ErrVolumeNotFound) {
				http.Error(w, "no volume for this lease", http.StatusNotFound)
				return
			}

			log.Error("reading volume", "lid", lid, "err", err)
			http.Error(w, "unable to read the volume", http.StatusInternalServerError)

			return
		}

		if vol.Spec.Owner != lid.Owner {
			http.Error(w, "no volume for this lease", http.StatusNotFound)
			return
		}

		size, _ := strconv.ParseUint(vol.Spec.Size, 10, 64)

		status := VolumeStatus{
			Phase:         string(vol.Status.Phase),
			SizeBytes:     size,
			ImportFrom:    vol.Spec.ImportFrom,
			AttachedLease: vol.Status.AttachedLease,
		}

		if vol.Status.RetainedUntil != nil {
			until := vol.Status.RetainedUntil.UTC()
			status.RetainedUntil = &until
		}

		if vol.IsReplica() {
			status.ReplicaOf = vol.Spec.ReplicaOf
			status.Promoted = vol.Status.Promoted

			if vol.Status.SyncedAt != nil {
				synced := vol.Status.SyncedAt.UTC()
				status.SyncedAt = &synced

				if !vol.Status.Promoted {
					lag := int64(time.Since(synced).Seconds())
					status.SyncLagSeconds = &lag
				}
			}
		}

		writeJSONStatus(w, http.StatusOK, status)
	}
}
