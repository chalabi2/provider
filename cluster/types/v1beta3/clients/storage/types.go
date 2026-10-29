package storage

import (
	"context"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
)

// VolumeState is the operator's answer for a single volume; mirrors the
// storage operator's REST shape.
type VolumeState struct {
	Name          string `json:"name"`
	Owner         string `json:"owner"`
	VID           string `json:"vid"`
	Class         string `json:"class"`
	Size          string `json:"size"`
	Phase         string `json:"phase"`
	PVName        string `json:"pv-name,omitempty"`
	AttachedLease string `json:"attached-lease,omitempty"`
	RetainedUntil string `json:"retained-until,omitempty"`
	// DSeq is the deployment the volume belongs to now, the adopting one
	// once an adoption is recorded.
	DSeq string `json:"dseq,omitempty"`
	// ImportFrom is the provider a moved volume's data is still being
	// copied from; empty once it all arrived.
	ImportFrom string `json:"import-from,omitempty"`
	// ReplicaOf names the primary a replica follows (owner/dseq/gseq/name).
	ReplicaOf string `json:"replica-of,omitempty"`
	// SyncedAt is when the data a replica holds was taken at its primary.
	SyncedAt string `json:"synced-at,omitempty"`
	// Syncing reports a replica whose copy of its primary is missing or
	// being written: it is no whole volume.
	Syncing bool `json:"syncing,omitempty"`
	// Promoted reports a replica that no longer follows its primary.
	Promoted bool `json:"promoted,omitempty"`
}

// Client is the daemon's interface to the storage operator. It satisfies
// waiter.Waitable (the operator is a mandatory dependency when the provider
// participates in the volume market) and bidengine.VolumeLookup (the local
// CRD pre-checks for adoption and attach bids).
type Client interface {
	Check(ctx context.Context) error
	String() string

	// RetainedVolume reports whether owner/vid is a local volume deployment
	// dseq may adopt: Retained, or already being handed to dseq - the
	// adoption bid pre-check.
	RetainedVolume(ctx context.Context, owner string, vid string, dseq uint64) (bool, error)
	// ParkedVolume reports whether a local volume in the Provisioned
	// (parked) phase matches the attach reference.
	ParkedVolume(ctx context.Context, ref dv1.VolumeRef) (bool, error)

	Stop()
}
