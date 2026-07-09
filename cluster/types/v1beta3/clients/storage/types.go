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
}

// Client is the daemon's interface to the storage operator. It satisfies
// waiter.Waitable (the operator is a mandatory dependency when the provider
// participates in the volume market) and bidengine.VolumeLookup (the local
// CRD pre-checks for adoption and attach bids).
type Client interface {
	Check(ctx context.Context) error
	String() string

	// RetainedVolume reports whether a local volume in the Retained phase
	// matches owner/vid - the adoption bid pre-check.
	RetainedVolume(ctx context.Context, owner string, vid string) (bool, error)
	// ParkedVolume reports whether a local volume in the Provisioned
	// (parked) phase matches the attach reference.
	ParkedVolume(ctx context.Context, ref dv1.VolumeRef) (bool, error)

	Stop()
}
