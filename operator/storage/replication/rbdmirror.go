package replication

import (
	"time"
)

// RBDMirrorConfig is the config stub for the opt-in rbd-mirror driver
// (IMPLEMENTATION.md §5.5 driver 2). Snapshot-mode rbd-mirror is only
// sound between provider pairs that explicitly trust each other, and only
// with per-volume RBD namespaces (or dedicated pools) and namespace-scoped
// peer users: Ceph bootstrap peer tokens are pool-level credentials and
// are treated as such - never described as "volume-scoped". Disabled by
// default; when enabled the provider advertises the sync cadence as a
// `replication-rpo` attribute.
type RBDMirrorConfig struct {
	// Enabled turns the driver on. Off by default: the stream-export
	// driver is the mainnet posture.
	Enabled bool
	// RPO is the snapshot-mode mirror cadence advertised to tenants.
	RPO time.Duration
}

// NewRBDMirrorDriver is the seam the opt-in rbd-mirror driver plugs into.
// Implementation is deliberately out of scope for local testing (it needs
// a mutually trusting two-cluster Rook pair with namespace-scoped peer
// users); it is validated via the manual runbook, out-of-band, never
// hidden behind green CI. Until then the seam returns ErrNotImplemented so
// misconfiguration fails loudly at startup rather than silently mirroring
// nothing.
func NewRBDMirrorDriver(_ RBDMirrorConfig) (Driver, error) {
	return nil, ErrNotImplemented
}
