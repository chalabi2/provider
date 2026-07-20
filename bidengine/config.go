package bidengine

import (
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	atttypes "pkg.akt.dev/go/node/types/attributes/v1"
)

// Config represents the configuration parameters for the bid engine.
// It controls pricing, deposits, timeouts and provider capabilities and attributes
type Config struct {
	PricingStrategy   BidPricingStrategy
	Deposit           sdk.Coin
	BidTimeout        time.Duration
	Attributes        atttypes.Attributes
	MaxGroupVolumes   int
	ReclamationWindow *time.Duration

	// Volumes caps bidding on storage-only (volume) orders. The zero value
	// (no classes) disables volume bidding.
	Volumes VolumeConfig

	// VolumeLookup answers local Volume CRD pre-checks for adoption and
	// attach orders. Nil (the storage operator client not wired) skips the
	// pre-checks; the chain gates remain authoritative.
	VolumeLookup VolumeLookup
}
