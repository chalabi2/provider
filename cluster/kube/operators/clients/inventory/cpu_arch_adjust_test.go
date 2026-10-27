package inventory

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	inventoryV1 "pkg.akt.dev/go/inventory/v1"
	attrtypes "pkg.akt.dev/go/node/types/attributes/v1"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"
)

func arm64NodeCPU() *inventoryV1.CPU {
	return &inventoryV1.CPU{
		Quantity: inventoryV1.NewResourcePairMilli(4000, 4000, 0, resource.DecimalSI),
		Info:     inventoryV1.CPUInfoS{{Arch: "arm64"}},
	}
}

func TestTryAdjustCPU_UnitWithoutCPUFitsAnyArch(t *testing.T) {
	require.True(t, tryAdjustCPU(arm64NodeCPU(), &rtypes.CPU{Units: rtypes.NewResourceValue(0)}))
}

func TestTryAdjustCPU_UnitWithoutArchIsAmd64(t *testing.T) {
	require.False(t, tryAdjustCPU(arm64NodeCPU(), &rtypes.CPU{Units: rtypes.NewResourceValue(500)}))
}

func TestTryAdjustCPU_UnitAskingForTheNodeArchFits(t *testing.T) {
	res := &rtypes.CPU{
		Units:      rtypes.NewResourceValue(500),
		Attributes: attrtypes.Attributes{{Key: "arch", Value: "arm64"}},
	}

	require.True(t, tryAdjustCPU(arm64NodeCPU(), res))
}
