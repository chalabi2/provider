package inventory

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testPV(name, capacity string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(capacity)},
		},
	}
}

func gib(n int64) int64 {
	return n << 30
}

func seePV(pvs map[string]corev1.PersistentVolume, allocated *resource.Quantity, pv *corev1.PersistentVolume) {
	trackPVCapacity(pvs, pv, pv.Spec.Capacity[corev1.ResourceStorage], allocated)
}

func TestTrackPVCapacity(t *testing.T) {
	pvs := make(map[string]corev1.PersistentVolume)
	allocated := resource.NewQuantity(0, resource.DecimalSI)

	seePV(pvs, allocated, testPV("pv-a", "1Gi"))
	seePV(pvs, allocated, testPV("pv-b", "2Gi"))
	require.Equal(t, gib(3), allocated.Value())

	// a status-only update leaves the count alone
	seePV(pvs, allocated, testPV("pv-a", "1Gi"))
	require.Equal(t, gib(3), allocated.Value())

	// an expanded volume adds its growth
	seePV(pvs, allocated, testPV("pv-a", "5Gi"))
	require.Equal(t, gib(7), allocated.Value())

	// deleting it subtracts what was counted, so the class returns to the
	// other volume alone instead of going short
	pv := testPV("pv-a", "5Gi")
	allocated.Sub(untrackPVCapacity(pvs, pv, pv.Spec.Capacity[corev1.ResourceStorage]))
	require.Equal(t, gib(2), allocated.Value())
	require.NotContains(t, pvs, "pv-a")
}

func TestUntrackPVCapacity(t *testing.T) {
	pvs := make(map[string]corev1.PersistentVolume)
	allocated := resource.NewQuantity(0, resource.DecimalSI)

	seePV(pvs, allocated, testPV("pv-a", "1Gi"))

	// a delete carrying a different capacity than the one counted, as after
	// a missed update, subtracts the counted one
	got := untrackPVCapacity(pvs, testPV("pv-a", "4Gi"), resource.MustParse("4Gi"))
	require.Equal(t, gib(1), got.Value())

	// a volume never counted falls back to the deleted object's capacity
	got = untrackPVCapacity(pvs, testPV("pv-x", "3Gi"), resource.MustParse("3Gi"))
	require.Equal(t, gib(3), got.Value())
}
