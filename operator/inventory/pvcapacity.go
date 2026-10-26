package inventory

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// trackPVCapacity counts a persistent volume seen as added or modified in its
// class's allocated storage. A volume seen for the first time adds its
// capacity, and one whose capacity changed since, as when its claim is
// expanded, adds the difference. Counting a volume only once would leave its
// growth out, and its deletion would then subtract more than was added.
func trackPVCapacity(pvs map[string]corev1.PersistentVolume, pv *corev1.PersistentVolume, capacity resource.Quantity, allocated *resource.Quantity) {
	prev, tracked := pvs[pv.Name]
	pvs[pv.Name] = *pv

	if !tracked {
		allocated.Add(capacity)
		return
	}

	prevCapacity, counted := prev.Spec.Capacity[corev1.ResourceStorage]
	if counted && prevCapacity.Cmp(capacity) == 0 {
		return
	}

	if counted {
		allocated.Sub(prevCapacity)
	}

	allocated.Add(capacity)
}

// untrackPVCapacity forgets a deleted persistent volume and returns the
// capacity to subtract from its class: the one counted for it, or the
// deleted object's when it was never counted.
func untrackPVCapacity(pvs map[string]corev1.PersistentVolume, pv *corev1.PersistentVolume, capacity resource.Quantity) resource.Quantity {
	prev, tracked := pvs[pv.Name]
	delete(pvs, pv.Name)

	if tracked {
		if counted, exists := prev.Spec.Capacity[corev1.ResourceStorage]; exists {
			return counted
		}
	}

	return capacity
}
