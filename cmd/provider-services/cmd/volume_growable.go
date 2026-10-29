package cmd

import (
	"context"

	"cosmossdk.io/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/akash-network/provider/operator/storage"
)

// expandableClasses keeps the volume classes whose Retain StorageClass lets
// a bound claim grow. Volumes are provisioned from that twin, so a class
// whose twin is missing or does not allow expansion is dropped: the bid
// engine then declines growth it could never apply, instead of billing the
// tenant for space the claim never gets.
func expandableClasses(ctx context.Context, kc kubernetes.Interface, classes []string, logger log.Logger) []string {
	kept := make([]string, 0, len(classes))

	for _, class := range classes {
		name := storage.RetainClass(class)

		sc, err := kc.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
		switch {
		case err != nil:
			logger.Error("declining volume growth on class: storage class unreadable", "class", class, "storage_class", name, "err", err)
		case sc.AllowVolumeExpansion == nil || !*sc.AllowVolumeExpansion:
			logger.Error("declining volume growth on class: storage class does not allow volume expansion", "class", class, "storage_class", name)
		default:
			kept = append(kept, class)
		}
	}

	return kept
}
