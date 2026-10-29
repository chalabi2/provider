package cmd

import (
	"context"
	"testing"

	"cosmossdk.io/log"
	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestExpandableClassesKeepsOnlyExpandableRetainTwins(t *testing.T) {
	yes, no := true, false
	kc := fake.NewSimpleClientset(
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "beta3-retain"}, AllowVolumeExpansion: &yes},
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "beta2-retain"}, AllowVolumeExpansion: &no},
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "beta1-retain"}},
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "default"}, AllowVolumeExpansion: &yes},
	)

	kept := expandableClasses(context.Background(), kc, []string{"beta1", "beta2", "beta3", "default", "missing"}, log.NewNopLogger())

	// only beta3's twin allows expansion; "default" expands but its twin
	// "default-retain" does not exist, and the volume would live on the twin
	require.Equal(t, []string{"beta3"}, kept)
}

func TestExpandableClassesEmpty(t *testing.T) {
	kept := expandableClasses(context.Background(), fake.NewSimpleClientset(), nil, log.NewNopLogger())
	require.Empty(t, kept)
}
