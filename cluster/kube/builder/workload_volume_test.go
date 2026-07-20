package builder

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"pkg.akt.dev/go/testutil"

	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

const (
	volumeAttachSDL   = "../../../testdata/deployment/deployment-v2.2-volume-attach.yaml"
	volumeAttachOwner = "akash1365yvmc4s7awdyj3n2sav7xfx76adc6dnmlx63"
	volumeAttachVID   = "myapp-pgdata"
)

// TestDeploymentVolumeAttach covers the third render case: a service
// referencing an AEP-87 first-class volume becomes a k8s Deployment whose
// pod mounts the volume through the deterministic pre-bound claim name.
func TestDeploymentVolumeAttach(t *testing.T) {
	lid := testutil.LeaseID(t)
	_, workload := testSetup(t, volumeAttachSDL, 0, lid)

	require.True(t, workload.hasVolumeRefs())

	deploymentBuilder := NewDeployment(workload)
	require.NotNil(t, deploymentBuilder)

	kdeployment, err := deploymentBuilder.Create()
	require.NoError(t, err)

	// RWO volume: exactly one replica
	require.NotNil(t, kdeployment.Spec.Replicas)
	require.Equal(t, int32(1), *kdeployment.Spec.Replicas)

	// the pod volume references the pre-bound claim by its deterministic name
	var claim *corev1.PersistentVolumeClaimVolumeSource
	var volumeName string
	for _, vol := range kdeployment.Spec.Template.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			claim = vol.PersistentVolumeClaim
			volumeName = vol.Name
		}
	}

	require.NotNil(t, claim)
	require.Equal(t, "db-data", volumeName)
	require.Equal(t, crd.VolumeName(volumeAttachOwner, volumeAttachVID), claim.ClaimName)
	require.False(t, claim.ReadOnly)

	// the container mounts it under the SDL mount path
	container := kdeployment.Spec.Template.Spec.Containers[0]
	require.Len(t, container.VolumeMounts, 1)
	require.Equal(t, "db-data", container.VolumeMounts[0].Name)
	require.Equal(t, "/var/lib/postgresql/data", container.VolumeMounts[0].MountPath)

	// no VolumeClaimTemplates path is involved: the workload carries no PVCs
	require.Empty(t, workload.pvcsObjs)
}

// TestWorkloadNoVolumeRefs pins that ordinary services are unaffected.
func TestWorkloadNoVolumeRefs(t *testing.T) {
	lid := testutil.LeaseID(t)
	_, workload := testSetup(t, "../../../testdata/deployment/deployment.yaml", 0, lid)

	require.False(t, workload.hasVolumeRefs())

	for _, vol := range workload.volumesObjs {
		require.Nil(t, vol.PersistentVolumeClaim)
	}
}
