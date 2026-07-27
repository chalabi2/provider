package inventory

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	inventoryv1 "pkg.akt.dev/go/inventory/v1"
	providerv1 "pkg.akt.dev/go/provider/v1"
)

type testClusterStatusClient struct {
	status *providerv1.ClusterStatus
	err    error
}

func (c testClusterStatusClient) StatusV1(context.Context) (*providerv1.ClusterStatus, error) {
	return c.status, c.err
}

type testCollector struct {
	name    string
	section EvidenceSection
	err     error
}

func (c testCollector) Name() string {
	return c.name
}

func (c testCollector) Collect(context.Context) (EvidenceSection, error) {
	return c.section, c.err
}

func TestClusterMaterialSource(t *testing.T) {
	status := &providerv1.ClusterStatus{
		Leases: providerv1.Leases{Active: 7},
		Inventory: providerv1.Inventory{
			Cluster: testCluster(),
		},
	}
	softwareIdentity := testSoftwareIdentity()
	source, err := NewClusterMaterialSource(ClusterMaterialSourceConfig{
		Status:            testClusterStatusClient{status: status},
		SoftwareVersion:   "v1.2.3",
		SoftwareSignature: []byte("release-signature"),
		SoftwareIdentity:  softwareIdentity,
		Collectors: []Collector{
			testCollector{
				name: "hardware",
				section: EvidenceSection{
					Payload: []byte("collector-payload"),
				},
			},
		},
	})
	require.NoError(t, err)

	material, err := source.SnapshotMaterial(context.Background())
	require.NoError(t, err)
	require.Equal(t, status.Inventory.Cluster, material.Cluster)
	require.Equal(t, uint32(7), material.ActiveLeases)
	require.Equal(t, "v1.2.3", material.SoftwareVersion)
	require.Equal(t, []byte("release-signature"), material.SoftwareSignature)
	require.Equal(t, softwareIdentity, material.SoftwareIdentity)
	require.NotSame(t, softwareIdentity, material.SoftwareIdentity)
	require.Len(t, material.EvidenceSections, 2)
	require.Equal(t, EvidenceSectionClusterStatus, material.EvidenceSections[0].Name)
	require.NotEmpty(t, material.EvidenceSections[0].Payload)
	require.Equal(t, inventoryv1.SnapshotEvidenceSection{
		Name:    "hardware",
		Payload: []byte("collector-payload"),
	}, material.EvidenceSections[1])

	var evidence providerv1.ClusterStatus
	require.NoError(t, evidence.Unmarshal(material.EvidenceSections[0].Payload))
	require.Equal(t, status.Leases.Active, evidence.Leases.Active)
	require.Equal(t,
		status.Inventory.Cluster.Nodes[0].Resources.CPU.Quantity.Allocatable.MilliValue(),
		evidence.Inventory.Cluster.Nodes[0].Resources.CPU.Quantity.Allocatable.MilliValue(),
	)
}

func TestClusterMaterialSourceValidatesConfig(t *testing.T) {
	source, err := NewClusterMaterialSource(ClusterMaterialSourceConfig{})
	require.ErrorIs(t, err, errMissingClusterStatusClient)
	require.Nil(t, source)
}

func TestClusterMaterialSourceReturnsStatusError(t *testing.T) {
	expected := errors.New("status failed")
	source, err := NewClusterMaterialSource(ClusterMaterialSourceConfig{
		Status: testClusterStatusClient{err: expected},
	})
	require.NoError(t, err)

	_, err = source.SnapshotMaterial(context.Background())
	require.ErrorIs(t, err, expected)
}

func TestClusterMaterialSourceRejectsMissingStatus(t *testing.T) {
	source, err := NewClusterMaterialSource(ClusterMaterialSourceConfig{
		Status: testClusterStatusClient{},
	})
	require.NoError(t, err)

	_, err = source.SnapshotMaterial(context.Background())
	require.ErrorIs(t, err, errMissingClusterStatus)
}

func TestClusterMaterialSourceReturnsCollectorError(t *testing.T) {
	expected := errors.New("collector failed")
	source, err := NewClusterMaterialSource(ClusterMaterialSourceConfig{
		Status: testClusterStatusClient{status: &providerv1.ClusterStatus{}},
		Collectors: []Collector{
			testCollector{name: "hardware", err: expected},
		},
	})
	require.NoError(t, err)

	_, err = source.SnapshotMaterial(context.Background())
	require.ErrorIs(t, err, expected)
	require.ErrorContains(t, err, "hardware")
}
