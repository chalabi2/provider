package inventory

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	inventoryv1 "pkg.akt.dev/go/inventory/v1"
)

type testMaterialSource struct {
	material SnapshotMaterial
	err      error
}

func (s testMaterialSource) SnapshotMaterial(context.Context) (SnapshotMaterial, error) {
	return s.material, s.err
}

func TestNewMaterialPayloadSourceValidatesConfig(t *testing.T) {
	source := testMaterialSource{}
	now := func() time.Time { return time.Unix(1, 0) }

	tests := []struct {
		name    string
		cfg     MaterialPayloadSourceConfig
		wantErr error
	}{
		{
			name: "success",
			cfg: MaterialPayloadSourceConfig{
				Source:   source,
				Provider: "akash1provider",
				ChainID:  "akashnet-2",
				Now:      now,
			},
		},
		{
			name: "missing source",
			cfg: MaterialPayloadSourceConfig{
				Provider: "akash1provider",
				ChainID:  "akashnet-2",
				Now:      now,
			},
			wantErr: errMissingMaterialSource,
		},
		{
			name: "missing provider",
			cfg: MaterialPayloadSourceConfig{
				Source:  source,
				ChainID: "akashnet-2",
				Now:     now,
			},
			wantErr: errMissingProviderAddress,
		},
		{
			name: "missing chain ID",
			cfg: MaterialPayloadSourceConfig{
				Source:   source,
				Provider: "akash1provider",
				Now:      now,
			},
			wantErr: errMissingChainID,
		},
		{
			name: "missing clock",
			cfg: MaterialPayloadSourceConfig{
				Source:   source,
				Provider: "akash1provider",
				ChainID:  "akashnet-2",
			},
			wantErr: errMissingSnapshotClock,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, err := NewMaterialPayloadSource(test.cfg)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				require.Nil(t, payload)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, payload)
		})
	}
}

func TestMaterialPayloadSourcePayload(t *testing.T) {
	nonce := bytes.Repeat([]byte{1}, NonceSize)
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	material := SnapshotMaterial{
		Cluster:           testCluster(),
		ActiveLeases:      3,
		SoftwareVersion:   "v1.2.3",
		SoftwareSignature: []byte("release-signature"),
		SoftwareIdentity:  testSoftwareIdentity(),
		EvidenceSections: []inventoryv1.SnapshotEvidenceSection{
			{
				Name:    "operator-inventory",
				Payload: []byte("operator-material"),
			},
		},
	}
	source, err := NewMaterialPayloadSource(MaterialPayloadSourceConfig{
		Source:   testMaterialSource{material: material},
		Provider: "akash1provider",
		ChainID:  "akashnet-2",
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)

	payload, err := source.Payload(context.Background(), SnapshotRequest{Nonce: nonce})
	require.NoError(t, err)

	var decoded inventoryv1.SnapshotPayload
	require.NoError(t, decoded.Unmarshal(payload))
	require.Equal(t, SnapshotPayloadSchemaVersion, decoded.SchemaVersion)
	require.Equal(t, "akash1provider", decoded.Provider)
	require.Equal(t, "akashnet-2", decoded.ChainID)
	require.Equal(t, nonce, decoded.Nonce)
	require.Equal(t, now, decoded.Timestamp)
	require.Equal(t, uint32(3), decoded.ResourceSummary.ActiveLeases)
	require.Equal(t, []inventoryv1.SnapshotEvidenceSection{
		{
			Name:    "operator-inventory",
			Payload: []byte("operator-material"),
		},
	}, decoded.EvidenceSections)
}

func TestMaterialPayloadSourcePayloadReturnsSourceError(t *testing.T) {
	expected := errors.New("source failed")
	source, err := NewMaterialPayloadSource(MaterialPayloadSourceConfig{
		Source:   testMaterialSource{err: expected},
		Provider: "akash1provider",
		ChainID:  "akashnet-2",
		Now:      func() time.Time { return time.Unix(1, 0) },
	})
	require.NoError(t, err)

	payload, err := source.Payload(context.Background(), SnapshotRequest{})
	require.ErrorIs(t, err, expected)
	require.Nil(t, payload)
}

func TestResourceSummaryFromClusterSaturatesUint32(t *testing.T) {
	const maxUint32 = 1<<32 - 1

	cluster := inventoryv1.Cluster{
		Nodes: inventoryv1.Nodes{
			{
				Resources: inventoryv1.NodeResources{
					CPU: inventoryv1.CPU{
						Quantity: inventoryv1.ResourcePair{
							Allocatable: resource.NewMilliQuantity((maxUint32+1)*1000, resource.DecimalSI),
						},
					},
					GPU: inventoryv1.GPU{
						Quantity: inventoryv1.ResourcePair{
							Allocatable: resource.NewQuantity(maxUint32+1, resource.DecimalSI),
						},
					},
				},
			},
		},
	}

	summary := ResourceSummaryFromCluster(cluster, 0, "", nil, nil)
	require.Equal(t, uint32(maxUint32), summary.TotalVCPUs)
	require.Equal(t, uint32(maxUint32), summary.TotalGPUs)
}

func testSoftwareIdentity() *inventoryv1.SoftwareIdentity {
	return &inventoryv1.SoftwareIdentity{
		Version:         "v1.2.3",
		ArtifactRef:     "ghcr.io/akash-network/provider:v1.2.3",
		DigestAlgorithm: "sha3-256",
		Digest:          bytes.Repeat([]byte{2}, 32),
		SignatureType:   "cosign_keyful",
		Signature:       []byte("release-signature"),
		SignatureRef:    "ghcr.io/akash-network/provider@sha256:signature",
		PublicKeyRef:    "github.com/akash-network/releases/provider.pub",
	}
}

func testCluster() inventoryv1.Cluster {
	return inventoryv1.Cluster{
		Nodes: inventoryv1.Nodes{
			{
				Name: "node-1",
				Resources: inventoryv1.NodeResources{
					CPU: inventoryv1.CPU{
						Quantity: inventoryv1.NewResourcePairMilli(2500, 2500, 0, resource.DecimalSI),
					},
					GPU: inventoryv1.GPU{
						Quantity: inventoryv1.NewResourcePair(1, 1, 0, resource.DecimalSI),
					},
					Memory: inventoryv1.Memory{
						Quantity: inventoryv1.NewResourcePair(16*1024*1024*1024, 16*1024*1024*1024, 0, resource.BinarySI),
					},
					EphemeralStorage: inventoryv1.NewResourcePair(10*1024*1024*1024, 10*1024*1024*1024, 0, resource.BinarySI),
				},
			},
			{
				Name: "node-2",
				Resources: inventoryv1.NodeResources{
					CPU: inventoryv1.CPU{
						Quantity: inventoryv1.NewResourcePairMilli(1500, 1500, 0, resource.DecimalSI),
					},
					GPU: inventoryv1.GPU{
						Quantity: inventoryv1.NewResourcePair(1, 1, 0, resource.DecimalSI),
					},
					Memory: inventoryv1.Memory{
						Quantity: inventoryv1.NewResourcePair(16*1024*1024*1024, 16*1024*1024*1024, 0, resource.BinarySI),
					},
				},
			},
		},
		Storage: inventoryv1.ClusterStorage{
			{
				Quantity: inventoryv1.NewResourcePair(100*1024*1024*1024, 100*1024*1024*1024, 0, resource.BinarySI),
				Info: inventoryv1.StorageInfo{
					Class: "beta2",
				},
			},
		},
	}
}
