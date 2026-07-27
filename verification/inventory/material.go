package inventory

import (
	"context"
	"errors"
	"fmt"

	inventoryv1 "pkg.akt.dev/go/inventory/v1"
	providerv1 "pkg.akt.dev/go/provider/v1"
)

const EvidenceSectionClusterStatus = "akash.provider.v1.ClusterStatus"

var (
	errMissingClusterStatusClient = errors.New("missing cluster status client")
	errMissingClusterStatus       = errors.New("missing provider cluster status")
)

type ClusterStatusClient interface {
	StatusV1(context.Context) (*providerv1.ClusterStatus, error)
}

type ClusterMaterialSourceConfig struct {
	Status            ClusterStatusClient
	SoftwareVersion   string
	SoftwareSignature []byte
	SoftwareIdentity  *inventoryv1.SoftwareIdentity
	Collectors        []Collector
}

type ClusterMaterialSource struct {
	status            ClusterStatusClient
	softwareVersion   string
	softwareSignature []byte
	softwareIdentity  *inventoryv1.SoftwareIdentity
	collectors        []Collector
}

func NewClusterMaterialSource(cfg ClusterMaterialSourceConfig) (*ClusterMaterialSource, error) {
	if cfg.Status == nil {
		return nil, errMissingClusterStatusClient
	}

	return &ClusterMaterialSource{
		status:            cfg.Status,
		softwareVersion:   cfg.SoftwareVersion,
		softwareSignature: append([]byte(nil), cfg.SoftwareSignature...),
		softwareIdentity:  cloneSoftwareIdentity(cfg.SoftwareIdentity),
		collectors:        append([]Collector(nil), cfg.Collectors...),
	}, nil
}

func (s *ClusterMaterialSource) SnapshotMaterial(ctx context.Context) (SnapshotMaterial, error) {
	status, err := s.status.StatusV1(ctx)
	if err != nil {
		return SnapshotMaterial{}, err
	}
	if status == nil {
		return SnapshotMaterial{}, errMissingClusterStatus
	}

	evidence, err := s.collect(ctx)
	if err != nil {
		return SnapshotMaterial{}, err
	}

	statusEvidence, err := clusterStatusEvidenceSection(status)
	if err != nil {
		return SnapshotMaterial{}, err
	}
	evidence = append([]inventoryv1.SnapshotEvidenceSection{statusEvidence}, evidence...)

	clusterInventory := status.GetInventory()
	leases := status.GetLeases()

	return SnapshotMaterial{
		Cluster:           clusterInventory.GetCluster(),
		ActiveLeases:      leases.GetActive(),
		EvidenceSections:  evidence,
		SoftwareVersion:   s.softwareVersion,
		SoftwareSignature: append([]byte(nil), s.softwareSignature...),
		SoftwareIdentity:  cloneSoftwareIdentity(s.softwareIdentity),
	}, nil
}

func (s *ClusterMaterialSource) collect(ctx context.Context) ([]inventoryv1.SnapshotEvidenceSection, error) {
	sections := make([]inventoryv1.SnapshotEvidenceSection, 0, len(s.collectors))
	for _, collector := range s.collectors {
		if collector == nil {
			continue
		}

		section, err := collector.Collect(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", collector.Name(), err)
		}

		name := section.Name
		if name == "" {
			name = collector.Name()
		}

		sections = append(sections, inventoryv1.SnapshotEvidenceSection{
			Name:    name,
			Payload: append([]byte(nil), section.Payload...),
		})
	}

	return sections, nil
}

func clusterStatusEvidenceSection(status *providerv1.ClusterStatus) (inventoryv1.SnapshotEvidenceSection, error) {
	payload, err := MarshalDeterministic(status)
	if err != nil {
		return inventoryv1.SnapshotEvidenceSection{}, err
	}

	return inventoryv1.SnapshotEvidenceSection{
		Name:    EvidenceSectionClusterStatus,
		Payload: payload,
	}, nil
}

var _ SnapshotMaterialSource = (*ClusterMaterialSource)(nil)
