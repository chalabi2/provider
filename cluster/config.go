package cluster

import (
	"time"

	"github.com/akash-network/provider/cluster/kube/builder"
)

// DefaultLeaseResizeManifestTimeout is how long a lease resize waits for the
// tenant's manifest at the new size by default. A client that rolls on its
// own, as Console does, sends it seconds after the accept; ten minutes, the
// chain's default lease_resize_ttl, gives a tenant sending it by hand as long
// to roll as the request had to be answered.
const DefaultLeaseResizeManifestTimeout = 10 * time.Minute

type Config struct {
	InventoryResourcePollPeriod     time.Duration
	InventoryResourceDebugFrequency uint
	InventoryExternalPortQuantity   uint
	CPUCommitLevel                  float64
	GPUCommitLevel                  float64
	MemoryCommitLevel               float64
	StorageCommitLevel              float64
	BlockedHostnames                []string
	DeploymentIngressStaticHosts    bool
	DeploymentIngressDomain         string
	MonitorMaxRetries               uint
	MonitorRetryPeriod              time.Duration
	MonitorRetryPeriodJitter        time.Duration
	MonitorHealthcheckPeriod        time.Duration
	MonitorHealthcheckPeriodJitter  time.Duration
	IngressMode                     builder.IngressMode
	GatewayName                     string
	GatewayNamespace                string
	GatewayProvider                 string
	ReclamationRPCTimeout           time.Duration
	ReclamationCloseRetryInterval   time.Duration
	ClusterSettings                 map[interface{}]interface{}
	// LeaseResizeManifestTimeout bounds how long a lease resize keeps the
	// lease's reservation pending, holding the new size on top of the pods
	// from before the resize, while it waits for the tenant's manifest at
	// the new size. Zero waits without a bound.
	LeaseResizeManifestTimeout time.Duration
}

func NewDefaultConfig() Config {
	return Config{
		InventoryResourcePollPeriod:     time.Second * 5,
		InventoryResourceDebugFrequency: 10,
		MonitorMaxRetries:               40,
		MonitorRetryPeriod:              time.Second * 4, // nolint revive
		MonitorRetryPeriodJitter:        time.Second * 15,
		IngressMode:                     builder.IngressModeIngress,
		GatewayName:                     "akash-gateway",
		GatewayNamespace:                "akash-gateway",
		GatewayProvider:                 "nginx",
		ReclamationRPCTimeout:           30 * time.Second,
		ReclamationCloseRetryInterval:   time.Minute,
		LeaseResizeManifestTimeout:      DefaultLeaseResizeManifestTimeout,
	}
}
