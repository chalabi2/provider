package inventory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/troian/pubsub"
	"k8s.io/apimachinery/pkg/api/resource"

	inventory "pkg.akt.dev/go/inventory/v1"

	"github.com/akash-network/provider/tools/fromctx"
)

func storageEntry(class string, capacity, allocated int64) inventory.Storage {
	return inventory.Storage{
		Quantity: inventory.NewResourcePair(capacity, capacity, allocated, resource.DecimalSI),
		Info: inventory.StorageInfo{
			Class: class,
		},
	}
}

// TestClusterStateVolumeOverlay pins the AEP-87 inventory leg: the volumes
// driver's allocated-only entries merge INTO the capacity drivers' classes
// (never appended as duplicates), so parked/retained volumes count against
// base-class headroom, and re-publishes do not accumulate.
func TestClusterStateVolumeOverlay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := pubsub.New(ctx, 100)
	ctx = context.WithValue(ctx, fromctx.CtxKeyPubSub, pubsub.PubSub(bus))

	state := &clusterState{
		ctx:            ctx,
		querierCluster: newQuerierCluster(),
	}

	go func() {
		_ = state.run()
	}()

	cfg := Config{
		ClusterStorage: []string{"beta3"},
	}

	bus.Pub(cfg, []string{topicInventoryConfig}, pubsub.WithRetain())

	// capacity driver reports 1000 capacity / 100 allocated for beta3
	bus.Pub(storageSignal{
		driver:  "ceph",
		storage: inventory.ClusterStorage{storageEntry("beta3", 1000, 100)},
	}, []string{topicInventoryStorage})

	// volumes driver overlays 250 allocated bytes (parked/retained volumes)
	// plus a class with no capacity provider, which must not appear
	bus.Pub(storageSignal{
		driver: driverVolumes,
		storage: inventory.ClusterStorage{
			storageEntry("beta3", 0, 250),
			storageEntry("beta2", 0, 50),
		},
	}, []string{topicInventoryStorage})

	require.Eventually(t, func() bool {
		cluster, err := state.Query(ctx)
		require.NoError(t, err)

		if len(cluster.Storage) != 1 {
			return false
		}

		entry := cluster.Storage[0]

		return entry.Info.Class == "beta3" && entry.Quantity.Allocated.Value() == 350
	}, 5*time.Second, 20*time.Millisecond)

	// a re-publish of the same overlay must not accumulate, and the base
	// driver's retained slice must not have been mutated by the overlay
	bus.Pub(storageSignal{
		driver: driverVolumes,
		storage: inventory.ClusterStorage{
			storageEntry("beta3", 0, 250),
		},
	}, []string{topicInventoryStorage})

	require.Eventually(t, func() bool {
		cluster, err := state.Query(ctx)
		require.NoError(t, err)

		if len(cluster.Storage) != 1 {
			return false
		}

		return cluster.Storage[0].Quantity.Allocated.Value() == 350
	}, 5*time.Second, 20*time.Millisecond)

	// volumes released: the overlay clears back to the capacity numbers
	bus.Pub(storageSignal{
		driver:  driverVolumes,
		storage: inventory.ClusterStorage{},
	}, []string{topicInventoryStorage})

	require.Eventually(t, func() bool {
		cluster, err := state.Query(ctx)
		require.NoError(t, err)

		if len(cluster.Storage) != 1 {
			return false
		}

		return cluster.Storage[0].Quantity.Allocated.Value() == 100
	}, 5*time.Second, 20*time.Millisecond)
}
