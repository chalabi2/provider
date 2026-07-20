package inventory

import (
	"context"
	"sort"
	"strconv"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	inventory "pkg.akt.dev/go/inventory/v1"

	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	"github.com/akash-network/provider/tools/fromctx"
)

// driverVolumes publishes AEP-87 first-class volume allocations. Unlike the
// capacity drivers (ceph, rancher) its entries carry allocated bytes only;
// the cluster-state merge overlays them onto the matching base class so
// parked and retained volumes are never double-counted as free after their
// PV moved off the sellable class onto the -retain twin.
const driverVolumes = "volumes"

type volumes struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// NewVolumes starts the Volume CRD scraper. The CRD may not be installed on
// non-participating clusters: absence is tolerated by polling, the same
// stance as the rook CRD discovery.
func NewVolumes(ctx context.Context) (QuerierStorage, error) {
	group, err := fromctx.ErrGroupFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	v := &volumes{
		ctx:    ctx,
		cancel: cancel,
	}

	startch := make(chan struct{}, 1)

	group.Go(func() error {
		return v.run(startch)
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-startch:
	}

	return v, nil
}

func (v *volumes) run(startch chan<- struct{}) error {
	defer v.cancel()

	bus := fromctx.MustPubSubFromCtx(v.ctx)
	log := fromctx.LogrFromCtx(v.ctx).WithName(driverVolumes)

	ac, err := fromctx.AkashClientFromCtx(v.ctx)
	if err != nil {
		return err
	}

	scrapeTicker := time.NewTicker(crdDiscoverPeriod)
	defer scrapeTicker.Stop()

	startch <- struct{}{}

	first := true

	var lastPublished string

	for {
		select {
		case <-v.ctx.Done():
			return v.ctx.Err()
		case <-scrapeTicker.C:
			list, err := ac.AkashV2beta2().Volumes(metav1.NamespaceAll).List(v.ctx, metav1.ListOptions{})
			if err != nil {
				// CRD not installed (or transient error): tolerated, retried
				if !kerrors.IsNotFound(err) {
					log.Error(err, "unable to list volumes")
				}

				continue
			}

			allocated := make(map[string]uint64)

			for i := range list.Items {
				vol := &list.Items[i]

				// a releasing volume is on its way out; everything else -
				// parked, attached, retained, adopting, exporting - is
				// durable leased/held bytes
				if vol.Status.Phase == crd.VolumePhaseReleasing {
					continue
				}

				size, err := strconv.ParseUint(vol.Spec.Size, 10, 64)
				if err != nil {
					log.Error(err, "invalid volume size", "volume", vol.Name, "size", vol.Spec.Size)
					continue
				}

				allocated[vol.Spec.Class] += size
			}

			classes := make([]string, 0, len(allocated))
			for class := range allocated {
				classes = append(classes, class)
			}
			sort.Strings(classes)

			res := make(inventory.ClusterStorage, 0, len(classes))
			fingerprint := ""

			for _, class := range classes {
				res = append(res, inventory.Storage{
					Quantity: inventory.ResourcePair{
						Allocated:   resource.NewQuantity(int64(allocated[class]), resource.DecimalSI), // nolint: gosec
						Allocatable: resource.NewQuantity(0, resource.DecimalSI),
						Capacity:    resource.NewQuantity(0, resource.DecimalSI),
					},
					Info: inventory.StorageInfo{
						Class: class,
					},
				})

				fingerprint += class + ":" + strconv.FormatUint(allocated[class], 10) + ";"
			}

			// publish on change and on the first (possibly empty) scrape so
			// a volume-free cluster still clears stale overlays
			if !first && fingerprint == lastPublished {
				continue
			}

			first = false
			lastPublished = fingerprint

			bus.Pub(storageSignal{
				driver:  driverVolumes,
				storage: res,
			}, []string{topicInventoryStorage})
		}
	}
}
