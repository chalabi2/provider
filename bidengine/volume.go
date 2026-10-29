package bidengine

import (
	"context"
	"sync"
	"time"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dtypes "pkg.akt.dev/go/node/deployment/v1beta4"
	mtypes "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v1beta5"
	provider "pkg.akt.dev/go/provider/v1"
	"pkg.akt.dev/go/sdl"
)

// VolumeConfig holds the provider caps for bidding on storage-only (volume)
// orders (decoupled storage market). Volume bidding is enabled per storage class: a class is
// eligible only when listed in Classes, which asserts the matching -retain
// StorageClass is installed in the cluster (the storage operator installs
// them). An empty Classes list disables volume bidding entirely.
type VolumeConfig struct {
	// Classes lists the storage classes offered as standalone volumes.
	Classes []string
	// MaxSize is the largest volume, in bytes, the provider bids on.
	// Zero means no provider-side cap (chain params still apply).
	MaxSize uint64
	// MaxRetention is the longest post-close retention window the provider
	// honors. Orders asking for more are declined.
	MaxRetention time.Duration
	// MaxReplicas is the highest replica count the provider supports
	// exporting. Zero declines any order with replication terms.
	MaxReplicas uint32
	// GrowableClasses lists the volume classes whose StorageClass can expand
	// a bound claim in place. A mounted volume of any other class is held to
	// its size: accepting its growth would bill the tenant for space the
	// claim never gets. A bucket grows by a quota raise and needs no class
	// here.
	GrowableClasses []string
	// BucketClasses lists the storage classes whose capacity backs bucket
	// volumes, served from the provider's S3 server. Empty declines every
	// bucket order.
	BucketClasses []string
}

// ClassEnabled reports whether the class is configured for volume bidding.
func (c VolumeConfig) ClassEnabled(class string) bool {
	return containsClass(c.Classes, class)
}

// BucketClassEnabled reports whether the class is configured for buckets.
func (c VolumeConfig) BucketClassEnabled(class string) bool {
	return containsClass(c.BucketClasses, class)
}

// ClassGrowable reports whether volumes of the class can grow in place.
func (c VolumeConfig) ClassGrowable(class string) bool {
	return containsClass(c.GrowableClasses, class)
}

func containsClass(classes []string, class string) bool {
	for _, cl := range classes {
		if cl == class {
			return true
		}
	}

	return false
}

// StorageInventory reports live per-class persistent storage headroom taken
// from the retained cluster inventory snapshot. It closes the gap between
// advertised on-chain capabilities and actual cluster state for the storage
// path: volume orders gate on it before reserving.
type StorageInventory interface {
	// AvailableStorage returns the currently available bytes for the class.
	// The second value is false when the class is unknown or no inventory
	// snapshot has been received yet.
	AvailableStorage(class string) (uint64, bool)
}

// VolumeLookup answers cheap local Volume CRD pre-checks for the bid engine.
// The chain gates are the authority (adoption bids are gated to the retaining
// provider, attach bids to the colocated provider); these checks only avoid
// broadcasting bids the chain would reject. The storage operator client
// provides the implementation; a nil lookup skips the pre-checks.
type VolumeLookup interface {
	// RetainedVolume reports whether owner/vid is a local Volume CRD the
	// deployment dseq may adopt — the adoption pre-check.
	RetainedVolume(ctx context.Context, owner string, vid string, dseq uint64) (bool, error)
	// ParkedVolume reports whether a local Volume CRD in the Parked phase
	// matches the attach reference.
	ParkedVolume(ctx context.Context, ref dv1.VolumeRef) (bool, error)
}

// inventoryWait bounds how long a volume order waits for the first
// inventory snapshot. Orders open when the provider starts are evaluated
// right away, before the snapshot arrives.
const inventoryWait = 2 * time.Minute

// readyInventory is a StorageInventory that tells when its first snapshot
// has arrived.
type readyInventory interface {
	Ready() <-chan struct{}
}

// storageInventory is the StorageInventory implementation fed by the
// bidengine service from the retained inventory-status pubsub topic.
type storageInventory struct {
	mu    sync.RWMutex
	avail map[string]uint64

	ready     chan struct{}
	readyOnce sync.Once
}

var (
	_ StorageInventory = (*storageInventory)(nil)
	_ readyInventory   = (*storageInventory)(nil)
)

func newStorageInventory() *storageInventory {
	return &storageInventory{ready: make(chan struct{})}
}

func (si *storageInventory) Ready() <-chan struct{} {
	return si.ready
}

func (si *storageInventory) update(inv *provider.Inventory) {
	avail := make(map[string]uint64, len(inv.Cluster.Storage))

	for _, storage := range inv.Cluster.Storage {
		val := storage.Quantity.Available().Value()
		if val < 0 {
			val = 0
		}

		avail[storage.Info.Class] = uint64(val)
	}

	si.mu.Lock()
	si.avail = avail
	si.mu.Unlock()

	si.readyOnce.Do(func() { close(si.ready) })
}

func (si *storageInventory) AvailableStorage(class string) (uint64, bool) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	if si.avail == nil {
		return 0, false
	}

	size, exists := si.avail[class]

	return size, exists
}

// volumeStorage extracts the class and size of a volume group's single
// storage entry. ValidateBasic guarantees the shape; ok is false otherwise.
func volumeStorage(gspec *dtypes.GroupSpec) (string, uint64, bool) {
	if len(gspec.Resources) != 1 || len(gspec.Resources[0].Storage) != 1 {
		return "", 0, false
	}

	storage := gspec.Resources[0].Storage[0]

	class, set := storage.Attributes.Find(sdl.StorageAttributeClass).AsString()
	if !set {
		return "", 0, false
	}

	return class, storage.Quantity.Value(), true
}

// shouldBidVolume is the storage-only (volume) order counterpart of
// shouldBid. CPU/GPU/endpoint gates do not apply (the resource shape is
// present-but-zero by validation); placement and audit gates are identical
// to the compute path. On top of those it requires the class in the
// provider's advertised capabilities AND configured for volume bidding
// (-retain class installed), the provider caps satisfied, and live per-class
// headroom in the retained cluster inventory.
func (o *order) shouldBidVolume(ctx context.Context, group *dtypes.Group) (bool, error) {
	gates := o.gates()

	if ok, err := gates.volumeTerms(group); err != nil || !ok {
		return ok, err
	}

	// a replica beside its primary adds no redundancy, and it never syncs:
	// a provider does not export a volume to itself
	if ref := group.GroupSpec.Volume.ReplicaOf; ref != nil {
		here, err := o.primaryLeasedHere(ctx, ref)
		if err != nil {
			return false, err
		}

		if here {
			o.log.Debug("unable to fulfill: replica of a volume this provider holds", "primary", ref.String())
			return false, nil
		}
	}

	if err := o.waitForInventory(ctx); err != nil {
		return false, err
	}

	class, size, _ := volumeStorage(&group.GroupSpec)
	if !gates.volumeHeadroom(class, size) {
		return false, nil
	}

	// adoption pre-check: the retained volume must exist locally. Cheap
	// local filter only — the chain gates adoption bids to the retaining
	// provider regardless, so no lookup wired means no filter.
	if vol := group.GroupSpec.Volume; vol.Adopt != nil && o.volumes != nil {
		found, err := o.volumes.RetainedVolume(ctx, group.ID.Owner, vol.Vid, group.ID.DSeq)
		if err != nil {
			return false, err
		}

		if !found {
			o.log.Debug("unable to fulfill: no retained volume for adoption", "owner", group.ID.Owner, "vid", vol.Vid)
			return false, nil
		}
	}

	return true, nil
}

// primaryLeasedHere reports whether this provider holds the active lease of
// the volume group ref names, as the chain has it.
func (o *order) primaryLeasedHere(ctx context.Context, ref *dv1.VolumeRef) (bool, error) {
	resp, err := o.session.Client().Query().Market().Leases(ctx, &mvbeta.QueryLeasesRequest{
		Filters: mtypes.LeaseFilters{
			Owner:    ref.Owner,
			DSeq:     ref.DSeq,
			GSeq:     ref.GSeq,
			Provider: o.session.Provider().Address().String(),
			State:    mtypes.LeaseActive.String(),
		},
	})
	if err != nil {
		return false, err
	}

	return len(resp.Leases) > 0, nil
}

// waitForInventory holds a volume order until the first inventory snapshot
// arrives, for at most inventoryWait; past that the headroom gate declines.
func (o *order) waitForInventory(ctx context.Context) error {
	ready, ok := o.storage.(readyInventory)
	if !ok {
		return nil
	}

	timer := time.NewTimer(inventoryWait)
	defer timer.Stop()

	select {
	case <-ready.Ready():
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// volumeTerms runs the shouldBidVolume gates that do not depend on live
// headroom or local Volume CRDs: placement, audit, class and provider caps.
func (g bidGates) volumeTerms(group *dtypes.Group) (bool, error) {
	gspec := &group.GroupSpec
	vol := gspec.Volume

	// does provider have required attributes?
	if !gspec.MatchAttributes(g.session.Provider().Attributes) {
		g.log.Debug("unable to fulfill: incompatible provider attributes")
		return false, nil
	}

	// does order have required attributes?
	if !g.cfg.Attributes.SubsetOf(gspec.Requirements.Attributes) {
		g.log.Debug("unable to fulfill: incompatible order attributes")
		return false, nil
	}

	attr, err := g.pass.GetAttributes()
	if err != nil {
		return false, err
	}

	// is the storage class in the advertised capabilities? The storage leg
	// of MatchResourcesRequirements is exactly the class capability check;
	// the GPU leg is inert on the zero-GPU volume shape.
	if !gspec.MatchResourcesRequirements(attr) {
		g.log.Debug("unable to fulfill: incompatible attributes for resources requirements", "wanted", gspec, "have", attr)
		return false, nil
	}

	// audit gate — identical to the compute path
	if ok, err := g.matchSignatureRequirements(gspec); err != nil || !ok {
		if err == nil {
			g.log.Debug("attribute signature requirements not met")
		}
		return false, err
	}

	if err := gspec.ValidateBasic(); err != nil {
		g.log.Error("unable to fulfill: volume group validation error", "err", err)
		return false, nil
	}

	class, size, ok := volumeStorage(gspec)
	if !ok {
		g.log.Error("unable to fulfill: malformed volume group storage")
		return false, nil
	}

	// participation knob: a bucket needs the provider's S3 server, with the
	// class naming the capacity behind it; a mounted volume needs the class
	// configured for volume bidding, which asserts the matching -retain
	// StorageClass is installed
	if vol.IsBucket() {
		if !g.cfg.Volumes.BucketClassEnabled(class) {
			g.log.Debug("unable to fulfill: storage class not configured for buckets", "class", class)
			return false, nil
		}
	} else if !g.cfg.Volumes.ClassEnabled(class) {
		g.log.Debug("unable to fulfill: storage class not configured for volumes", "class", class)
		return false, nil
	}

	// provider caps
	if g.cfg.Volumes.MaxSize > 0 && size > g.cfg.Volumes.MaxSize {
		g.log.Debug("unable to fulfill: volume size exceeds provider cap", "size", size, "max", g.cfg.Volumes.MaxSize)
		return false, nil
	}

	if vol.Retention > g.cfg.Volumes.MaxRetention {
		g.log.Debug("unable to fulfill: volume retention exceeds provider cap", "retention", vol.Retention, "max", g.cfg.Volumes.MaxRetention)
		return false, nil
	}

	if vol.MaxReplicas > g.cfg.Volumes.MaxReplicas {
		g.log.Debug("unable to fulfill: volume max_replicas exceeds provider cap", "max_replicas", vol.MaxReplicas, "max", g.cfg.Volumes.MaxReplicas)
		return false, nil
	}

	if vol.ReplicaOf != nil && g.cfg.Volumes.MaxReplicas == 0 {
		g.log.Debug("unable to fulfill: replica volume but replication not supported")
		return false, nil
	}

	return true, nil
}

// volumeHeadroom reports whether the class has need more bytes of live
// headroom in the retained cluster inventory.
func (g bidGates) volumeHeadroom(class string, need uint64) bool {
	avail, found := g.storage.AvailableStorage(class)
	if !found {
		g.log.Debug("unable to fulfill: no live inventory for storage class", "class", class)
		return false
	}

	if avail < need {
		g.log.Debug("unable to fulfill: insufficient storage class headroom", "class", class, "available", avail, "requested", need)
		return false
	}

	return true
}
