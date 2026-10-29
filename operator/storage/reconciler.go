package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"cosmossdk.io/log"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mv1 "pkg.akt.dev/go/node/market/v1"

	"github.com/akash-network/provider/bucket"
	"github.com/akash-network/provider/cluster/kube/builder"
	clusterutil "github.com/akash-network/provider/cluster/util"
	"github.com/akash-network/provider/operator/storage/replication"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	akashclientset "github.com/akash-network/provider/pkg/client/clientset/versioned"
)

// ChainQuery is the narrow chain read the reconciler needs: adoption
// verification, and where a moved volume's data comes from. A nil
// implementation freezes adoptions in the Adopting phase (GC stays frozen),
// and volumes won on a re-listed order in Pending, until chain access is
// available.
type ChainQuery interface {
	// GroupVolumePolicy returns the VolumePolicy of the on-chain group, or
	// nil when the group is not a volume group.
	GroupVolumePolicy(ctx context.Context, id dv1.GroupID) (*dv1.VolumePolicy, error)

	// VolumeLeases returns every lease on a volume group naming owner/vid.
	VolumeLeases(ctx context.Context, owner, vid string) ([]replication.VolumeLease, error)
}

// reconciler drives a Volume CRD to its desired state through the PV
// choreography of IMPLEMENTATION.md §5.2. The PV never passes through the
// unclaimed Available phase: it moves holder-PVC -> target-PVC -> holder-PVC
// by direct claimRef rewrites, so the kube binder can never hand a Retain PV
// with tenant data to an arbitrary pending PVC.
//
// All state is re-derivable: the CRD records desired state (spec + status
// written by the daemon and the chain watch), the PV/PVCs are inspected
// fresh on every pass, and adoption is verified against chain state.
type reconciler struct {
	kc    kubernetes.Interface
	ac    akashclientset.Interface
	ns    string // namespace the Volume CRDs live in
	volNS string // parking namespace for holder PVCs
	chain ChainQuery
	log   log.Logger
	now   func() time.Time

	// buckets serves bucket volumes; nil when the provider serves none
	buckets bucket.Backend

	// nodeHint annotates the initial provisioning claim with
	// volume.kubernetes.io/selected-node. Node-constrained provisioners
	// (rancher.io/local-path) cannot provision an Immediate-binding claim
	// without it ("configuration error, no node was specified") because no
	// consuming pod ever schedules the parked holder PVC. Network-attached
	// provisioners (Ceph RBD) don't need the hint; leave it off there so
	// topology stays unconstrained.
	nodeHint bool
}

func newReconciler(kc kubernetes.Interface, ac akashclientset.Interface, ns, volNS string, chain ChainQuery, logger log.Logger) *reconciler {
	return &reconciler{
		kc:    kc,
		ac:    ac,
		ns:    ns,
		volNS: volNS,
		chain: chain,
		log:   logger,
		now:   time.Now,
	}
}

// pvcSelectedNodeAnnotation is how the WaitForFirstConsumer scheduler hands
// external provisioners the chosen node; setting it directly is the
// established way to drive node-constrained provisioning without a pod.
const pvcSelectedNodeAnnotation = "volume.kubernetes.io/selected-node"

// hintNode picks the node the provisioning claim is pinned to: the first
// Ready, schedulable node carrying the class capability label (the same
// label the inventory operator's node discovery writes and the scheduler
// affinity in cluster/kube/builder keys on), by name for determinism.
func (r *reconciler) hintNode(ctx context.Context, class string) (string, error) {
	sel := fmt.Sprintf("%s.class.%s=1", builder.AkashServiceCapabilityStorage, class)

	nodes, err := r.kc.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return "", err
	}

	names := make([]string, 0, len(nodes.Items))
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if node.Spec.Unschedulable {
			continue
		}
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
				names = append(names, node.Name)
				break
			}
		}
	}

	if len(names) == 0 {
		return "", fmt.Errorf("%w: no ready node carries storage class %q", ErrVolumeOperator, class)
	}

	sort.Strings(names)

	return names[0], nil
}

func (r *reconciler) reconcile(ctx context.Context, vol *crd.Volume) error {
	vol = vol.DeepCopy()

	if vol.IsBucket() {
		return r.reconcileBucket(ctx, vol)
	}

	// the single destruction path: a positive chain-computable deadline,
	// never EventLeaseClosed inference
	if r.gcDue(vol) {
		return r.gc(ctx, vol)
	}

	switch vol.Status.Phase {
	case crd.VolumePhaseExporting:
		// migration export in flight: GC frozen, PV untouched. Once the
		// lease has closed (retention stamped) and the move's hold ended,
		// the export duty is over: thaw to Retained, and the next pass
		// takes the single destruction path.
		if released, err := r.moveReleased(ctx, vol); err != nil || !released {
			return err
		}

		r.log.Info("moved volume released", "volume", vol.Name, "exported_to", vol.Status.ExportedTo)

		vol.Status.Phase = crd.VolumePhaseRetained

		return r.updateStatus(ctx, vol)
	case crd.VolumePhaseReleasing:
		// destruction stamped but deadline not reached (defensive; the
		// teardown path stamps retainedUntil alongside)
		return nil
	default:
	}

	if vol.Status.PVName == "" {
		return r.provision(ctx, vol)
	}

	if handled, err := r.maybeAdopt(ctx, vol); handled || err != nil {
		return err
	}

	// won again on a re-listed order with the whole copy this provider
	// held before: it is still the latest only if no provider held the
	// volume since
	if vol.Status.Phase == crd.VolumePhasePending && vol.Spec.ImportFrom == "" {
		if err := r.recordImportSource(ctx, vol, true); err != nil {
			return err
		}
	}

	if vol.Status.AttachedLease != "" {
		if err := r.ensureAttached(ctx, vol); err != nil {
			return err
		}
	} else if err := r.ensureParked(ctx, vol); err != nil {
		return err
	}

	return r.ensureSize(ctx, vol)
}

// exportGrace is how long a volume that moved away is still kept once its
// final copy reached the provider it moved to: time for that provider to
// verify the copy, or to pull it again.
const exportGrace = time.Hour

// moveReleased reports whether a volume that moved away may go: its lease
// closed for a move (retention stamped) and the hold that keeps it for the
// provider it moved to ended. The hold ends exportGrace after a final
// export to that provider ran to its end, but never before the retention
// the tenant leased ends, and it ends at heldUntil, close + max(retention,
// hold), whether a copy arrived or not. A volume that was never whole here
// has nothing to give, so only its retention keeps it, as it does a record
// stamped before holds existed.
func (r *reconciler) moveReleased(ctx context.Context, vol *crd.Volume) (bool, error) {
	if vol.Status.RetainedUntil == nil {
		// the reclamation window: the lease is still paid
		return false, nil
	}

	now := r.now()

	if !now.After(vol.Status.RetainedUntil.Time) {
		return false, nil
	}

	held := vol.Status.HeldUntil
	if held == nil || vol.Incomplete() || now.After(held.Time) {
		return true, nil
	}

	exported := vol.Status.ExportedAt
	if exported == nil || !now.After(exported.Add(exportGrace)) {
		return false, nil
	}

	return r.destinationHolds(ctx, vol)
}

// destinationHolds reports whether the provider a moved volume's final copy
// went to still holds the volume's lease. One that gave up on the copy
// lets its lease go, the order re-lists, and the next provider copies from
// here again. Without the chain there is no telling, and the hold runs to
// its bound.
func (r *reconciler) destinationHolds(ctx context.Context, vol *crd.Volume) (bool, error) {
	if r.chain == nil {
		return false, nil
	}

	dseq, err := strconv.ParseUint(vol.Spec.GroupID.DSeq, 10, 64)
	if err != nil {
		return false, fmt.Errorf("%w: invalid dseq %q: %s", ErrVolumeOperator, vol.Spec.GroupID.DSeq, err.Error())
	}

	leases, err := r.chain.VolumeLeases(ctx, vol.Spec.Owner, vol.Spec.VID)
	if err != nil {
		return false, err
	}

	for _, lease := range leases {
		if lease.Provider == vol.Status.ExportedTo && lease.DSeq == dseq && lease.Policy.Vid == vol.Spec.VID && lease.State == mv1.LeaseActive {
			return true, nil
		}
	}

	return false, nil
}

// gcDue reports whether the volume passed its retention deadline. Exporting
// and Adopting phases are excluded structurally: destruction only ever
// happens out of Retained or Releasing.
func (r *reconciler) gcDue(vol *crd.Volume) bool {
	switch vol.Status.Phase {
	case crd.VolumePhaseRetained, crd.VolumePhaseReleasing:
	default:
		return false
	}

	return vol.Status.RetainedUntil != nil && r.now().After(vol.Status.RetainedUntil.Time)
}

// provision creates the holder PVC against the -retain class and, once the
// binder pins a PV to it, labels the PV with the volume identity and parks.
func (r *reconciler) provision(ctx context.Context, vol *crd.Volume) error {
	if vol.Spec.ImportFrom == "" {
		if err := r.recordImportSource(ctx, vol, false); err != nil {
			return err
		}
	}

	pvc, err := r.ensurePVC(ctx, r.volNS, vol, "")
	if err != nil {
		return err
	}

	if pvc.Spec.VolumeName == "" {
		// not bound yet; the PVC/PV watch or the resync sweep retries
		if vol.Status.Phase != crd.VolumePhasePending {
			vol.Status.Phase = crd.VolumePhasePending
			return r.updateStatus(ctx, vol)
		}
		return nil
	}

	pv, err := r.kc.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	labels := pv.Labels
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[builder.AkashManagedLabelName] = builder.ValTrue
	labels[builder.AkashComponentLabelName] = builder.AkashComponentVolume
	labels[LabelVolumeOwner] = vol.Spec.Owner
	labels[LabelVolumeVID] = vol.Spec.VID
	labels[LabelVolumeDSeq] = vol.Spec.GroupID.DSeq

	pv.Labels = labels

	if _, err := r.kc.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		return err
	}

	r.log.Info("volume provisioned", "volume", vol.Name, "pv", pv.Name, "class", vol.Spec.Class)

	vol.Status.PVName = pv.Name
	vol.Status.Phase = parkedPhase(vol)

	return r.updateStatus(ctx, vol)
}

// parkedPhase is the phase a volume parks in: Importing while its data is
// still arriving, from the provider that held it before or, for a replica,
// as its first copy of the primary; Provisioned, and attachable, otherwise.
func parkedPhase(vol *crd.Volume) crd.VolumePhase {
	if vol.Spec.ImportFrom != "" || vol.AwaitsFirstCopy() {
		return crd.VolumePhaseImporting
	}

	return crd.VolumePhaseProvisioned
}

// recordImportSource records where a volume moved here is copied from,
// when it was moved here at all. localCopy is whether the PV holds this
// provider's own whole copy from its earlier lease.
func (r *reconciler) recordImportSource(ctx context.Context, vol *crd.Volume, localCopy bool) error {
	source, err := r.importSource(ctx, vol, localCopy)
	if err != nil || source == "" {
		return err
	}

	vol.Spec.ImportFrom = source
	vol.Status.LocalCopy = localCopy

	r.log.Info("volume moved here; its data will be copied in", "volume", vol.Name, "source", source)

	if err := r.updateStatus(ctx, vol); err != nil {
		return err
	}

	// a Volume CRD installed before moves existed prunes the field; parked
	// without it, the volume would come up empty and attachable while its
	// data is still at the source
	if vol.Spec.ImportFrom != source {
		return fmt.Errorf("%w: volume %s was moved here, but the installed Volume CRD drops spec.importFrom: apply the current CRD", ErrVolumeOperator, vol.Name)
	}

	return nil
}

// importSource names the provider a volume won here must be copied from.
// The first order of a volume starts it empty. A re-listed one (a later
// order sequence) follows a migration or an eviction, so the volume is
// copied from the provider that held it last, unless that was this
// provider and its copy is still here (localCopy). Without chain access
// there is no telling, and the volume stays Pending: provisioning it as
// empty could let a deployment attach a volume whose data is elsewhere.
func (r *reconciler) importSource(ctx context.Context, vol *crd.Volume, localCopy bool) (string, error) {
	lease := vol.Spec.LeaseID
	if lease.OSeq <= 1 {
		return "", nil
	}

	if r.chain == nil {
		return "", fmt.Errorf("%w: volume %s was moved here but chain access is not configured", ErrVolumeOperator, vol.Name)
	}

	dseq, err := strconv.ParseUint(lease.DSeq, 10, 64)
	if err != nil {
		return "", fmt.Errorf("%w: invalid dseq %q: %s", ErrVolumeOperator, lease.DSeq, err.Error())
	}

	leases, err := r.chain.VolumeLeases(ctx, vol.Spec.Owner, vol.Spec.VID)
	if err != nil {
		return "", err
	}

	sources := replication.MigrationSources(leases, vol.Spec.VID, dseq, lease.Provider, localCopy)
	if len(sources) == 0 {
		return "", nil
	}

	return sources[0], nil
}

// maybeAdopt detects an adoption in flight - the CRD spec was rewritten to a
// new deployment while the PV still carries the dead one's dseq label - and
// verifies it against chain state before rewriting the PV identity.
func (r *reconciler) maybeAdopt(ctx context.Context, vol *crd.Volume) (bool, error) {
	pv, err := r.kc.CoreV1().PersistentVolumes().Get(ctx, vol.Status.PVName, metav1.GetOptions{})
	if err != nil {
		return true, err
	}

	deadDSeq := pv.Labels[LabelVolumeDSeq]
	if deadDSeq == "" || deadDSeq == vol.Spec.GroupID.DSeq {
		return false, nil
	}

	// freeze GC before anything else; the phase is the freeze
	if vol.Status.Phase != crd.VolumePhaseAdopting {
		vol.Status.Phase = crd.VolumePhaseAdopting
		if err := r.updateStatus(ctx, vol); err != nil {
			return true, err
		}
	}

	if vol.Incomplete() {
		// the copy never finished arriving: the PV holds part of the data
		// at best, which no deployment may take as the volume
		r.log.Error("adoption rejected: the volume never finished arriving here",
			"volume", vol.Name, "dead_dseq", deadDSeq, "new_dseq", vol.Spec.GroupID.DSeq)

		vol.Status.Phase = crd.VolumePhaseRetained

		return true, r.updateStatus(ctx, vol)
	}

	if r.chain == nil {
		// chain access not wired; stay Adopting (GC frozen) until it is
		return true, nil
	}

	dseq, err := strconv.ParseUint(vol.Spec.GroupID.DSeq, 10, 64)
	if err != nil {
		return true, fmt.Errorf("%w: invalid dseq %q: %s", ErrVolumeOperator, vol.Spec.GroupID.DSeq, err.Error())
	}

	policy, err := r.chain.GroupVolumePolicy(ctx, dv1.GroupID{
		Owner: vol.Spec.Owner,
		DSeq:  dseq,
		GSeq:  vol.Spec.GroupID.GSeq,
	})
	if err != nil {
		// transient chain failure: stay Adopting, retry on resync
		return true, err
	}

	if !adoptionValid(policy, pv.Labels, vol) {
		// a Released PV is never re-bound for an unverified deployment,
		// and never for a different owner. The retention deadline stands.
		r.log.Error("adoption rejected: chain policy does not reference the retained volume",
			"volume", vol.Name, "dead_dseq", deadDSeq, "new_dseq", vol.Spec.GroupID.DSeq)

		vol.Status.Phase = crd.VolumePhaseRetained

		return true, r.updateStatus(ctx, vol)
	}

	pv.Labels[LabelVolumeDSeq] = vol.Spec.GroupID.DSeq
	if _, err := r.kc.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		return true, err
	}

	r.log.Info("volume adopted", "volume", vol.Name, "dead_dseq", deadDSeq, "dseq", vol.Spec.GroupID.DSeq)

	// re-park under the new identity; the retention clock is void
	vol.Status.RetainedUntil = nil
	vol.Status.Phase = crd.VolumePhaseProvisioned

	if err := r.updateStatus(ctx, vol); err != nil {
		return true, err
	}

	return true, r.ensureParked(ctx, vol)
}

// adoptionValid checks the adoption deployment's VolumePolicy against the
// identity recorded on the PV: same owner, same vid, and an adopt reference
// naming exactly the dead deployment the PV data belongs to.
func adoptionValid(policy *dv1.VolumePolicy, pvLabels map[string]string, vol *crd.Volume) bool {
	if policy == nil || policy.Adopt == nil {
		return false
	}

	if policy.Vid != vol.Spec.VID {
		return false
	}

	if pvLabels[LabelVolumeOwner] != vol.Spec.Owner || pvLabels[LabelVolumeVID] != vol.Spec.VID {
		return false
	}

	if policy.Adopt.Owner != pvLabels[LabelVolumeOwner] {
		return false
	}

	return strconv.FormatUint(policy.Adopt.DSeq, 10) == pvLabels[LabelVolumeDSeq]
}

// ensureAttached moves the PV's claim from the holder PVC to the target PVC
// in the compute lease namespace: target created first (with spec.volumeName
// pre-set), claimRef patched directly to it, holder deleted last.
func (r *reconciler) ensureAttached(ctx context.Context, vol *crd.Volume) error {
	lid, err := parseLeaseID(vol.Status.AttachedLease)
	if err != nil {
		return fmt.Errorf("%w: attached lease %q: %s", ErrVolumeOperator, vol.Status.AttachedLease, err.Error())
	}

	targetNS := clusterutil.LeaseIDToNamespace(lid)

	pv, err := r.kc.CoreV1().PersistentVolumes().Get(ctx, vol.Status.PVName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if !claimRefIs(pv, targetNS, vol.Name) {
		// 1. target PVC first, pre-bound to the PV by name
		pvc, err := r.ensurePVC(ctx, targetNS, vol, pv.Name)
		if err != nil {
			return err
		}

		// 2. claimRef directly holder -> target; never through Available
		if err := r.bindPV(ctx, pv, pvc); err != nil {
			return err
		}

		// 3. holder released last
		if err := r.deletePVC(ctx, r.volNS, vol.Name); err != nil {
			return err
		}

		r.log.Info("volume attached", "volume", vol.Name, "attached_lease", vol.Status.AttachedLease, "ns", targetNS)
	}

	if vol.Status.Phase != crd.VolumePhaseAttached {
		vol.Status.Phase = crd.VolumePhaseAttached
		return r.updateStatus(ctx, vol)
	}

	return nil
}

// ensureParked re-parks the PV on the holder PVC. It runs for freshly
// provisioned, detached, retained and adopted volumes alike: a parked claim
// is what keeps the Retain PV out of the binder's reach.
func (r *reconciler) ensureParked(ctx context.Context, vol *crd.Volume) error {
	pv, err := r.kc.CoreV1().PersistentVolumes().Get(ctx, vol.Status.PVName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if !claimRefIs(pv, r.volNS, vol.Name) {
		// remember the stale claim (an attach PVC whose namespace may
		// still exist on out-of-band closes)
		stale := pv.Spec.ClaimRef

		// 1. holder PVC first, pre-bound to the PV by name
		pvc, err := r.ensurePVC(ctx, r.volNS, vol, pv.Name)
		if err != nil {
			return err
		}

		// 2. claimRef directly target -> holder
		if err := r.bindPV(ctx, pv, pvc); err != nil {
			return err
		}

		// 3. stale attach claim removed last (namespace deletion usually
		// got there first; not-found is the normal case)
		if stale != nil && (stale.Namespace != r.volNS || stale.Name != vol.Name) {
			if err := r.deletePVC(ctx, stale.Namespace, stale.Name); err != nil {
				return err
			}
		}

		r.log.Info("volume parked", "volume", vol.Name, "pv", pv.Name)
	}

	// retained volumes stay Retained (the GC clock is running), a volume
	// whose data is still arriving parks as Importing, and everything else
	// as Provisioned
	phase := parkedPhase(vol)
	if vol.Status.Phase == crd.VolumePhaseRetained {
		phase = crd.VolumePhaseRetained
	}

	if vol.Status.Phase != phase {
		vol.Status.Phase = phase
		return r.updateStatus(ctx, vol)
	}

	return nil
}

// ensureSize grows the claim that holds the PV, the holder PVC while parked
// or the attach PVC in the lease namespace while attached, to the volume's
// recorded size, which a lease resize raises. The -retain class's CSI driver
// then expands the PV (online for RBD). The record never shrinks
// (DeployVolume refuses to), so a claim at or above it is left alone, and an
// unbound claim cannot be resized yet: a later pass grows it.
func (r *reconciler) ensureSize(ctx context.Context, vol *crd.Volume) error {
	size, err := strconv.ParseUint(vol.Spec.Size, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: invalid volume size %q: %s", ErrVolumeOperator, vol.Spec.Size, err.Error())
	}

	pv, err := r.kc.CoreV1().PersistentVolumes().Get(ctx, vol.Status.PVName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	ref := pv.Spec.ClaimRef
	if ref == nil || ref.Name != vol.Name {
		return nil
	}

	pvc, err := r.kc.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			// mid choreography; a later pass sees the claim that replaces it
			return nil
		}

		return err
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		return nil
	}

	want := resource.NewQuantity(int64(size), resource.DecimalSI) // nolint: gosec
	if cur, set := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; set && cur.Cmp(*want) >= 0 {
		return nil
	}

	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"resources": map[string]any{
				"requests": corev1.ResourceList{corev1.ResourceStorage: *want},
			},
		},
	})
	if err != nil {
		return err
	}

	_, err = r.kc.CoreV1().PersistentVolumeClaims(ref.Namespace).Patch(ctx, ref.Name, k8stypes.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return err
	}

	r.log.Info("volume grow requested", "volume", vol.Name, "ns", ref.Namespace, "size", vol.Spec.Size)

	return nil
}

// gc is the single destruction path. It flips the PV to reclaimPolicy=Delete
// so removing the claim destroys the backing image through the provisioner,
// then removes PV and CRD.
func (r *reconciler) gc(ctx context.Context, vol *crd.Volume) error {
	if vol.Status.Phase != crd.VolumePhaseReleasing {
		vol.Status.Phase = crd.VolumePhaseReleasing
		if err := r.updateStatus(ctx, vol); err != nil {
			return err
		}
	}

	if vol.Status.PVName != "" {
		pv, err := r.kc.CoreV1().PersistentVolumes().Get(ctx, vol.Status.PVName, metav1.GetOptions{})
		switch {
		case err == nil:
			if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
				pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
				if pv, err = r.kc.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
					return err
				}
			}

			if ref := pv.Spec.ClaimRef; ref != nil {
				if err := r.deletePVC(ctx, ref.Namespace, ref.Name); err != nil {
					return err
				}
			}

			if err := r.kc.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{}); err != nil && !kerrors.IsNotFound(err) {
				return err
			}
		case kerrors.IsNotFound(err):
		default:
			return err
		}
	}

	// holder PVC by name, in case the claimRef was stale
	if err := r.deletePVC(ctx, r.volNS, vol.Name); err != nil {
		return err
	}

	r.log.Info("volume released", "volume", vol.Name, "pv", vol.Status.PVName)

	return r.deleteVolume(ctx, vol)
}

func (r *reconciler) deleteVolume(ctx context.Context, vol *crd.Volume) error {
	err := r.ac.AkashV2beta2().Volumes(r.ns).Delete(ctx, vol.Name, metav1.DeleteOptions{})
	if err != nil && !kerrors.IsNotFound(err) {
		return err
	}

	return nil
}

// ensurePVC gets or creates the volume's PVC in the namespace. volumeName is
// set on creation when the PV already exists (attach and re-park paths);
// empty for the initial provisioning claim.
func (r *reconciler) ensurePVC(ctx context.Context, ns string, vol *crd.Volume, volumeName string) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := r.kc.CoreV1().PersistentVolumeClaims(ns).Get(ctx, vol.Name, metav1.GetOptions{})
	if err == nil {
		return pvc, nil
	}

	if !kerrors.IsNotFound(err) {
		return nil, err
	}

	size, err := strconv.ParseUint(vol.Spec.Size, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid volume size %q: %s", ErrVolumeOperator, vol.Spec.Size, err.Error())
	}

	class := RetainClass(vol.Spec.Class)

	var annotations map[string]string
	if r.nodeHint && volumeName == "" {
		// initial provisioning claim only: attach/re-park claims pre-bind
		// to an existing PV and never provision
		node, err := r.hintNode(ctx, vol.Spec.Class)
		if err != nil {
			return nil, err
		}
		annotations = map[string]string{pvcSelectedNodeAnnotation: node}
	}

	pvc = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        vol.Name,
			Namespace:   ns,
			Annotations: annotations,
			Labels: map[string]string{
				builder.AkashManagedLabelName:   builder.ValTrue,
				builder.AkashComponentLabelName: builder.AkashComponentVolume,
				LabelVolumeOwner:                vol.Spec.Owner,
				LabelVolumeVID:                  vol.Spec.VID,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &class,
			VolumeName:       volumeName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *resource.NewQuantity(int64(size), resource.DecimalSI), // nolint: gosec
				},
			},
		},
	}

	return r.kc.CoreV1().PersistentVolumeClaims(ns).Create(ctx, pvc, metav1.CreateOptions{})
}

// bindPV rewrites the PV's claimRef directly to the PVC. The PV goes from
// one claim to the next without an unclaimed window.
func (r *reconciler) bindPV(ctx context.Context, pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) error {
	pv.Spec.ClaimRef = &corev1.ObjectReference{
		APIVersion: "v1",
		Kind:       "PersistentVolumeClaim",
		Namespace:  pvc.Namespace,
		Name:       pvc.Name,
		UID:        pvc.UID,
	}

	_, err := r.kc.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

	return err
}

func (r *reconciler) deletePVC(ctx context.Context, ns, name string) error {
	err := r.kc.CoreV1().PersistentVolumeClaims(ns).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !kerrors.IsNotFound(err) {
		return err
	}

	return nil
}

// updateStatus writes the volume and takes the stored copy back, so a
// second write in the same pass carries the current resource version.
func (r *reconciler) updateStatus(ctx context.Context, vol *crd.Volume) error {
	updated, err := r.ac.AkashV2beta2().Volumes(r.ns).Update(ctx, vol, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	*vol = *updated

	return nil
}

func claimRefIs(pv *corev1.PersistentVolume, ns, name string) bool {
	return pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == ns && pv.Spec.ClaimRef.Name == name
}
