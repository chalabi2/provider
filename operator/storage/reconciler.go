package storage

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"cosmossdk.io/log"

	dv1 "pkg.akt.dev/go/node/deployment/v1"

	"github.com/akash-network/provider/cluster/kube/builder"
	clusterutil "github.com/akash-network/provider/cluster/util"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	akashclientset "github.com/akash-network/provider/pkg/client/clientset/versioned"
)

// ChainQuery is the narrow chain read the reconciler needs: adoption
// verification. A nil implementation freezes adoptions in the Adopting
// phase (GC stays frozen) until chain access is available.
type ChainQuery interface {
	// GroupVolumePolicy returns the VolumePolicy of the on-chain group, or
	// nil when the group is not a volume group.
	GroupVolumePolicy(ctx context.Context, id dv1.GroupID) (*dv1.VolumePolicy, error)
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

	// the single destruction path: a positive chain-computable deadline,
	// never EventLeaseClosed inference
	if r.gcDue(vol) {
		return r.gc(ctx, vol)
	}

	switch vol.Status.Phase {
	case crd.VolumePhaseExporting:
		// migration export in flight: GC frozen, PV untouched. Once the
		// lease has closed (retention stamped) and the deadline passed,
		// the export duty is over: thaw to Retained, and the next pass
		// takes the single destruction path.
		if vol.Status.RetainedUntil != nil && r.now().After(vol.Status.RetainedUntil.Time) {
			vol.Status.Phase = crd.VolumePhaseRetained
			return r.updateStatus(ctx, vol)
		}

		return nil
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

	if vol.Status.AttachedLease != "" {
		return r.ensureAttached(ctx, vol)
	}

	return r.ensureParked(ctx, vol)
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
	vol.Status.Phase = crd.VolumePhaseProvisioned

	return r.updateStatus(ctx, vol)
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
			"volume", vol.Name, "dead-dseq", deadDSeq, "new-dseq", vol.Spec.GroupID.DSeq)

		vol.Status.Phase = crd.VolumePhaseRetained

		return true, r.updateStatus(ctx, vol)
	}

	pv.Labels[LabelVolumeDSeq] = vol.Spec.GroupID.DSeq
	if _, err := r.kc.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		return true, err
	}

	r.log.Info("volume adopted", "volume", vol.Name, "dead-dseq", deadDSeq, "dseq", vol.Spec.GroupID.DSeq)

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

		r.log.Info("volume attached", "volume", vol.Name, "lease", vol.Status.AttachedLease, "namespace", targetNS)
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

	// retained volumes stay Retained (the GC clock is running); everything
	// else parks as Provisioned
	phase := crd.VolumePhaseProvisioned
	if vol.Status.Phase == crd.VolumePhaseRetained {
		phase = crd.VolumePhaseRetained
	}

	if vol.Status.Phase != phase {
		vol.Status.Phase = phase
		return r.updateStatus(ctx, vol)
	}

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

func (r *reconciler) updateStatus(ctx context.Context, vol *crd.Volume) error {
	_, err := r.ac.AkashV2beta2().Volumes(r.ns).Update(ctx, vol, metav1.UpdateOptions{})

	return err
}

func claimRefIs(pv *corev1.PersistentVolume, ns, name string) bool {
	return pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == ns && pv.Spec.ClaimRef.Name == name
}
