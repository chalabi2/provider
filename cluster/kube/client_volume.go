package kube

import (
	"context"
	"fmt"
	"strings"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dtypes "pkg.akt.dev/go/node/deployment/v1"
	dvbeta "pkg.akt.dev/go/node/deployment/v1beta5"
	mtypes "pkg.akt.dev/go/node/market/v1"

	"github.com/akash-network/provider/cluster/kube/builder"
	kubeclienterrors "github.com/akash-network/provider/cluster/kube/errors"
	ctypes "github.com/akash-network/provider/cluster/types/v1beta3"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

// AEP-87 volume lifecycle. All verbs delegate to the Volume CRD - the
// daemon records desired state, the storage operator owns the PV
// choreography (provision/park/bind) and the GC. The daemon never touches
// PersistentVolumes directly.

func volumeLabels(lid mtypes.LeaseID) map[string]string {
	labels := map[string]string{
		builder.AkashManagedLabelName:   "true",
		builder.AkashComponentLabelName: builder.AkashComponentVolume,
	}

	return builder.AppendLeaseLabels(lid, labels)
}

func volumeLeaseSelector(lid mtypes.LeaseID) string {
	sb := &strings.Builder{}

	kubeSelectorForLease(sb, lid)
	_, _ = fmt.Fprintf(sb, ",%s=%s", builder.AkashComponentLabelName, builder.AkashComponentVolume)

	return sb.String()
}

func (c *client) DeployVolume(ctx context.Context, lid mtypes.LeaseID, group *dvbeta.Group) error {
	if group == nil {
		return fmt.Errorf("%w: no group provided for volume lease %s", kubeclienterrors.ErrInternalError, lid)
	}

	vol, err := crd.NewVolume(c.ns, lid, &group.GroupSpec)
	if err != nil {
		return err
	}

	vol.Labels = volumeLabels(lid)

	obj, err := wrapKubeCall("akash-volumes-get", func() (*crd.Volume, error) {
		return c.ac.AkashV2beta2().Volumes(c.ns).Get(ctx, vol.Name, metav1.GetOptions{})
	})

	switch {
	case err == nil:
		// The volume already exists: a re-deploy of the same lease or an
		// adoption/migration re-lease of the same owner/vid. The identity
		// must match; the group/lease bookkeeping moves to the new lease.
		if obj.Spec.Owner != vol.Spec.Owner || obj.Spec.VID != vol.Spec.VID {
			return fmt.Errorf("%w: %s owner/vid %s/%s", kubeclienterrors.ErrVolumeMismatch, vol.Name, obj.Spec.Owner, obj.Spec.VID)
		}

		uobj := obj.DeepCopy()
		uobj.Labels = vol.Labels
		uobj.Spec = vol.Spec

		_, err = wrapKubeCall("akash-volumes-update", func() (*crd.Volume, error) {
			return c.ac.AkashV2beta2().Volumes(c.ns).Update(ctx, uobj, metav1.UpdateOptions{})
		})

		return err
	case kerrors.IsNotFound(err):
		_, err = wrapKubeCall("akash-volumes-create", func() (*crd.Volume, error) {
			return c.ac.AkashV2beta2().Volumes(c.ns).Create(ctx, vol, metav1.CreateOptions{})
		})

		return err
	default:
		return err
	}
}

func (c *client) AttachVolume(ctx context.Context, lid mtypes.LeaseID, ref dtypes.VolumeRef) error {
	name := crd.VolumeName(ref.Owner, ref.Name)

	obj, err := wrapKubeCall("akash-volumes-get", func() (*crd.Volume, error) {
		return c.ac.AkashV2beta2().Volumes(c.ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return fmt.Errorf("%w: %s", kubeclienterrors.ErrVolumeNotFound, ref)
		}
		return err
	}

	// v1 volumes are RWO: exactly one attachment. The chain's attachment
	// index is the authority; this guards the cluster record.
	if lease := obj.Status.AttachedLease; lease != "" && lease != lid.String() {
		return fmt.Errorf("%w: %s attached to %s", kubeclienterrors.ErrVolumeAttached, ref, lease)
	}

	uobj := obj.DeepCopy()
	uobj.Status.AttachedLease = lid.String()
	uobj.Status.Phase = crd.VolumePhaseAttached

	_, err = wrapKubeCall("akash-volumes-update", func() (*crd.Volume, error) {
		return c.ac.AkashV2beta2().Volumes(c.ns).Update(ctx, uobj, metav1.UpdateOptions{})
	})

	return err
}

func (c *client) DetachVolume(ctx context.Context, lid mtypes.LeaseID, ref dtypes.VolumeRef) error {
	name := crd.VolumeName(ref.Owner, ref.Name)

	obj, err := wrapKubeCall("akash-volumes-get", func() (*crd.Volume, error) {
		return c.ac.AkashV2beta2().Volumes(c.ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		// Detach rides teardown paths; a missing record is not an error.
		if kerrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if obj.Status.AttachedLease != lid.String() {
		return nil
	}

	uobj := obj.DeepCopy()
	uobj.Status.AttachedLease = ""
	uobj.Status.Phase = crd.VolumePhaseProvisioned

	_, err = wrapKubeCall("akash-volumes-update", func() (*crd.Volume, error) {
		return c.ac.AkashV2beta2().Volumes(c.ns).Update(ctx, uobj, metav1.UpdateOptions{})
	})

	return err
}

func (c *client) TeardownVolume(ctx context.Context, lid mtypes.LeaseID) error {
	list, err := wrapKubeCall("akash-volumes-list", func() (*crd.VolumeList, error) {
		return c.ac.AkashV2beta2().Volumes(c.ns).List(ctx, metav1.ListOptions{
			LabelSelector: volumeLeaseSelector(lid),
		})
	})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	now := metav1.NewTime(time.Now())

	for i := range list.Items {
		obj := list.Items[i].DeepCopy()

		obj.Status.AttachedLease = ""

		if obj.Spec.Reclaim == dtypes.VolumeReclaimDelete.String() {
			// reclaim delete: destroy immediately after close+detach. The
			// operator GC remains the single destruction path.
			obj.Status.Phase = crd.VolumePhaseReleasing
			obj.Status.RetainedUntil = &now
		} else {
			retention, terr := time.ParseDuration(obj.Spec.Retention)
			if terr != nil {
				return fmt.Errorf("%w: volume %s retention %q: %s", kubeclienterrors.ErrInternalError, obj.Name, obj.Spec.Retention, terr.Error())
			}

			// The chain-computable deadline is closedAt + retention; the
			// daemon processes the close event at closedAt, the operator
			// re-stamps from chain state when it reconciles.
			retainedUntil := metav1.NewTime(now.Add(retention))
			obj.Status.Phase = crd.VolumePhaseRetained
			obj.Status.RetainedUntil = &retainedUntil
		}

		_, err = wrapKubeCall("akash-volumes-update", func() (*crd.Volume, error) {
			return c.ac.AkashV2beta2().Volumes(c.ns).Update(ctx, obj, metav1.UpdateOptions{})
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *client) VolumeStatus(ctx context.Context, ref dtypes.VolumeRef) (*crd.Volume, error) {
	name := crd.VolumeName(ref.Owner, ref.Name)

	obj, err := wrapKubeCall("akash-volumes-get", func() (*crd.Volume, error) {
		return c.ac.AkashV2beta2().Volumes(c.ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s", kubeclienterrors.ErrVolumeNotFound, ref)
		}
		return nil, err
	}

	return obj, nil
}

func (c *client) DeployedVolumes(ctx context.Context) ([]ctypes.VolumeDeployment, error) {
	list, err := wrapKubeCall("akash-volumes-list", func() (*crd.VolumeList, error) {
		return c.ac.AkashV2beta2().Volumes(c.ns).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=true,%s=%s", builder.AkashManagedLabelName, builder.AkashComponentLabelName, builder.AkashComponentVolume),
		})
	})
	if err != nil {
		// A cluster without the Volume CRD installed simply has no volumes;
		// non-participating providers must not fail startup here.
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	volumes := make([]ctypes.VolumeDeployment, 0, len(list.Items))

	for i := range list.Items {
		lid, gspec, err := list.Items[i].FromCRD()
		if err != nil {
			c.log.Error("unable to load volume", "name", list.Items[i].Name, "err", err)
			continue
		}

		volumes = append(volumes, ctypes.VolumeDeployment{
			LeaseID: lid,
			Group:   gspec,
		})
	}

	return volumes, nil
}
