package storage

import (
	"context"
	"fmt"
	"strconv"

	dv1 "pkg.akt.dev/go/node/deployment/v1"

	"github.com/akash-network/provider/bucket"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

// reconcileBucket drives a bucket volume. There is no PV: the bucket lives
// on the S3 server. It is provisioned and sized while its lease is active,
// suspended (every key refused) once the lease closes, resumed with no keys
// when another deployment of the owner adopts it, and deleted at the
// retention deadline, the same single destruction path as a mounted volume.
func (r *reconciler) reconcileBucket(ctx context.Context, vol *crd.Volume) error {
	if r.buckets == nil {
		return fmt.Errorf("%w: volume %s is a bucket and this operator serves none", ErrVolumeOperator, vol.Name)
	}

	ref := bucket.RefFor(vol.Name, vol.Spec.Owner, vol.Spec.VID)

	// an adoption runs before GC, so a bucket whose adopting lease was won
	// just before the deadline is handed over rather than deleted
	if vol.Status.Bucket != "" && vol.Spec.GroupID.DSeq != vol.Status.BucketDSeq && vol.Status.Phase != crd.VolumePhaseReleasing {
		if handled, err := r.adoptBucket(ctx, vol, ref); handled || err != nil {
			return err
		}
	}

	if r.gcDue(vol) {
		return r.gcBucket(ctx, vol, ref)
	}

	switch vol.Status.Phase {
	case crd.VolumePhaseExporting:
		// the reclaim window: the lease is still active and its keys work
		if vol.Status.RetainedUntil != nil && r.now().After(vol.Status.RetainedUntil.Time) {
			vol.Status.Phase = crd.VolumePhaseRetained
			return r.updateStatus(ctx, vol)
		}

		return nil
	case crd.VolumePhaseReleasing:
		return nil
	default:
	}

	if vol.Status.Bucket == "" {
		return r.provisionBucket(ctx, vol, ref)
	}

	if vol.Status.Phase == crd.VolumePhaseRetained {
		// every pass, so a bucket resumed by hand is suspended again
		return r.buckets.Suspend(ctx, ref)
	}

	if vol.Spec.Size != vol.Status.BucketSize && vol.Status.BucketDSeq == vol.Spec.GroupID.DSeq {
		return r.ensureBucket(ctx, vol, ref)
	}

	return nil
}

func (r *reconciler) provisionBucket(ctx context.Context, vol *crd.Volume, ref bucket.Ref) error {
	if err := r.ensureBucket(ctx, vol, ref); err != nil {
		return err
	}

	r.log.Info("bucket provisioned", "volume", vol.Name, "bucket", ref.Bucket)

	vol.Status.Bucket = ref.Bucket
	vol.Status.BucketDSeq = vol.Spec.GroupID.DSeq
	if vol.Status.Phase == "" || vol.Status.Phase == crd.VolumePhasePending {
		vol.Status.Phase = crd.VolumePhaseProvisioned
	}

	return r.updateStatus(ctx, vol)
}

// ensureBucket brings the bucket's quota to the leased size. Ensure is
// idempotent, so a pass that fails after it only repeats it.
func (r *reconciler) ensureBucket(ctx context.Context, vol *crd.Volume, ref bucket.Ref) error {
	size, err := strconv.ParseInt(vol.Spec.Size, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: invalid volume size %q: %s", ErrVolumeOperator, vol.Spec.Size, err.Error())
	}

	if err := r.buckets.Ensure(ctx, bucket.Spec{Ref: ref, SizeBytes: size}); err != nil {
		return fmt.Errorf("%w: bucket of %s: %w", ErrVolumeOperator, vol.Name, err)
	}

	if vol.Status.BucketSize != vol.Spec.Size {
		if vol.Status.BucketSize != "" {
			r.log.Info("bucket quota set", "volume", vol.Name, "size", vol.Spec.Size)
		}

		vol.Status.BucketSize = vol.Spec.Size

		if vol.Status.Bucket != "" {
			return r.updateStatus(ctx, vol)
		}
	}

	return nil
}

// adoptBucket hands a retained bucket to the deployment that adopts it.
// It waits for that deployment's lease: until the lease is won the bucket
// stays suspended and its retention deadline stands, so an adoption that
// never gets a lease cannot keep a bucket nobody pays for. The chain must
// confirm the adoption names the deployment the bucket belongs to, and the
// keys of the previous lease are revoked before the bucket opens again.
// handled is false when the bucket stays retained, for the rest of the pass
// to keep it suspended and collect it at its deadline.
func (r *reconciler) adoptBucket(ctx context.Context, vol *crd.Volume, ref bucket.Ref) (bool, error) {
	if vol.Spec.LeaseID.DSeq != vol.Spec.GroupID.DSeq {
		if vol.Status.Phase == crd.VolumePhaseAdopting {
			vol.Status.Phase = crd.VolumePhaseRetained
			if err := r.updateStatus(ctx, vol); err != nil {
				return true, err
			}
		}

		return false, nil
	}

	// the lease is won: freeze GC while the adoption completes
	if vol.Status.Phase != crd.VolumePhaseAdopting {
		vol.Status.Phase = crd.VolumePhaseAdopting
		if err := r.updateStatus(ctx, vol); err != nil {
			return true, err
		}
	}

	if r.chain == nil {
		return true, nil
	}

	dseq, err := strconv.ParseUint(vol.Spec.GroupID.DSeq, 10, 64)
	if err != nil {
		return true, fmt.Errorf("%w: invalid dseq %q: %s", ErrVolumeOperator, vol.Spec.GroupID.DSeq, err.Error())
	}

	policy, err := r.chain.GroupVolumePolicy(ctx, dv1.GroupID{Owner: vol.Spec.Owner, DSeq: dseq, GSeq: vol.Spec.GroupID.GSeq})
	if err != nil {
		return true, err
	}

	if !bucketAdoptionValid(policy, vol) {
		r.log.Error("adoption rejected: chain policy does not reference the retained bucket",
			"volume", vol.Name, "dead_dseq", vol.Status.BucketDSeq, "new_dseq", vol.Spec.GroupID.DSeq)

		vol.Status.Phase = crd.VolumePhaseRetained

		return false, r.updateStatus(ctx, vol)
	}

	if err := r.buckets.Resume(ctx, ref); err != nil {
		return true, fmt.Errorf("%w: resume bucket of %s: %w", ErrVolumeOperator, vol.Name, err)
	}

	r.log.Info("bucket adopted", "volume", vol.Name, "dead_dseq", vol.Status.BucketDSeq, "dseq", vol.Spec.GroupID.DSeq)

	vol.Status.BucketDSeq = vol.Spec.GroupID.DSeq
	vol.Status.RetainedUntil = nil
	vol.Status.Phase = crd.VolumePhaseProvisioned

	if err := r.updateStatus(ctx, vol); err != nil {
		return true, err
	}

	// the quota follows the adopting lease's size
	return true, r.ensureBucket(ctx, vol, ref)
}

// bucketAdoptionValid checks the adopting deployment's policy against the
// bucket: a bucket of the same owner and vid whose adopt reference names
// exactly the deployment the bucket's keys were issued under.
func bucketAdoptionValid(policy *dv1.VolumePolicy, vol *crd.Volume) bool {
	if policy == nil || policy.Adopt == nil || !policy.IsBucket() {
		return false
	}

	if policy.Vid != vol.Spec.VID || policy.Adopt.Owner != vol.Spec.Owner {
		return false
	}

	return strconv.FormatUint(policy.Adopt.DSeq, 10) == vol.Status.BucketDSeq
}

// gcBucket deletes the bucket, its objects and its keys, then the record.
func (r *reconciler) gcBucket(ctx context.Context, vol *crd.Volume, ref bucket.Ref) error {
	if vol.Status.Phase != crd.VolumePhaseReleasing {
		vol.Status.Phase = crd.VolumePhaseReleasing
		if err := r.updateStatus(ctx, vol); err != nil {
			return err
		}
	}

	if err := r.buckets.Delete(ctx, ref); err != nil {
		return fmt.Errorf("%w: delete bucket of %s: %w", ErrVolumeOperator, vol.Name, err)
	}

	r.log.Info("bucket released", "volume", vol.Name, "bucket", ref.Bucket)

	return r.deleteVolume(ctx, vol)
}
