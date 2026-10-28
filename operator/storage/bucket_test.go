package storage

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mtypes "pkg.akt.dev/go/node/market/v1"
	"pkg.akt.dev/go/testutil"

	"github.com/akash-network/provider/bucket"
	"github.com/akash-network/provider/bucket/buckettest"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

const testBucketSize = "1048576"

func testBucketVolume(t *testing.T, lid mtypes.LeaseID, phase crd.VolumePhase) *crd.Volume {
	t.Helper()

	vol := testVolume(t, lid, phase)
	vol.Spec.Access = dv1.VolumeAccessBucket.String()
	vol.Spec.Size = testBucketSize

	return vol
}

// provisioned is vol as a pass that provisioned its bucket left it.
func provisioned(vol *crd.Volume, phase crd.VolumePhase) *crd.Volume {
	vol.Status.Phase = phase
	vol.Status.Bucket = bucket.RefFor(vol.Name, vol.Spec.Owner, vol.Spec.VID).Bucket
	vol.Status.BucketDSeq = vol.Spec.GroupID.DSeq
	vol.Status.BucketSize = vol.Spec.Size

	return vol
}

func bucketScaffold(t *testing.T, chain *fakeChain, vol *crd.Volume) (*scaffold, *buckettest.Fake, bucket.Ref) {
	t.Helper()

	s := makeScaffold(t, chain, nil, []runtime.Object{vol})
	fake := buckettest.NewFake()
	s.r.buckets = fake

	return s, fake, bucket.RefFor(vol.Name, vol.Spec.Owner, vol.Spec.VID)
}

func ensureFakeBucket(t *testing.T, fake *buckettest.Fake, ref bucket.Ref) {
	t.Helper()

	size, err := strconv.ParseInt(testBucketSize, 10, 64)
	require.NoError(t, err)
	require.NoError(t, fake.Ensure(context.Background(), bucket.Spec{Ref: ref, SizeBytes: size}))
}

func reconcileVolume(t *testing.T, s *scaffold, name string) (*crd.Volume, error) {
	t.Helper()

	vol, err := s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)

	err = s.r.reconcile(context.Background(), vol)

	after, gerr := s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), name, metav1.GetOptions{})
	if gerr != nil {
		return nil, err
	}

	return after, err
}

func TestBucketProvisioned(t *testing.T) {
	lid := testutil.LeaseID(t)
	s, fake, ref := bucketScaffold(t, nil, testBucketVolume(t, lid, crd.VolumePhasePending))

	vol, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)

	require.True(t, fake.Exists(ref))
	require.Equal(t, int64(1048576), fake.Quota(ref))
	require.Equal(t, crd.VolumePhaseProvisioned, vol.Status.Phase)
	require.Equal(t, ref.Bucket, vol.Status.Bucket)
	require.Equal(t, strconv.FormatUint(lid.DSeq, 10), vol.Status.BucketDSeq)
	require.Equal(t, testBucketSize, vol.Status.BucketSize)
	require.Empty(t, vol.Status.PVName, "a bucket got a PV")
}

func TestBucketProvisionFailureLeavesPending(t *testing.T) {
	s, fake, ref := bucketScaffold(t, nil, testBucketVolume(t, testutil.LeaseID(t), crd.VolumePhasePending))
	fake.Fail("Ensure", errors.New("rgw down"))

	vol, err := reconcileVolume(t, s, ref.UserID)
	require.ErrorIs(t, err, ErrVolumeOperator)
	require.Equal(t, crd.VolumePhasePending, vol.Status.Phase)
	require.Empty(t, vol.Status.Bucket)
}

func TestBucketGrowsQuota(t *testing.T) {
	vol := provisioned(testBucketVolume(t, testutil.LeaseID(t), crd.VolumePhaseProvisioned), crd.VolumePhaseProvisioned)
	vol.Spec.Size = "4194304"
	s, fake, ref := bucketScaffold(t, nil, vol)
	ensureFakeBucket(t, fake, ref)

	after, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Equal(t, int64(4194304), fake.Quota(ref))
	require.Equal(t, "4194304", after.Status.BucketSize)
}

func TestBucketRetainedIsSuspended(t *testing.T) {
	vol := provisioned(testBucketVolume(t, testutil.LeaseID(t), crd.VolumePhaseRetained), crd.VolumePhaseRetained)
	until := metav1.NewTime(time.Now().Add(time.Hour))
	vol.Status.RetainedUntil = &until
	s, fake, ref := bucketScaffold(t, nil, vol)
	ensureFakeBucket(t, fake, ref)

	_, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.True(t, fake.Suspended(ref))

	// lifted by hand: the next pass suspends it again
	require.NoError(t, fake.Resume(context.Background(), ref))
	_, err = reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.True(t, fake.Suspended(ref))
}

func bucketAdoptionScaffold(t *testing.T, policy *dv1.VolumePolicy) (*scaffold, *buckettest.Fake, bucket.Ref, bucket.Key) {
	t.Helper()

	lid := testutil.LeaseID(t)
	vol := provisioned(testBucketVolume(t, lid, crd.VolumePhaseRetained), crd.VolumePhaseRetained)
	until := metav1.NewTime(time.Now().Add(time.Hour))
	vol.Status.RetainedUntil = &until

	// the chain event moved the spec to the adopting deployment, whose lease
	// the daemon recorded
	vol.Spec.GroupID.DSeq = strconv.FormatUint(lid.DSeq+7, 10)
	vol.Spec.LeaseID.DSeq = vol.Spec.GroupID.DSeq

	s, fake, ref := bucketScaffold(t, &fakeChain{policy: policy}, vol)
	ensureFakeBucket(t, fake, ref)

	key, err := fake.CreateKey(context.Background(), ref, "old", bucket.KeyReadWrite)
	require.NoError(t, err)
	require.NoError(t, fake.Suspend(context.Background(), ref))

	return s, fake, ref, key
}

func adoptPolicy(owner string, deadDSeq string) *dv1.VolumePolicy {
	dseq, _ := strconv.ParseUint(deadDSeq, 10, 64)

	return &dv1.VolumePolicy{
		Vid:    testVID,
		Access: dv1.VolumeAccessBucket,
		Adopt:  &dv1.VolumeRef{Owner: owner, DSeq: dseq, GSeq: 1, Name: testVID},
	}
}

func TestBucketAdoptionResumesWithoutOldKeys(t *testing.T) {
	s, fake, ref, old := bucketAdoptionScaffold(t, nil)

	before, err := s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), ref.UserID, metav1.GetOptions{})
	require.NoError(t, err)
	s.chain.policy = adoptPolicy(before.Spec.Owner, before.Status.BucketDSeq)

	after, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)

	require.False(t, fake.Suspended(ref))
	keys, err := fake.ListKeys(context.Background(), ref)
	require.NoError(t, err)
	require.Empty(t, keys)
	_, err = fake.Get(context.Background(), ref, old, "anything")
	require.Error(t, err)

	require.Equal(t, crd.VolumePhaseProvisioned, after.Status.Phase)
	require.Equal(t, before.Spec.GroupID.DSeq, after.Status.BucketDSeq)
	require.Nil(t, after.Status.RetainedUntil)
}

func TestBucketAdoptionRejected(t *testing.T) {
	for name, mutate := range map[string]func(p *dv1.VolumePolicy){
		"another owner":      func(p *dv1.VolumePolicy) { p.Adopt.Owner = "akash1someoneelse" },
		"another deployment": func(p *dv1.VolumePolicy) { p.Adopt.DSeq++ },
		"another vid":        func(p *dv1.VolumePolicy) { p.Vid = "other" },
		"a mounted volume":   func(p *dv1.VolumePolicy) { p.Access = dv1.VolumeAccessMount },
		"no adoption":        func(p *dv1.VolumePolicy) { p.Adopt = nil },
	} {
		t.Run(name, func(t *testing.T) {
			s, fake, ref, old := bucketAdoptionScaffold(t, nil)

			before, err := s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), ref.UserID, metav1.GetOptions{})
			require.NoError(t, err)
			policy := adoptPolicy(before.Spec.Owner, before.Status.BucketDSeq)
			mutate(policy)
			s.chain.policy = policy

			after, err := reconcileVolume(t, s, ref.UserID)
			require.NoError(t, err)

			require.True(t, fake.Suspended(ref))
			keys, err := fake.ListKeys(context.Background(), ref)
			require.NoError(t, err)
			require.Equal(t, []bucket.KeyInfo{old.KeyInfo}, keys)
			require.Equal(t, crd.VolumePhaseRetained, after.Status.Phase)
			require.Equal(t, before.Status.BucketDSeq, after.Status.BucketDSeq)
			require.NotNil(t, after.Status.RetainedUntil)
		})
	}
}

// TestBucketAdoptionWithoutLeaseDoesNotStopGC: an adopting deployment that
// never gets a lease cannot keep the bucket past its deadline.
func TestBucketAdoptionWithoutLeaseDoesNotStopGC(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := provisioned(testBucketVolume(t, lid, crd.VolumePhaseRetained), crd.VolumePhaseRetained)
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	vol.Status.RetainedUntil = &past
	vol.Spec.GroupID.DSeq = strconv.FormatUint(lid.DSeq+7, 10)

	s, fake, ref := bucketScaffold(t, nil, vol)
	s.r.chain = nil
	ensureFakeBucket(t, fake, ref)
	require.NoError(t, fake.Suspend(context.Background(), ref))

	after, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Nil(t, after)
	require.False(t, fake.Exists(ref))
}

func TestBucketAdoptingFreezesGC(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := provisioned(testBucketVolume(t, lid, crd.VolumePhaseAdopting), crd.VolumePhaseAdopting)
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	vol.Status.RetainedUntil = &past
	vol.Spec.GroupID.DSeq = strconv.FormatUint(lid.DSeq+7, 10)
	vol.Spec.LeaseID.DSeq = vol.Spec.GroupID.DSeq

	s, fake, ref := bucketScaffold(t, nil, vol)
	s.r.chain = nil
	ensureFakeBucket(t, fake, ref)

	after, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseAdopting, after.Status.Phase)
	require.True(t, fake.Exists(ref))
}

func TestBucketGC(t *testing.T) {
	vol := provisioned(testBucketVolume(t, testutil.LeaseID(t), crd.VolumePhaseRetained), crd.VolumePhaseRetained)
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	vol.Status.RetainedUntil = &past
	s, fake, ref := bucketScaffold(t, nil, vol)
	ensureFakeBucket(t, fake, ref)

	after, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Nil(t, after, "the record outlived its bucket")
	require.False(t, fake.Exists(ref))
}

func TestBucketGCFailureKeepsRecord(t *testing.T) {
	vol := provisioned(testBucketVolume(t, testutil.LeaseID(t), crd.VolumePhaseRetained), crd.VolumePhaseRetained)
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	vol.Status.RetainedUntil = &past
	s, fake, ref := bucketScaffold(t, nil, vol)
	ensureFakeBucket(t, fake, ref)
	fake.Fail("Delete", errors.New("rgw down"))

	after, err := reconcileVolume(t, s, ref.UserID)
	require.ErrorIs(t, err, ErrVolumeOperator)
	require.Equal(t, crd.VolumePhaseReleasing, after.Status.Phase)
	require.True(t, fake.Exists(ref))
}

func TestBucketExportingThaws(t *testing.T) {
	vol := provisioned(testBucketVolume(t, testutil.LeaseID(t), crd.VolumePhaseExporting), crd.VolumePhaseExporting)
	s, fake, ref := bucketScaffold(t, nil, vol)
	ensureFakeBucket(t, fake, ref)

	// the reclaim window: keys keep working
	after, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseExporting, after.Status.Phase)
	require.False(t, fake.Suspended(ref))

	past := metav1.NewTime(time.Now().Add(-time.Minute))
	after.Status.RetainedUntil = &past
	_, err = s.ac.AkashV2beta2().Volumes(testNS).Update(context.Background(), after, metav1.UpdateOptions{})
	require.NoError(t, err)

	after, err = reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseRetained, after.Status.Phase)
}

func TestBucketWithoutBackend(t *testing.T) {
	s, _, ref := bucketScaffold(t, nil, testBucketVolume(t, testutil.LeaseID(t), crd.VolumePhasePending))
	s.r.buckets = nil

	vol, err := reconcileVolume(t, s, ref.UserID)
	require.ErrorIs(t, err, ErrVolumeOperator)
	require.Equal(t, crd.VolumePhasePending, vol.Status.Phase)
}

// TestBucketAdoptionWaitsForLease: the chain's adoption event alone leaves
// the bucket suspended under its deadline; the adopting lease completes it.
func TestBucketAdoptionWaitsForLease(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := provisioned(testBucketVolume(t, lid, crd.VolumePhaseAdopting), crd.VolumePhaseAdopting)
	until := metav1.NewTime(time.Now().Add(time.Hour))
	vol.Status.RetainedUntil = &until
	vol.Spec.GroupID.DSeq = strconv.FormatUint(lid.DSeq+7, 10)

	s, fake, ref := bucketScaffold(t, &fakeChain{}, vol)
	ensureFakeBucket(t, fake, ref)
	require.NoError(t, fake.Suspend(context.Background(), ref))
	s.chain.policy = adoptPolicy(vol.Spec.Owner, vol.Status.BucketDSeq)

	after, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseRetained, after.Status.Phase, "GC stays frozen without a lease")
	require.True(t, fake.Suspended(ref))
	require.Equal(t, strconv.FormatUint(lid.DSeq, 10), after.Status.BucketDSeq)
	require.NotNil(t, after.Status.RetainedUntil)
	require.Zero(t, s.chain.calls, "verified an adoption with no lease")

	// the daemon records the adopting lease
	after.Spec.LeaseID.DSeq = after.Spec.GroupID.DSeq
	_, err = s.ac.AkashV2beta2().Volumes(testNS).Update(context.Background(), after, metav1.UpdateOptions{})
	require.NoError(t, err)

	after, err = reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseProvisioned, after.Status.Phase)
	require.False(t, fake.Suspended(ref))
	require.Nil(t, after.Status.RetainedUntil)
}

// TestBucketLeasedAdoptionBeatsDeadline: a lease won just before the
// deadline hands the bucket over even when the pass runs after it.
func TestBucketLeasedAdoptionBeatsDeadline(t *testing.T) {
	s, fake, ref, _ := bucketAdoptionScaffold(t, nil)

	vol, err := s.ac.AkashV2beta2().Volumes(testNS).Get(context.Background(), ref.UserID, metav1.GetOptions{})
	require.NoError(t, err)
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	vol.Status.RetainedUntil = &past
	_, err = s.ac.AkashV2beta2().Volumes(testNS).Update(context.Background(), vol, metav1.UpdateOptions{})
	require.NoError(t, err)
	s.chain.policy = adoptPolicy(vol.Spec.Owner, vol.Status.BucketDSeq)

	after, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.NotNil(t, after, "a leased adoption was collected")
	require.Equal(t, crd.VolumePhaseProvisioned, after.Status.Phase)
	require.True(t, fake.Exists(ref))
}

// TestBucketReleasingIsNotAdopted: once destruction started, no adoption
// brings the bucket back.
func TestBucketReleasingIsNotAdopted(t *testing.T) {
	lid := testutil.LeaseID(t)
	vol := provisioned(testBucketVolume(t, lid, crd.VolumePhaseReleasing), crd.VolumePhaseReleasing)
	until := metav1.NewTime(time.Now().Add(time.Hour))
	vol.Status.RetainedUntil = &until
	vol.Spec.GroupID.DSeq = strconv.FormatUint(lid.DSeq+7, 10)
	vol.Spec.LeaseID.DSeq = vol.Spec.GroupID.DSeq

	s, fake, ref := bucketScaffold(t, &fakeChain{}, vol)
	ensureFakeBucket(t, fake, ref)
	require.NoError(t, fake.Suspend(context.Background(), ref))
	s.chain.policy = adoptPolicy(vol.Spec.Owner, vol.Status.BucketDSeq)

	after, err := reconcileVolume(t, s, ref.UserID)
	require.NoError(t, err)
	require.Equal(t, crd.VolumePhaseReleasing, after.Status.Phase)
	require.True(t, fake.Suspended(ref))
}
