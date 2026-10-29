package v2beta2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dtypes "pkg.akt.dev/go/node/deployment/v1beta4"
	attrtypes "pkg.akt.dev/go/node/types/attributes/v1"
	rtypes "pkg.akt.dev/go/node/types/resources/v1beta4"
	"pkg.akt.dev/go/testutil"
)

func testVolumeGroupSpec(vid string, size uint64) *dtypes.GroupSpec {
	return &dtypes.GroupSpec{
		Name: "us-west",
		Volume: &dv1.VolumePolicy{
			Vid:            vid,
			Reclaim:        dv1.VolumeReclaimRetain,
			Retention:      168 * time.Hour,
			MaxAttachments: 1,
		},
		Resources: dtypes.ResourceUnits{
			{
				Resources: rtypes.Resources{
					ID: 1,
					CPU: &rtypes.CPU{
						Units: rtypes.NewResourceValue(0),
					},
					GPU: &rtypes.GPU{
						Units: rtypes.NewResourceValue(0),
					},
					Memory: &rtypes.Memory{
						Quantity: rtypes.NewResourceValue(0),
					},
					Storage: rtypes.Volumes{
						{
							Name:     vid,
							Quantity: rtypes.NewResourceValue(size),
							Attributes: attrtypes.Attributes{
								{Key: "class", Value: "beta2"},
								{Key: "persistent", Value: "true"},
							},
						},
					},
					Endpoints: rtypes.Endpoints{},
				},
				Count: 1,
			},
		},
	}
}

func TestVolumeName(t *testing.T) {
	name := VolumeName("akash1owner", "myapp-pgdata")

	require.Len(t, name, len("volume-")+12)
	require.Equal(t, name, VolumeName("akash1owner", "myapp-pgdata"))
	require.NotEqual(t, name, VolumeName("akash1other", "myapp-pgdata"))
	require.NotEqual(t, name, VolumeName("akash1owner", "other-vid"))
}

func TestNewVolumeRoundTrip(t *testing.T) {
	lid := testutil.LeaseID(t)
	gspec := testVolumeGroupSpec("myapp-pgdata", 100)

	vol, err := NewVolume("akash-services", lid, gspec)
	require.NoError(t, err)

	require.Equal(t, VolumeName(lid.Owner, "myapp-pgdata"), vol.Name)
	require.Equal(t, lid.Owner, vol.Spec.Owner)
	require.Equal(t, "myapp-pgdata", vol.Spec.VID)
	require.Equal(t, "beta2", vol.Spec.Class)
	require.Equal(t, "100", vol.Spec.Size)
	require.Equal(t, "retain", vol.Spec.Reclaim)
	require.Equal(t, VolumePhasePending, vol.Status.Phase)

	rlid, rspec, err := vol.FromCRD()
	require.NoError(t, err)
	require.Equal(t, lid, rlid)

	require.NotNil(t, rspec.Volume)
	require.Equal(t, "myapp-pgdata", rspec.Volume.Vid)
	require.Equal(t, dv1.VolumeReclaimRetain, rspec.Volume.Reclaim)
	require.Equal(t, 168*time.Hour, rspec.Volume.Retention)

	require.Len(t, rspec.Resources, 1)
	require.Len(t, rspec.Resources[0].Storage, 1)
	require.Equal(t, uint64(100), rspec.Resources[0].Storage[0].Quantity.Value())

	class, set := rspec.Resources[0].Storage[0].Attributes.Find("class").AsString()
	require.True(t, set)
	require.Equal(t, "beta2", class)

	persistent, set := rspec.Resources[0].Storage[0].Attributes.Find("persistent").AsBool()
	require.True(t, set)
	require.True(t, persistent)
}

func TestNewVolumeBucketRoundTrip(t *testing.T) {
	lid := testutil.LeaseID(t)
	gspec := testVolumeGroupSpec("assets", 100)
	gspec.Volume.Access = dv1.VolumeAccessBucket
	gspec.Volume.MaxAttachments = 0

	vol, err := NewVolume("akash-services", lid, gspec)
	require.NoError(t, err)
	require.Equal(t, "bucket", vol.Spec.Access)
	require.True(t, vol.IsBucket())

	_, rspec, err := vol.FromCRD()
	require.NoError(t, err)
	require.Equal(t, dv1.VolumeAccessBucket, rspec.Volume.Access)
	require.Equal(t, uint32(0), rspec.Volume.MaxAttachments)
}

func TestNewVolumeMountHasNoAccess(t *testing.T) {
	vol, err := NewVolume("akash-services", testutil.LeaseID(t), testVolumeGroupSpec("myapp-pgdata", 100))
	require.NoError(t, err)

	// mount volumes keep the record they had before buckets existed
	require.Equal(t, "", vol.Spec.Access)
	require.False(t, vol.IsBucket())

	_, rspec, err := vol.FromCRD()
	require.NoError(t, err)
	require.Equal(t, dv1.VolumeAccessMount, rspec.Volume.Access)
	require.Equal(t, uint32(1), rspec.Volume.MaxAttachments)
}

func TestVolumeMatches(t *testing.T) {
	vol := &Volume{Spec: VolumeSpec{Owner: "akash1owner", VID: "pgdata"}}

	require.True(t, vol.Matches("akash1owner", "pgdata"))
	require.False(t, vol.Matches("akash1other", "pgdata"))
	require.False(t, vol.Matches("akash1owner", "other"))
}

func TestNewVolumeRejectsNonVolumeGroups(t *testing.T) {
	lid := testutil.LeaseID(t)

	_, err := NewVolume("akash-services", lid, &dtypes.GroupSpec{Name: "compute"})
	require.ErrorIs(t, err, ErrInvalidArgs)

	gspec := testVolumeGroupSpec("myapp-pgdata", 100)
	gspec.Resources[0].Storage = rtypes.Volumes{}
	_, err = NewVolume("akash-services", lid, gspec)
	require.ErrorIs(t, err, ErrInvalidArgs)
}

func TestNewVolumeReplicaStartsWithoutACopy(t *testing.T) {
	lid := testutil.LeaseID(t)
	gspec := testVolumeGroupSpec("myapp-pgdata-replica", 100)
	primary := dv1.VolumeRef{Owner: lid.Owner, DSeq: 7, GSeq: 1, Name: "myapp-pgdata"}
	gspec.Volume.ReplicaOf = &primary

	vol, err := NewVolume("akash-services", lid, gspec)
	require.NoError(t, err)

	require.Equal(t, primary.String(), vol.Spec.ReplicaOf)
	require.True(t, vol.IsReplica())
	require.True(t, vol.Status.Syncing)
	require.True(t, vol.AwaitsFirstCopy())
	require.True(t, vol.Incomplete())

	plain, err := NewVolume("akash-services", lid, testVolumeGroupSpec("myapp-pgdata", 100))
	require.NoError(t, err)
	require.False(t, plain.IsReplica())
	require.False(t, plain.Status.Syncing)
	require.False(t, plain.Incomplete())
}

func TestVolumeIncomplete(t *testing.T) {
	synced := metav1.Now()

	for name, tc := range map[string]struct {
		vol        Volume
		incomplete bool
		awaits     bool
	}{
		"plain volume":             {Volume{}, false, false},
		"moved, still copying in":  {Volume{Spec: VolumeSpec{ImportFrom: "akash1source"}}, true, false},
		"replica, no copy yet":     {Volume{Spec: VolumeSpec{ReplicaOf: "ref"}}, true, true},
		"replica, synced":          {Volume{Spec: VolumeSpec{ReplicaOf: "ref"}, Status: VolumeStatus{SyncedAt: &synced}}, false, false},
		"replica, round writing":   {Volume{Spec: VolumeSpec{ReplicaOf: "ref"}, Status: VolumeStatus{SyncedAt: &synced, Syncing: true}}, true, false},
		"replica, promoted":        {Volume{Spec: VolumeSpec{ReplicaOf: "ref"}, Status: VolumeStatus{Promoted: true}}, false, false},
		"adopted, round unlanded":  {Volume{Status: VolumeStatus{Syncing: true}}, true, false},
		"promoted, round unlanded": {Volume{Spec: VolumeSpec{ReplicaOf: "ref"}, Status: VolumeStatus{Promoted: true, Syncing: true}}, true, false},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.incomplete, tc.vol.Incomplete())
			require.Equal(t, tc.awaits, tc.vol.AwaitsFirstCopy())
		})
	}
}

func TestVolumeStatusDeepCopiesSyncedAt(t *testing.T) {
	synced := metav1.Now()
	vol := &Volume{Status: VolumeStatus{SyncedAt: &synced}}

	cp := vol.DeepCopy()
	cp.Status.SyncedAt.Time = cp.Status.SyncedAt.Add(time.Hour)

	require.Equal(t, synced, *vol.Status.SyncedAt)
}
