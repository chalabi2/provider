package v2beta2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dtypes "pkg.akt.dev/go/node/deployment/v1beta5"
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

func TestNewVolumeRejectsNonVolumeGroups(t *testing.T) {
	lid := testutil.LeaseID(t)

	_, err := NewVolume("akash-services", lid, &dtypes.GroupSpec{Name: "compute"})
	require.ErrorIs(t, err, ErrInvalidArgs)

	gspec := testVolumeGroupSpec("myapp-pgdata", 100)
	gspec.Resources[0].Storage = rtypes.Volumes{}
	_, err = NewVolume("akash-services", lid, gspec)
	require.ErrorIs(t, err, ErrInvalidArgs)
}
