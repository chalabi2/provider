package rest

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mtypes "pkg.akt.dev/go/node/market/v1"
	apclient "pkg.akt.dev/go/provider/client"
	ajwt "pkg.akt.dev/go/util/jwt"

	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

func volumePath(lid mtypes.LeaseID) string {
	return apclient.LeasePath(lid) + "/volume"
}

func TestVolumeStatusReportsAVolumeBeingCopiedIn(t *testing.T) {
	runBucketTest(t, false, func(vol *crd.Volume) {
		vol.Spec.Access = ""
		vol.Spec.ImportFrom = "akash1previous"
		vol.Status.Phase = crd.VolumePhaseImporting
	}, func(bt *bucketTest) {
		code, body := bt.do(http.MethodGet, volumePath(bt.lid), "", ajwt.PermissionScopeStatus)
		require.Equal(t, http.StatusOK, code, body)

		var status VolumeStatus
		require.NoError(t, json.Unmarshal([]byte(body), &status))
		require.Equal(t, VolumeStatus{Phase: "Importing", SizeBytes: 1048576, ImportFrom: "akash1previous"}, status)
	})
}

func TestVolumeStatusReportsWhenAClosedVolumeIsDestroyed(t *testing.T) {
	until := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

	runBucketTest(t, false, func(vol *crd.Volume) {
		vol.Spec.Access = ""
		vol.Status.Phase = crd.VolumePhaseRetained
		retained := metav1.NewTime(until)
		vol.Status.RetainedUntil = &retained
	}, func(bt *bucketTest) {
		code, body := bt.do(http.MethodGet, volumePath(bt.lid), "", fullAccess)
		require.Equal(t, http.StatusOK, code, body)

		var status VolumeStatus
		require.NoError(t, json.Unmarshal([]byte(body), &status))
		require.Equal(t, "Retained", status.Phase)
		require.True(t, until.Equal(*status.RetainedUntil))
		require.Empty(t, status.ImportFrom)
	})
}

func TestVolumeStatusNeedsTheStatusScope(t *testing.T) {
	runBucketTest(t, false, nil, func(bt *bucketTest) {
		code, _ := bt.do(http.MethodGet, volumePath(bt.lid), "", ajwt.PermissionScopeLogs)
		require.Equal(t, http.StatusUnauthorized, code, "an unrelated scope reads the volume")

		code, _ = bt.do(http.MethodGet, volumePath(bt.lid), "")
		require.Equal(t, http.StatusUnauthorized, code, "an anonymous request reads the volume")
	})
}

func TestVolumeStatusOfALeaseWithoutAVolume(t *testing.T) {
	runBucketTest(t, false, nil, func(bt *bucketTest) {
		other := bt.lid
		other.DSeq++

		code, body := bt.do(http.MethodGet, volumePath(other), "", fullAccess)
		require.Equal(t, http.StatusNotFound, code)
		require.Equal(t, "no volume for this lease\n", body)
	})
}

func TestVolumeStatusRefusesAnotherOwnersVolume(t *testing.T) {
	runBucketTest(t, false, func(vol *crd.Volume) {
		vol.Spec.Owner = "akash1someoneelse"
	}, func(bt *bucketTest) {
		code, _ := bt.do(http.MethodGet, volumePath(bt.lid), "", fullAccess)
		require.Equal(t, http.StatusNotFound, code)
	})
}

func TestVolumeStatusReportsHowFarBehindAReplicaIs(t *testing.T) {
	synced := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)

	runBucketTest(t, false, func(vol *crd.Volume) {
		vol.Spec.Access = ""
		vol.Spec.ReplicaOf = "akash1owner/7/1/primary-data"
		vol.Status.Phase = crd.VolumePhaseProvisioned
		at := metav1.NewTime(synced)
		vol.Status.SyncedAt = &at
	}, func(bt *bucketTest) {
		code, body := bt.do(http.MethodGet, volumePath(bt.lid), "", ajwt.PermissionScopeStatus)
		require.Equal(t, http.StatusOK, code, body)

		var status VolumeStatus
		require.NoError(t, json.Unmarshal([]byte(body), &status))
		require.Equal(t, "akash1owner/7/1/primary-data", status.ReplicaOf)
		require.True(t, synced.Equal(*status.SyncedAt))
		require.NotNil(t, status.SyncLagSeconds)
		require.InDelta(t, 3600, *status.SyncLagSeconds, 60)
		require.False(t, status.Promoted)
	})
}

func TestVolumeStatusReportsAPromotedReplica(t *testing.T) {
	runBucketTest(t, false, func(vol *crd.Volume) {
		vol.Spec.Access = ""
		vol.Spec.ReplicaOf = "akash1owner/7/1/primary-data"
		vol.Status.Phase = crd.VolumePhaseAttached
		at := metav1.NewTime(time.Now().Add(-time.Hour))
		vol.Status.SyncedAt = &at
		vol.Status.Promoted = true
	}, func(bt *bucketTest) {
		code, body := bt.do(http.MethodGet, volumePath(bt.lid), "", ajwt.PermissionScopeStatus)
		require.Equal(t, http.StatusOK, code, body)

		var status VolumeStatus
		require.NoError(t, json.Unmarshal([]byte(body), &status))
		require.True(t, status.Promoted)
		require.Nil(t, status.SyncLagSeconds, "a promoted replica follows nothing, so it lags nothing")
	})
}
