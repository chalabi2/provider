package storage

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	"pkg.akt.dev/go/testutil"

	cstorage "github.com/akash-network/provider/cluster/types/v1beta3/clients/storage"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

func clientForTest(t *testing.T, handler http.Handler) cstorage.Client {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	addr := server.Listener.Addr().(*net.TCPAddr)

	endpoint := &net.SRV{
		Target: addr.IP.String(),
		Port:   uint16(addr.Port), // nolint: gosec
	}

	cl, err := NewClient(context.Background(), testutil.Logger(t), endpoint)
	require.NoError(t, err)
	t.Cleanup(cl.Stop)

	return cl
}

func TestStorageClientCheck(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
	})

	cl := clientForTest(t, mux)

	require.NoError(t, cl.Check(context.Background()))
}

func TestStorageClientLookups(t *testing.T) {
	owner := testutil.AccAddress(t).String()

	retained := crd.VolumeName(owner, "retained-vid")
	parked := crd.VolumeName(owner, "parked-vid")
	attached := crd.VolumeName(owner, "attached-vid")

	states := map[string]cstorage.VolumeState{
		retained: {Owner: owner, VID: "retained-vid", Phase: string(crd.VolumePhaseRetained)},
		parked:   {Owner: owner, VID: "parked-vid", Phase: string(crd.VolumePhaseProvisioned)},
		attached: {Owner: owner, VID: "attached-vid", Phase: string(crd.VolumePhaseAttached), AttachedLease: "some-lease"},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/volume/", func(rw http.ResponseWriter, req *http.Request) {
		reqOwner, vid, ok := splitOwnerVid(req.URL.Path[len("/volume/"):])
		if !ok {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		state, exists := states[crd.VolumeName(reqOwner, vid)]
		if !exists {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode(state)
	})

	cl := clientForTest(t, mux)
	ctx := context.Background()

	found, err := cl.RetainedVolume(ctx, owner, "retained-vid")
	require.NoError(t, err)
	require.True(t, found)

	// parked volume is not retained
	found, err = cl.RetainedVolume(ctx, owner, "parked-vid")
	require.NoError(t, err)
	require.False(t, found)

	found, err = cl.ParkedVolume(ctx, dv1.VolumeRef{Owner: owner, Name: "parked-vid"})
	require.NoError(t, err)
	require.True(t, found)

	// attached volume is not parked
	found, err = cl.ParkedVolume(ctx, dv1.VolumeRef{Owner: owner, Name: "attached-vid"})
	require.NoError(t, err)
	require.False(t, found)

	// unknown volume: no error, just not found
	found, err = cl.ParkedVolume(ctx, dv1.VolumeRef{Owner: owner, Name: "missing-vid"})
	require.NoError(t, err)
	require.False(t, found)
}

func splitOwnerVid(path string) (string, string, bool) {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i], path[i+1:], true
		}
	}

	return "", "", false
}
