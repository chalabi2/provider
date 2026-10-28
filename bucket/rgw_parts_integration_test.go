//go:build rgw_integration

package bucket

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRGWWritesLargeObjectsInParts runs against the live RGW of
// rgw_integration_test.go, with parts small enough to keep the test quick.
func TestRGWWritesLargeObjectsInParts(t *testing.T) {
	endpoint := os.Getenv("AP_BUCKET_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("AP_BUCKET_TEST_ENDPOINT is not set")
	}

	backend, err := New(Config{
		Backend:               BackendRGW,
		Classes:               []string{"beta2"},
		Endpoint:              endpoint,
		Region:                "default",
		AdminEndpoint:         endpoint,
		AdminAccessKey:        os.Getenv(EnvAdminAccessKey),
		AdminSecretKey:        os.Getenv(EnvAdminSecretKey),
		AllowInsecureEndpoint: true,
	})
	require.NoError(t, err)

	// S3 refuses parts under 5 MiB but the last
	r := backend.(*rgw)
	r.partSize = 5 << 20
	r.multipartAbove = 5 << 20

	ctx := context.Background()
	id := fmt.Sprintf("%x", time.Now().UnixNano())
	spec := Spec{Ref: RefFor("volume-parts-"+id, "akash1parts"+id, "parts-"+id), SizeBytes: 64 << 20}

	require.NoError(t, backend.Ensure(ctx, spec))
	t.Cleanup(func() {
		assert.NoError(t, backend.Delete(context.Background(), spec.Ref))
	})

	objects, err := backend.Objects(ctx, spec.Ref)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, objects.Close())
	})

	body := make([]byte, 12<<20+12345)
	_, err = rand.Read(body)
	require.NoError(t, err)
	sum := sha256.Sum256(body)

	info := ObjectInfo{Key: "large/object.bin", Size: int64(len(body)), ContentType: "application/x-test", Metadata: map[string]string{"parts": "3"}}
	require.NoError(t, objects.Put(ctx, info, bytes.NewReader(body), sum[:]))

	got, rc, err := objects.Get(ctx, info.Key)
	require.NoError(t, err)
	read, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	assert.True(t, bytes.Equal(body, read), "the object read back differs")
	assert.Equal(t, info, got)

	wrong := sha256.Sum256([]byte("another object"))
	err = objects.Put(ctx, ObjectInfo{Key: "large/corrupt.bin", Size: int64(len(body))}, bytes.NewReader(body), wrong[:])
	require.Error(t, err, "an object that does not match its digest is written")

	_, _, err = objects.Get(ctx, "large/corrupt.bin")
	assert.ErrorIs(t, err, ErrObjectNotFound)

	// the refused upload was aborted rather than left holding its parts
	session := objects.(*rgwObjects)
	resp, err := session.do(ctx, s3Call{method: http.MethodGet, query: url.Values{"uploads": {""}}})
	require.NoError(t, err)
	listing, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(listing))
	assert.NotContains(t, string(listing), "<Upload>", "an aborted upload is still listed")
}
