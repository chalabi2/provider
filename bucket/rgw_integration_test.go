//go:build rgw_integration

package bucket_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/ceph/go-ceph/rgw/admin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/akash-network/provider/bucket"
	"github.com/akash-network/provider/bucket/buckettest"
)

// These tests run against a live RGW, reached at AP_BUCKET_TEST_ENDPOINT
// for both its admin API and S3, with the admin user's keys in
// AP_BUCKET_ADMIN_ACCESS_KEY and AP_BUCKET_ADMIN_SECRET_KEY:
//
//	go test -tags rgw_integration ./bucket/...
const envTestEndpoint = "AP_BUCKET_TEST_ENDPOINT"

const testRegion = "default"

func rgwForTest(t *testing.T) (bucket.Backend, *admin.API, s3DataPlane) {
	t.Helper()

	endpoint := os.Getenv(envTestEndpoint)
	if endpoint == "" {
		t.Skipf("%s is not set", envTestEndpoint)
	}

	cfg := bucket.Config{
		Backend:               bucket.BackendRGW,
		Classes:               []string{"beta2"},
		Endpoint:              endpoint,
		Region:                testRegion,
		AdminEndpoint:         endpoint,
		AdminAccessKey:        os.Getenv(bucket.EnvAdminAccessKey),
		AdminSecretKey:        os.Getenv(bucket.EnvAdminSecretKey),
		AllowInsecureEndpoint: true,
	}

	backend, err := bucket.New(cfg)
	require.NoError(t, err)

	api, err := admin.New(endpoint, cfg.AdminAccessKey, cfg.AdminSecretKey, &http.Client{Timeout: 30 * time.Second})
	require.NoError(t, err)

	return backend, api, s3DataPlane{endpoint: endpoint}
}

func TestRGWConformance(t *testing.T) {
	backend, _, data := rgwForTest(t)

	buckettest.Conformance(t, backend, data)
}

func TestRGWProvisionsALockedDownUser(t *testing.T) {
	backend, api, _ := rgwForTest(t)
	ctx := context.Background()

	spec := ensureForTest(t, backend, 5<<20)

	user, err := api.GetUser(ctx, admin.User{ID: spec.UserID})
	require.NoError(t, err)

	for _, key := range user.Keys {
		assert.NotEqual(t, spec.UserID, key.User, "the provider kept a key of the bucket's user")
	}
	require.NotNil(t, user.MaxBuckets)
	assert.Equal(t, -1, *user.MaxBuckets, "the user may create buckets")

	quota, err := api.GetUserQuota(ctx, admin.QuotaSpec{UID: spec.UserID})
	require.NoError(t, err)
	require.NotNil(t, quota.Enabled)
	assert.True(t, *quota.Enabled)
	require.NotNil(t, quota.MaxSize)
	assert.Equal(t, int64(5<<20), *quota.MaxSize)

	info, err := api.GetBucketInfo(ctx, admin.Bucket{Bucket: spec.Bucket})
	require.NoError(t, err)
	assert.Equal(t, spec.UserID, info.Owner)
}

// TestRGWKeysCannotPublish: no key, and no request without one, can make
// the bucket's data readable without a key, delete the bucket or make
// another.
func TestRGWKeysCannotPublish(t *testing.T) {
	backend, _, data := rgwForTest(t)
	ctx := context.Background()

	spec := ensureForTest(t, backend, 5<<20)

	key, err := backend.CreateKey(ctx, spec.Ref, "app", bucket.KeyReadWrite)
	require.NoError(t, err)

	// first, while the bucket is empty and could be deleted
	_, err = data.do(ctx, spec.Ref, key, s3Request{method: http.MethodDelete})
	assert.Error(t, err, "a key deletes its bucket")

	require.NoError(t, data.Put(ctx, spec.Ref, key, "private.txt", []byte("private")))

	allUsers := `uri="http://acs.amazonaws.com/groups/global/AllUsers"`
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/*"]}]}`, spec.Bucket)
	openBlock := `<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>`

	for name, req := range map[string]s3Request{
		"public bucket policy":       {method: http.MethodPut, query: "?policy", body: []byte(policy)},
		"bucket policy removal":      {method: http.MethodDelete, query: "?policy"},
		"public access block change": {method: http.MethodPut, query: "?publicAccessBlock", body: []byte(openBlock)},
		"public access block delete": {method: http.MethodDelete, query: "?publicAccessBlock"},
		"public bucket acl":          {method: http.MethodPut, query: "?acl", header: map[string]string{"x-amz-acl": "public-read"}},
		"public object acl":          {method: http.MethodPut, path: "/private.txt", query: "?acl", header: map[string]string{"x-amz-acl": "public-read"}},
		"bucket cors":                {method: http.MethodPut, query: "?cors", body: []byte(`<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`)},
		"bucket website":             {method: http.MethodPut, query: "?website", body: []byte(`<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>`)},
		"another bucket":             {method: http.MethodPut, bucket: spec.Bucket + "-2"},
		"bucket delete":              {method: http.MethodDelete},
	} {
		_, err := data.do(ctx, spec.Ref, key, req)
		assert.Error(t, err, name)
	}

	// RGW stores these ACLs but ignores them
	_, err = data.do(ctx, spec.Ref, key, s3Request{method: http.MethodPut, path: "/public.txt", header: map[string]string{"x-amz-acl": "public-read"}, body: []byte("public")})
	require.NoError(t, err)
	_, err = data.do(ctx, spec.Ref, key, s3Request{method: http.MethodPut, path: "/granted.txt", header: map[string]string{"x-amz-grant-read": allUsers}, body: []byte("granted")})
	require.NoError(t, err)

	anonymous := bucket.Key{}
	for _, object := range []string{"/private.txt", "/public.txt", "/granted.txt"} {
		_, err := data.do(ctx, spec.Ref, anonymous, s3Request{method: http.MethodGet, path: object})
		assert.Error(t, err, "anonymous read of %s", object)
	}
	_, err = data.do(ctx, spec.Ref, anonymous, s3Request{method: http.MethodGet})
	assert.Error(t, err, "anonymous listing")
	_, err = data.do(ctx, spec.Ref, anonymous, s3Request{method: http.MethodPut, path: "/anonymous.txt", body: []byte("x")})
	assert.Error(t, err, "anonymous write")

	body, err := data.Get(ctx, spec.Ref, key, "private.txt")
	require.NoError(t, err)
	assert.Equal(t, []byte("private"), body)
}

func ensureForTest(t *testing.T, backend bucket.Backend, size int64) bucket.Spec {
	t.Helper()

	id := fmt.Sprintf("%x", time.Now().UnixNano())
	spec := bucket.Spec{Ref: bucket.RefFor("volume-it-"+id, "akash1integration"+id, "it-"+id), SizeBytes: size}

	require.NoError(t, backend.Ensure(context.Background(), spec))
	t.Cleanup(func() {
		assert.NoError(t, backend.Delete(context.Background(), spec.Ref))
	})

	return spec
}

type s3Request struct {
	method string
	bucket string
	path   string
	query  string
	header map[string]string
	body   []byte
}

// s3DataPlane makes path-style S3 requests signed with a tenant key, or
// anonymous ones for a key without an access key id.
type s3DataPlane struct {
	endpoint string
}

func (d s3DataPlane) Put(ctx context.Context, ref bucket.Ref, key bucket.Key, object string, body []byte) error {
	_, err := d.do(ctx, ref, key, s3Request{method: http.MethodPut, path: "/" + object, body: body})
	return err
}

func (d s3DataPlane) Get(ctx context.Context, ref bucket.Ref, key bucket.Key, object string) ([]byte, error) {
	return d.do(ctx, ref, key, s3Request{method: http.MethodGet, path: "/" + object})
}

func (d s3DataPlane) do(ctx context.Context, ref bucket.Ref, key bucket.Key, r s3Request) ([]byte, error) {
	name := r.bucket
	if name == "" {
		name = ref.Bucket
	}

	req, err := http.NewRequestWithContext(ctx, r.method, d.endpoint+"/"+name+r.path+r.query, bytes.NewReader(r.body))
	if err != nil {
		return nil, err
	}

	sum := sha256.Sum256(r.body)
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)
	for k, v := range r.header {
		req.Header.Set(k, v)
	}

	if key.AccessKeyID != "" {
		creds := aws.Credentials{AccessKeyID: key.AccessKeyID, SecretAccessKey: key.SecretAccessKey}
		if err := v4.NewSigner().SignHTTP(ctx, creds, req, hash, "s3", testRegion, time.Now()); err != nil {
			return nil, err
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%s %s%s: %s: %.200s", r.method, r.path, r.query, resp.Status, body)
	}

	return body, nil
}
