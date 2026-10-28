package rest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdk "github.com/cosmos/cosmos-sdk/types"

	dv1 "pkg.akt.dev/go/node/deployment/v1"
	mtypes "pkg.akt.dev/go/node/market/v1"
	apclient "pkg.akt.dev/go/provider/client"
	"pkg.akt.dev/go/testutil"
	ajwt "pkg.akt.dev/go/util/jwt"

	"github.com/akash-network/provider/bucket"
	"github.com/akash-network/provider/bucket/buckettest"
	kubeclienterrors "github.com/akash-network/provider/cluster/kube/errors"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

const (
	testBucketEndpoint = "https://s3.provider.example.com"
	testBucketVID      = "assets"
)

type bucketTest struct {
	t       *testing.T
	ctx     context.Context
	ckey    cryptotypes.PrivKey
	lid     mtypes.LeaseID
	ref     bucket.Ref
	backend *buckettest.Fake
	url     *url.URL
	// client authenticates with a full access token, as the owner's CLI does
	client apclient.Client
}

// runBucketTest serves a bucket lease of the test owner, with vol adjusting
// its record and buckets reporting whether the provider serves buckets.
func runBucketTest(t *testing.T, buckets bool, vol func(*crd.Volume), fn func(*bucketTest)) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mocks := createMocks()
	backend := buckettest.NewFake()

	if buckets {
		mocks.buckets = newBucketGateway(testutil.Logger(t), backend, bucket.Config{Endpoint: testBucketEndpoint, Region: "default"}, mocks.pcclient)
	}

	ckey, pkey := testutil.Key(t), testutil.Key(t)
	caddr := sdk.AccAddress(ckey.PubKey().Address())
	paddr := sdk.AccAddress(pkey.PubKey().Address())

	lid := testutil.LeaseID(t)
	lid.Owner = caddr.String()
	lid.Provider = paddr.String()

	record := testBucketRecord(lid)
	if vol != nil {
		vol(record)
	}

	ref := bucket.RefFor(record.Name, record.Spec.Owner, record.Spec.VID)
	require.NoError(t, backend.Ensure(ctx, bucket.Spec{Ref: ref, SizeBytes: 1 << 20}))

	mocks.pcclient.On("LeaseVolume", mock.Anything, lid).Return(record, nil)
	mocks.pcclient.On("LeaseVolume", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("%w: lease", kubeclienterrors.ErrVolumeNotFound))

	withServer(ctx, t, []cryptotypes.PrivKey{pkey, ckey}, mocks.pclient, mocks, func(host string, certs *certQuerier) {
		client, err := apclient.NewClient(ctx, paddr,
			apclient.WithAuthJWTSigner(&testJwtSigner{key: ckey, addr: caddr}),
			apclient.WithProviderURL(host),
			apclient.WithCertQuerier(certs))
		require.NoError(t, err)

		u, err := url.Parse(host)
		require.NoError(t, err)

		fn(&bucketTest{
			t:       t,
			ctx:     ctx,
			ckey:    ckey,
			lid:     lid,
			ref:     ref,
			backend: backend,
			url:     u,
			client:  client,
		})
	})
}

func testBucketRecord(lid mtypes.LeaseID) *crd.Volume {
	name := crd.VolumeName(lid.Owner, testBucketVID)

	return &crd.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "lease"},
		Spec: crd.VolumeSpec{
			Owner:   lid.Owner,
			VID:     testBucketVID,
			GroupID: crd.VolumeGroupID{DSeq: strconv.FormatUint(lid.DSeq, 10), GSeq: lid.GSeq},
			Class:   "beta2",
			Size:    "1048576",
			LeaseID: crd.LeaseIDFromAkash(lid),
			Access:  dv1.VolumeAccessBucket.String(),
		},
		Status: crd.VolumeStatus{
			Phase:      crd.VolumePhaseProvisioned,
			Bucket:     bucket.RefFor(name, lid.Owner, testBucketVID).Bucket,
			BucketDSeq: strconv.FormatUint(lid.DSeq, 10),
			BucketSize: "1048576",
		},
	}
}

// fullAccess stands for a token with full access in do.
const fullAccess ajwt.PermissionScope = ""

// do sends a request with a token holding scopes (fullAccess alone for a
// full access token), or with no token when scopes is empty.
func (bt *bucketTest) do(method, path string, body string, scopes ...ajwt.PermissionScope) (int, string) {
	bt.t.Helper()

	uri, err := apclient.MakeURI(bt.url, path)
	require.NoError(bt.t, err)

	req, err := http.NewRequestWithContext(bt.ctx, method, uri, strings.NewReader(body))
	require.NoError(bt.t, err)
	if len(scopes) > 0 {
		req.Header.Set("Authorization", "Bearer "+bt.token(scopes))
	}

	resp, err := bt.client.NewReqClient(bt.ctx).Do(req)
	require.NoError(bt.t, err)
	defer func() {
		_ = resp.Body.Close()
	}()

	reply, err := io.ReadAll(resp.Body)
	require.NoError(bt.t, err)

	return resp.StatusCode, string(reply)
}

func (bt *bucketTest) token(scopes []ajwt.PermissionScope) string {
	bt.t.Helper()

	addr := sdk.AccAddress(bt.ckey.PubKey().Address())
	now := time.Now()

	leases := ajwt.Leases{Access: ajwt.AccessTypeScoped, Scope: scopes}
	if len(scopes) == 1 && scopes[0] == fullAccess {
		leases = ajwt.Leases{Access: ajwt.AccessTypeFull}
	}

	claims := ajwt.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    addr.String(),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(15 * time.Minute)),
		},
		Version: "v1",
		Leases:  leases,
	}

	tok, err := jwt.NewWithClaims(ajwt.SigningMethodES256K, &claims).SignedString(&testJwtSigner{key: bt.ckey, addr: addr})
	require.NoError(bt.t, err)

	return tok
}

func keyRequest(name, access string) string {
	return fmt.Sprintf(`{"name":%q,"access":%q}`, name, access)
}

func TestBucketKeyLifecycle(t *testing.T) {
	runBucketTest(t, true, nil, func(bt *bucketTest) {
		info, err := bt.client.BucketInfo(bt.ctx, bt.lid)
		require.NoError(t, err)
		require.Equal(t, apclient.BucketInfo{
			Bucket:    bt.ref.Bucket,
			Endpoint:  testBucketEndpoint,
			Region:    "default",
			SizeBytes: 1 << 20,
			Keys:      []apclient.BucketKeyInfo{},
		}, info)

		key, err := bt.client.CreateBucketKey(bt.ctx, bt.lid, apclient.BucketKeyRequest{Name: "app", Access: apclient.BucketKeyReadWrite})
		require.NoError(t, err)
		require.Equal(t, "app", key.Name)
		require.Equal(t, apclient.BucketKeyReadWrite, key.Access)
		require.NotEmpty(t, key.AccessKeyID)
		require.NotEmpty(t, key.SecretAccessKey)

		// the key opens the bucket
		require.NoError(t, bt.backend.Put(bt.ctx, bt.ref, bucket.Key{
			KeyInfo:         bucket.KeyInfo{Name: key.Name, Access: bucket.KeyReadWrite, AccessKeyID: key.AccessKeyID},
			SecretAccessKey: key.SecretAccessKey,
		}, "hello.txt", []byte("hello")))

		info, err = bt.client.BucketInfo(bt.ctx, bt.lid)
		require.NoError(t, err)
		require.Equal(t, []apclient.BucketKeyInfo{key.BucketKeyInfo}, info.Keys)
		require.Equal(t, uint64(len("hello")), info.UsedBytes)

		// a listing never carries a secret
		status, body := bt.do(http.MethodGet, apclient.BucketPath(bt.lid), "", fullAccess)
		require.Equal(t, http.StatusOK, status)
		require.NotContains(t, body, key.SecretAccessKey)
		require.NotContains(t, body, "secret")

		require.NoError(t, bt.client.RevokeBucketKey(bt.ctx, bt.lid, "app"))

		info, err = bt.client.BucketInfo(bt.ctx, bt.lid)
		require.NoError(t, err)
		require.Empty(t, info.Keys)

		var rerr apclient.ClientResponseError
		require.ErrorAs(t, bt.client.RevokeBucketKey(bt.ctx, bt.lid, "app"), &rerr)
		require.Equal(t, http.StatusNotFound, rerr.Status)
	})
}

func TestBucketKeyCreateSecretIsNotCached(t *testing.T) {
	runBucketTest(t, true, nil, func(bt *bucketTest) {
		uri, err := apclient.MakeURI(bt.url, apclient.BucketKeysPath(bt.lid))
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(bt.ctx, http.MethodPost, uri, bytes.NewBufferString(keyRequest("app", "read")))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+bt.token([]ajwt.PermissionScope{ajwt.PermissionScopeBucketKeys}))

		resp, err := bt.client.NewReqClient(bt.ctx).Do(req)
		require.NoError(t, err)
		defer func() {
			_ = resp.Body.Close()
		}()

		require.Equal(t, http.StatusCreated, resp.StatusCode)
		require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	})
}

func TestBucketKeyCreateRefusals(t *testing.T) {
	runBucketTest(t, true, nil, func(bt *bucketTest) {
		keys := apclient.BucketKeysPath(bt.lid)

		status, _ := bt.do(http.MethodPost, keys, keyRequest("app", "readwrite"), fullAccess)
		require.Equal(t, http.StatusCreated, status)

		for name, tc := range map[string]struct {
			body   string
			status int
		}{
			"taken name":     {keyRequest("app", "read"), http.StatusConflict},
			"invalid name":   {keyRequest("App_1", "read"), http.StatusBadRequest},
			"empty name":     {keyRequest("", "read"), http.StatusBadRequest},
			"full access":    {keyRequest("other", "full"), http.StatusBadRequest},
			"unknown field":  {`{"name":"other","access":"read","policy":"public"}`, http.StatusBadRequest},
			"not json":       {`name=other`, http.StatusBadRequest},
			"oversized body": {fmt.Sprintf(`{"name":"other","access":"read","pad":%q}`, strings.Repeat("x", 2048)), http.StatusBadRequest},
		} {
			status, _ := bt.do(http.MethodPost, keys, tc.body, fullAccess)
			require.Equal(t, tc.status, status, name)
		}

		for i := 1; i < bucket.MaxKeys; i++ {
			status, _ := bt.do(http.MethodPost, keys, keyRequest(fmt.Sprintf("key-%d", i), "read"), fullAccess)
			require.Equal(t, http.StatusCreated, status)
		}

		status, body := bt.do(http.MethodPost, keys, keyRequest("one-too-many", "read"), fullAccess)
		require.Equal(t, http.StatusConflict, status)
		require.Contains(t, body, bucket.ErrTooManyKeys.Error())
	})
}

func TestBucketScopes(t *testing.T) {
	runBucketTest(t, true, nil, func(bt *bucketTest) {
		info := apclient.BucketPath(bt.lid)
		keys := apclient.BucketKeysPath(bt.lid)

		status, _ := bt.do(http.MethodGet, info, "", ajwt.PermissionScopeBucket)
		require.Equal(t, http.StatusOK, status)
		status, _ = bt.do(http.MethodPost, keys, keyRequest("app", "read"), ajwt.PermissionScopeBucket)
		require.Equal(t, http.StatusUnauthorized, status, "the bucket scope creates keys")

		status, _ = bt.do(http.MethodPost, keys, keyRequest("app", "read"), ajwt.PermissionScopeBucketKeys)
		require.Equal(t, http.StatusCreated, status)
		status, _ = bt.do(http.MethodGet, info, "", ajwt.PermissionScopeBucketKeys)
		require.Equal(t, http.StatusUnauthorized, status, "the bucket-keys scope reads the bucket")

		status, _ = bt.do(http.MethodDelete, apclient.BucketKeyPath(bt.lid, "app"), "", ajwt.PermissionScopeStatus)
		require.Equal(t, http.StatusUnauthorized, status, "an unrelated scope revokes keys")
		status, _ = bt.do(http.MethodDelete, apclient.BucketKeyPath(bt.lid, "app"), "", ajwt.PermissionScopeBucketKeys)
		require.Equal(t, http.StatusNoContent, status)

		status, _ = bt.do(http.MethodGet, info, "")
		require.Equal(t, http.StatusUnauthorized, status, "an anonymous request reads the bucket")
	})
}

func TestBucketInactiveLeaseKeys(t *testing.T) {
	for name, vol := range map[string]func(*crd.Volume){
		"lease closed":       func(v *crd.Volume) { v.Status.Phase = crd.VolumePhaseRetained },
		"reclaiming":         func(v *crd.Volume) { v.Status.Phase = crd.VolumePhaseExporting },
		"adoption in flight": func(v *crd.Volume) { v.Spec.GroupID.DSeq += "7" },
		"lease not recorded": func(v *crd.Volume) { v.Spec.LeaseID.DSeq += "7" },
		"adopting":           func(v *crd.Volume) { v.Status.Phase = crd.VolumePhaseAdopting },
	} {
		t.Run(name, func(t *testing.T) {
			runBucketTest(t, true, vol, func(bt *bucketTest) {
				_, err := bt.backend.CreateKey(bt.ctx, bt.ref, "old", bucket.KeyRead)
				require.NoError(t, err)

				status, _ := bt.do(http.MethodPost, apclient.BucketKeysPath(bt.lid), keyRequest("app", "read"), fullAccess)
				require.Equal(t, http.StatusConflict, status)

				status, _ = bt.do(http.MethodGet, apclient.BucketPath(bt.lid), "", fullAccess)
				require.Equal(t, http.StatusOK, status)

				// revoking stays possible whatever the lease's state
				status, _ = bt.do(http.MethodDelete, apclient.BucketKeyPath(bt.lid, "old"), "", fullAccess)
				require.Equal(t, http.StatusNoContent, status)
			})
		})
	}
}

func TestBucketKeysWaitForAMoveToLand(t *testing.T) {
	importing := func(v *crd.Volume) {
		v.Status.Phase = crd.VolumePhaseImporting
		v.Spec.ImportFrom = "akash1previousprovider"
	}

	runBucketTest(t, true, importing, func(bt *bucketTest) {
		status, body := bt.do(http.MethodPost, apclient.BucketKeysPath(bt.lid), keyRequest("app", "read"), fullAccess)
		require.Equal(t, http.StatusConflict, status)
		require.Contains(t, body, "copied in")

		status, _ = bt.do(http.MethodGet, apclient.BucketPath(bt.lid), "", fullAccess)
		require.Equal(t, http.StatusOK, status)
	})
}

func TestBucketNotFound(t *testing.T) {
	for name, vol := range map[string]func(*crd.Volume){
		"mounted volume": func(v *crd.Volume) { v.Spec.Access = "" },
		"another owner":  func(v *crd.Volume) { v.Spec.Owner = "akash1someoneelse" },
		"being deleted":  func(v *crd.Volume) { v.Status.Phase = crd.VolumePhaseReleasing },
		"no such lease":  nil,
	} {
		t.Run(name, func(t *testing.T) {
			runBucketTest(t, true, vol, func(bt *bucketTest) {
				lid := bt.lid
				if vol == nil {
					lid.OSeq++
				}

				for _, req := range []struct{ method, path, body string }{
					{http.MethodGet, apclient.BucketPath(lid), ""},
					{http.MethodPost, apclient.BucketKeysPath(lid), keyRequest("app", "read")},
					{http.MethodDelete, apclient.BucketKeyPath(lid, "app"), ""},
				} {
					status, _ := bt.do(req.method, req.path, req.body, fullAccess)
					require.Equal(t, http.StatusNotFound, status, req.method)
				}
			})
		})
	}
}

func TestBucketBeingProvisioned(t *testing.T) {
	runBucketTest(t, true, func(v *crd.Volume) { v.Status.Bucket = ""; v.Status.Phase = crd.VolumePhasePending }, func(bt *bucketTest) {
		status, _ := bt.do(http.MethodGet, apclient.BucketPath(bt.lid), "", fullAccess)
		require.Equal(t, http.StatusServiceUnavailable, status)

		status, _ = bt.do(http.MethodPost, apclient.BucketKeysPath(bt.lid), keyRequest("app", "read"), fullAccess)
		require.Equal(t, http.StatusConflict, status)
	})
}

func TestBucketBackendErrorsStayInTheLog(t *testing.T) {
	runBucketTest(t, true, nil, func(bt *bucketTest) {
		bt.backend.Fail("Usage", errors.New("rgw at 10.0.0.7 answered 500"))
		bt.backend.Fail("CreateKey", errors.New("rgw at 10.0.0.7 answered 500"))

		status, body := bt.do(http.MethodGet, apclient.BucketPath(bt.lid), "", fullAccess)
		require.Equal(t, http.StatusInternalServerError, status)
		require.NotContains(t, body, "10.0.0.7")

		status, body = bt.do(http.MethodPost, apclient.BucketKeysPath(bt.lid), keyRequest("app", "read"), fullAccess)
		require.Equal(t, http.StatusInternalServerError, status)
		require.NotContains(t, body, "10.0.0.7")
	})
}

func TestBucketRoutesNeedBuckets(t *testing.T) {
	runBucketTest(t, false, nil, func(bt *bucketTest) {
		status, _ := bt.do(http.MethodGet, apclient.BucketPath(bt.lid), "", fullAccess)
		require.Equal(t, http.StatusNotFound, status)

		status, _ = bt.do(http.MethodPost, apclient.BucketKeysPath(bt.lid), keyRequest("app", "read"), fullAccess)
		require.NotEqual(t, http.StatusCreated, status)
	})
}
