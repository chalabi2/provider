package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync"

	"cosmossdk.io/log"
	"github.com/gorilla/mux"

	mtypes "pkg.akt.dev/go/node/market/v1"
	apclient "pkg.akt.dev/go/provider/client"

	"github.com/akash-network/provider/bucket"
	kubeclienterrors "github.com/akash-network/provider/cluster/kube/errors"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

// maxBucketKeyRequest bounds a key request body; it holds a name and an
// access level.
const maxBucketKeyRequest = 1 << 10

type leaseVolumes interface {
	LeaseVolume(ctx context.Context, lid mtypes.LeaseID) (*crd.Volume, error)
}

// bucketGateway serves a bucket lease's owner the bucket's endpoint, usage
// and access keys. The lease comes from the request path and the
// authenticated owner, so an owner reaches only its own buckets.
type bucketGateway struct {
	log     log.Logger
	backend bucket.Backend
	config  bucket.Config
	volumes leaseVolumes

	// keys serializes key changes, so concurrent requests cannot take one
	// name twice or pass the key limit
	keys sync.Mutex
}

func newBucketGateway(log log.Logger, backend bucket.Backend, config bucket.Config, volumes leaseVolumes) *bucketGateway {
	return &bucketGateway{
		log:     log.With("module", "gateway-bucket"),
		backend: backend,
		config:  config,
		volumes: volumes,
	}
}

func (g *bucketGateway) info(w http.ResponseWriter, r *http.Request) {
	lid := requestLeaseID(r)

	vol, ref, ok := g.bucketOf(w, r, lid)
	if !ok {
		return
	}

	if vol.Status.Bucket == "" {
		http.Error(w, "the bucket is being provisioned", http.StatusServiceUnavailable)
		return
	}

	usage, err := g.backend.Usage(r.Context(), ref)
	if err != nil {
		g.writeError(w, lid, err)
		return
	}

	keys, err := g.backend.ListKeys(r.Context(), ref)
	if err != nil {
		g.writeError(w, lid, err)
		return
	}

	size, _ := strconv.ParseUint(vol.Spec.Size, 10, 64)

	info := apclient.BucketInfo{
		Bucket:    ref.Bucket,
		Endpoint:  g.config.Endpoint,
		Region:    g.config.Region,
		SizeBytes: size,
		UsedBytes: uint64(max(usage.UsedBytes, 0)),
		Keys:      make([]apclient.BucketKeyInfo, 0, len(keys)),
	}
	for _, key := range keys {
		info.Keys = append(info.Keys, bucketKeyInfo(key))
	}

	writeJSONStatus(w, http.StatusOK, info)
}

func (g *bucketGateway) createKey(w http.ResponseWriter, r *http.Request) {
	lid := requestLeaseID(r)

	vol, ref, ok := g.bucketOf(w, r, lid)
	if !ok {
		return
	}

	if !bucketActive(vol) {
		http.Error(w, "the bucket's lease is not active", http.StatusConflict)
		return
	}

	var req apclient.BucketKeyRequest

	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBucketKeyRequest))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid key request", http.StatusBadRequest)
		return
	}

	g.keys.Lock()
	key, err := g.backend.CreateKey(r.Context(), ref, req.Name, bucket.KeyAccess(req.Access))
	g.keys.Unlock()

	if err != nil {
		g.writeError(w, lid, err)
		return
	}

	g.log.Info("bucket key created", "lid", lid, "volume", vol.Name, "key", key.Name, "access", key.Access)

	// the only response that ever holds the secret
	w.Header().Set("Cache-Control", "no-store")
	writeJSONStatus(w, http.StatusCreated, apclient.BucketKey{
		BucketKeyInfo:   bucketKeyInfo(key.KeyInfo),
		SecretAccessKey: key.SecretAccessKey,
	})
}

func (g *bucketGateway) revokeKey(w http.ResponseWriter, r *http.Request) {
	lid := requestLeaseID(r)

	vol, ref, ok := g.bucketOf(w, r, lid)
	if !ok {
		return
	}

	if vol.Status.Bucket == "" {
		g.writeError(w, lid, bucket.ErrKeyNotFound)
		return
	}

	name := mux.Vars(r)["keyName"]

	// revoking is always allowed, whatever the lease's state
	g.keys.Lock()
	err := g.backend.RevokeKey(r.Context(), ref, name)
	g.keys.Unlock()

	if err != nil {
		g.writeError(w, lid, err)
		return
	}

	g.log.Info("bucket key revoked", "lid", lid, "volume", vol.Name, "key", name)

	w.WriteHeader(http.StatusNoContent)
}

// bucketOf finds the bucket whose own lease is lid. A lease of a mounted
// volume, of a compute deployment or of a bucket being deleted has none.
func (g *bucketGateway) bucketOf(w http.ResponseWriter, r *http.Request, lid mtypes.LeaseID) (*crd.Volume, bucket.Ref, bool) {
	vol, err := g.volumes.LeaseVolume(r.Context(), lid)
	if err != nil {
		if errors.Is(err, kubeclienterrors.ErrVolumeNotFound) {
			http.Error(w, "no bucket for this lease", http.StatusNotFound)
			return nil, bucket.Ref{}, false
		}

		g.log.Error("reading bucket volume", "lid", lid, "err", err)
		http.Error(w, "unable to read the bucket", http.StatusInternalServerError)

		return nil, bucket.Ref{}, false
	}

	if !vol.IsBucket() || vol.Spec.Owner != lid.Owner || vol.Status.Phase == crd.VolumePhaseReleasing {
		http.Error(w, "no bucket for this lease", http.StatusNotFound)
		return nil, bucket.Ref{}, false
	}

	return vol, bucket.RefFor(vol.Name, vol.Spec.Owner, vol.Spec.VID), true
}

// bucketActive reports whether keys may be issued: the bucket is
// provisioned under its deployment's lease and no adoption is in flight.
func bucketActive(vol *crd.Volume) bool {
	return vol.Status.Phase == crd.VolumePhaseProvisioned &&
		vol.Status.Bucket != "" &&
		vol.Status.BucketDSeq == vol.Spec.GroupID.DSeq &&
		vol.Spec.LeaseID.DSeq == vol.Spec.GroupID.DSeq
}

func (g *bucketGateway) writeError(w http.ResponseWriter, lid mtypes.LeaseID, err error) {
	for _, known := range []struct {
		err    error
		status int
	}{
		{bucket.ErrInvalidKeyName, http.StatusBadRequest},
		{bucket.ErrInvalidAccess, http.StatusBadRequest},
		{bucket.ErrKeyExists, http.StatusConflict},
		{bucket.ErrTooManyKeys, http.StatusConflict},
		{bucket.ErrKeyNotFound, http.StatusNotFound},
		{bucket.ErrNotFound, http.StatusNotFound},
	} {
		if errors.Is(err, known.err) {
			http.Error(w, known.err.Error(), known.status)
			return
		}
	}

	// backend details stay in the provider's log
	g.log.Error("bucket backend", "lid", lid, "err", err)
	http.Error(w, "bucket backend error", http.StatusInternalServerError)
}

func bucketKeyInfo(key bucket.KeyInfo) apclient.BucketKeyInfo {
	return apclient.BucketKeyInfo{
		Name:        key.Name,
		Access:      apclient.BucketKeyAccess(key.Access),
		AccessKeyID: key.AccessKeyID,
	}
}

func writeJSONStatus(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", contentTypeJSON)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
