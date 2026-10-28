package rest

import (
	"context"
	"net"
	"net/http"
	"time"

	gcontext "github.com/gorilla/context"

	"cosmossdk.io/log"
	sdk "github.com/cosmos/cosmos-sdk/types"

	"github.com/akash-network/provider"
	"github.com/akash-network/provider/bucket"
	clfromctx "github.com/akash-network/provider/cluster/types/v1beta3/fromctx"
	gwutils "github.com/akash-network/provider/gateway/utils"
	"github.com/akash-network/provider/tools/fromctx"
)

// ServerOption configures optional gateway features.
type ServerOption func(*serverOptions)

type serverOptions struct {
	bucketBackend bucket.Backend
	bucketConfig  bucket.Config
}

// WithBuckets serves the bucket endpoints of bucket leases from backend.
func WithBuckets(backend bucket.Backend, config bucket.Config) ServerOption {
	return func(o *serverOptions) {
		o.bucketBackend = backend
		o.bucketConfig = config
	}
}

func NewServer(
	ctx context.Context,
	log log.Logger,
	pclient provider.Client,
	cquery gwutils.CertGetter,
	sni string,
	address string,
	pid sdk.Address,
	clusterConfig map[interface{}]interface{},
	opts ...ServerOption,
) (*http.Server, error) {
	log = log.With("module", "gateway-rest")

	var options serverOptions
	for _, opt := range opts {
		opt(&options)
	}

	var buckets *bucketGateway
	if options.bucketBackend != nil {
		buckets = newBucketGateway(log, options.bucketBackend, options.bucketConfig, pclient.Cluster())
	}

	restMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gcontext.Set(r, fromctx.CtxKeyKubeConfig, fromctx.MustKubeConfigFromCtx(ctx))
			gcontext.Set(r, fromctx.CtxKeyKubeClientSet, fromctx.MustKubeClientFromCtx(ctx))
			gcontext.Set(r, fromctx.CtxKeyAkashClientSet, fromctx.MustAkashClientFromCtx(ctx))

			gcontext.Set(r, clfromctx.CtxKeyClientInventory, clfromctx.ClientInventoryFromContext(ctx))
			gcontext.Set(r, clfromctx.CtxKeyClientHostname, clfromctx.ClientHostnameFromContext(ctx))

			if ip := clfromctx.ClientIPFromContext(ctx); ip != nil {
				gcontext.Set(r, clfromctx.CtxKeyClientIP, ip)
			}

			next.ServeHTTP(w, r)
		})
	}

	tlsCfg, err := gwutils.NewServerTLSConfig(ctx, cquery, sni)
	if err != nil {
		return nil, err
	}

	srv := &http.Server{
		Addr:              address,
		ReadHeaderTimeout: 3 * time.Second,
		Handler:           newRouter(log, pid, pclient, clusterConfig, buckets, restMiddleware),
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
		TLSConfig: tlsCfg,
	}

	return srv, nil
}
