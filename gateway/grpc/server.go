package grpc

import (
	"context"
	"crypto/x509"
	"fmt"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"pkg.akt.dev/go/util/ctxlog"

	"pkg.akt.dev/go/grpc/gogoreflection"
	leasev1 "pkg.akt.dev/go/provider/lease/v1"
	providerv1 "pkg.akt.dev/go/provider/v1"
	ajwt "pkg.akt.dev/go/util/jwt"
	vtv1 "pkg.akt.dev/go/volume/v1"

	"github.com/akash-network/provider"
	gwutils "github.com/akash-network/provider/gateway/utils"
	"github.com/akash-network/provider/tools/fromctx"
	ptypes "github.com/akash-network/provider/types"
)

type ContextKey string

const (
	ContextKeyClaims = ContextKey("owner")
)

type grpcProviderV1 struct {
	ctx    context.Context
	client provider.StatusClient
}

func (gm *grpcProviderV1) BidScreening(ctx context.Context, request *providerv1.BidScreeningRequest) (*providerv1.BidScreeningResponse, error) {
	return nil, status.Error(codes.Unimplemented, "BidScreening not implemented")
}

var _ providerv1.ProviderRPCServer = (*grpcProviderV1)(nil)

func ContextWithClaims(ctx context.Context, claims *ajwt.Claims) context.Context {
	return context.WithValue(ctx, ContextKeyClaims, claims)
}

func ClaimsFromCtx(ctx context.Context) *ajwt.Claims {
	val := ctx.Value(ContextKeyClaims)
	if val == nil {
		return &ajwt.Claims{}
	}

	return val.(*ajwt.Claims)
}

// ServerOption extends the gateway gRPC server with optional services.
type ServerOption func(*serverOptions)

type serverOptions struct {
	volumeTransfer vtv1.VolumeTransferServer
}

// WithVolumeTransfer registers the AEP-87 VolumeTransfer data plane on
// the gateway (mTLS with x/cert provider identities; the service derives
// authorization from chain state). Nil is ignored - providers not in the
// volume market serve no transfer endpoint.
func WithVolumeTransfer(srv vtv1.VolumeTransferServer) ServerOption {
	return func(opts *serverOptions) {
		opts.volumeTransfer = srv
	}
}

func NewServer(ctx context.Context, endpoint string, cquery gwutils.CertGetter, client provider.Client, opts ...ServerOption) error {
	options := &serverOptions{}
	for _, opt := range opts {
		opt(options)
	}

	tlsCfg, err := gwutils.NewServerTLSConfig(ctx, cquery, endpoint)
	if err != nil {
		return err
	}

	group, err := fromctx.ErrGroupFromCtx(ctx)
	if err != nil {
		return err
	}

	log := ctxlog.LogcFromCtx(ctx).With("module", "gateway-grpc")

	grpcSrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             30 * time.Second,
		PermitWithoutStream: false,
	}), grpc.ChainUnaryInterceptor(authInterceptor(ctx)))

	pRPC := &grpcProviderV1{
		ctx:    ctx,
		client: client,
	}

	providerv1.RegisterProviderRPCServer(grpcSrv, pRPC)

	leaseRPC := &grpcLeaseV1{
		cclient: client.Cluster(),
	}
	leasev1.RegisterLeaseRPCServer(grpcSrv, leaseRPC)

	if options.volumeTransfer != nil {
		vtv1.RegisterVolumeTransferServer(grpcSrv, options.volumeTransfer)
	}

	gogoreflection.Register(grpcSrv)

	group.Go(func() error {
		grpcLis, err := net.Listen("tcp", endpoint)
		if err != nil {
			return err
		}

		log.Info(fmt.Sprintf("grpc listening on \"%s\"", endpoint))

		return grpcSrv.Serve(grpcLis)
	})

	group.Go(func() error {
		<-ctx.Done()

		grpcSrv.GracefulStop()

		return ctx.Err()
	})

	return nil
}

// authInterceptor returns a gRPC unary interceptor that validates JWT/mTLS credentials.
// The serverCtx carries application-level values (e.g. AccountQuerier) that the
// per-request gRPC context does not inherit, so we merge them here.
func authInterceptor(serverCtx context.Context) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		log := ctxlog.LogcFromCtx(serverCtx).With("module", "gateway-grpc")

		var tokString string
		var peerCerts []*x509.Certificate

		if p, ok := peer.FromContext(ctx); ok {
			if mtls, ok := p.AuthInfo.(credentials.TLSInfo); ok {
				peerCerts = mtls.State.PeerCertificates
			}
		}

		if md, ok := metadata.FromIncomingContext(ctx); ok {
			tokens := md["authorization"]
			if len(tokens) > 0 {
				tokString = tokens[0]
				// Strip "Bearer " prefix to match REST auth extraction.
				if parts := strings.Fields(tokString); len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
					tokString = parts[1]
				}
			}
		}

		log.Debug("grpc auth", "method", info.FullMethod, "has_token", tokString != "", "peer_certs", len(peerCerts))

		// Use serverCtx for AuthProcess so it can find AccountQuerier and other
		// application-level values that are not present in the per-request gRPC ctx.
		claims, err := gwutils.AuthProcess(serverCtx, peerCerts, tokString)
		if err != nil {
			log.Error("grpc auth failed", "method", info.FullMethod, "err", err)
			return nil, err
		}

		log.Debug("grpc auth ok", "method", info.FullMethod, "issuer", claims.Issuer, "access", claims.Leases.Access)

		ctx = ContextWithClaims(ctx, claims)

		return handler(ctx, req)
	}
}

func (gm *grpcProviderV1) GetStatus(ctx context.Context, _ *emptypb.Empty) (*providerv1.Status, error) {
	return gm.client.StatusV1(ctx)
}

func (gm *grpcProviderV1) StreamStatus(_ *emptypb.Empty, stream providerv1.ProviderRPC_StreamStatusServer) error {
	bus, err := fromctx.PubSubFromCtx(gm.ctx)
	if err != nil {
		return err
	}

	events := bus.Sub(ptypes.PubSubTopicProviderStatus)

	for {
		select {
		case <-gm.ctx.Done():
			return gm.ctx.Err()
		case <-stream.Context().Done():
			return stream.Context().Err()
		case evt := <-events:
			val := evt.(providerv1.Status)
			if err := stream.Send(&val); err != nil {
				return err
			}
		}
	}
}
