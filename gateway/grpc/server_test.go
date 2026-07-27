package grpc

import (
	"context"
	"testing"

	"cosmossdk.io/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"pkg.akt.dev/go/util/ctxlog"

	"github.com/akash-network/provider/utils/httperror"
)

func TestAuthInterceptorRejectsMultipleAuthorizationHeaders(t *testing.T) {
	serverCtx := ctxlog.WithLogger(context.Background(), log.NewNopLogger())
	interceptor := authInterceptor(serverCtx)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(
			"authorization", "Bearer token-a",
			"authorization", "Bearer token-b",
		),
	)

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) {
		t.Fatal("handler called")
		return nil, nil
	})

	require.ErrorIs(t, err, httperror.ErrInvalidAuthHeader)
}
