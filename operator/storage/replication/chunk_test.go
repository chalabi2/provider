package replication

import (
	"bytes"
	"context"
	"crypto/rand"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	vtv1 "pkg.akt.dev/go/volume/v1"
)

// payloadSource exports a fixed payload in default-size chunks, with none
// of the Server's authorization in the way.
type payloadSource struct {
	vtv1.UnimplementedVolumeTransferServer
	payload []byte
}

func (s *payloadSource) Export(_ *vtv1.ExportRequest, stream vtv1.VolumeTransfer_ExportServer) error {
	_, err := SendChunks(stream, bytes.NewReader(s.payload), 0, DefaultChunkSize)
	return err
}

// The destination dials with gRPC's default receive limit, so a full chunk
// framed with its offset and digest has to fit under it.
func TestDefaultChunksFitAClientsDefaultMessageLimit(t *testing.T) {
	payload := make([]byte, 2*DefaultChunkSize+1)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	vtv1.RegisterVolumeTransferServer(server, &payloadSource{payload: payload})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	stream, err := vtv1.NewVolumeTransferClient(conn).Export(context.Background(), &vtv1.ExportRequest{})
	require.NoError(t, err)

	spool, err := os.CreateTemp(t.TempDir(), "spool-*")
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	received, err := ReceiveChunks(stream, spool, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(len(payload)), received)

	got, err := os.ReadFile(spool.Name())
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload, got), "the spooled stream differs from the exported payload")
}
