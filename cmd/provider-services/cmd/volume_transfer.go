package cmd

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"github.com/spf13/viper"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sdk "github.com/cosmos/cosmos-sdk/types"

	"cosmossdk.io/log"

	aclient "pkg.akt.dev/go/node/client/v1beta3"
	mv1 "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v1beta5"
	ptypes "pkg.akt.dev/go/node/provider/v1beta4"
	vtv1 "pkg.akt.dev/go/volume/v1"

	providerflags "github.com/akash-network/provider/cmd/provider-services/cmd/flags"
	gwutils "github.com/akash-network/provider/gateway/utils"
	"github.com/akash-network/provider/operator/inventory"
	"github.com/akash-network/provider/operator/storage/replication"
	"github.com/akash-network/provider/tools/fromctx"
)

// volumeTransfer is the AEP-87 volume data plane of a provider: the
// VolumeTransfer server other providers copy volumes out of, and the
// migrations that copy in the volumes moved here.
type volumeTransfer struct {
	server     vtv1.VolumeTransferServer
	migrations *replication.Migrations
}

// buildVolumeTransfer assembles the volume data plane (IMPLEMENTATION.md
// §2.7, §5.5): authorization derived from chain state on both ends, exports
// and imports through the stream-export driver - rbd against
// rook-ceph-tools for RBD-backed PVs, a directory-tree copy for
// local-path. Both directions authenticate with this provider's x/cert
// identity.
//
// The rbd-mirror driver is an opt-in stub: selecting it fails loudly at
// startup (replication.ErrNotImplemented) rather than silently mirroring
// nothing.
func buildVolumeTransfer(ctx context.Context, cl aclient.Client, certs gwutils.CertGetter, self string, logger log.Logger) (*volumeTransfer, error) {
	switch driver := viper.GetString(FlagVolumeReplicationDriver); driver {
	case replication.DriverStreamExport:
	case replication.DriverRBDMirror:
		if _, err := replication.NewRBDMirrorDriver(replication.RBDMirrorConfig{Enabled: true}); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: unknown volume replication driver %q", errInvalidConfig, driver)
	}

	kc, err := fromctx.KubeClientFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	ac, err := fromctx.AkashClientFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	kubecfg, err := fromctx.KubeConfigFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	ns := viper.GetString(providerflags.FlagK8sManifestNS)
	rookNS := viper.GetString(FlagVolumeReplicationRookNS)

	baseDir := viper.GetString(FlagVolumeReplicationDir)
	if baseDir == "" {
		baseDir = filepath.Join(os.TempDir(), "akash-volume-replication")
	}

	locator := func(ctx context.Context, namespace, appLabel string) (string, error) {
		pods, err := kc.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app=" + appLabel,
		})
		if err != nil {
			return "", err
		}

		if len(pods.Items) == 0 {
			return "", fmt.Errorf("no pods found with selector app=%s", appLabel)
		}

		return pods.Items[0].Name, nil
	}

	files := replication.NewFileDriver(baseDir)
	driver := replication.NewAutoDriver(
		replication.NewRBDDriver(inventory.NewRemotePodCommandExecutor(kubecfg, kc), locator, rookNS),
		files,
	)

	chain := replication.NewChainState(cl.Query())
	volumes := replication.NewCRDVolumeSource(ac, kc, ns)
	port := viper.GetInt(FlagVolumeTransferPort)

	dial := func(ctx context.Context, source string) (vtv1.VolumeTransferClient, io.Closer, error) {
		resp, err := cl.Query().Provider().Provider(ctx, &ptypes.QueryProviderRequest{Owner: source})
		if err != nil {
			return nil, nil, err
		}

		endpoint, err := transferEndpoint(resp.Provider.HostURI, port)
		if err != nil {
			return nil, nil, err
		}

		own, err := certs.GetMTLS(ctx)
		if err != nil {
			return nil, nil, err
		}

		return replication.Dial(ctx, endpoint, source, own, certs)
	}

	evict := func(ctx context.Context, lid mv1.LeaseID) error {
		msg := &mvbeta.MsgLeaseStartReclaim{
			ID:     lid,
			Reason: mv1.LeaseClosedReasonVolumeEvict,
		}

		_, err := cl.Tx().BroadcastMsgs(ctx, []sdk.Msg{msg}, aclient.WithResultCodeAsError())

		return err
	}

	return &volumeTransfer{
		server: replication.NewServer(driver, chain, volumes, self, logger),
		migrations: replication.NewMigrations(replication.MigrationsConfig{
			Volumes:   ac,
			Namespace: ns,
			Self:      self,
			Chain:     chain,
			Source:    volumes,
			Driver:    driver,
			Dial:      dial,
			Evict:     evict,
			Sweep:     files.RemoveStale,
			SpoolDir:  filepath.Join(baseDir, "spool"),
			Interval:  viper.GetDuration(FlagVolumeMigrationInterval),
		}, logger),
	}, nil
}

// transferEndpoint is where a provider serves volume transfers: the host of
// its on-chain host URI, at the transfer port.
func transferEndpoint(hostURI string, port int) (string, error) {
	u, err := url.Parse(hostURI)
	if err != nil {
		return "", fmt.Errorf("%w: provider host URI %q: %s", errInvalidConfig, hostURI, err.Error())
	}

	if u.Hostname() == "" {
		return "", fmt.Errorf("%w: provider host URI %q names no host", errInvalidConfig, hostURI)
	}

	return net.JoinHostPort(u.Hostname(), strconv.Itoa(port)), nil
}
