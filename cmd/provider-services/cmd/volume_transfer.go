package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"cosmossdk.io/log"

	aclient "pkg.akt.dev/go/node/client"
	vtv1 "pkg.akt.dev/go/volume/v1"

	providerflags "github.com/akash-network/provider/cmd/provider-services/cmd/flags"
	"github.com/akash-network/provider/operator/inventory"
	"github.com/akash-network/provider/operator/storage/replication"
	"github.com/akash-network/provider/tools/fromctx"
)

// buildVolumeTransferServer assembles the AEP-87 VolumeTransfer data
// plane served on the provider gateway (IMPLEMENTATION.md §2.7, §5.5):
// authorization derived from chain state on both ends, exports served by
// the stream-export driver - rbd against rook-ceph-tools for RBD-backed
// PVs, plain file copy for local-path.
//
// The rbd-mirror driver is an opt-in stub: selecting it fails loudly at
// startup (replication.ErrNotImplemented) rather than silently mirroring
// nothing.
func buildVolumeTransferServer(ctx context.Context, qc aclient.QueryClient, self string, logger log.Logger) (vtv1.VolumeTransferServer, error) {
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

	driver := replication.NewAutoDriver(
		replication.NewRBDDriver(inventory.NewRemotePodCommandExecutor(kubecfg, kc), locator, rookNS),
		replication.NewFileDriver(baseDir),
	)

	return replication.NewServer(
		driver,
		replication.NewChainState(qc),
		replication.NewCRDVolumeSource(ac, kc, ns),
		self,
		logger,
	), nil
}
