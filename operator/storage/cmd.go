package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	sdkclient "github.com/cosmos/cosmos-sdk/client"
	sdk "github.com/cosmos/cosmos-sdk/types"

	cflags "pkg.akt.dev/go/cli/flags"
	aclient "pkg.akt.dev/go/node/client/discovery"
	"pkg.akt.dev/go/sdkutil"

	providerflags "github.com/akash-network/provider/cmd/provider-services/cmd/flags"
	"github.com/akash-network/provider/operator/common"
	"github.com/akash-network/provider/tools/fromctx"
)

func Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "storage",
		Short:        "kubernetes operator managing AEP-87 first-class volumes",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns := viper.GetString(providerflags.FlagK8sManifestNS)
			volNS := viper.GetString(FlagVolumesNS)
			resync := viper.GetDuration(FlagResyncInterval)
			nodeHint := viper.GetBool(FlagProvisionNodeHint)

			logger := common.OpenLogger().With("operator", "storage")

			ctx := cmd.Context()

			opcfg := common.GetOperatorConfigFromViper()
			if _, err := sdk.AccAddressFromBech32(opcfg.ProviderAddress); err != nil {
				return fmt.Errorf("%w: provider address must be valid bech32", err)
			}

			chain, err := chainClientFromFlags(ctx, opcfg.ProviderAddress)
			if err != nil {
				// the reconciler is chain-optional: provisioning, attach,
				// detach and GC run on CRD state alone; adoptions freeze
				// until chain access comes back
				logger.Error("chain access unavailable; adoption verification and restart re-list disabled", "err", err)
				chain = nil
			}

			restPort, err := common.DetectPort(ctx, cmd.Flags(), common.FlagRESTPort, "operator-storage", "rest")
			if err != nil {
				return err
			}

			listenAddress := viper.GetString(common.FlagRESTAddress)
			restAddr := fmt.Sprintf("%s:%d", listenAddress, restPort)

			group := fromctx.MustErrGroupFromCtx(ctx)

			op, err := newStorageOperator(ctx, logger, ns, volNS, opcfg, chain, resync, nodeHint)
			if err != nil {
				return err
			}

			router := op.server.GetRouter()

			// fixme ovrclk/engineering#609
			// nolint: gosec
			srv := http.Server{Addr: restAddr, Handler: router}

			group.Go(func() error {
				logger.Info("HTTP listening", "address", restAddr)
				return srv.ListenAndServe()
			})

			group.Go(func() error {
				<-ctx.Done()
				_ = srv.Close()

				return ctx.Err()
			})

			group.Go(func() error {
				return op.run(ctx)
			})

			fromctx.MustStartupChFromCtx(ctx) <- struct{}{}
			err = group.Wait()

			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
				return err
			}

			return nil
		},
	}

	common.AddOperatorFlags(cmd)
	common.AddProviderFlag(cmd)

	cmd.Flags().String(FlagVolumesNS, defaultVolumesNS, "namespace parked volume holder PVCs live in")
	if err := viper.BindPFlag(FlagVolumesNS, cmd.Flags().Lookup(FlagVolumesNS)); err != nil {
		panic(err)
	}

	cmd.Flags().Duration(FlagResyncInterval, 5*time.Minute, "full reconcile sweep interval")
	if err := viper.BindPFlag(FlagResyncInterval, cmd.Flags().Lookup(FlagResyncInterval)); err != nil {
		panic(err)
	}

	cmd.Flags().Bool(FlagProvisionNodeHint, false, "annotate provisioning claims with the selected node (required for node-constrained provisioners like local-path; leave off for Ceph RBD)")
	if err := viper.BindPFlag(FlagProvisionNodeHint, cmd.Flags().Lookup(FlagProvisionNodeHint)); err != nil {
		panic(err)
	}

	// the chain node powering event subscription and adoption verification.
	// Registered locally (not only on the provider-services root) so the
	// operator command works standalone - the e2e harness runs it without
	// the root command's persistent flags.
	cmd.Flags().String(cflags.FlagNode, "http://localhost:26657", "chain node RPC endpoint; unreachable runs chain-degraded (no adoptions, no event stream)")
	if err := viper.BindPFlag(cflags.FlagNode, cmd.Flags().Lookup(cflags.FlagNode)); err != nil {
		panic(err)
	}

	return cmd
}

// chainClientFromFlags builds the operator's keyless chain client off the
// --node flag: a comet RPC connection for the event stream and a discovered
// akash query client for state reads. The operator subcommand does not
// inherit the CLI's initialized client context (the operator parent PreRun
// replaces it), so the pieces are assembled here.
func chainClientFromFlags(ctx context.Context, provider string) (ChainClient, error) {
	nodeURI := viper.GetString(cflags.FlagNode)
	if nodeURI == "" {
		return nil, fmt.Errorf("%w: no chain node configured", ErrVolumeOperator)
	}

	rpcClient, err := sdkclient.NewClientFromNode(nodeURI)
	if err != nil {
		return nil, err
	}

	if err := rpcClient.Start(); err != nil {
		return nil, err
	}

	encodingConfig := sdkutil.MakeEncodingConfig()

	cctx := sdkclient.Context{}.
		WithCodec(encodingConfig.Codec).
		WithInterfaceRegistry(encodingConfig.InterfaceRegistry).
		WithNodeURI(nodeURI).
		WithClient(rpcClient)

	qc, err := aclient.DiscoverQueryClient(ctx, cctx)
	if err != nil {
		return nil, err
	}

	return newChainClient(qc, rpcClient, provider), nil
}
