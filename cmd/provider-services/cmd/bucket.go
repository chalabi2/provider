package cmd

import (
	"errors"
	"fmt"

	sdkclient "github.com/cosmos/cosmos-sdk/client"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/spf13/cobra"

	"pkg.akt.dev/go/cli"
	cflags "pkg.akt.dev/go/cli/flags"
	mtypes "pkg.akt.dev/go/node/market/v1"
	apclient "pkg.akt.dev/go/provider/client"
)

const flagBucketKeyAccess = "access"

type bucketCmdContext struct {
	client sdkclient.Context
	lid    mtypes.LeaseID
}

// bucketCmd manages the bucket of a bucket lease: where it is, what it
// holds, and its access keys.
func bucketCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "bucket",
		Short:        "manage the S3 bucket of a bucket lease",
		SilenceUsage: true,
	}

	key := &cobra.Command{
		Use:          "key",
		Short:        "manage a bucket's access keys",
		SilenceUsage: true,
	}
	key.AddCommand(bucketKeyCreateCmd(), bucketKeyRevokeCmd())

	cmd.AddCommand(bucketInfoCmd(), key)

	return cmd
}

func bucketInfoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "info",
		Short:        "show the bucket's endpoint, usage and access keys",
		SilenceUsage: true,
		Args:         cobra.ExactArgs(0),
		PreRunE:      ProviderPersistentPreRunE,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withBucketClient(cmd, func(gclient apclient.Client, cctx bucketCmdContext) error {
				info, err := gclient.BucketInfo(cmd.Context(), cctx.lid)
				if err != nil {
					return showErrorToUser(err)
				}

				return cli.PrintJSON(cctx.client, info)
			})
		},
	}

	addBucketFlags(cmd)

	return cmd
}

func bucketKeyCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "create <name>",
		Short:        "create an access key; its secret is shown once",
		SilenceUsage: true,
		Args:         cobra.ExactArgs(1),
		PreRunE:      ProviderPersistentPreRunE,
		RunE: func(cmd *cobra.Command, args []string) error {
			access, err := cmd.Flags().GetString(flagBucketKeyAccess)
			if err != nil {
				return err
			}

			return withBucketClient(cmd, func(gclient apclient.Client, cctx bucketCmdContext) error {
				key, err := gclient.CreateBucketKey(cmd.Context(), cctx.lid, apclient.BucketKeyRequest{
					Name:   args[0],
					Access: apclient.BucketKeyAccess(access),
				})
				if err != nil {
					return showErrorToUser(err)
				}

				_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "Store the secret access key now: the provider never shows it again.")

				return cli.PrintJSON(cctx.client, key)
			})
		},
	}

	addBucketFlags(cmd)
	cmd.Flags().String(flagBucketKeyAccess, string(apclient.BucketKeyReadWrite), "what the key may do: readwrite or read")

	return cmd
}

func bucketKeyRevokeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "revoke <name>",
		Short:        "revoke an access key",
		SilenceUsage: true,
		Args:         cobra.ExactArgs(1),
		PreRunE:      ProviderPersistentPreRunE,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withBucketClient(cmd, func(gclient apclient.Client, cctx bucketCmdContext) error {
				if err := gclient.RevokeBucketKey(cmd.Context(), cctx.lid, args[0]); err != nil {
					return showErrorToUser(err)
				}

				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Revoked %s.\n", args[0])

				return nil
			})
		},
	}

	addBucketFlags(cmd)

	return cmd
}

func addBucketFlags(cmd *cobra.Command) {
	AddProviderOperationFlagsToCmd(cmd)
	addLeaseFlags(cmd)
	addAuthFlags(cmd)
}

func withBucketClient(cmd *cobra.Command, fn func(apclient.Client, bucketCmdContext) error) error {
	ctx := cmd.Context()

	cl, err := cli.ClientFromContext(ctx)
	if err != nil && !errors.Is(err, cli.ErrContextValueNotSet) {
		return err
	}

	cctx, err := cli.GetClientTxContext(cmd)
	if err != nil {
		return err
	}

	bid, err := cflags.BidIDFromFlags(cmd.Flags(), cflags.WithOwner(cctx.FromAddress))
	if err != nil {
		return err
	}

	paddr, err := sdk.AccAddressFromBech32(bid.Provider)
	if err != nil {
		return err
	}

	gclient, err := setupProviderClient(ctx, cctx, cmd.Flags(), queryClientOrNil(cl), paddr, true)
	if err != nil {
		return err
	}

	return fn(gclient, bucketCmdContext{client: cctx, lid: bid.LeaseID()})
}
