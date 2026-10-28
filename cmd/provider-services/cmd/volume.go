package cmd

import (
	"github.com/spf13/cobra"

	"pkg.akt.dev/go/cli"
	apclient "pkg.akt.dev/go/provider/client"
)

// volumeCmd reads the volume of a volume lease on its provider.
func volumeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "volume",
		Short:        "read the volume of a volume lease",
		SilenceUsage: true,
	}

	cmd.AddCommand(volumeStatusCmd())

	return cmd
}

func volumeStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "status",
		Short:        "show the volume's phase on its provider, and where a moved volume is still being copied from",
		SilenceUsage: true,
		Args:         cobra.ExactArgs(0),
		PreRunE:      ProviderPersistentPreRunE,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withLeaseClient(cmd, func(gclient apclient.Client, cctx leaseCmdContext) error {
				status, err := gclient.LeaseVolume(cmd.Context(), cctx.lid)
				if err != nil {
					return showErrorToUser(err)
				}

				return cli.PrintJSON(cctx.client, status)
			})
		},
	}

	addLeaseClientFlags(cmd)

	return cmd
}
