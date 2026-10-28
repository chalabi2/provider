package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	cflags "pkg.akt.dev/go/cli/flags"
)

func TestBucketCommands(t *testing.T) {
	root := bucketCmd()

	for _, path := range [][]string{{"info"}, {"key", "create"}, {"key", "revoke"}} {
		cmd, _, err := root.Find(path)
		require.NoError(t, err, path)
		require.Equal(t, path[len(path)-1], cmd.Name())

		provider := cmd.Flags().Lookup(cflags.FlagProvider)
		require.NotNil(t, provider, path)
		require.Equal(t, []string{"true"}, provider.Annotations["cobra_annotation_bash_completion_one_required_flag"], path)
		require.NotNil(t, cmd.Flags().Lookup(cflags.FlagDSeq), path)
	}

	create, _, err := root.Find([]string{"key", "create"})
	require.NoError(t, err)
	require.Equal(t, "readwrite", create.Flags().Lookup(flagBucketKeyAccess).DefValue)
	require.Error(t, create.Args(create, nil), "a key without a name")
}
