package operator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperatorsCmdRegistration(t *testing.T) {
	cmd := OperatorsCmd()

	expected := map[string]bool{
		"inventory": false,
		"ip":        false,
		"hostname":  false,
		"storage":   false,
	}

	for _, sub := range cmd.Commands() {
		if _, exists := expected[sub.Use]; exists {
			expected[sub.Use] = true
		}
	}

	for name, found := range expected {
		require.True(t, found, "operator subcommand %q is not registered", name)
	}
}
