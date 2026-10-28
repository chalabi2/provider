package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransferEndpointIsTheHostURIHostAtTheTransferPort(t *testing.T) {
	for _, tc := range []struct {
		hostURI  string
		endpoint string
	}{
		{hostURI: "https://provider.example.com:8443", endpoint: "provider.example.com:8444"},
		{hostURI: "https://provider.example.com", endpoint: "provider.example.com:8444"},
		{hostURI: "https://[2001:db8::1]:8443", endpoint: "[2001:db8::1]:8444"},
	} {
		endpoint, err := transferEndpoint(tc.hostURI, 8444)
		require.NoError(t, err)
		require.Equal(t, tc.endpoint, endpoint)
	}
}

func TestTransferEndpointRefusesAHostURIWithoutAHost(t *testing.T) {
	for _, hostURI := range []string{"", "provider.example.com:8443", "https://:8443", "://bad"} {
		_, err := transferEndpoint(hostURI, 8444)
		require.ErrorIs(t, err, errInvalidConfig, hostURI)
	}
}
