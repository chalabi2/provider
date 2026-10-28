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

func TestTransferEndpointsNameWhereEachProviderServes(t *testing.T) {
	endpoints, err := parseTransferEndpoints([]string{"akash1aaa=localhost:38453", "akash1bbb=10.0.0.2:48453"})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"akash1aaa": "localhost:38453", "akash1bbb": "10.0.0.2:48453"}, endpoints)
}

func TestTransferEndpointsRefuseWhatNamesNoEndpoint(t *testing.T) {
	for _, pair := range []string{"akash1aaa", "=localhost:38453", "akash1aaa=localhost", "akash1aaa=:", "akash1aaa=:38453", "akash1aaa=localhost:"} {
		_, err := parseTransferEndpoints([]string{pair})
		require.ErrorIs(t, err, errInvalidConfig, pair)
	}
}
