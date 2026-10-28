package bucket

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validConfig() Config {
	return Config{
		Backend:        BackendRGW,
		Classes:        []string{"beta2"},
		Endpoint:       "https://s3.provider.example.com",
		Region:         "default",
		AdminEndpoint:  "http://rook-ceph-rgw-akash-s3.rook-ceph.svc",
		AdminAccessKey: "admin-access",
		AdminSecretKey: "admin-secret",
	}
}

func TestConfigValidate(t *testing.T) {
	require.NoError(t, Config{}.Validate(), "disabled buckets need no configuration")
	require.NoError(t, validConfig().Validate())

	insecure := validConfig()
	insecure.Endpoint = "http://127.0.0.1:7480"
	insecure.AllowInsecureEndpoint = true
	require.NoError(t, insecure.Validate())

	for name, mutate := range map[string]func(*Config){
		"unknown backend":    func(c *Config) { c.Backend = "minio" },
		"no classes":         func(c *Config) { c.Classes = nil },
		"no endpoint":        func(c *Config) { c.Endpoint = "" },
		"relative endpoint":  func(c *Config) { c.Endpoint = "s3.provider.example.com" },
		"http endpoint":      func(c *Config) { c.Endpoint = "http://s3.provider.example.com" },
		"ftp endpoint":       func(c *Config) { c.Endpoint = "ftp://s3.provider.example.com"; c.AllowInsecureEndpoint = true },
		"no region":          func(c *Config) { c.Region = "" },
		"no admin endpoint":  func(c *Config) { c.AdminEndpoint = "" },
		"no admin access":    func(c *Config) { c.AdminAccessKey = "" },
		"no admin secret":    func(c *Config) { c.AdminSecretKey = "" },
		"bad admin endpoint": func(c *Config) { c.AdminEndpoint = "::" },
	} {
		cfg := validConfig()
		mutate(&cfg)
		assert.ErrorIs(t, cfg.Validate(), ErrInvalidConfig, name)
	}
}

func TestConfigClassEnabled(t *testing.T) {
	cfg := validConfig()

	assert.True(t, cfg.ClassEnabled("beta2"))
	assert.False(t, cfg.ClassEnabled("beta3"))
	assert.False(t, Config{}.ClassEnabled("beta2"))
}

func TestNew(t *testing.T) {
	_, err := New(Config{})
	assert.ErrorIs(t, err, ErrInvalidConfig, "a backend without buckets enabled")

	cfg := validConfig()
	cfg.Endpoint = "http://s3.provider.example.com"
	_, err = New(cfg)
	assert.ErrorIs(t, err, ErrInvalidConfig, "an invalid configuration")

	backend, err := New(validConfig())
	require.NoError(t, err)
	assert.NotNil(t, backend)
}

func TestConfigFromViper(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	t.Setenv(EnvAdminAccessKey, "admin-access")
	t.Setenv(EnvAdminSecretKey, "admin-secret")

	cmd := &cobra.Command{Use: "test"}
	require.NoError(t, AddFlags(cmd))
	require.NoError(t, cmd.ParseFlags([]string{
		"--" + FlagBackend, BackendRGW,
		"--" + FlagClasses, "beta2,beta3",
		"--" + FlagEndpoint, "https://s3.provider.example.com",
		"--" + FlagAdminEndpoint, "http://rgw.rook-ceph.svc",
	}))

	assert.Equal(t, Config{
		Backend:        BackendRGW,
		Classes:        []string{"beta2", "beta3"},
		Endpoint:       "https://s3.provider.example.com",
		Region:         "default",
		AdminEndpoint:  "http://rgw.rook-ceph.svc",
		AdminAccessKey: "admin-access",
		AdminSecretKey: "admin-secret",
	}, ConfigFromViper())

	assert.Nil(t, cmd.Flags().Lookup("bucket-admin-secret-key"), "a secret is settable by flag")
}
