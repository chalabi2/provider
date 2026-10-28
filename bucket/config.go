package bucket

import (
	"errors"
	"fmt"
	"net/url"
	"slices"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	FlagBackend               = "bucket-backend"
	FlagClasses               = "bucket-classes"
	FlagEndpoint              = "bucket-endpoint"
	FlagRegion                = "bucket-region"
	FlagAdminEndpoint         = "bucket-admin-endpoint"
	FlagAllowInsecureEndpoint = "bucket-allow-insecure-endpoint"

	// The admin credentials are read only from the environment: a flag would
	// show them in the process list.
	keyAdminAccessKey = "bucket-admin-access-key"
	keyAdminSecretKey = "bucket-admin-secret-key"
	EnvAdminAccessKey = "AP_BUCKET_ADMIN_ACCESS_KEY"
	EnvAdminSecretKey = "AP_BUCKET_ADMIN_SECRET_KEY" //nolint:gosec // the variable's name, not a credential

	BackendRGW = "rgw"
)

var ErrInvalidConfig = errors.New("invalid bucket configuration")

// Config enables buckets and points the provider at its S3 server.
type Config struct {
	// Backend is empty when the provider does not serve buckets.
	Backend string
	// Classes are the storage classes whose capacity backs buckets; a bucket
	// order must name one of them.
	Classes []string
	// Endpoint is the public S3 URL tenants are given.
	Endpoint string
	// Region is the region S3 clients sign requests with.
	Region string
	// AdminEndpoint is the S3 server's in-cluster URL, for its admin API and
	// for creating buckets. It must never be the public endpoint.
	AdminEndpoint  string
	AdminAccessKey string
	AdminSecretKey string
	// AllowInsecureEndpoint accepts a plain-HTTP public endpoint, for local
	// development only.
	AllowInsecureEndpoint bool
}

func (c Config) Enabled() bool {
	return c.Backend != ""
}

func (c Config) ClassEnabled(class string) bool {
	return slices.Contains(c.Classes, class)
}

func (c Config) Validate() error {
	if !c.Enabled() {
		return nil
	}

	if c.Backend != BackendRGW {
		return fmt.Errorf("%w: unknown backend %q (want %q)", ErrInvalidConfig, c.Backend, BackendRGW)
	}

	if len(c.Classes) == 0 {
		return fmt.Errorf("%w: --%s names no storage class", ErrInvalidConfig, FlagClasses)
	}

	endpoint, err := url.Parse(c.Endpoint)
	if err != nil || endpoint.Host == "" {
		return fmt.Errorf("%w: --%s %q is not a URL", ErrInvalidConfig, FlagEndpoint, c.Endpoint)
	}

	if endpoint.Scheme != "https" && (endpoint.Scheme != "http" || !c.AllowInsecureEndpoint) {
		return fmt.Errorf("%w: --%s must be https (got %q)", ErrInvalidConfig, FlagEndpoint, endpoint.Scheme)
	}

	if c.Region == "" {
		return fmt.Errorf("%w: --%s is empty", ErrInvalidConfig, FlagRegion)
	}

	if admin, err := url.Parse(c.AdminEndpoint); err != nil || admin.Host == "" {
		return fmt.Errorf("%w: --%s %q is not a URL", ErrInvalidConfig, FlagAdminEndpoint, c.AdminEndpoint)
	}

	if c.AdminAccessKey == "" || c.AdminSecretKey == "" {
		return fmt.Errorf("%w: set %s and %s", ErrInvalidConfig, EnvAdminAccessKey, EnvAdminSecretKey)
	}

	return nil
}

// New builds the configured backend.
func New(c Config) (Backend, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	switch c.Backend {
	case BackendRGW:
		return newRGW(c)
	default:
		return nil, fmt.Errorf("%w: buckets are not enabled", ErrInvalidConfig)
	}
}

// AddFlags registers the bucket flags on a command that serves buckets.
func AddFlags(cmd *cobra.Command) error {
	flags := cmd.Flags()
	flags.String(FlagBackend, "", "serve bucket volumes from this S3 server: rgw. Empty disables buckets")
	flags.StringSlice(FlagClasses, nil, "storage classes whose capacity backs buckets")
	flags.String(FlagEndpoint, "", "public S3 URL given to tenants; must be https")
	flags.String(FlagRegion, "default", "region S3 clients sign requests with")
	flags.String(FlagAdminEndpoint, "", "in-cluster URL of the S3 server's admin API; never the public endpoint")
	flags.Bool(FlagAllowInsecureEndpoint, false, "accept an http public endpoint (local development only)")

	for _, name := range []string{FlagBackend, FlagClasses, FlagEndpoint, FlagRegion, FlagAdminEndpoint, FlagAllowInsecureEndpoint} {
		if err := viper.BindPFlag(name, flags.Lookup(name)); err != nil {
			return err
		}
	}

	if err := viper.BindEnv(keyAdminAccessKey, EnvAdminAccessKey); err != nil {
		return err
	}

	return viper.BindEnv(keyAdminSecretKey, EnvAdminSecretKey)
}

// ConfigFromViper reads the configuration AddFlags registered.
func ConfigFromViper() Config {
	return Config{
		Backend:               viper.GetString(FlagBackend),
		Classes:               viper.GetStringSlice(FlagClasses),
		Endpoint:              viper.GetString(FlagEndpoint),
		Region:                viper.GetString(FlagRegion),
		AdminEndpoint:         viper.GetString(FlagAdminEndpoint),
		AdminAccessKey:        viper.GetString(keyAdminAccessKey),
		AdminSecretKey:        viper.GetString(keyAdminSecretKey),
		AllowInsecureEndpoint: viper.GetBool(FlagAllowInsecureEndpoint),
	}
}
