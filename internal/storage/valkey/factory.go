package valkey

import (
	"context"
	"errors"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
)

var (
	ErrInvalidConfig = errors.New("invalid Valkey connection configuration")
	ErrUnavailable   = errors.New("Valkey readiness check failed")
)

// Config defines bounded connection behavior for Valkey database 1. URI may
// include credentials and is therefore excluded from every safe surface.
type Config struct {
	URI                 string
	ClientName          string
	DialTimeout         time.Duration
	WriteTimeout        time.Duration
	ConnLifetime        time.Duration
	BlockingPoolSize    int
	BlockingPoolMinSize int
}

func DefaultConfig(uri string) Config {
	return Config{
		URI:                 uri,
		ClientName:          "dev-health-worker",
		DialTimeout:         5 * time.Second,
		WriteTimeout:        10 * time.Second,
		ConnLifetime:        30 * time.Minute,
		BlockingPoolSize:    4,
		BlockingPoolMinSize: 0,
	}
}

func (c Config) Validate() error {
	if c.URI == "" || c.ClientName == "" || c.DialTimeout <= 0 || c.WriteTimeout <= 0 || c.ConnLifetime <= 0 {
		return ErrInvalidConfig
	}
	if c.BlockingPoolSize <= 0 || c.BlockingPoolMinSize < 0 || c.BlockingPoolMinSize > c.BlockingPoolSize {
		return ErrInvalidConfig
	}
	options, err := valkeygo.ParseURL(c.URI)
	if err != nil || options.SelectDB != 1 {
		return ErrInvalidConfig
	}
	return nil
}

func (c Config) SafeAttributes() map[string]any {
	return map[string]any{
		"configured":            c.URI != "",
		"client_name":           c.ClientName,
		"dial_timeout":          c.DialTimeout.String(),
		"write_timeout":         c.WriteTimeout.String(),
		"max_connection_age":    c.ConnLifetime.String(),
		"blocking_pool_size":    c.BlockingPoolSize,
		"blocking_pool_minimum": c.BlockingPoolMinSize,
	}
}

func Open(ctx context.Context, config Config) (valkeygo.Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	options, err := valkeygo.ParseURL(config.URI)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	options.ClientName = config.ClientName
	options.Dialer.Timeout = config.DialTimeout
	options.ConnWriteTimeout = config.WriteTimeout
	options.ConnLifetime = config.ConnLifetime
	options.BlockingPoolSize = config.BlockingPoolSize
	options.BlockingPoolMinSize = config.BlockingPoolMinSize
	options.BlockingPoolCleanup = config.ConnLifetime
	options.ClientSetInfo = valkeygo.DisableClientSetInfo

	client, err := valkeygo.NewClient(options)
	if err != nil {
		return nil, ErrUnavailable
	}
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		return nil, ErrUnavailable
	}
	return client, nil
}
