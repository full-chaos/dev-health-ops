package clickhouse

import (
	"context"
	"errors"
	"net/http"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/platform/dbspan"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

var (
	ErrInvalidConfig = errors.New("invalid ClickHouse connection configuration")
	ErrUnavailable   = errors.New("ClickHouse readiness check failed")
)

// Config defines a bounded ClickHouse client. DSN is secret-bearing and must
// never be added to logs or safe startup attributes.
type Config struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	ConnMaxLifetime time.Duration
}

func DefaultConfig(dsn string) Config {
	return Config{
		DSN:             dsn,
		MaxOpenConns:    4,
		MaxIdleConns:    4,
		DialTimeout:     PoolDialTimeout(),
		ReadTimeout:     30 * time.Second,
		ConnMaxLifetime: PoolConnMaxLifetime(),
	}
}

func (c Config) Validate() error {
	if c.DSN == "" || c.MaxOpenConns <= 0 || c.MaxIdleConns < 0 || c.MaxIdleConns > c.MaxOpenConns {
		return ErrInvalidConfig
	}
	if c.DialTimeout <= 0 || c.ReadTimeout <= 0 || c.ConnMaxLifetime <= 0 {
		return ErrInvalidConfig
	}
	if _, err := clickhouse.ParseDSN(c.DSN); err != nil {
		return ErrInvalidConfig
	}
	return nil
}

func (c Config) SafeAttributes() map[string]any {
	return map[string]any{
		"configured":           c.DSN != "",
		"max_open_connections": c.MaxOpenConns,
		"max_idle_connections": c.MaxIdleConns,
		"dial_timeout":         c.DialTimeout.String(),
		"read_timeout":         c.ReadTimeout.String(),
		"max_connection_age":   c.ConnMaxLifetime.String(),
	}
}

func clickHouseOptions(config Config) (*clickhouse.Options, error) {
	options, err := clickhouse.ParseDSN(config.DSN)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	options.MaxOpenConns = config.MaxOpenConns
	options.MaxIdleConns = config.MaxIdleConns
	options.DialTimeout = config.DialTimeout
	options.ReadTimeout = config.ReadTimeout
	options.ConnMaxLifetime = config.ConnMaxLifetime
	options.Debug = false
	options.Debugf = nil
	options.Logger = nil
	// The driver builds its own http.Transport for http(s) DSNs with the environment proxy; credentials must not ride it.
	options.HTTPProxyURL = nil
	options.TransportFunc = func(t *http.Transport) (http.RoundTripper, error) {
		t.Proxy = nil
		KeepIdleConns(t, config.MaxIdleConns)
		return t, nil
	}
	return options, nil
}

func Open(ctx context.Context, config Config) (driver.Conn, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	options, err := clickHouseOptions(config)
	if err != nil {
		return nil, err
	}

	// The login and password the driver settled on (it parses the DSN itself, by
	// its own rules) are redacted with the DSN's components.
	resolved := []string{options.Auth.Username, options.Auth.Password}
	connection, err := clickhouse.Open(options)
	if err != nil {
		return nil, secrets.WithRedactedCauseAlso(ErrUnavailable, config.DSN, err, resolved...)
	}
	if err := connection.Ping(ctx); err != nil {
		_ = connection.Close()
		return nil, secrets.WithRedactedCauseAlso(ErrUnavailable, config.DSN, err, resolved...)
	}
	if config.MaxIdleConns > 1 {
		WarmPool(ctx, connection.Ping, config.MaxIdleConns)
	}
	return newSpanConn(retryConn{connection}, dbspan.NewEmitter(dbspan.SystemClickHouse, "")), nil
}
