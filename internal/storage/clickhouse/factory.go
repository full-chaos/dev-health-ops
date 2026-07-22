package clickhouse

import (
	"context"
	"errors"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
		MaxIdleConns:    2,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     30 * time.Second,
		ConnMaxLifetime: 30 * time.Minute,
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

func Open(ctx context.Context, config Config) (driver.Conn, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

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

	connection, err := clickhouse.Open(options)
	if err != nil {
		return nil, ErrUnavailable
	}
	if err := connection.Ping(ctx); err != nil {
		_ = connection.Close()
		return nil, ErrUnavailable
	}
	return connection, nil
}
