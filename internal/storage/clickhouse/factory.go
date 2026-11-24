package clickhouse

import (
	"context"
	"errors"
	"net/http"
	"slices"
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
	// Settings are session settings sent with every query; they win over DSN parameters.
	Settings map[string]any
	// BatchWriteBound is the total bound of one batch (PrepareBatch to Send); zero = max(DefaultBatchWriteBound, ReadTimeout).
	BatchWriteBound time.Duration
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
	for name, value := range config.Settings {
		if options.Settings == nil {
			options.Settings = clickhouse.Settings{}
		}
		options.Settings[name] = value
	}
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
	return OpenWith(ctx, config, func(ctx context.Context, ping func(context.Context) error) error { return ping(ctx) })
}

// OpenWith is Open with its readiness Ping run through wait, which may run the
// ping more than once (a bounded first-connect wait). On failure it returns
// the same redacted ErrUnavailable error Open returns.
func OpenWith(ctx context.Context, config Config, wait func(context.Context, func(context.Context) error) error) (driver.Conn, error) {
	return openWith(ctx, config, wait, func(err error, resolved []string) error {
		return secrets.WithRedactedCauseAlso(ErrUnavailable, config.DSN, err, resolved...)
	})
}

// OpenRawCause is Open for the two dho sync CLI openers (internal/synccli), which
// redact the original error text once, with one union of every credential on
// their print path (the ClickHouse DSN, the database, the provider credentials):
// the DSN-only redaction of Open would remove bytes that union still has to
// match. On failure the error is ErrUnavailable with the driver's cause text
// unredacted; the caller must redact it before it is printed or logged. Nothing
// else may call it (TestOpenRawCauseHasExactlyTheTwoSyncCLICallers).
func OpenRawCause(ctx context.Context, config Config) (driver.Conn, error) {
	return openWith(ctx, config, func(ctx context.Context, ping func(context.Context) error) error { return ping(ctx) },
		func(err error, resolved []string) error { return RawCauseError{cause: err, resolved: resolved} })
}

// RawCauseError is ErrUnavailable plus the driver's cause text as it came, and
// the login and password the driver settled on (its own DSN parser accepts forms
// net/url does not), so the caller's union can match them.
type RawCauseError struct {
	cause    error
	resolved []string
}

func (e RawCauseError) Error() string { return ErrUnavailable.Error() + ": " + e.cause.Error() }
func (e RawCauseError) Unwrap() error { return ErrUnavailable }

// Resolved is the login and password the driver settled on.
func (e RawCauseError) Resolved() []string { return slices.Clone(e.resolved) }

func openWith(ctx context.Context, config Config, wait func(context.Context, func(context.Context) error) error, failure func(error, []string) error) (driver.Conn, error) {
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
		return nil, failure(err, resolved)
	}
	if err := wait(ctx, connection.Ping); err != nil {
		_ = connection.Close()
		return nil, failure(err, resolved)
	}
	if config.MaxIdleConns > 1 {
		WarmPool(ctx, connection.Ping, config.MaxIdleConns)
	}
	return wrapConn(connection, batchWriteBound(config.BatchWriteBound, config.ReadTimeout)), nil
}

func wrapConn(connection driver.Conn, batchWriteBound time.Duration) driver.Conn {
	return newSpanConn(retryConn{detachedBatchConn{Conn: connection, bound: batchWriteBound}}, dbspan.NewEmitter(dbspan.SystemClickHouse, ""))
}
