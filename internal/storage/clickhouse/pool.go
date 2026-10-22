package clickhouse

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// ConnMaxLifetimeEnv overrides the lifetime of a pooled ClickHouse connection (a Go duration, e.g. "4m").
const ConnMaxLifetimeEnv = "DEV_HEALTH_CLICKHOUSE_CONN_MAX_LIFETIME"

// DefaultConnMaxLifetime sits under the 15 minute idle scaling of the ClickHouse Cloud service (open connections drop when it sleeps), so a pooled connection is
// retired by us before a drop. A dropped connection costs a redial (TCP + TLS handshake,
// several round trips) at ~20 ms RTT. No app-level keepalive query: that would keep a sleeping Cloud service awake (cost); TCP keepalive only.
const DefaultConnMaxLifetime = 5 * time.Minute

// DialTimeoutEnv overrides the dial timeout (a Go duration).
const DialTimeoutEnv = "DEV_HEALTH_CLICKHOUSE_DIAL_TIMEOUT"

// DefaultDialTimeout covers a TCP + TLS handshake at ~20 ms RTT with ample headroom.
const DefaultDialTimeout = 10 * time.Second

// PoolDialTimeout is DefaultDialTimeout, or the value of DialTimeoutEnv when it parses to a positive duration.
func PoolDialTimeout() time.Duration {
	if parsed, err := time.ParseDuration(os.Getenv(DialTimeoutEnv)); err == nil && parsed > 0 {
		return parsed
	}
	return DefaultDialTimeout
}

// WarmBudget bounds the whole warm-up; it never fails a start.
const WarmBudget = 5 * time.Second

// PoolConnMaxLifetime is DefaultConnMaxLifetime, or the value of ConnMaxLifetimeEnv when it parses to a positive duration.
func PoolConnMaxLifetime() time.Duration {
	if parsed, err := time.ParseDuration(os.Getenv(ConnMaxLifetimeEnv)); err == nil && parsed > 0 {
		return parsed
	}
	return DefaultConnMaxLifetime
}

// KeepIdleConns sizes the idle pool of an http(s) transport; the driver's own transport keeps one idle connection.
func KeepIdleConns(transport *http.Transport, idle int) {
	transport.MaxIdleConns = idle
	transport.MaxIdleConnsPerHost = idle
}

// WarmPool pings idle times at once so the pool holds idle warm connections before the first request. Errors are
// ignored: a cold pool only costs the first request a dial. It returns after WarmBudget at the latest.
func WarmPool(ctx context.Context, ping func(context.Context) error, idle int) {
	if idle < 1 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, WarmBudget)
	defer cancel()
	var wg sync.WaitGroup
	for range idle {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = ping(ctx)
		}()
	}
	wg.Wait()
}

// AsyncInsertSetting is the server setting every shared constructor defaults to 0: ClickHouse Cloud defaults
// async_insert to 1, which would change write semantics (a write returns before it is durable). A per-query
// setting still overrides it.
const AsyncInsertSetting = "async_insert"

// WithAsyncInsertOff adds async_insert=0 to a DSN unless the DSN sets it already.
func WithAsyncInsertOff(dsn string) string {
	if strings.Contains(dsn, AsyncInsertSetting+"=") {
		return dsn
	}
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + AsyncInsertSetting + "=0"
}
