package clickhouse

import (
	"context"
	"net/http"
	"os"
	"sync"
	"time"
)

// ConnMaxLifetimeEnv overrides the lifetime of a pooled ClickHouse connection (a Go duration, e.g. "4m").
const ConnMaxLifetimeEnv = "DEV_HEALTH_CLICKHOUSE_CONN_MAX_LIFETIME"

// DefaultConnMaxLifetime sits under the ~5 minute idle cutoff assumed for ClickHouse Cloud, so a pooled connection is
// retired by us before the server or a load balancer drops it. A dropped connection costs a redial (TCP + TLS handshake,
// several round trips) at ~55 ms RTT.
const DefaultConnMaxLifetime = 4 * time.Minute

// DefaultDialTimeout covers a TCP + TLS handshake at ~55 ms RTT with headroom.
const DefaultDialTimeout = 10 * time.Second

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
