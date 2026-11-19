package config

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	nonNativeMeterName = "github.com/full-chaos/dev-health-ops/internal/platform/config"
	nonNativeGauge     = "dev_health_config_clickhouse_non_native_dsn"
)

var clickHouseServiceKeys = []string{"CLICKHOUSE_URI", "API_CLICKHOUSE_URI"}

// ObserveNonNativeClickHouse writes one WARN line and sets the gauge to 1 for each key in
// cfg.NonNativeClickHouseKeys, and to 0 for each other service key. The line names the key and scheme only, never the
// DSN. Call it once at start, after the process meter provider is installed.
func ObserveNonNativeClickHouse(ctx context.Context, logger *slog.Logger, cfg Config) {
	if logger == nil {
		logger = slog.Default()
	}
	flagged := map[string]bool{}
	for _, key := range cfg.NonNativeClickHouseKeys {
		flagged[key] = true
		logger.WarnContext(ctx, "ClickHouse service DSN uses a non-native scheme; use clickhouse://",
			"key", key, "scheme", clickHouseHTTPScheme(cfg.clickHouseDSN(key)))
	}
	for _, key := range clickHouseServiceKeys {
		recordNonNative(ctx, key, flagged[key])
	}
}

func (c Config) clickHouseDSN(key string) secrets.Value {
	if key == "API_CLICKHOUSE_URI" {
		return c.APIClickHouseURI
	}
	return c.ClickHouseURI
}

func recordNonNative(ctx context.Context, key string, flagged bool) {
	defer func() { _ = recover() }()
	gauge, err := otel.GetMeterProvider().Meter(nonNativeMeterName).Int64Gauge(nonNativeGauge,
		metric.WithDescription("1 when a ClickHouse service DSN key holds an http or https DSN, else 0"))
	if err != nil {
		return
	}
	value := int64(0)
	if flagged {
		value = 1
	}
	gauge.Record(ctx, value, metric.WithAttributes(attribute.String("key", key)))
}
