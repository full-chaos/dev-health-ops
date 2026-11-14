package health

import (
	"context"
	"log/slog"
	"time"
)

// ReadinessStartGrace is how long after process start a dependency refusal
// is logged at WARN while the process has never answered Ready: the pools are
// usually still opening. It must cover the chart's worker readiness probe
// window, initialDelaySeconds + periodSeconds x failureThreshold (10 + 10 x 6
// in deploy/helm/dev-health/values.yaml goWorkers.defaultProbes);
// deploy/helm/dev-health's readiness start grace test pins that.
const ReadinessStartGrace = 70 * time.Second

type registryContextKey struct{}

func withRegistry(ctx context.Context, registry *Registry) context.Context {
	if registry == nil {
		return ctx
	}
	return context.WithValue(ctx, registryContextKey{}, registry)
}

// DependencyRefusalLevel is the level for a required check's own log line
// about why it refused. ctx is the context the registry ran the check with.
// WARN before the process's first Ready answer and inside
// ReadinessStartGrace; ERROR after first Ready, at grace expiry, and for a
// check not run by a registry. Only the log level depends on it: the
// readiness answer never does.
func DependencyRefusalLevel(ctx context.Context) slog.Level {
	registry, _ := ctx.Value(registryContextKey{}).(*Registry)
	if registry == nil {
		return slog.LevelError
	}
	return registry.dependencyRefusalLevel()
}

func (r *Registry) dependencyRefusalLevel() slog.Level {
	if r.firstReady.Load() {
		return slog.LevelError
	}
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	if now().Sub(r.startedAt) < r.startGrace {
		return slog.LevelWarn
	}
	return slog.LevelError
}
