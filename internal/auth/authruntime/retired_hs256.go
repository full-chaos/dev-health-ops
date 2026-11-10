package authruntime

import (
	"context"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// warnRetiredHS256Settings logs one WARN for each retired HS256 setting that is set: nothing reads it, and a leftover
// value is config hygiene, never a refused start (D6465). It names the setting, never its value.
func warnRetiredHS256Settings(ctx context.Context, lookup func(string) (string, bool), logger *slog.Logger) {
	for _, name := range authconfig.RetiredHS256Settings {
		if _, set := lookup(name); set {
			logger.WarnContext(ctx, "retired setting is set but unused (HS256 retired); unset it", "key", name)
		}
	}
}
