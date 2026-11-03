package apiservice

import (
	"log/slog"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

func newShadow(cfg AuthProxyConfig, lookup func(string) (string, bool), shared *workloadtoken.Client, logger *slog.Logger) *policy.Shadow {
	return nil
}
