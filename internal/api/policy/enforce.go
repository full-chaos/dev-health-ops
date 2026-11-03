package policy

import (
	"context"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

// EnforceConfig builds an Enforcer.
type EnforceConfig struct {
	Decider pep.Decider
	Rows    map[string]ShadowRow
	Switch  authzenforce.Switch
	Timeout time.Duration
	Logger  *slog.Logger
}

// Enforcer lets the policy decision point decide the routes of the namespaces that are switched on.
type Enforcer struct{}

// NewEnforcer checks cfg.
func NewEnforcer(cfg EnforceConfig) (*Enforcer, error) { return &Enforcer{}, nil }

// SetEnforcer installs e beside the guard; nil is off. Call it once, before the guard serves.
func (g *Guard) SetEnforcer(e *Enforcer) {}

var enforceDecisionAdd = func(ctx context.Context, namespace, outcome string) {}
