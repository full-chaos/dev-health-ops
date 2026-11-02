package policy

import (
	"context"
	"log/slog"

	"github.com/full-chaos/dev-health-go/pep"
)

// The shadow's environment switches.
const (
	// EnvShadowPDP turns the shadow on: true or false; unset is off.
	EnvShadowPDP = "AUTH_SHADOW_PDP"
	// EnvShadowPDPHighRisk lets high-risk rows ask the PDP, which writes an audit row per decision.
	EnvShadowPDPHighRisk = "AUTH_SHADOW_PDP_HIGH_RISK"
	// EnvShadowPDPSample is the sample percent, 1 to 100; unset is 100.
	EnvShadowPDPSample = "AUTH_SHADOW_PDP_SAMPLE"
	// EnvShadowPDPInFlight bounds the calls in flight, 1 or more; unset is 32.
	EnvShadowPDPInFlight = "AUTH_SHADOW_PDP_MAX_IN_FLIGHT"
)

// ShadowEnvDeps are what a service brings to ShadowFromEnv.
type ShadowEnvDeps struct {
	// Rows is the service's action map.
	Rows map[string]ShadowRow
	// NewDecider builds the service's PEP client. It is called only when the switch is on; its
	// error text is never logged.
	NewDecider func() (pep.Decider, error)
	Logger     *slog.Logger
}

// ShadowFromEnv builds the shadow from the environment. It never fails a start: off, or any
// missing input or bad value, is a nil Shadow; a bad value logs an ERROR naming the variable
// (never its value) and sets the config-invalid gauge.
func ShadowFromEnv(lookup func(string) (string, bool), deps ShadowEnvDeps) *Shadow { return nil }

var recordShadowState func(ctx context.Context, enabled, configInvalid bool)
