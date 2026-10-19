// Package llmmodelgate decides which model an org's AI call may use: its own,
// the platform's, or none (D6107). One decision table serves every ops
// caller, so investment explain and work-unit explain cannot drift from acr's
// gate rows B1-B5.
package llmorgsettings

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
)

const (
	// ACRServiceModelFeatureKey is ACR AI for an org; PlatformModelFallbackFeatureKey is the
	// platform-admin grant that lets an org without its own model use the platform's.
	ACRServiceModelFeatureKey       = "acr_service_model"
	PlatformModelFallbackFeatureKey = "platform_model_fallback"

	CodeOwnModelGateClosed      = "own_model_gate_closed"
	CodeServiceModelNotEntitled = "service_model_not_entitled"
	CodeAIFeaturesTurnedOff     = "ai_features_turned_off"
	// CodeNothing is row B5: no own model, no acr_service_model, no grant. One constant so the
	// code can change in one line.
	CodeNothing = CodeServiceModelNotEntitled

	// CodeUnavailable is the 503 detail for an unknown entitlement or an unusable saved config.
	CodeUnavailable = "Service unavailable"
)

// Kind is the verdict.
type Kind int

const (
	// Own: run on the org's own saved model; the platform model is never drawn.
	Own Kind = iota + 1
	// Platform: run on the platform model.
	Platform
	// Refuse: make no model call.
	Refuse
)

// Decision is the gate's verdict. Code and Status are set for Refuse.
type Decision struct {
	Kind   Kind
	Code   string
	Status int
}

// Inputs are the facts the table reads.
type Inputs struct {
	// Saved: the org has a saved BYO configuration (any purpose; behind a closed gate still saved).
	Saved bool
	// ConfigUsable: the saved configuration resolves for this call.
	ConfigUsable bool
	// GateClosed: the byo_llm plan gate is closed for the org.
	GateClosed bool
	// ServiceModelOn is the effective acr_service_model.
	ServiceModelOn bool
	// Fallback is platform_model_fallback.
	Fallback bool
}

// Decide is the D6107 table. Rows: B1 own model; B1-off ai_features_turned_off; B2 own_model_gate_closed
// (even with the grant); B1' 503; B3 platform; B4 service_model_not_entitled; B5 CodeNothing.
// ai_features_turned_off outranks a closed gate: acr_service_model off stops ACR AI for every org.
func Decide(in Inputs) Decision {
	refuse := func(code string) Decision { return Decision{Kind: Refuse, Code: code, Status: http.StatusForbidden} }
	if in.Saved {
		switch {
		case !in.ServiceModelOn:
			return refuse(CodeAIFeaturesTurnedOff)
		case in.GateClosed:
			return refuse(CodeOwnModelGateClosed)
		case !in.ConfigUsable:
			return Decision{Kind: Refuse, Code: CodeUnavailable, Status: http.StatusServiceUnavailable}
		}
		return Decision{Kind: Own}
	}
	switch {
	case in.ServiceModelOn && in.Fallback:
		return Decision{Kind: Platform}
	case in.ServiceModelOn:
		return refuse(CodeServiceModelNotEntitled)
	}
	return refuse(CodeNothing)
}

// EffectiveServiceModel is the acr_service_model an org really has: an active org override (Decide put
// it first), OFF when the feature is globally disabled or invalid, else ON for an org with a saved BYO
// configuration, else the plan decision.
func EffectiveServiceModel(decision licensing.Decision, saved bool) bool {
	switch decision.Reason {
	case licensing.ReasonEnabledByOrgOverride, licensing.ReasonOrgOverrideDisabled,
		licensing.ReasonGlobalDisabled, licensing.ReasonInvalidFeatureState:
		return decision.Allowed
	}
	return saved || decision.Allowed
}

// Source reads the table's inputs for one org. An error is an unknown fact; the gate refuses on it.
type Source interface {
	Inputs(ctx context.Context, orgID string) (Inputs, error)
}

// Gate decides an org's model for one call.
type Gate struct {
	Source Source
	Logger *slog.Logger
}

// Decide never draws the platform model on an error: an unknown fact is not a grant.
func (g Gate) Decide(ctx context.Context, orgID string) Decision {
	logger := g.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if g.Source == nil {
		logger.ErrorContext(ctx, "llm model gate: no source", slog.String("org_id", orgID))
		return Decision{Kind: Refuse, Code: CodeUnavailable, Status: http.StatusServiceUnavailable}
	}
	in, err := g.Source.Inputs(ctx, orgID)
	if err != nil {
		logger.ErrorContext(ctx, "llm model gate: inputs unavailable", slog.String("org_id", orgID), slog.String("error", err.Error()))
		return Decision{Kind: Refuse, Code: CodeUnavailable, Status: http.StatusServiceUnavailable}
	}
	d := Decide(in)
	if d.Kind == Refuse {
		logger.InfoContext(ctx, "llm model gate: refused", slog.String("org_id", orgID), slog.String("code", d.Code), slog.Int("status", d.Status))
	}
	return d
}

// ConfigSource is the org's gated BYO configuration for the caller's purpose.
type ConfigSource interface {
	OrgConfig(ctx context.Context, orgID string) (OrgConfig, bool, error)
}

// PgSource reads the inputs from the licensing decisions and the org's settings.
type PgSource struct {
	Licensing licensing.Store
	Saved     func(ctx context.Context, orgID string) (bool, error)
	Config    ConfigSource
}

func (s PgSource) Inputs(ctx context.Context, orgID string) (Inputs, error) {
	saved, err := s.Saved(ctx, orgID)
	if err != nil {
		return Inputs{}, err
	}
	cfg, found, err := s.Config.OrgConfig(ctx, orgID)
	switch {
	case errors.Is(err, ErrStoredSettingUnreadable):
		// Saved but unusable: row B1'.
		found, saved = false, true
	case err != nil:
		return Inputs{}, fmt.Errorf("llmmodelgate: org config: %w", err)
	}
	saved = saved || found || cfg.GateClosed
	serviceModel, err := s.Licensing.Decide(ctx, orgID, ACRServiceModelFeatureKey)
	if err != nil {
		return Inputs{}, fmt.Errorf("llmmodelgate: %s: %w", ACRServiceModelFeatureKey, err)
	}
	fallback, err := s.Licensing.Decide(ctx, orgID, PlatformModelFallbackFeatureKey)
	if err != nil {
		return Inputs{}, fmt.Errorf("llmmodelgate: %s: %w", PlatformModelFallbackFeatureKey, err)
	}
	return Inputs{
		Saved: saved, ConfigUsable: found, GateClosed: cfg.GateClosed,
		ServiceModelOn: EffectiveServiceModel(serviceModel, saved), Fallback: fallback.Allowed,
	}, nil
}
