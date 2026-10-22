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
	CodeOwnModelTurnedOff       = "own_model_turned_off"
	// CodeNothing is row B5: no acr_service_model (MCP ruling: ai_features_turned_off).
	CodeNothing = CodeAIFeaturesTurnedOff

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
	// ConfigReason names why a saved configuration cannot serve (a ConfigReason value; never a stored value).
	ConfigReason ConfigReason
	// GateClosed: the byo_llm plan gate is closed for the org.
	GateClosed bool
	// TurnedOff: an org admin turned the saved configuration off; the org has no usable own model.
	TurnedOff bool
	// ServiceModelOn is the effective acr_service_model.
	ServiceModelOn bool
	// Fallback is platform_model_fallback.
	Fallback bool
}

// Decide is the D6107 table, in the order of decide.go: the effective acr_service_model comes first.
// Flag off: ai_features_turned_off for every org (B1-off, B5), before any BYO, gate or grant question.
// Flag on: a saved config is B2 own_model_gate_closed (even with the grant), a saved config an org admin turned off is
// no own model (platform with the grant, else own_model_turned_off), B1' a 403 whose detail is the ConfigReason (missing_credentials, invalid_base_url, unknown_provider, not_configured, unreadable_row) when it cannot serve, B1 own
// model; no config is B3 platform (with the grant) or B4 service_model_not_entitled.
// unreadable_row is gate-only: the org LLM status route answers an undecryptable stored value with an error (its
// Python behaviour), so only the gate names it. A base_url the parser rejects is invalid_base_url here, while the
// status route returns an error for it (urlsplit parity).
func Decide(in Inputs) Decision {
	refuse := func(code string) Decision { return Decision{Kind: Refuse, Code: code, Status: http.StatusForbidden} }
	if !in.ServiceModelOn {
		return refuse(CodeAIFeaturesTurnedOff)
	}
	if in.Saved {
		switch {
		case in.GateClosed:
			return refuse(CodeOwnModelGateClosed)
		case in.TurnedOff:
			if in.Fallback {
				return Decision{Kind: Platform}
			}
			return refuse(CodeOwnModelTurnedOff)
		case !in.ConfigUsable:
			reason := in.ConfigReason
			if reason == "" || reason == ConfigActive {
				reason = ConfigUnreadableRow
			}
			return refuse(string(reason))
		}
		return Decision{Kind: Own}
	}
	if in.Fallback {
		return Decision{Kind: Platform}
	}
	return refuse(CodeServiceModelNotEntitled)
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
	switch d.Kind {
	case Refuse:
		logger.InfoContext(ctx, "llm model gate: refused", slog.String("org_id", orgID), slog.String("code", d.Code), slog.Int("status", d.Status))
	case Own:
		logger.InfoContext(ctx, "llm model gate: admitted", slog.String("org_id", orgID), slog.String("decision", "own"), slog.String("model_source", "org_byo"))
	case Platform:
		logger.InfoContext(ctx, "llm model gate: admitted", slog.String("org_id", orgID), slog.String("decision", "platform"), slog.String("model_source", "platform"))
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
	Queryer   SavedQueryer
	Config    ConfigSource
}

func (s PgSource) Inputs(ctx context.Context, orgID string) (Inputs, error) {
	saved, err := HasSavedBYO(ctx, s.Queryer, orgID)
	if err != nil {
		return Inputs{}, err
	}
	cfg, found, err := s.Config.OrgConfig(ctx, orgID)
	reason := ConfigNotConfigured
	switch {
	case errors.Is(err, ErrStoredSettingUnreadable):
		// Saved but unusable: row B1'.
		found, saved, reason = false, true, ConfigUnreadableRow
	case err != nil:
		return Inputs{}, fmt.Errorf("llmmodelgate: org config: %w", err)
	}
	saved = saved || found || cfg.GateClosed
	serviceModel, err := s.Licensing.Decide(ctx, orgID, ACRServiceModelFeatureKey)
	if err != nil {
		return Inputs{}, fmt.Errorf("llmmodelgate: %s: %w", ACRServiceModelFeatureKey, err)
	}
	serviceModelOn, err := EffectiveServiceModel(ctx, s.Queryer, orgID, serviceModel)
	if err != nil {
		return Inputs{}, fmt.Errorf("llmorgsettings: effective %s: %w", ACRServiceModelFeatureKey, err)
	}
	// platform_model_fallback decides only an org with no saved config (or one turned off) and acr_service_model on; every other
	// path is settled without it, so a failed lookup there must not refuse an org that does not need it.
	fallback := licensing.Decision{}
	if (!saved || cfg.TurnedOff) && serviceModelOn {
		if fallback, err = s.Licensing.Decide(ctx, orgID, PlatformModelFallbackFeatureKey); err != nil {
			return Inputs{}, fmt.Errorf("llmmodelgate: %s: %w", PlatformModelFallbackFeatureKey, err)
		}
	}
	usable := false
	if found && serviceModelOn && !cfg.GateClosed {
		checked, checkErr := CheckOrgConfig(ctx, cfg)
		if checkErr != nil {
			// A base_url the parser rejects is a verdict on the saved value, not an infrastructure error.
			checked = ConfigInvalidBaseURL
		}
		usable, reason = checked == ConfigActive, checked
	}
	return Inputs{
		Saved: saved, ConfigUsable: usable, ConfigReason: reason, GateClosed: cfg.GateClosed, TurnedOff: cfg.TurnedOff,
		ServiceModelOn: serviceModelOn, Fallback: fallback.Allowed,
	}, nil
}
