package acr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const llmConfigSchemaVersion = "acr_llm_config.v1"

// NoConfigHeader marks the 404 that means "the org has no BYO config".
// NoConfigReasonHeader rides beside it only when BYO settings are stored but
// the byo_llm gate is closed, an org admin turned them off, or none serves this purpose, so acr refuses the turn instead of reading "no
// config"; the marker value stays "none" for the worker client.
const (
	NoConfigHeader          = "X-Org-Llm-Config"
	NoConfigValue           = "none"
	NoConfigReasonHeader    = "X-Org-Llm-Config-Reason"
	NoConfigReasonGateShut  = "byo_llm_gate_closed"
	NoConfigReasonTurnedOff = "byo_llm_turned_off"
	// NoConfigReasonBaseURLCarriesKey names a stored config whose base_url holds
	// an api key; the route never serves such a url.
	NoConfigReasonBaseURLCarriesKey = "byo_base_url_carries_key"
)

// noConfigReasonFor names a BYO org whose saved configs are for other purposes
// only: byo_no_<purpose>_config (byo_no_investigate_config on the acr route).
func noConfigReasonFor(purpose llmorgsettings.Purpose) string {
	return "byo_no_" + string(purpose) + "_config"
}

// LLMConfigSource reads an org's stored BYO LLM config.
type LLMConfigSource interface {
	OrgConfig(ctx context.Context, orgID string) (llmorgsettings.OrgConfig, bool, error)
}

// llmConfigResponse is the 200 body. APIKey is a plain string on purpose: this
// is the one value the route exists to serve. It is built last, in
// llmConfigBody, and never reaches a logger.
type llmConfigResponse struct {
	SchemaVersion string `json:"schema_version"`
	OrgID         string `json:"org_id"`
	Provider      string `json:"provider"`
	Model         string `json:"model"`
	BaseURL       string `json:"base_url"`
	APIKey        string `json:"api_key"`
	PromptCache   bool   `json:"prompt_cache,omitempty"`
	Version       string `json:"version"`
}

func llmConfigBody(orgID string, cfg llmorgsettings.OrgConfig) llmConfigResponse {
	return llmConfigResponse{
		SchemaVersion: llmConfigSchemaVersion, OrgID: orgID,
		Provider: cfg.Provider, Model: cfg.Model, BaseURL: cfg.BaseURL,
		APIKey: cfg.APIKey.Reveal(), PromptCache: cfg.PromptCache, Version: cfg.Version,
	}
}

var llmConfigRequests = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/apiservice/acr").Int64Counter(
		"devhealth_acr_llm_config_requests_total",
		metric.WithDescription("Requests to the internal org llm-config route, by outcome."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_acr_llm_config_requests_total")
	}
	return counter
}()

func countLLMConfig(ctx context.Context, outcome string) {
	llmConfigRequests.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// llmConfigHandler serves GET .../orgs/{org_id}/llm-config. Order: bearer
// (401; only the acr-api workload token, 403 for another workload), org id
// (404), store (503), config (200 or 404). No
// branch logs or returns the key, the token, or an error that could carry one.
func llmConfigHandler(source LLMConfigSource, logger *slog.Logger, workload *WorkloadAuth) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		w.Header().Set("Cache-Control", "no-store")
		if source == nil {
			logger.ErrorContext(ctx, "acr llm-config: route not configured")
			countLLMConfig(ctx, "unconfigured")
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
			return
		}
		token, ok := bearerToken(r)
		if !ok {
			unauthorized(ctx, w, "missing")
			return
		}
		if !isWorkloadToken(token) {
			unauthorized(ctx, w, "invalid")
			return
		}
		if _, reason := workload.decide(ctx, token); reason != "" {
			refuseWorkloadLLMConfig(ctx, w, logger, reason)
			return
		}
		countWorkloadAccepted(ctx, routeLLMConfig)
		orgID := r.PathValue("org_id")
		if _, err := uuid.Parse(orgID); err != nil {
			countLLMConfig(ctx, "bad_org")
			writeDetail(w, http.StatusNotFound, "Not found")
			return
		}
		cfg, found, err := source.OrgConfig(ctx, orgID)
		if errors.Is(err, llmorgsettings.ErrStoredSettingUnreadable) {
			logger.ErrorContext(ctx, "acr llm-config: stored config unreadable", slog.String("org_id", orgID))
			countLLMConfig(ctx, "unreadable")
			writeDetail(w, http.StatusConflict, "Stored config unreadable")
			return
		}
		if err != nil {
			logger.ErrorContext(ctx, "acr llm-config: store unavailable",
				slog.String("org_id", orgID), slog.String("error_type", errorKind(err)))
			countLLMConfig(ctx, "store_unavailable")
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
			return
		}
		if !found {
			outcome, reason := "not_found", "none_configured"
			switch {
			case cfg.GateClosed:
				outcome, reason = "byo_llm_gate_closed", "byo_llm_gate_closed"
			case cfg.TurnedOff:
				outcome, reason = NoConfigReasonTurnedOff, NoConfigReasonTurnedOff
			case cfg.UnresolvedPurpose != "":
				outcome, reason = "byo_no_purpose_config", noConfigReasonFor(cfg.UnresolvedPurpose)
			}
			logger.InfoContext(ctx, "acr llm-config: none served",
				slog.String("org_id", orgID), slog.String("reason", reason))
			countLLMConfig(ctx, outcome)
			// A machine-readable marker: a client that must tell "this org has
			// no config" from a missing route (an old replica, a wrong target)
			// reads this header, never the bare 404.
			w.Header().Set(NoConfigHeader, NoConfigValue)
			switch {
			case cfg.GateClosed:
				w.Header().Set(NoConfigReasonHeader, NoConfigReasonGateShut)
			case cfg.TurnedOff:
				w.Header().Set(NoConfigReasonHeader, NoConfigReasonTurnedOff)
			case cfg.UnresolvedPurpose != "":
				w.Header().Set(NoConfigReasonHeader, noConfigReasonFor(cfg.UnresolvedPurpose))
			}
			writeDetail(w, http.StatusNotFound, "Not found")
			return
		}
		if cfg.KeyInBaseURL || llmorgsettings.BaseURLCarriesKey(cfg.BaseURL, cfg.APIKey.Reveal()) {
			logger.WarnContext(ctx, "acr llm-config: none served",
				slog.String("org_id", orgID), slog.String("reason", NoConfigReasonBaseURLCarriesKey))
			countLLMConfig(ctx, NoConfigReasonBaseURLCarriesKey)
			w.Header().Set(NoConfigHeader, NoConfigValue)
			w.Header().Set(NoConfigReasonHeader, NoConfigReasonBaseURLCarriesKey)
			writeDetail(w, http.StatusNotFound, "Not found")
			return
		}
		body := llmConfigBody(orgID, cfg)
		logger.InfoContext(ctx, "acr llm-config: served",
			slog.String("org_id", orgID), slog.String("version", cfg.Version))
		countLLMConfig(ctx, "served")
		out := pyjson.NewObject()
		out.Set("schema_version", body.SchemaVersion)
		out.Set("org_id", body.OrgID)
		out.Set("provider", body.Provider)
		out.Set("model", body.Model)
		out.Set("base_url", body.BaseURL)
		out.Set("api_key", body.APIKey)
		out.Set("version", body.Version)
		// Sent only for an org that opted in: acr's decoder rejects fields it
		// does not know, so a body without it stays valid for every other org.
		if body.PromptCache {
			out.Set("prompt_cache", true)
		}
		// A Go-only route has no response model: the JSONResponse writer, not WriteModel.
		policy.WriteJSON(w, http.StatusOK, out, http.Header{"X-Content-Type-Options": {"nosniff"}})
	})
}

// refuseWorkloadLLMConfig answers a refused workload token: 403 for a valid
// token of another workload, 503 when the key source could not be read, 401
// for every other reason. The reason is counted and logged, never sent.
func refuseWorkloadLLMConfig(ctx context.Context, w http.ResponseWriter, logger *slog.Logger, reason string) {
	countWorkloadRefused(ctx, routeLLMConfig, reason)
	logWorkloadRefused(ctx, logger, routeLLMConfig, reason)
	if reason == ReasonUnavailable {
		writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
		return
	}
	if reason == ReasonPrincipal {
		countLLMConfig(ctx, "forbidden_workload")
		writeDetail(w, http.StatusForbidden, "Forbidden")
		return
	}
	unauthorized(ctx, w, "workload")
}

func unauthorized(ctx context.Context, w http.ResponseWriter, outcome string) {
	countLLMConfig(ctx, "unauthorized_"+outcome)
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeDetail(w, http.StatusUnauthorized, "Unauthorized")
}

func bearerToken(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return token, token != ""
}

// errorKind names an error's concrete type without its text, which may carry
// a connection string or row value.
func errorKind(err error) string {
	return fmt.Sprintf("%T", err)
}
