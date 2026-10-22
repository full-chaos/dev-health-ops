package admin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-6976: POST /llm-settings/readiness (settings.py:352-466,
// run_llm_settings_readiness). Certifies the org's OWN saved BYO LLM
// configuration by running a real 2-round probe (llmreadinessprobe.go)
// against it and persisting the outcome to the same settings row GET
// /llm-settings/status (CHAOS-6252a, llmsettingsstatus.go) already reads --
// this route is what WRITES that record; CHAOS-6252a's own doc comment
// named this route as deliberately out of its scope, and this file is that
// follow-up.
//
// Ported (design ruling D2839, 2026-09-28): the 404/no-config and
// impersonated-write guards, the safe_error_code taxonomy (reused from
// llmsettingsstatus.go, not duplicated), and the AgentReadinessRecord
// persistence shape.
//
// NOT ported, Ask Dev-role-only (D2839 item 3, same cut CHAOS-6252a already
// made for GET /status): RoleReadinessService/SettingsRoleCertificationStore
// per-role write (settings.py:421-452) and the role_readiness combine step
// in the response (settings.py:300-326). This route never writes a
// legacy_agent role certification, so a live Ask Dev selection that also
// requires one (production_runtime.py's _candidate, role_profile branch)
// stays gated exactly as it is today under CHAOS-6252a -- unaffected,
// because prod Ask Dev is off.
//
// NOT reproduced: CHAOS-6975 (Python's DELETE /llm-settings left this record
// behind). Go's deleteLLMSettings clears it and the role certification rows.
//
// See llmreadinessprobe.go's file doc comment for the probe wire shape and
// the PINNED fingerprint divergence.

// certifiedByoAgentProviders are the BYO providers with a readiness prober: openai (Chat Completions)
// and anthropic (the Messages API acr runs). It is narrower than llmorgsettings.IsKnownProvider, which
// also accepts gemini/qwen/local/ollama/lmstudio for the generic BYO credential surface. A saved
// config on any other provider is a real configuration the preflight cannot check yet.
var certifiedByoAgentProviders = map[string]struct{}{"openai": {}, "anthropic": {}}

// readinessProberFor returns the readiness prober of a certified provider; doer is the test override
// (nil in production).
func readinessProberFor(provider string, doer providerfoundation.HTTPDoer) (readinessProber, bool) {
	switch provider {
	case "openai":
		return newOpenAICompatibleReadinessProber(doer), true
	case "anthropic":
		return newAnthropicReadinessProber(doer), true
	}
	return nil, false
}

// readinessCredentialHash is a stable, non-reversible discriminator over an
// api_key (deliberately reusing hashedLogValue's shape -- sha256 hex,
// first 16 chars -- rather than inventing a second truncation scheme; see
// llmsettingsstatus.go's identical use for base_url).
func readinessCredentialHash(apiKey string) string {
	return hashedLogValue(apiKey)
}

// readinessFingerprint is this port's CREDENTIAL-ONLY fingerprint -- see
// llmreadinessprobe.go's file doc comment for the PINNED DIVERGENCE from
// Python's production_runtime._readiness_fingerprint this represents.
func readinessFingerprint(provider, model, baseURL, apiKey string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		"byo", provider, model, baseURL, readinessCredentialHash(apiKey), askDevReadinessVersion,
	}, "\x00")))
	return hex.EncodeToString(sum[:])[:24]
}

// readinessNoBYOConfigMessage is production_runtime.py's
// resolve_byo_certification_provider "No BYO LLM configuration is saved for
// this organization." message, reused verbatim for both its 404 raise
// sites (byo is None; byo.certified but not byo.usable).
const readinessNoBYOConfigMessage = "No BYO LLM configuration is saved for this organization."

// readinessProviderUnavailableMessage is the 404 for a provider without a readiness prober. It names
// the provider, not the model; a provider string that is not a plain lowercase name is shown as
// "unknown".
func readinessProviderUnavailableMessage(provider string) string {
	if !readinessProviderNameRe.MatchString(provider) {
		provider = "unknown"
	}
	return "BYO preflight is not available yet for provider " + provider + "."
}

var readinessProviderNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,39}$`)

// readinessBYOConfig is the four raw settings rows _byo_candidate reads,
// mirroring production_runtime.py's plain-string reads (no "auto"
// substitution -- that only applies to the generic llmorgsettings resolver,
// never this Ask Dev-specific candidate builder).
type readinessBYOConfig struct {
	provider string
	model    string
	apiKey   secrets.Hidden
	baseURL  string
}

func (h *handlers) loadReadinessBYOConfig(ctx context.Context, orgID string) (readinessBYOConfig, error) {
	var cfg readinessBYOConfig
	var apiKey string
	for key, dest := range map[string]*string{
		"provider": &cfg.provider,
		"model":    &cfg.model,
		"api_key":  &apiKey,
		"base_url": &cfg.baseURL,
	} {
		row, err := settingByKeyOn(ctx, h.store.Pool, orgID, llmCategory, key)
		if err != nil {
			return readinessBYOConfig{}, err
		}
		value, err := h.settingValue(row)
		if err != nil {
			return readinessBYOConfig{}, err
		}
		*dest = strOrEmpty(value)
	}
	cfg.apiKey = secrets.NewHidden(apiKey)
	return cfg, nil
}

// postLLMSettingsReadiness is run_llm_settings_readiness.
func (h *handlers) postLLMSettingsReadiness(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := policy.UserFrom(ctx)
	orgID, ok := adminOrgID(w, user)
	if !ok {
		return
	}
	if !h.requireBYOLLMAccess(ctx, w, orgID, false) {
		return
	}
	// block_impersonated_write (settings.py:401-407) -- same shape as
	// putLLMSettings' budget-limit-change guard, applied unconditionally
	// here (every readiness run is a write: it certifies and persists).
	if user.ImpersonatedBy != nil && *user.ImpersonatedBy != "" {
		llmDetail(http.StatusForbidden, w,
			"error", "impersonated_write_forbidden",
			"message", "BYO LLM readiness checks are unavailable while impersonating")
		return
	}

	cfg, err := h.loadReadinessBYOConfig(ctx, orgID)
	if err != nil {
		h.internalError(ctx, w, "read byo llm config", err)
		return
	}
	if cfg.provider == "" && cfg.model == "" && !cfg.apiKey.Configured() && cfg.baseURL == "" {
		policy.WriteDetail(w, http.StatusNotFound, readinessNoBYOConfigMessage, nil)
		return
	}
	resolvedProvider := cfg.provider
	if resolvedProvider == "" {
		resolvedProvider = "openai"
	}
	prober, certified := readinessProberFor(resolvedProvider, h.upstreamDoer)
	if !certified {
		policy.WriteDetail(w, http.StatusNotFound, readinessProviderUnavailableMessage(resolvedProvider), nil)
		return
	}
	validURL, _, err := llmorgsettings.ValidateBaseURLChecked(ctx, cfg.baseURL)
	if err != nil {
		h.internalError(ctx, w, "validate byo base url", err)
		return
	}
	usable := cfg.model != "" && cfg.apiKey.Configured() && validURL
	if !usable {
		policy.WriteDetail(w, http.StatusNotFound, readinessNoBYOConfigMessage, nil)
		return
	}

	// h.upstreamDoer is Deps.HTTPDoer AS GIVEN (nil in production; a route success-path test sets it via
	// adminDepsExtra), handed to the prober of the resolved provider above.
	outcome, safeErrorCode := prober.probe(ctx, resolvedProvider, cfg.model, cfg.baseURL, cfg.apiKey.Reveal())

	record := agentReadinessRecord{
		Fingerprint:      readinessFingerprint(resolvedProvider, cfg.model, cfg.baseURL, cfg.apiKey.Reveal()),
		ReadinessVersion: askDevReadinessVersion,
		CheckedAt:        h.store.now().UTC().Format(time.RFC3339Nano),
		Outcome:          outcome,
		SafeErrorCode:    safeErrorCode,
	}
	payload, err := json.Marshal(record)
	if err != nil {
		h.internalError(ctx, w, "encode readiness record", err)
		return
	}
	value := string(payload)
	description := "Safe Ask Dev provider certification result"
	if _, err := h.upsertSetting(ctx, h.store.Pool, orgID, llmCategory, askDevAgentReadinessKey, &value, false, &description); err != nil {
		h.internalError(ctx, w, "persist readiness record", err)
		return
	}

	out, err := h.llmSettingsStatusResponse(ctx, orgID)
	if err != nil {
		h.internalError(ctx, w, "read llm settings status", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}
