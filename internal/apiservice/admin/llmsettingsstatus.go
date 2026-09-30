package admin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

// CHAOS-6252a: GET /llm-settings/status base fields only (evaluate_org_llm_status,
// settings.py's _llm_settings_status_response over credentials.py). The
// readiness/binary_transport_readiness/readiness_checked_at/
// readiness_safe_failure_reason fields below are a DELIBERATE, DOCUMENTED
// simplification, not a bug: they surface whatever the org's persisted
// ask_dev_agent_readiness record already says, read-only. Python's full
// _llm_settings_status_response ALSO checks that record's currency against
// the org's CURRENT BYO candidate fingerprint and READINESS_VERSION
// (production_runtime._byo_candidate/_readiness_fingerprint) and combines it
// with a separate per-role certification store before deciding "stale" vs
// "ready"/"failed" -- that whole state machine, plus the POST
// /llm-settings/readiness probe that WRITES this record, is Ask Dev-role
// machinery (CHAOS-6252b, folded into the CHAOS-6262 Ask Dev deletion class:
// prod Ask Dev is OFF until the MCP project lands, so it is not ported here).
// A consequence worth naming: this handler can never report "stale" -- a
// record whose underlying BYO config has since changed is reported at its
// last-known outcome ("ready"/"failed") until 6252b lands, not as stale.
// Python's DELETE /llm-settings left this record behind (CHAOS-6975); Go's
// DELETE (HTTP and the operator CLI) clears it, so after a delete this handler
// reads not_configured/never_checked.

const (
	llmStatusReasonNotConfigured   = "not_configured"
	llmStatusReasonUnknownProvider = "unknown_provider"
	llmStatusReasonMissingCreds    = "missing_credentials"
	llmStatusReasonInvalidBaseURL  = "invalid_base_url"
	llmStatusReasonActive          = "active"

	askDevAgentReadinessKey            = "ask_dev_agent_readiness"
	byoBaseURLFallbackResourceID       = "llm.base_url"
	byoBaseURLFallbackAlertWindowHours = 24
	readinessOutcomeReady              = "ready"
)

// orgLLMStatusEvaluation is credentials.py's OrgLLMStatusEvaluation.
type orgLLMStatusEvaluation struct {
	Configured   bool
	Active       bool
	ReasonCode   string
	ProviderName string
	BaseURLHash  string
}

// hashedLogValue is credentials.py's _hashed_log_value: sha256 hex digest,
// first 16 hex characters, over the value with CR/LF stripped first.
func hashedLogValue(value string) string {
	safe := strings.NewReplacer("\r", "", "\n", "").Replace(value)
	sum := sha256.Sum256([]byte(safe))
	return hex.EncodeToString(sum[:])[:16]
}

// baseURLHash is credentials.py's _base_url_hash: "" for an empty/
// whitespace-only base_url (never hashed), else hashedLogValue of the
// trimmed value.
func baseURLHash(baseURL string) string {
	trimmed := strings.TrimSpace(baseURL)
	if trimmed == "" {
		return ""
	}
	return hashedLogValue(trimmed)
}

func strOrEmpty(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

// evaluateOrgLLMStatus is credentials.py's evaluate_org_llm_status, over the
// same three llm-category settings rows llmSettingsResponse reads (decrypted
// the same way via h.settingValue).
func (h *handlers) evaluateOrgLLMStatus(ctx context.Context, orgID string) (orgLLMStatusEvaluation, error) {
	providerRow, err := settingByKeyOn(ctx, h.store.Pool, orgID, llmCategory, "provider")
	if err != nil {
		return orgLLMStatusEvaluation{}, err
	}
	providerValue, err := h.settingValue(providerRow)
	if err != nil {
		return orgLLMStatusEvaluation{}, err
	}
	providerName := llmorgsettings.NormalizeProvider(strOrEmpty(providerValue))
	if providerName == "" || providerName == "auto" || providerName == "mock" || providerName == "none" {
		return orgLLMStatusEvaluation{ReasonCode: llmStatusReasonNotConfigured}, nil
	}
	if !llmorgsettings.IsKnownProvider(providerName) {
		return orgLLMStatusEvaluation{Configured: true, ReasonCode: llmStatusReasonUnknownProvider, ProviderName: providerName}, nil
	}

	apiKeyRow, err := settingByKeyOn(ctx, h.store.Pool, orgID, llmCategory, "api_key")
	if err != nil {
		return orgLLMStatusEvaluation{}, err
	}
	apiKeyValue, err := h.settingValue(apiKeyRow)
	if err != nil {
		return orgLLMStatusEvaluation{}, err
	}
	baseURLRow, err := settingByKeyOn(ctx, h.store.Pool, orgID, llmCategory, "base_url")
	if err != nil {
		return orgLLMStatusEvaluation{}, err
	}
	baseURLValue, err := h.settingValue(baseURLRow)
	if err != nil {
		return orgLLMStatusEvaluation{}, err
	}
	apiKey := strOrEmpty(apiKeyValue)
	baseURL := strOrEmpty(baseURLValue)
	hash := baseURLHash(baseURL)

	if apiKey == "" && baseURL == "" {
		return orgLLMStatusEvaluation{Configured: true, ReasonCode: llmStatusReasonMissingCreds, ProviderName: providerName, BaseURLHash: hash}, nil
	}
	if !llmorgsettings.CredentialsComplete(providerName, apiKey) {
		return orgLLMStatusEvaluation{Configured: true, ReasonCode: llmStatusReasonMissingCreds, ProviderName: providerName, BaseURLHash: hash}, nil
	}
	ok, _, verr := llmorgsettings.ValidateBaseURLChecked(ctx, baseURL)
	if verr != nil {
		return orgLLMStatusEvaluation{}, verr
	}
	if !ok {
		return orgLLMStatusEvaluation{Configured: true, ReasonCode: llmStatusReasonInvalidBaseURL, ProviderName: providerName, BaseURLHash: hash}, nil
	}
	return orgLLMStatusEvaluation{Configured: true, Active: true, ReasonCode: llmStatusReasonActive, ProviderName: providerName, BaseURLHash: hash}, nil
}

// latestOrgByoBaseURLFallbackAt is credentials.py's
// latest_recent_org_byo_base_url_fallback_at: the most recent matching
// fallback audit row's created_at within the alert window, or nil. Only
// meaningful when the evaluation itself is invalid_base_url (Python's own
// early-return guard, reproduced here).
func (h *handlers) latestOrgByoBaseURLFallbackAt(ctx context.Context, orgID string, eval orgLLMStatusEvaluation) (*time.Time, error) {
	if eval.ReasonCode != llmStatusReasonInvalidBaseURL || eval.BaseURLHash == "" {
		return nil, nil
	}
	// round-1 review (codex, P3, acknowledged, not fixed): Postgres's `->>`
	// always extracts as text regardless of the underlying JSON value's
	// type, while Python's _audit_changes_match compares typed values from
	// json.loads (a stored JSON number would be an int/float there, never
	// equal to a string). _base_url_hash always writes a hex string, so a
	// numeric changes.base_url_hash should never occur in practice -- this
	// is a real but purely theoretical divergence, left as-is rather than
	// adding a jsonb_typeof guard for a case the writer path cannot produce.
	cutoff := h.store.now().UTC().Add(-byoBaseURLFallbackAlertWindowHours * time.Hour)
	var createdAt time.Time
	err := h.store.Pool.QueryRow(ctx, `
SELECT created_at FROM audit_logs
WHERE org_id = $1 AND resource_type = 'setting' AND resource_id = $2
  AND created_at >= $3
  AND changes ->> 'provider' = $4
  AND changes ->> 'base_url_hash' = $5
  AND changes ->> 'reason_code' = $6
ORDER BY created_at DESC LIMIT 1`,
		orgID, byoBaseURLFallbackResourceID, cutoff, eval.ProviderName, eval.BaseURLHash, llmStatusReasonInvalidBaseURL,
	).Scan(&createdAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &createdAt, nil
}

// agentReadinessRecord is readiness.py's AgentReadinessRecord, as persisted
// (json.dumps(asdict(record)), sort_keys) under category=llm,
// key=ask_dev_agent_readiness.
type agentReadinessRecord struct {
	Fingerprint      string  `json:"fingerprint"`
	ReadinessVersion string  `json:"readiness_version"`
	CheckedAt        string  `json:"checked_at"`
	Outcome          string  `json:"outcome"`
	SafeErrorCode    *string `json:"safe_error_code"`
}

// loadAgentReadinessRecord is SettingsAgentReadinessStore.load, minus the
// currency check (CHAOS-6252a scope, see the file doc comment): a malformed
// or legacy blob is treated as "never checked", matching Python's own
// except-and-return-None handling in load().
//
// Python's load() reads fingerprint/readiness_version/checked_at with plain
// dict subscripting (`payload["fingerprint"]`), which raises KeyError -- caught
// and treated as absent -- when the key is MISSING, not just falsy. A round-1
// review finding (codex, P2) caught this port doing the check the wrong way:
// unmarshalling straight into the agentReadinessRecord struct, whose string
// fields zero-value to "" for a missing key exactly the same as for a
// present-but-empty one, so `{"outcome":"ready"}` alone was wrongly accepted
// as a complete, current-shaped record. Checking key PRESENCE first (via a
// map, matching Python's own KeyError semantics) before decoding into the
// typed struct closes that gap.
func (h *handlers) loadAgentReadinessRecord(ctx context.Context, orgID string) (*agentReadinessRecord, error) {
	row, err := settingByKeyOn(ctx, h.store.Pool, orgID, llmCategory, askDevAgentReadinessKey)
	if err != nil {
		return nil, err
	}
	value, err := h.settingValue(row)
	if err != nil {
		return nil, err
	}
	if value == nil || *value == "" {
		return nil, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(*value), &raw); err != nil {
		return nil, nil
	}
	for _, key := range []string{"fingerprint", "readiness_version", "checked_at", "outcome"} {
		if _, present := raw[key]; !present {
			return nil, nil
		}
	}
	var record agentReadinessRecord
	if err := json.Unmarshal([]byte(*value), &record); err != nil {
		return nil, nil
	}
	if record.Outcome != readinessOutcomeReady && record.Outcome != "failed" {
		return nil, nil
	}
	return &record, nil
}

// readinessSafeFailureMessage is readiness.py's readiness_failure_state,
// message half only (CHAOS-6252a never computes the first, role-certification
// half -- that combination is 6252b/Ask Dev scope).
func readinessSafeFailureMessage(safeErrorCode *string) string {
	code := ""
	if safeErrorCode != nil {
		code = *safeErrorCode
	}
	switch code {
	case "provider_not_configured":
		return "Ask Dev could not authenticate with the configured model endpoint."
	case "timeout":
		return "The configured Ask Dev model timed out during readiness."
	case "rate_limited":
		return "The configured Ask Dev model rate limit was reached."
	case "model_not_supported":
		return "The configured Ask Dev model is unavailable to this provider account."
	case "invalid_request":
		return "The configured Ask Dev model rejected a required agent request capability."
	case "invalid_response":
		return "The configured model did not satisfy the Ask Dev agent capability contract."
	case "provider_contract_violation":
		return "The configured model returned multiple tool decisions in one turn, violating Ask Dev's required sequential tool-call contract."
	case "output_exhausted":
		return "The configured Ask Dev model exhausted its output/reasoning token budget before completing a valid response."
	case "provider_unavailable":
		return "The configured Ask Dev model endpoint is unavailable."
	default:
		return "The configured Ask Dev model failed readiness."
	}
}

// llmSettingsStatusResponse assembles LLMSettingsStatusResponse (settings.py's
// _llm_settings_status_response, CHAOS-6252a scope -- see this file's doc
// comment for exactly what is and is not reproduced).
func (h *handlers) llmSettingsStatusResponse(ctx context.Context, orgID string) (*pyjson.Object, error) {
	eval, err := h.evaluateOrgLLMStatus(ctx, orgID)
	if err != nil {
		return nil, err
	}
	lastFallbackAt, err := h.latestOrgByoBaseURLFallbackAt(ctx, orgID, eval)
	if err != nil {
		return nil, err
	}
	record, err := h.loadAgentReadinessRecord(ctx, orgID)
	if err != nil {
		return nil, err
	}

	readiness := "never_checked"
	var readinessCheckedAt *string
	var readinessFailureReason *string
	if record != nil {
		// Codex r3 P1 (CHAOS-6976): a certification is only trustworthy
		// against the BYO config it was actually run against. Recompute the
		// CURRENT fingerprint (same defaulting postLLMSettingsReadiness
		// applies: an empty provider resolves to "openai") and compare --
		// executed repro was POST readiness (certify) -> PUT settings with a
		// new api_key -> GET status, which without this check still
		// reported the OLD certification's "ready" for a credential that
		// was never actually probed. A mismatch falls back to
		// "never_checked" (never a fabricated "stale" third state -- that
		// full currency/role-combine state machine is CHAOS-6252b, not
		// reproduced here per this file's own doc comment above) so a
		// stale certification is never displayed as current.
		cfg, cfgErr := h.loadReadinessBYOConfig(ctx, orgID)
		if cfgErr != nil {
			return nil, cfgErr
		}
		resolvedProvider := cfg.provider
		if resolvedProvider == "" {
			resolvedProvider = "openai"
		}
		currentFingerprint := readinessFingerprint(resolvedProvider, cfg.model, cfg.baseURL, cfg.apiKey)
		if currentFingerprint == record.Fingerprint {
			if dt, ok := pytime.FromISOFormat(record.CheckedAt); ok {
				formatted := pytime.Pydantic(dt)
				readinessCheckedAt = &formatted
			}
			if record.Outcome == readinessOutcomeReady {
				readiness = "ready"
			} else {
				readiness = "failed"
				message := readinessSafeFailureMessage(record.SafeErrorCode)
				readinessFailureReason = &message
			}
		}
	}

	out := pyjson.NewObject()
	out.Set("configured", eval.Configured)
	out.Set("active", eval.Active)
	out.Set("degraded", eval.ReasonCode == llmStatusReasonInvalidBaseURL)
	out.Set("reason_code", eval.ReasonCode)
	if lastFallbackAt != nil {
		out.Set("last_fallback_at", pyTimeString(*lastFallbackAt))
	} else {
		out.Set("last_fallback_at", nil)
	}
	out.Set("readiness", readiness)
	out.Set("binary_transport_readiness", readiness)
	if readinessCheckedAt != nil {
		out.Set("readiness_checked_at", *readinessCheckedAt)
	} else {
		out.Set("readiness_checked_at", nil)
	}
	if readinessFailureReason != nil {
		out.Set("readiness_safe_failure_reason", *readinessFailureReason)
	} else {
		out.Set("readiness_safe_failure_reason", nil)
	}
	return out, nil
}

// getLLMSettingsStatus is settings.py's get_llm_settings_status.
func (h *handlers) getLLMSettingsStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if !h.requireBYOLLMAccess(ctx, w, orgID, false) {
		return
	}
	out, err := h.llmSettingsStatusResponse(ctx, orgID)
	if err != nil {
		h.internalError(ctx, w, "read llm settings status", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}
