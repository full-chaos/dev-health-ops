package admin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestReadinessProbeTimeoutClassification is the Go-only counterpart to the
// live-python-oracle's fault scenarios (llmreadinessprobe_live_python_oracle_test.go)
// for the ONE safe_error_code that oracle deliberately does not cover: a
// real client-side deadline. A live round trip that must actually exceed a
// wall-clock timeout does not belong in a per-scenario oracle loop (it would
// make every run pay the full timeout, and a live Python subprocess call
// against a stalling stub is exactly the "duration ≈ a timeout constant"
// shape AGENTS.md's timeout-fallback-masquerade warning is about) — this
// test instead proves the CLASSIFICATION (context.DeadlineExceeded ->
// "timeout", matching errors.py's LLMTimeoutError -> AgentProviderErrorCode.TIMEOUT)
// directly and fast, with a deliberately tiny client timeout against a stub
// that never responds.
func TestReadinessProbeTimeoutClassification(t *testing.T) {
	// Defer order matters: httptest.Server.Close blocks until every
	// outstanding handler goroutine returns, and this handler never returns
	// on its own -- close(stalls) MUST unblock it before Close is called, so
	// this defer is registered (and therefore LIFO-runs) AFTER server's.
	stalls := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-stalls
	}))
	defer server.Close()
	defer close(stalls)

	prober := &openAICompatibleReadinessProber{client: &http.Client{Timeout: 20 * time.Millisecond}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	outcome, safeErrorCode := prober.probe(ctx, "openai", "scripted-ready", server.URL, "test-key")

	if outcome != readinessOutcomeFailed {
		t.Fatalf("outcome = %q, want %q", outcome, readinessOutcomeFailed)
	}
	if safeErrorCode == nil || *safeErrorCode != errCodeTimeout {
		t.Fatalf("safe_error_code = %v, want %q", safeErrorCode, errCodeTimeout)
	}
}

// TestReadinessFingerprintChangesOnInputChange is the D2839 item 2 required
// test: since this port's fingerprint is a PINNED DIVERGENCE from Python's
// production_runtime._readiness_fingerprint (credential-only inputs, no
// Ask Dev tool-registry digests -- see llmreadinessprobe.go's file doc
// comment), this test is what actually pins the contract instead: the
// fingerprint changes when ANY of its declared inputs changes, and only
// when one of them does.
func TestReadinessFingerprintChangesOnInputChange(t *testing.T) {
	base := readinessFingerprint("openai", "gpt-5-mini", "https://api.openai.com/v1", "sk-test-key")

	variants := map[string]string{
		"provider": readinessFingerprint("azure-openai", "gpt-5-mini", "https://api.openai.com/v1", "sk-test-key"),
		"model":    readinessFingerprint("openai", "gpt-5-nano", "https://api.openai.com/v1", "sk-test-key"),
		"base_url": readinessFingerprint("openai", "gpt-5-mini", "https://api.openai.com/v2", "sk-test-key"),
		"api_key":  readinessFingerprint("openai", "gpt-5-mini", "https://api.openai.com/v1", "sk-different-key"),
	}
	for name, variant := range variants {
		if variant == base {
			t.Errorf("changing %s did not change the fingerprint", name)
		}
	}

	repeat := readinessFingerprint("openai", "gpt-5-mini", "https://api.openai.com/v1", "sk-test-key")
	if repeat != base {
		t.Fatalf("fingerprint is not deterministic: %q != %q", repeat, base)
	}
}

// TestCertifiedByoAgentProvidersMatchesPython pins the set to policy.py's
// CERTIFIED_BYO_AGENT_PROVIDERS = frozenset({"openai"}) -- narrower than
// llmorgsettings.IsKnownProvider, which also accepts anthropic/gemini/qwen/
// local/ollama/lmstudio for the generic (non-Ask-Dev) BYO LLM surface.
func TestCertifiedByoAgentProvidersMatchesPython(t *testing.T) {
	if _, ok := certifiedByoAgentProviders["openai"]; !ok {
		t.Error(`"openai" must be certified`)
	}
	for _, other := range []string{"anthropic", "gemini", "qwen", "local", "ollama", "lmstudio", "", "auto", "mock", "none"} {
		if _, ok := certifiedByoAgentProviders[other]; ok {
			t.Errorf("%q must not be certified (Ask Dev binary-transport gate is openai-only)", other)
		}
	}
	if len(certifiedByoAgentProviders) != 1 {
		t.Errorf("len(certifiedByoAgentProviders) = %d, want 1 (openai only)", len(certifiedByoAgentProviders))
	}
}

// isRound2Request reports whether a decoded wire request body carries a
// "tool" role message -- the same round-detector llmreadinessprobe_live_python_oracle_test.go's
// stub uses to tell round 1 (the tool call) from round 2 (the final answer).
func isRound2Request(body []byte) bool {
	var req struct {
		Messages []struct {
			Role string `json:"role"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return false
	}
	for _, m := range req.Messages {
		if m.Role == "tool" {
			return true
		}
	}
	return false
}

func writeToolCallJSON(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":null,` +
		`"tool_calls":[{"id":"call-1","type":"function","function":{"name":"readiness_echo_v1","arguments":"{\"nonce\":\"ready-v1\"}"}}]}}]}`))
}

func writeFinalAnswerJSON(w http.ResponseWriter, contentJSON string) {
	body, _ := json.Marshal(map[string]any{"choices": []map[string]any{{
		"index": 0, "finish_reason": "stop",
		"message": map[string]any{"role": "assistant", "content": contentJSON},
	}}})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// TestReadinessProbeDoesNotFollowRedirects is codex r1 P1 (CHAOS-6976): a
// redirect response must never be followed automatically -- Python's
// hardened client (_http.py) disables redirects unconditionally, since the
// SSRF-validated base_url is not the same thing as wherever a 3xx points.
// The "private" target here stands in for an internal host the redirect
// could otherwise reach.
func TestReadinessProbeDoesNotFollowRedirects(t *testing.T) {
	var privateHits int32
	private := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&privateHits, 1)
		writeToolCallJSON(w)
	}))
	defer private.Close()

	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, private.URL, http.StatusTemporaryRedirect)
	}))
	defer public.Close()

	prober := newOpenAICompatibleReadinessProber()
	outcome, safeErrorCode := prober.probe(context.Background(), "openai", "scripted-redirect", public.URL, "test-key")

	if atomic.LoadInt32(&privateHits) != 0 {
		t.Fatalf("the redirect was followed: private target was hit %d time(s)", privateHits)
	}
	if outcome != readinessOutcomeFailed {
		t.Fatalf("outcome = %q, want %q (a 3xx is not a 2xx completion)", outcome, readinessOutcomeFailed)
	}
	if safeErrorCode == nil || *safeErrorCode != errCodeProviderUnavailable {
		t.Fatalf("safe_error_code = %v, want %q (an unfollowed 3xx falls to the catch-all)", safeErrorCode, errCodeProviderUnavailable)
	}
}

// TestReadinessProbeRejectsExtraEnvelopeField is codex r1 P1 (CHAOS-6976):
// openai_compatible.py's _validate_envelope_fields rejects a final-answer
// payload carrying any field beyond the compact {"kind","value"} pair or
// the full 9-field DECISION_FIELDS set -- a stray extra field must not be
// silently ignored and certified ready.
func TestReadinessProbeRejectsExtraEnvelopeField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !isRound2Request(body) {
			writeToolCallJSON(w)
			return
		}
		writeFinalAnswerJSON(w, `{"kind":"final_answer","value":{"nonce":"ready-v1"},"extra":"unexpected"}`)
	}))
	defer server.Close()

	prober := newOpenAICompatibleReadinessProber()
	outcome, safeErrorCode := prober.probe(context.Background(), "openai", "scripted-extra-field", server.URL, "test-key")

	if outcome != readinessOutcomeFailed {
		t.Fatalf("outcome = %q, want %q", outcome, readinessOutcomeFailed)
	}
	if safeErrorCode == nil || *safeErrorCode != errCodeInvalidResponse {
		t.Fatalf("safe_error_code = %v, want %q", safeErrorCode, errCodeInvalidResponse)
	}
}

// TestReadinessProbeClassifiesQuotaExhaustionNotRateLimit is codex r1 P1
// (CHAOS-6976): errors.py's classify_provider_error checks
// insufficient_quota/"current quota" BEFORE its rate-limit branch, even
// though both can carry HTTP 429 -- quota exhaustion is
// provider_not_configured (LLMAuthError), never rate_limited.
func TestReadinessProbeClassifiesQuotaExhaustionNotRateLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"type":"insufficient_quota","message":"You exceeded your current quota, please check your plan and billing details."}}`))
	}))
	defer server.Close()

	prober := newOpenAICompatibleReadinessProber()
	outcome, safeErrorCode := prober.probe(context.Background(), "openai", "scripted-quota", server.URL, "test-key")

	if outcome != readinessOutcomeFailed {
		t.Fatalf("outcome = %q, want %q", outcome, readinessOutcomeFailed)
	}
	if safeErrorCode == nil || *safeErrorCode != errCodeProviderNotConfigured {
		t.Fatalf("safe_error_code = %v, want %q (not %q)", safeErrorCode, errCodeProviderNotConfigured, errCodeRateLimited)
	}
}

// TestReadinessProbeRetriesATransientFailure is codex r1 P1 (CHAOS-6976):
// Python's openai SDK retries a transient failure (max_retries=2, so up to
// 3 total attempts) before giving up -- a single 500 that recovers on retry
// must still certify ready, not fail on the first attempt alone.
func TestReadinessProbeRetriesATransientFailure(t *testing.T) {
	var round1Attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !isRound2Request(body) {
			if atomic.AddInt32(&round1Attempts, 1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"type":"server_error","message":"transient failure"}}`))
				return
			}
			writeToolCallJSON(w)
			return
		}
		writeFinalAnswerJSON(w, `{"kind":"final_answer","value":{"nonce":"ready-v1"}}`)
	}))
	defer server.Close()

	prober := newOpenAICompatibleReadinessProber()
	outcome, safeErrorCode := prober.probe(context.Background(), "openai", "scripted-transient-then-ready", server.URL, "test-key")

	if outcome != readinessOutcomeReady {
		t.Fatalf("outcome = %q (safe_error_code=%v), want %q after retry recovers", outcome, safeErrorCode, readinessOutcomeReady)
	}
	if attempts := atomic.LoadInt32(&round1Attempts); attempts < 2 {
		t.Fatalf("round 1 was attempted %d time(s), want at least 2 (the retry)", attempts)
	}
}

// TestReadinessMessagesMatchPython pins the two 404 message literals to
// production_runtime.py's resolve_byo_certification_provider (settings.py:
// 352-466's DevRuntimeUnavailable.safe_message, surfaced as the HTTPException
// detail verbatim).
func TestReadinessMessagesMatchPython(t *testing.T) {
	if readinessNoBYOConfigMessage != "No BYO LLM configuration is saved for this organization." {
		t.Errorf("readinessNoBYOConfigMessage = %q", readinessNoBYOConfigMessage)
	}
	if readinessModelNotSupportedMessage != "The configured Ask Dev model is not supported." {
		t.Errorf("readinessModelNotSupportedMessage = %q", readinessModelNotSupportedMessage)
	}
}
