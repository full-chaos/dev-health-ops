package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
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
