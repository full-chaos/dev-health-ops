package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// TestReadinessProbeMatchesLivePython is the CHAOS-6976 differential oracle
// (D2839 item 6): it runs THIS PORT'S readinessProber and readiness.py's
// REAL, UNMODIFIED AgentReadinessService.certify/OpenAICompatibleAgentProvider
// (testdata/llmreadinessoracle/certify_oracle.py -- see that file's doc
// comment for why it calls certify() directly rather than going through the
// full HTTP route: the settings-resolution/SSRF-gate layer around it is
// UNCHANGED by this port and already covered by CHAOS-6252a's own
// venue-oracle test, and SSRF-blocks a local stub base_url by design) against
// ONE shared stub server, per scenario, and diffs outcome/safe_error_code.
//
// Scenarios cover "ready" and every safe_error_code this route can persist
// EXCEPT "timeout": a real client-side deadline is exercised instead by
// TestReadinessProbeTimeoutClassification (Go-only, fast, no live Python) --
// see that test's doc comment for why a live round-trip timeout scenario
// does not belong in this oracle.
func TestReadinessProbeMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	root := repoRootForReadinessOracle(t)
	python := pyoracle.Resolve(t, root)
	versionOut, versionErr := exec.Command(python, pyoracle.VersionProbeArgs...).Output()
	pyoracle.RequireDeployed(t, python, versionOut, versionErr)

	server := httptest.NewServer(http.HandlerFunc(scriptedReadinessStub))
	defer server.Close()

	scenarios := []struct {
		name  string
		model string
	}{
		{"ready", "scripted-ready"},
		{"unauthorized -> provider_not_configured", "scripted-unauthorized"},
		{"rate limited -> rate_limited", "scripted-ratelimited"},
		{"bad request -> invalid_request", "scripted-badrequest"},
		{"model not found -> model_not_supported", "scripted-modelnotfound"},
		{"server error -> provider_unavailable", "scripted-servererror"},
		{"two tool calls -> provider_contract_violation", "scripted-contractviolation"},
		{"finish_reason length -> output_exhausted", "scripted-outputexhausted"},
		{"malformed final answer -> invalid_response", "scripted-invalidresponse"},
		// D2908 condition 2 (codex r1's 4 P1s, each as its own oracle case
		// against the live Python producer on the SAME stub -- red on
		// 38f494576, green on fd13de082's fix):
		{"extra envelope field -> invalid_response", "scripted-extrafield"},
		{"quota exhaustion (429) -> provider_not_configured, not rate_limited", "scripted-quota"},
		{"one transient 500 then success -> ready", "scripted-transient"},
		{"redirect is not followed -> provider_unavailable", "scripted-redirect"},
	}

	scriptPath := filepath.Join(root, "internal", "apiservice", "admin", "testdata", "llmreadinessoracle", "certify_oracle.py")

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// The "transient" scenario is STATEFUL (round 1 fails once, then
			// succeeds): reset it immediately before EACH plane's own run so
			// Go and Python each see the identical fresh sequence, never one
			// plane consuming the other's retry state.
			if scenario.model == "scripted-transient" {
				atomic.StoreInt32(&transientRound1Attempts, 0)
			}
			prober := newOpenAICompatibleReadinessProber(nil)
			goOutcome, goSafeErrorCode := prober.probe(t.Context(), "openai", scenario.model, server.URL, "go-oracle-key")

			if scenario.model == "scripted-transient" {
				atomic.StoreInt32(&transientRound1Attempts, 0)
			}
			cmd := exec.Command(python, scriptPath, server.URL, scenario.model)
			cmd.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("python producer: %v\n%s", err, out)
			}
			var pyResult struct {
				Outcome       string  `json:"outcome"`
				SafeErrorCode *string `json:"safe_error_code"`
			}
			lastLine := lastNonEmptyLine(string(out))
			if jsonErr := json.Unmarshal([]byte(lastLine), &pyResult); jsonErr != nil {
				t.Fatalf("decode python producer output: %v\n%s", jsonErr, out)
			}

			if goOutcome != pyResult.Outcome {
				t.Errorf("outcome: go=%q python=%q", goOutcome, pyResult.Outcome)
			}
			goCode, pyCode := "", ""
			if goSafeErrorCode != nil {
				goCode = *goSafeErrorCode
			}
			if pyResult.SafeErrorCode != nil {
				pyCode = *pyResult.SafeErrorCode
			}
			if goCode != pyCode {
				t.Errorf("safe_error_code: go=%q python=%q", goCode, pyCode)
			}
		})
	}

	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if t.Failed() {
		// A mismatch was found (t.Errorf inside a subtest marks the parent
		// failed too, but execution still reaches here): never write an
		// "executed" proof for a run that did not actually agree with
		// Python. ci/check_go.sh's own `go test` exit-code check already
		// halts the verb on this failure -- this guard is for anyone
		// running the test directly and inspecting the proof dir by hand.
		return
	}
	if err := os.WriteFile(filepath.Join(proof, "admin-llmreadiness-probe"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func lastNonEmptyLine(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.TrimSpace(lines[i]) != "" {
			return lines[i]
		}
	}
	return ""
}

func repoRootForReadinessOracle(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	dir := wd
	for i := 0; i < 8; i++ {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("could not locate repo root (go.mod) from %s", wd)
	return ""
}

// transientRound1Attempts backs the "scripted-transient" scenario: round 1
// fails on its FIRST call and succeeds on every call after. Reset (by the
// test, between each plane's run) via atomic.StoreInt32 -- see the
// scenario loop's own comment on why this must be reset per plane.
var transientRound1Attempts int32

// redirectTargetPath is the "scripted-redirect" scenario's Location: it
// simulates an SSRF target that WOULD happily certify readiness if a
// client actually followed the redirect there -- so a client that follows
// it observes "ready" (wrong), and one that correctly refuses (both
// production planes) observes the 3xx itself instead. Path-routed
// directly in scriptedReadinessStub, ahead of every other dispatch, since
// a followed redirect carries the ORIGINAL round 1 body (307 preserves
// method+body) to this same handler.
const redirectTargetPath = "/redirected-target"

// scriptedReadinessStub is the ONE stub both planes hit for every scenario
// (never two independent reimplementations of the same fake -- see
// AGENTS.md's oracle rules). Scenario is selected by the wire "model"
// field, which both readinessProber.probe and certify_oracle.py send
// unchanged on every round.
func scriptedReadinessStub(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == redirectTargetPath {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chatcmpl-redirected","object":"chat.completion","created":1785283200,"model":"scripted-redirect",` +
			`"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":null,` +
			`"tool_calls":[{"id":"scripted-call-redirected","type":"function","function":{"name":"readiness_echo_v1","arguments":"{\"nonce\":\"ready-v1\"}"}}]}}],` +
			`"usage":{"prompt_tokens":7,"completion_tokens":5,"total_tokens":12}}`))
		return
	}
	var req struct {
		Model      string `json:"model"`
		ToolChoice string `json:"tool_choice"`
		Messages   []struct {
			Role string `json:"role"`
		} `json:"messages"`
	}
	body, _ := jsonDecodeBody(r)
	_ = json.Unmarshal(body, &req)
	round2 := false
	for _, m := range req.Messages {
		if m.Role == "tool" {
			round2 = true
		}
	}

	// D2908 condition 3a: assert the wire tool_choice matches
	// build_completion_request's own rule (openai_compatible.py:1016-1018)
	// -- "required" while a tool is still pending (round 1) and omitted
	// once a final answer is allowed (round 2, tools=[]) -- on EVERY
	// request either plane sends. A caller (Go OR Python) that gets this
	// wrong degrades to a 500 here rather than the mismatch going
	// unnoticed, which is exactly the class of gap a hand-mutated
	// tool_choice slipped past in r1's own P3 finding.
	wantToolChoice := "required"
	if round2 {
		wantToolChoice = ""
	}
	if req.ToolChoice != wantToolChoice {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"type":"server_error","message":"oracle stub: unexpected tool_choice"}}`))
		return
	}

	writeJSON := func(status int, payload any) {
		encoded, _ := json.Marshal(payload)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(encoded)
	}
	completion := func(message map[string]any, finishReason string) {
		writeJSON(200, map[string]any{
			"id":      "chatcmpl-oracle",
			"object":  "chat.completion",
			"created": 1785283200,
			"model":   req.Model,
			"choices": []map[string]any{
				{"index": 0, "message": message, "finish_reason": finishReason},
			},
			"usage": map[string]any{"prompt_tokens": 7, "completion_tokens": 5, "total_tokens": 12},
		})
	}
	toolCallMessage := func() map[string]any {
		return map[string]any{
			"role":    "assistant",
			"content": nil,
			"tool_calls": []map[string]any{
				{
					"id":   "scripted-call-oracle",
					"type": "function",
					"function": map[string]any{
						"name":      "readiness_echo_v1",
						"arguments": `{"nonce":"ready-v1"}`,
					},
				},
			},
		}
	}
	finalAnswerMessage := func() map[string]any {
		return map[string]any{
			"role":    "assistant",
			"content": `{"kind":"final_answer","value":{"nonce":"ready-v1"}}`,
		}
	}

	switch req.Model {
	case "scripted-ready":
		if !round2 {
			completion(toolCallMessage(), "tool_calls")
		} else {
			completion(finalAnswerMessage(), "stop")
		}
	case "scripted-unauthorized":
		writeJSON(401, map[string]any{"error": map[string]any{"type": "invalid_request_error", "code": "invalid_api_key", "message": "Incorrect API key provided"}})
	case "scripted-ratelimited":
		writeJSON(429, map[string]any{"error": map[string]any{"type": "rate_limit_error", "message": "Rate limit reached"}})
	case "scripted-badrequest":
		writeJSON(400, map[string]any{"error": map[string]any{"type": "invalid_request_error", "message": "Unsupported parameter"}})
	case "scripted-modelnotfound":
		writeJSON(404, map[string]any{"error": map[string]any{"type": "invalid_request_error", "code": "model_not_found", "message": "The model does not exist"}})
	case "scripted-servererror":
		writeJSON(500, map[string]any{"error": map[string]any{"type": "server_error", "message": "Internal server error"}})
	case "scripted-contractviolation":
		msg := toolCallMessage()
		msg["tool_calls"] = append(msg["tool_calls"].([]map[string]any), map[string]any{
			"id":   "scripted-call-oracle-2",
			"type": "function",
			"function": map[string]any{
				"name":      "readiness_echo_v1",
				"arguments": `{"nonce":"ready-v1"}`,
			},
		})
		completion(msg, "tool_calls")
	case "scripted-outputexhausted":
		if !round2 {
			completion(toolCallMessage(), "length")
		} else {
			completion(finalAnswerMessage(), "length")
		}
	case "scripted-invalidresponse":
		if !round2 {
			completion(toolCallMessage(), "tool_calls")
		} else {
			completion(map[string]any{"role": "assistant", "content": "not json"}, "stop")
		}
	case "scripted-extrafield":
		// D2908 condition 2 (codex r1 P1 #2): round 2's final answer carries
		// an extra top-level field beyond the compact {kind,value} pair --
		// openai_compatible.py's _validate_envelope_fields rejects this;
		// this port's normalizeRound2Decision must too.
		if !round2 {
			completion(toolCallMessage(), "tool_calls")
		} else {
			completion(map[string]any{
				"role":    "assistant",
				"content": `{"kind":"final_answer","value":{"nonce":"ready-v1"},"extra":"unexpected"}`,
			}, "stop")
		}
	case "scripted-quota":
		// D2908 condition 2 (codex r1 P1 #4): a 429 carrying quota-exhaustion
		// text must classify as provider_not_configured, never rate_limited
		// (errors.py checks quota text before its rate-limit branch).
		writeJSON(429, map[string]any{"error": map[string]any{"type": "insufficient_quota", "message": "You exceeded your current quota, please check your plan and billing details."}})
	case "scripted-transient":
		// D2908 condition 2 (codex r1 P1 #3): round 1's FIRST attempt fails
		// with a transient 500; every attempt after succeeds. Both planes'
		// own SDK/client retry budget must recover from this to "ready" --
		// see the scenario loop's reset of transientRound1Attempts before
		// each plane's run.
		if !round2 {
			if atomic.AddInt32(&transientRound1Attempts, 1) == 1 {
				writeJSON(500, map[string]any{"error": map[string]any{"type": "server_error", "message": "transient failure"}})
				return
			}
			completion(toolCallMessage(), "tool_calls")
		} else {
			completion(finalAnswerMessage(), "stop")
		}
	case "scripted-redirect":
		// D2908 condition 2 (codex r1 P1 #1): a 3xx must never be followed
		// automatically by either plane's HTTP client -- both must observe
		// the redirect response itself (a non-2xx) and classify it, never
		// silently chase it to wherever Location points. The Go-only
		// TestReadinessProbeDoesNotFollowRedirects additionally proves a
		// redirect target is never actually CONTACTED; this oracle case
		// proves outcome/safe_error_code PARITY with live Python for the
		// case where it isn't.
		// A relative Location is valid (RFC 7231 §7.1.2) and resolves
		// against this same stub -- see redirectTargetPath's doc comment
		// for why the target is a live, "compromised" endpoint rather
		// than an unreachable one: outcome parity must actually
		// DISCRIMINATE a client that follows from one that doesn't, not
		// just agree because neither can reach anything either way.
		w.Header().Set("Location", redirectTargetPath)
		w.WriteHeader(http.StatusTemporaryRedirect)
	default:
		writeJSON(500, map[string]any{"error": map[string]any{"type": "server_error", "message": "unknown scenario"}})
	}
}

func jsonDecodeBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := r.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			break
		}
	}
	return buf, nil
}
