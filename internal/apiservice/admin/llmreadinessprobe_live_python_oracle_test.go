package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	}

	scriptPath := filepath.Join(root, "internal", "apiservice", "admin", "testdata", "llmreadinessoracle", "certify_oracle.py")

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			prober := newOpenAICompatibleReadinessProber()
			goOutcome, goSafeErrorCode := prober.probe(t.Context(), "openai", scenario.model, server.URL, "go-oracle-key")

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

// scriptedReadinessStub is the ONE stub both planes hit for every scenario
// (never two independent reimplementations of the same fake -- see
// AGENTS.md's oracle rules). Scenario is selected by the wire "model"
// field, which both readinessProber.probe and certify_oracle.py send
// unchanged on every round.
func scriptedReadinessStub(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Model    string `json:"model"`
		Messages []struct {
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
