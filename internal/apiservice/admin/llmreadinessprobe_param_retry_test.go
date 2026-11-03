package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type paramFakeCall struct {
	round2 bool
	body   map[string]any
}

// paramFakeOpenAI answers 400 to a request carrying a key in refuse (the
// error body names that key), and a valid readiness exchange otherwise.
type paramFakeOpenAI struct {
	server *httptest.Server
	mu     sync.Mutex
	calls  []paramFakeCall
}

func newParamFakeOpenAI(t *testing.T, refuse map[string]string) *paramFakeOpenAI {
	t.Helper()
	f := &paramFakeOpenAI{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		round2 := isRound2Request(raw)
		f.mu.Lock()
		f.calls = append(f.calls, paramFakeCall{round2: round2, body: body})
		f.mu.Unlock()
		for key, errBody := range refuse {
			if _, present := body[key]; present {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(errBody))
				return
			}
		}
		if !round2 {
			writeToolCallJSON(w)
			return
		}
		writeFinalAnswerJSON(w, `{"kind":"final_answer","value":{"nonce":"ready-v1"}}`)
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *paramFakeOpenAI) snapshot() []paramFakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]paramFakeCall(nil), f.calls...)
}

func (f *paramFakeOpenAI) countKey(key string, round2 bool) int {
	n := 0
	for _, c := range f.snapshot() {
		if _, ok := c.body[key]; ok && c.round2 == round2 {
			n++
		}
	}
	return n
}

func paramRefusal(param, code string) string {
	return fmt.Sprintf(`{"error":{"message":"Unsupported value: '%s' does not support 0 with this model.","type":"invalid_request_error","param":"%s","code":"%s"}}`, param, param, code)
}

func readinessProbeOver(f *paramFakeOpenAI, model string) (string, *string) {
	prober := newOpenAICompatibleReadinessProber(newReadinessHTTPClient(&http.Transport{Proxy: nil}))
	return prober.probe(context.Background(), "openai", model, f.server.URL, "test-key")
}

func wantReady(t *testing.T, outcome string, code *string) {
	t.Helper()
	if outcome != readinessOutcomeReady {
		got := "<nil>"
		if code != nil {
			got = *code
		}
		t.Fatalf("outcome = %q safe_error_code = %s, want ready", outcome, got)
	}
}

func wantInvalidRequest(t *testing.T, outcome string, code *string) {
	t.Helper()
	if outcome != readinessOutcomeFailed || code == nil || *code != errCodeInvalidRequest {
		t.Fatalf("outcome = %q safe_error_code = %v, want failed/invalid_request", outcome, code)
	}
}

// CHAOS-12116: gpt-6-luna refuses a non-default temperature; the probe must not send one.
func TestReadinessProbeGPT6LunaPassesWhenTemperatureRefused(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{"temperature": paramRefusal("temperature", "unsupported_value")})
	outcome, code := readinessProbeOver(f, "gpt-6-luna")
	wantReady(t, outcome, code)
	if n := f.countKey("temperature", false) + f.countKey("temperature", true); n != 0 {
		t.Fatalf("gpt-6-luna requests carrying temperature = %d, want 0", n)
	}
}

// An unknown-class model still gets the first guess (temperature 0); the refusal drops it and the round retries once.
func TestReadinessProbeDropsRefusedTemperatureAndRetries(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{"temperature": paramRefusal("temperature", "unsupported_value")})
	outcome, code := readinessProbeOver(f, "future-model-x")
	wantReady(t, outcome, code)
	if got := f.countKey("temperature", false); got != 1 {
		t.Fatalf("round 1 requests with temperature = %d, want exactly 1 (the refused one)", got)
	}
	if got := f.countKey("temperature", true); got != 0 {
		t.Fatalf("round 2 requests with temperature = %d, want 0 (the drop carries over)", got)
	}
}

func TestReadinessProbeDropsRefusedParallelToolCallsAndRetries(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{"parallel_tool_calls": paramRefusal("parallel_tool_calls", "unsupported_parameter")})
	outcome, code := readinessProbeOver(f, "future-model-x")
	wantReady(t, outcome, code)
	if got := f.countKey("parallel_tool_calls", false); got != 1 {
		t.Fatalf("round 1 requests with parallel_tool_calls = %d, want exactly 1", got)
	}
}

func TestReadinessProbeDropsRefusedReasoningEffortAndRetries(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{"reasoning_effort": paramRefusal("reasoning_effort", "unsupported_value")})
	outcome, code := readinessProbeOver(f, "gpt-5-mini")
	wantReady(t, outcome, code)
	if got := f.countKey("reasoning_effort", false); got != 1 {
		t.Fatalf("round 1 requests with reasoning_effort = %d, want exactly 1", got)
	}
	if got := f.countKey("reasoning_effort", true); got != 0 {
		t.Fatalf("round 2 requests with reasoning_effort = %d, want 0", got)
	}
}

func TestReadinessProbeSwapsRefusedMaxCompletionTokensForMaxTokens(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{"max_completion_tokens": paramRefusal("max_completion_tokens", "unsupported_parameter")})
	outcome, code := readinessProbeOver(f, "future-model-x")
	wantReady(t, outcome, code)
	for _, c := range f.snapshot() {
		if c.body["max_completion_tokens"] != nil {
			continue
		}
		if got, _ := c.body["max_tokens"].(float64); int(got) != readinessMaxOutputTokens {
			t.Fatalf("retried request max_tokens = %v, want %d", c.body["max_tokens"], readinessMaxOutputTokens)
		}
	}
	if got := f.countKey("max_tokens", true); got != 1 {
		t.Fatalf("round 2 requests with max_tokens = %d, want 1 (the swap carries over)", got)
	}
	if got := f.countKey("max_completion_tokens", true); got != 0 {
		t.Fatalf("round 2 requests with max_completion_tokens = %d, want 0", got)
	}
}

func TestReadinessProbeParamNamedOnlyInMessageIsDropped(t *testing.T) {
	msg := `{"error":{"message":"Unsupported parameter: 'temperature' is not supported with this model.","type":"invalid_request_error"}}`
	f := newParamFakeOpenAI(t, map[string]string{"temperature": msg})
	outcome, code := readinessProbeOver(f, "future-model-x")
	wantReady(t, outcome, code)
}

func TestReadinessProbeBadRequestWithoutParamStaysInvalidRequest(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{"temperature": `{"error":{"message":"The request was malformed.","type":"invalid_request_error","param":null,"code":null}}`})
	outcome, code := readinessProbeOver(f, "future-model-x")
	wantInvalidRequest(t, outcome, code)
	if got := len(f.snapshot()); got != 1 {
		t.Fatalf("requests = %d, want 1 (a 400 naming no parameter is not retried)", got)
	}
}

func TestReadinessProbeUnboundedParamStaysInvalidRequest(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{"tools": paramRefusal("tools", "unsupported_parameter")})
	outcome, code := readinessProbeOver(f, "future-model-x")
	wantInvalidRequest(t, outcome, code)
	if got := len(f.snapshot()); got != 1 {
		t.Fatalf("requests = %d, want 1 (a parameter outside the bounded set is not dropped)", got)
	}
}

func TestReadinessProbeSecondBadRequestAfterRetryFails(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{
		"temperature":         paramRefusal("temperature", "unsupported_value"),
		"parallel_tool_calls": paramRefusal("parallel_tool_calls", "unsupported_parameter"),
	})
	outcome, code := readinessProbeOver(f, "future-model-x")
	wantInvalidRequest(t, outcome, code)
	if got := len(f.snapshot()); got != 2 {
		t.Fatalf("requests = %d, want 2 (one refused, one retry, then stop)", got)
	}
}

func TestReadinessProbeGPT5MiniSendsNoTemperatureAndNeverRetries(t *testing.T) {
	f := newParamFakeOpenAI(t, nil)
	outcome, code := readinessProbeOver(f, "gpt-5-mini")
	wantReady(t, outcome, code)
	if got := len(f.snapshot()); got != 2 {
		t.Fatalf("requests = %d, want 2 (one per round, no retry)", got)
	}
	if n := f.countKey("temperature", false) + f.countKey("temperature", true); n != 0 {
		t.Fatalf("gpt-5-mini requests carrying temperature = %d, want 0", n)
	}
	if got := f.countKey("reasoning_effort", false); got != 1 {
		t.Fatalf("gpt-5-mini round 1 reasoning_effort sent %d times, want 1", got)
	}
}

func TestReadinessPrefixTablesTreatGPT6AsReasoningClass(t *testing.T) {
	for _, model := range []string{"gpt-6-luna", "GPT-6", " gpt-6-mini "} {
		if supportsTemperature(model) {
			t.Errorf("supportsTemperature(%q) = true, want false", model)
		}
		if got := reasoningEffort(model); got != "minimal" {
			t.Errorf("reasoningEffort(%q) = %q, want minimal", model, got)
		}
	}
	if !supportsTemperature("gpt-4o") || reasoningEffort("gpt-4o") != "" {
		t.Error("gpt-4o class changed")
	}
}

func detailProberOver(logger *slog.Logger) *openAICompatibleReadinessProber {
	prober := newOpenAICompatibleReadinessProber(newReadinessHTTPClient(&http.Transport{Proxy: nil}))
	prober.setLogger(logger)
	return prober
}

func TestReadinessProbeRecordNamesDroppedParam(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{"temperature": paramRefusal("temperature", "unsupported_value")})
	res := detailProberOver(nil).probeDetail(context.Background(), "openai", "future-model-x", f.server.URL, "test-key")
	wantReady(t, res.Outcome, res.SafeErrorCode)
	if !reflect.DeepEqual(res.DroppedParams, []string{"temperature"}) {
		t.Fatalf("DroppedParams = %v, want [temperature]", res.DroppedParams)
	}
	payload, err := json.Marshal(agentReadinessRecord{Outcome: res.Outcome, DroppedParams: res.DroppedParams})
	if err != nil || !strings.Contains(string(payload), `"dropped_params":["temperature"]`) {
		t.Fatalf("persisted record = %s (err %v), want dropped_params naming temperature", payload, err)
	}
}

func TestReadinessProbeFailureKeepsProviderErrorParamAndCode(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{"tools": paramRefusal("tools", "unsupported_parameter")})
	res := detailProberOver(nil).probeDetail(context.Background(), "openai", "future-model-x", f.server.URL, "test-key")
	wantInvalidRequest(t, res.Outcome, res.SafeErrorCode)
	if res.ErrorParam != "tools" || res.ErrorCode != "unsupported_parameter" {
		t.Fatalf("ErrorParam/ErrorCode = %q/%q, want tools/unsupported_parameter", res.ErrorParam, res.ErrorCode)
	}
}

func TestReadinessProbeNeverKeepsAnUnsafeProviderParam(t *testing.T) {
	hostile := `{"error":{"message":"bad","type":"invalid_request_error","param":"<script>alert(1)</script>","code":"x y"}}`
	f := newParamFakeOpenAI(t, map[string]string{"tools": hostile})
	res := detailProberOver(nil).probeDetail(context.Background(), "openai", "future-model-x", f.server.URL, "test-key")
	wantInvalidRequest(t, res.Outcome, res.SafeErrorCode)
	if res.ErrorParam != "" || res.ErrorCode != "" {
		t.Fatalf("ErrorParam/ErrorCode = %q/%q, want both dropped (not plain tokens)", res.ErrorParam, res.ErrorCode)
	}
}

func TestReadinessFailureMessageNamesTheRefusedParam(t *testing.T) {
	invalid := errCodeInvalidRequest
	timeout := errCodeTimeout
	cases := []struct {
		name   string
		record agentReadinessRecord
		want   string
	}{
		{"param known", agentReadinessRecord{SafeErrorCode: &invalid, ErrorParam: "tools"}, "The configured Ask Dev model refused the request parameter tools."},
		{"param unknown", agentReadinessRecord{SafeErrorCode: &invalid}, "The configured Ask Dev model rejected a required agent request capability."},
		{"param not a token", agentReadinessRecord{SafeErrorCode: &invalid, ErrorParam: "a b<c>"}, "The configured Ask Dev model rejected a required agent request capability."},
		{"other code ignores param", agentReadinessRecord{SafeErrorCode: &timeout, ErrorParam: "tools"}, "The configured Ask Dev model timed out during readiness."},
	}
	for _, c := range cases {
		if got := readinessFailureMessage(&c.record); got != c.want {
			t.Errorf("%s: message = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestReadinessProbeLogsOneInfoLinePerFailedRoundWithoutTheKey(t *testing.T) {
	const key = "canary-key-for-redaction-test"
	longMessage := "Unsupported value: 'tools' " + key + " " + strings.Repeat("x", 400)
	body := fmt.Sprintf(`{"error":{"message":%q,"type":"invalid_request_error","param":"tools","code":"unsupported_value"}}`, longMessage)
	f := newParamFakeOpenAI(t, map[string]string{"tools": body})
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	res := detailProberOver(logger).probeDetail(context.Background(), "openai", "future-model-x", f.server.URL+"/v1", key)
	wantInvalidRequest(t, res.Outcome, res.SafeErrorCode)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("log lines = %d, want 1:\n%s", len(lines), buf.String())
	}
	if strings.Contains(buf.String(), key) || strings.Contains(buf.String(), f.server.URL) {
		t.Fatalf("the log line carries the key or the base URL: %s", lines[0])
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["level"] != "INFO" || entry["round"] != float64(1) || entry["status"] != float64(400) ||
		entry["error_code"] != "unsupported_value" || entry["error_param"] != "tools" || entry["error_type"] != "invalid_request_error" {
		t.Fatalf("log fields = %v", entry)
	}
	if msg, _ := entry["error_message"].(string); len([]rune(msg)) != 200 {
		t.Fatalf("error_message length = %d, want 200", len([]rune(msg)))
	}
}

func TestReadinessProbeLogsEachRefusedAttemptWithItsRound(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{
		"temperature":         paramRefusal("temperature", "unsupported_value"),
		"parallel_tool_calls": paramRefusal("parallel_tool_calls", "unsupported_parameter"),
	})
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	res := detailProberOver(logger).probeDetail(context.Background(), "openai", "future-model-x", f.server.URL, "test-key")
	wantInvalidRequest(t, res.Outcome, res.SafeErrorCode)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("log lines = %d, want 2 (refused attempt, refused retry):\n%s", len(lines), buf.String())
	}
	for i, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if entry["round"] != float64(1) || entry["attempt"] != float64(i+1) {
			t.Fatalf("line %d round/attempt = %v/%v", i, entry["round"], entry["attempt"])
		}
	}
	if !reflect.DeepEqual(res.DroppedParams, []string{"temperature"}) && !reflect.DeepEqual(res.DroppedParams, []string{"parallel_tool_calls"}) {
		t.Fatalf("DroppedParams = %v, want exactly the one dropped before the second refusal", res.DroppedParams)
	}
}

func TestReadinessProbeAnthropicProberKeepsPlainProbeSeam(t *testing.T) {
	prober, ok := readinessProberFor("anthropic", nil)
	if !ok {
		t.Fatal("anthropic prober missing")
	}
	if _, detailed := prober.(readinessDetailProber); detailed {
		t.Fatal("the anthropic prober must stay on the plain probe seam")
	}
}

func TestReadinessProbeMessageParamMustMatchWholeToken(t *testing.T) {
	modelSending := map[string]string{
		"temperature":           "future-model-x",
		"reasoning_effort":      "gpt-5-mini",
		"parallel_tool_calls":   "future-model-x",
		"max_completion_tokens": "future-model-x",
	}
	for bounded, model := range modelSending {
		for _, suffix := range []string{"-extra", "_extra", ".extra", "[0]"} {
			msg := fmt.Sprintf(`{"error":{"message":"Unsupported parameter: '%s%s' is not supported.","type":"invalid_request_error"}}`, bounded, suffix)
			f := newParamFakeOpenAI(t, map[string]string{bounded: msg})
			outcome, code := readinessProbeOver(f, model)
			wantInvalidRequest(t, outcome, code)
			if got := f.countKey(bounded, false); got != 1 {
				t.Fatalf("%s%s: requests carrying %s = %d, want 1 (not retried)", bounded, suffix, bounded, got)
			}
		}
	}
}

func TestReadinessProbeMessageParamWithTrailingPunctuationIsStillDropped(t *testing.T) {
	msg := `{"error":{"message":"Unsupported parameter: temperature.","type":"invalid_request_error"}}`
	f := newParamFakeOpenAI(t, map[string]string{"temperature": msg})
	outcome, code := readinessProbeOver(f, "future-model-x")
	wantReady(t, outcome, code)
}

func TestReadinessProbeRedactsKeyAndBaseURLFromEveryProviderField(t *testing.T) {
	const key = "canary-key-for-redaction-test"
	for _, field := range []string{"message", "type", "param", "code"} {
		fields := map[string]string{"message": "bad request", "type": "invalid_request_error", "param": "tools", "code": "unsupported_parameter"}
		f := newParamFakeOpenAI(t, nil)
		leak := key + " " + f.server.URL + "/v1"
		if field == "param" || field == "code" {
			leak = key
		}
		fields[field] = leak
		body, _ := json.Marshal(map[string]any{"error": fields})
		f = newParamFakeOpenAI(t, map[string]string{"tools": string(body)})
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
		res := detailProberOver(logger).probeDetail(context.Background(), "openai", "future-model-x", f.server.URL+"/v1", key)
		wantInvalidRequest(t, res.Outcome, res.SafeErrorCode)
		for _, secret := range []string{key, f.server.URL} {
			if strings.Contains(buf.String(), secret) {
				t.Fatalf("%s carries %q: the log line leaks it: %s", field, secret, buf.String())
			}
			if strings.Contains(res.ErrorParam, secret) || strings.Contains(res.ErrorCode, secret) {
				t.Fatalf("%s carries %q: the record leaks it: %+v", field, secret, res)
			}
		}
		if (field == "param" && res.ErrorParam != "") || (field == "code" && res.ErrorCode != "") {
			t.Fatalf("%s held a secret but the record kept %q/%q, want it dropped", field, res.ErrorParam, res.ErrorCode)
		}
	}
}

func TestReadinessProbeRedactsBaseURLCredentials(t *testing.T) {
	secrets := providerSecrets("https://user:hunter2pass@example.test/v1/", "k-1234")
	got, hit := redactSecrets("saw hunter2pass and https://user:hunter2pass@example.test/v1 and k-1234", secrets)
	if !hit || strings.Contains(got, "hunter2pass") || strings.Contains(got, "k-1234") || strings.Contains(got, "example.test") {
		t.Fatalf("redacted = %q", got)
	}
}
