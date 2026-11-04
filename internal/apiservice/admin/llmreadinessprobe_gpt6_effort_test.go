package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// The two OpenAI 400 bodies recorded for gpt-6-luna on askdev-1 (CHAOS-12116 reopened).
const (
	lunaRefusesMinimal         = `{"error":{"message":"'reasoning_effort' does not support 'minimal' with this model. Supported values are: 'none', 'low', 'medium', 'high', and 'xhigh'.","type":"invalid_request_error","param":"reasoning_effort","code":"unsupported_value"}}`
	lunaRefusesToolsWithEffort = `{"error":{"message":"Function tools with reasoning_effort are not supported for gpt-6-luna in /v1/chat/completions. To use function tools, use /v1/responses or set reasoning_effort to 'none'.","type":"invalid_request_error","param":"reasoning_effort","code":"unsupported_value"}}`
)

// lunaFake behaves as the recorded gpt-6-luna: with function tools in the
// request, reasoning_effort minimal is refused (body 1), an absent
// reasoning_effort is refused (body 2), and only "none" is accepted.
func lunaFake(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var efforts []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		effort, has := body["reasoning_effort"].(string)
		round2 := isRound2Request(raw)
		mu.Lock()
		if has {
			efforts = append(efforts, effort)
		} else {
			efforts = append(efforts, "<absent>")
		}
		mu.Unlock()
		_, tools := body["tools"]
		if !round2 && tools {
			switch {
			case effort == "minimal":
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(lunaRefusesMinimal))
				return
			case effort != "none":
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(lunaRefusesToolsWithEffort))
				return
			}
		}
		if round2 && effort == "minimal" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(lunaRefusesMinimal))
			return
		}
		if !round2 {
			writeToolCallJSON(w)
			return
		}
		writeFinalAnswerJSON(w, `{"kind":"final_answer","value":{"nonce":"ready-v1"}}`)
	}))
	t.Cleanup(server.Close)
	return server, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), efforts...) }
}

func TestReadinessProbeGPT6LunaPassesWithReasoningEffortNone(t *testing.T) {
	server, efforts := lunaFake(t)
	f := &paramFakeOpenAI{server: server}
	outcome, code := readinessProbeOver(f, "gpt-6-luna")
	wantReady(t, outcome, code)
	got := efforts()
	if len(got) != 3 {
		t.Fatalf("efforts sent = %v; want minimal refused once, then none in round 1 and none in round 2 (3 requests)", got)
	}
	last := got[len(got)-1]
	if last != "none" {
		t.Fatalf("efforts sent = %v; the last request must carry reasoning_effort none, not %q", got, last)
	}
	for _, e := range got[:len(got)-1] {
		if e == "<absent>" {
			t.Fatalf("efforts sent = %v; a refused reasoning_effort must be swapped for none, never dropped", got)
		}
	}
}

// A model that refuses every effort value ends the ladder with the param omitted.
func TestReadinessProbeRefusedEffortEndsOmitted(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{"reasoning_effort": paramRefusal("reasoning_effort", "unsupported_value")})
	outcome, code := readinessProbeOver(f, "gpt-5-mini")
	wantReady(t, outcome, code)
	if got := f.countKey("reasoning_effort", true); got != 0 {
		t.Fatalf("round 2 requests with reasoning_effort = %d, want 0", got)
	}
}

// Only a refusal of reasoning_effort is answered with none: another parameter's
// refusal that mentions 'none' drops that parameter and leaves the effort alone.
func TestReadinessProbeOtherParamRefusalMentioningNoneLeavesTheEffort(t *testing.T) {
	refusal := `{"error":{"message":"'parallel_tool_calls' does not support false. Supported values are: 'none'.","type":"invalid_request_error","param":"parallel_tool_calls","code":"unsupported_value"}}`
	f := newParamFakeOpenAI(t, map[string]string{"parallel_tool_calls": refusal})
	outcome, code := readinessProbeOver(f, "gpt-5-mini")
	wantReady(t, outcome, code)
	if got := f.countKey("parallel_tool_calls", true); got != 0 {
		t.Fatalf("round 2 requests with parallel_tool_calls = %d, want 0 (dropped)", got)
	}
	for _, c := range f.snapshot() {
		if v, ok := c.body["reasoning_effort"]; ok && v == "none" {
			t.Fatalf("a parallel_tool_calls refusal swapped reasoning_effort to none: %v", c.body)
		}
	}
}

// Only a 400 or 422 is a refusal the probe answers: a 500 whose body happens to name the parameter is not.
func TestReadinessProbeServerErrorNamingEffortAndNoneIsNotSwapped(t *testing.T) {
	var mu sync.Mutex
	effortRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		if _, has := body["reasoning_effort"]; has {
			mu.Lock()
			effortRequests++
			mu.Unlock()
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"'reasoning_effort' is not supported; try 'none'.","type":"invalid_request_error","param":"reasoning_effort","code":"x"}}`))
	}))
	t.Cleanup(server.Close)
	readinessProbeOver(&paramFakeOpenAI{server: server}, "gpt-5-mini")
	mu.Lock()
	defer mu.Unlock()
	if effortRequests != 1 {
		t.Fatalf("requests = %d, want 1 (the 500 is not a 400/422 refusal, so no swap request follows)", effortRequests)
	}
}

// ladderFake answers by effort ("<absent>" when the field is missing) and round: refuse(effort, round2)
// returns true to refuse with status and message, which always name reasoning_effort.
func ladderFake(t *testing.T, status int, message string, refuse func(effort string, round2 bool) bool) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var efforts []string
	body, _ := json.Marshal(map[string]any{"error": map[string]any{
		"message": message, "type": "invalid_request_error", "param": "reasoning_effort", "code": "unsupported_value",
	}})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(raw, &req)
		effort, has := req["reasoning_effort"].(string)
		if !has {
			effort = "<absent>"
		}
		round2 := isRound2Request(raw)
		mu.Lock()
		efforts = append(efforts, effort)
		mu.Unlock()
		if refuse(effort, round2) {
			w.WriteHeader(status)
			_, _ = w.Write(body)
			return
		}
		if !round2 {
			writeToolCallJSON(w)
			return
		}
		writeFinalAnswerJSON(w, `{"kind":"final_answer","value":{"nonce":"ready-v1"}}`)
	}))
	t.Cleanup(server.Close)
	return server, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), efforts...) }
}

func probeWithLog(server *httptest.Server) (readinessProbeResult, []map[string]any) {
	var buf bytes.Buffer
	res := detailProberOver(slog.New(slog.NewJSONHandler(&buf, nil))).probeDetail(context.Background(), "openai", "gpt-6-luna", server.URL, "test-key")
	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil {
			lines = append(lines, m)
		}
	}
	return res, lines
}

// (a) any 400/422 refusal of reasoning_effort, with any text, makes the next probe send none.
func TestReadinessProbeEffortRefusalAnyTextSendsNoneNext(t *testing.T) {
	messages := []string{
		"Do not set reasoning_effort to 'none'",
		"Supported values are: none, low, medium.",
		"",
		"'reasoning_effort' does not support 'minimal'. Supported values are: 'low'.",
	}
	for _, status := range []int{http.StatusBadRequest, http.StatusUnprocessableEntity} {
		for _, msg := range messages {
			server, efforts := ladderFake(t, status, msg, func(e string, _ bool) bool { return e == "minimal" })
			outcome, code := readinessProbeOver(&paramFakeOpenAI{server: server}, "gpt-6-luna")
			wantReady(t, outcome, code)
			if got, want := efforts(), []string{"minimal", "none", "none"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("status %d msg %q: efforts = %v, want %v", status, msg, got, want)
			}
		}
	}
}

// (b) none refused too: the next probe omits the param, and later rounds omit it.
func TestReadinessProbeNoneRefusedTooOmitsTheParam(t *testing.T) {
	server, efforts := ladderFake(t, http.StatusBadRequest, "nope", func(e string, _ bool) bool { return e != "<absent>" })
	res, _ := probeWithLog(server)
	wantReady(t, res.Outcome, res.SafeErrorCode)
	if got, want := efforts(), []string{"minimal", "none", "<absent>", "<absent>"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("efforts = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(res.DroppedParams, []string{"reasoning_effort"}) {
		t.Fatalf("DroppedParams = %v, want [reasoning_effort]", res.DroppedParams)
	}
}

// (c) all three steps refused: exactly 3 requests, classified failure, no 4th request.
func TestReadinessProbeAllLadderStepsRefusedStopsAtThree(t *testing.T) {
	server, efforts := ladderFake(t, http.StatusUnprocessableEntity, "nope", func(string, bool) bool { return true })
	res, lines := probeWithLog(server)
	wantInvalidRequest(t, res.Outcome, res.SafeErrorCode)
	if got, want := efforts(), []string{"minimal", "none", "<absent>"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("efforts = %v, want %v", got, want)
	}
	if len(lines) != 3 {
		t.Fatalf("log lines = %d, want 3", len(lines))
	}
}

// (d) a round already at none that is refused omits the param: at most 2 probes.
func TestReadinessProbeRefusedNoneRoundTwoOmitsInTwoProbes(t *testing.T) {
	server, efforts := ladderFake(t, http.StatusBadRequest, "nope", func(e string, round2 bool) bool {
		return e == "minimal" || (round2 && e == "none")
	})
	outcome, code := readinessProbeOver(&paramFakeOpenAI{server: server}, "gpt-6-luna")
	wantReady(t, outcome, code)
	if got, want := efforts(), []string{"minimal", "none", "none", "<absent>"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("efforts = %v, want %v", got, want)
	}
}

// (g) each failed step writes one Info line naming reasoning_effort with its attempt number.
func TestReadinessProbeLadderLogsEachFailedStep(t *testing.T) {
	server, _ := ladderFake(t, http.StatusBadRequest, "nope", func(e string, _ bool) bool { return e != "<absent>" })
	res, lines := probeWithLog(server)
	wantReady(t, res.Outcome, res.SafeErrorCode)
	if len(lines) != 2 {
		t.Fatalf("log lines = %d, want 2: %v", len(lines), lines)
	}
	for i, l := range lines {
		if l["error_param"] != "reasoning_effort" || l["level"] != "INFO" || l["round"] != float64(1) || l["attempt"] != float64(i+1) {
			t.Fatalf("line %d = %v, want INFO round 1 attempt %d error_param reasoning_effort", i, l, i+1)
		}
	}
}
