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
	"time"
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

func TestReadinessProbeGPT5MiniRefusedMinimalLandsOnNone(t *testing.T) {
	server, efforts := lunaFake(t)
	f := &paramFakeOpenAI{server: server}
	outcome, code := readinessProbeOver(f, "gpt-5-mini")
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

// The catalog starts gpt-6-luna at none: the recorded refusals are never hit.
func TestReadinessProbeGPT6LunaStartsAtNoneAndPasses(t *testing.T) {
	server, efforts := lunaFake(t)
	outcome, code := readinessProbeOver(&paramFakeOpenAI{server: server}, "gpt-6-luna")
	wantReady(t, outcome, code)
	if got, want := efforts(), []string{"none", "none"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("efforts sent = %v, want %v", got, want)
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
	res := detailProberOver(slog.New(slog.NewJSONHandler(&buf, nil))).probeDetail(context.Background(), "openai", "gpt-5-mini", server.URL, "test-key")
	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil && m["level"] == "INFO" {
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
			outcome, code := readinessProbeOver(&paramFakeOpenAI{server: server}, "gpt-5-mini")
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
	outcome, code := readinessProbeOver(&paramFakeOpenAI{server: server}, "gpt-5-mini")
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

// seqFake refuses a request with the param refuse(body) names (400, error naming it); "" accepts.
// seq records "<effort>[-param...]" per request: the effort, then each of the two tested params the request lacks.
func seqFake(t *testing.T, refuse func(body map[string]any) string) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seq []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		round2 := isRound2Request(raw)
		effort, has := body["reasoning_effort"].(string)
		if !has {
			effort = "<absent>"
		}
		entry := effort
		if _, ok := body["parallel_tool_calls"]; !ok {
			entry += "-ptc"
		}
		if _, ok := body["max_completion_tokens"]; !ok {
			entry += "-mct"
		}
		if round2 {
			entry = "r2:" + entry
		}
		mu.Lock()
		seq = append(seq, entry)
		mu.Unlock()
		if param := refuse(body); param != "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(paramRefusal(param, "unsupported_value")))
			return
		}
		if !round2 {
			writeToolCallJSON(w)
			return
		}
		writeFinalAnswerJSON(w, `{"kind":"final_answer","value":{"nonce":"ready-v1"}}`)
	}))
	t.Cleanup(server.Close)
	return server, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), seq...) }
}

// Round 2 never carries parallel_tool_calls.
func r2suffix(s string) string {
	if s == "-ptc" {
		return s
	}
	return "-ptc" + s
}

func has(body map[string]any, k string) bool { _, ok := body[k]; return ok }

// CHAOS-12116: a ladder step refused for another bounded parameter takes the one-drop retry.
func TestReadinessProbeLadderStepTakesOneDropRetry(t *testing.T) {
	for _, other := range []string{"parallel_tool_calls", "max_completion_tokens"} {
		suffix := map[string]string{"parallel_tool_calls": "-ptc", "max_completion_tokens": "-mct"}[other]
		t.Run("none+"+other, func(t *testing.T) {
			// none + other: minimal is refused for effort; none carries the other param and is refused for it.
			server, seq := seqFake(t, func(b map[string]any) string {
				if b["reasoning_effort"] == "minimal" {
					return "reasoning_effort"
				}
				if has(b, other) {
					return other
				}
				return ""
			})
			res, _ := probeWithLog(server)
			wantReady(t, res.Outcome, res.SafeErrorCode)
			want := []string{"minimal", "none", "none" + suffix, "r2:none" + r2suffix(suffix)}
			if got := seq(); !reflect.DeepEqual(got, want) {
				t.Fatalf("none+%s: seq = %v, want %v", other, got, want)
			}
			if !reflect.DeepEqual(res.DroppedParams, []string{other}) {
				t.Fatalf("none+%s: DroppedParams = %v", other, res.DroppedParams)
			}

		})
		t.Run("omitted+"+other, func(t *testing.T) {
			// omitted + other: effort is refused whenever present; the omit request is refused for the other param.
			server, seq := seqFake(t, func(b map[string]any) string {
				if has(b, "reasoning_effort") {
					return "reasoning_effort"
				}
				if has(b, other) {
					return other
				}
				return ""
			})
			res, _ := probeWithLog(server)
			wantReady(t, res.Outcome, res.SafeErrorCode)
			want := []string{"minimal", "none", "<absent>", "<absent>" + suffix, "r2:<absent>" + r2suffix(suffix)}
			if got := seq(); !reflect.DeepEqual(got, want) {
				t.Fatalf("omitted+%s: seq = %v, want %v", other, got, want)
			}
			if !reflect.DeepEqual(res.DroppedParams, []string{other, "reasoning_effort"}) {
				t.Fatalf("omitted+%s: DroppedParams = %v", other, res.DroppedParams)
			}
		})
	}
}

// A drop retry refused again for reasoning_effort continues the ladder to omit.
func TestReadinessProbeDropRetryRefusedForEffortContinuesLadder(t *testing.T) {
	server, seq := seqFake(t, func(b map[string]any) string {
		if b["reasoning_effort"] == "minimal" && !has(b, "parallel_tool_calls") {
			return "reasoning_effort"
		}
		if has(b, "parallel_tool_calls") {
			return "parallel_tool_calls"
		}
		if has(b, "reasoning_effort") {
			return "reasoning_effort"
		}
		return ""
	})
	res, _ := probeWithLog(server)
	wantReady(t, res.Outcome, res.SafeErrorCode)
	want := []string{"minimal", "minimal-ptc", "none-ptc", "<absent>-ptc", "r2:<absent>-ptc"}
	if got := seq(); !reflect.DeepEqual(got, want) {
		t.Fatalf("seq = %v, want %v", got, want)
	}
}

// Every request refused: the round makes exactly readinessRoundMaxRequests requests, then fails classified.
func TestReadinessProbeEveryRequestRefusedStopsAtBound(t *testing.T) {
	if readinessRoundMaxRequests != 6 {
		t.Fatalf("readinessRoundMaxRequests = %d, want 6", readinessRoundMaxRequests)
	}
	server, seq := seqFake(t, func(b map[string]any) string {
		for _, k := range []string{"reasoning_effort", "parallel_tool_calls", "max_completion_tokens"} {
			if has(b, k) {
				return k
			}
		}
		return "temperature"
	})
	res, lines := probeWithLog(server)
	wantInvalidRequest(t, res.Outcome, res.SafeErrorCode)
	if got := len(seq()); got > readinessRoundMaxRequests {
		t.Fatalf("requests = %d, want <= %d: %v", got, readinessRoundMaxRequests, seq())
	}
	if len(lines) != len(seq()) {
		t.Fatalf("log lines = %d, requests = %d", len(lines), len(seq()))
	}
	// Longest path for a real model: gpt-6 sends no temperature, so 5 of the 6 capped requests.
	server, seq = seqFake(t, func(b map[string]any) string {
		for _, k := range []string{"reasoning_effort", "parallel_tool_calls", "max_completion_tokens", "temperature"} {
			if has(b, k) {
				return k
			}
		}
		return "model"
	})
	res, _ = probeWithLog(server)
	wantInvalidRequest(t, res.Outcome, res.SafeErrorCode)
	if got := len(seq()); got != readinessRoundMaxRequests-1 {
		t.Fatalf("requests = %d, want %d: %v", got, readinessRoundMaxRequests-1, seq())
	}
}

// CHAOS-12116: the round cap counts physical HTTP requests, retries included.
func TestReadinessProbeRoundCapCountsRetriedHTTPRequests(t *testing.T) {
	terminals := []struct {
		name   string
		status int
		code   string
	}{
		{"5xx", http.StatusInternalServerError, errCodeProviderUnavailable},
		{"429", http.StatusTooManyRequests, errCodeRateLimited},
		{"timeout", 0, errCodeTimeout},
	}
	for _, term := range terminals {
		for prior := 0; prior <= 4; prior++ {
			t.Run(fmt.Sprintf("%s/prior=%d", term.name, prior), func(t *testing.T) {
				var mu sync.Mutex
				count := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, _ := io.ReadAll(r.Body)
					var body map[string]any
					_ = json.Unmarshal(raw, &body)
					mu.Lock()
					count++
					n := count
					mu.Unlock()
					if n <= prior {
						for _, k := range []string{"reasoning_effort", "parallel_tool_calls", "max_completion_tokens"} {
							if has(body, k) {
								w.WriteHeader(http.StatusBadRequest)
								_, _ = w.Write([]byte(paramRefusal(k, "unsupported_value")))
								return
							}
						}
					}
					if term.status == 0 {
						select {
						case <-r.Context().Done():
						case <-time.After(2 * time.Second):
						}
						return
					}
					w.WriteHeader(term.status)
				}))
				t.Cleanup(server.Close)
				prober := newOpenAICompatibleReadinessProber(&http.Client{Timeout: 100 * time.Millisecond, Transport: &http.Transport{Proxy: nil}})
				res := prober.probeDetail(context.Background(), "openai", "gpt-6-luna", server.URL, "test-key")
				mu.Lock()
				got := count
				mu.Unlock()
				want := prior + maxProbeAttempts
				if want > readinessRoundMaxRequests {
					want = readinessRoundMaxRequests
				}
				if got > readinessRoundMaxRequests || got != want {
					t.Fatalf("requests = %d, want %d (cap %d)", got, want, readinessRoundMaxRequests)
				}
				if res.Outcome != readinessOutcomeFailed || res.SafeErrorCode == nil || *res.SafeErrorCode != term.code {
					t.Fatalf("outcome = %q code = %v, want failed/%s", res.Outcome, res.SafeErrorCode, term.code)
				}
			})
		}
	}
}

// A retryable failure followed by success still retries while the budget lasts.
func TestReadinessProbeRetryThenSuccessStillRetries(t *testing.T) {
	var mu sync.Mutex
	count := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		count++
		n := count
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !isRound2Request(raw) {
			writeToolCallJSON(w)
			return
		}
		writeFinalAnswerJSON(w, `{"kind":"final_answer","value":{"nonce":"ready-v1"}}`)
	}))
	t.Cleanup(server.Close)
	res := detailProberOver(nil).probeDetail(context.Background(), "openai", "gpt-6-luna", server.URL, "test-key")
	if res.Outcome != readinessOutcomeReady {
		t.Fatalf("outcome = %q, want ready", res.Outcome)
	}
	mu.Lock()
	defer mu.Unlock()
	if count != 3 {
		t.Fatalf("requests = %d, want 3 (500 retried, then round 1 and 2)", count)
	}
}
