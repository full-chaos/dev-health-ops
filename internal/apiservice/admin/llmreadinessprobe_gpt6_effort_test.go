package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

// A model that refuses minimal but does not offer none keeps the old behaviour: drop and retry once.
func TestReadinessProbeRefusedEffortWithoutANoneOfferIsStillDropped(t *testing.T) {
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

// A round already sending none that is refused (round 2 here) falls back to dropping the field.
func TestReadinessProbeRefusedNoneIsDropped(t *testing.T) {
	refusal := `{"error":{"message":"'reasoning_effort' does not support 'none' here. Supported values are: 'none'.","type":"invalid_request_error","param":"reasoning_effort","code":"unsupported_value"}}`
	var mu sync.Mutex
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		effort, has := body["reasoning_effort"].(string)
		mu.Lock()
		if has {
			seen = append(seen, effort)
		} else {
			seen = append(seen, "<absent>")
		}
		mu.Unlock()
		if has && isRound2Request(raw) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(refusal))
			return
		}
		if has && effort == "minimal" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(lunaRefusesMinimal))
			return
		}
		if !isRound2Request(raw) {
			writeToolCallJSON(w)
			return
		}
		writeFinalAnswerJSON(w, `{"kind":"final_answer","value":{"nonce":"ready-v1"}}`)
	}))
	t.Cleanup(server.Close)
	outcome, code := readinessProbeOver(&paramFakeOpenAI{server: server}, "gpt-6-luna")
	wantReady(t, outcome, code)
	mu.Lock()
	defer mu.Unlock()
	if seen[len(seen)-1] != "<absent>" {
		t.Fatalf("efforts sent = %v; a refused none must end in a dropped field", seen)
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

func TestNoneListedAsOffer(t *testing.T) {
	cases := []struct {
		name, msg string
		want      bool
	}{
		{"single quotes list", "'reasoning_effort' does not support 'minimal'. Supported values are: 'none', 'low', 'medium', 'high', and 'xhigh'.", true},
		{"double quotes list", `Supported values are: "none", "low".`, true},
		{"case-insensitive header", `SUPPORTED VALUES ARE: 'None', 'low'.`, true},
		{"tools form single", "use /v1/responses or set reasoning_effort to 'none'.", true},
		{"tools form double", `set reasoning_effort to "none".`, true},
		{"negative mention", "'reasoning_effort' does not support 'none' with this model. Supported values are: 'low', 'medium'.", false},
		{"try mention", "unsupported; try 'none'.", false},
		{"list without none", "Supported values are: 'low', 'high'.", false},
		{"none after list sentence", "Supported values are: 'low'. Do not use 'none'.", false},
		{"set to other value", "set reasoning_effort to 'low'.", false},
		{"empty", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := noneListedAsOffer(c.msg); got != c.want {
				t.Fatalf("noneListedAsOffer(%q) = %v, want %v", c.msg, got, c.want)
			}
		})
	}
}

// refuseEffortFake refuses every reasoning_effort other than "none" with the given status and message.
func refuseEffortFake(t *testing.T, status int, message string) (*httptest.Server, func() []string) {
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
		mu.Lock()
		if has {
			efforts = append(efforts, effort)
		} else {
			efforts = append(efforts, "<absent>")
		}
		mu.Unlock()
		if has && effort != "none" {
			w.WriteHeader(status)
			_, _ = w.Write(body)
			return
		}
		if !isRound2Request(raw) {
			writeToolCallJSON(w)
			return
		}
		writeFinalAnswerJSON(w, `{"kind":"final_answer","value":{"nonce":"ready-v1"}}`)
	}))
	t.Cleanup(server.Close)
	return server, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), efforts...) }
}

func TestReadinessProbeNoneRefusedAsUnsupportedIsNeverSent(t *testing.T) {
	msg := "'reasoning_effort' does not support 'none' with this model. Supported values are: 'low', 'medium'."
	for _, status := range []int{http.StatusBadRequest, http.StatusUnprocessableEntity} {
		server, efforts := refuseEffortFake(t, status, msg)
		outcome, code := readinessProbeOver(&paramFakeOpenAI{server: server}, "gpt-6-luna")
		wantReady(t, outcome, code)
		for _, e := range efforts() {
			if e == "none" {
				t.Fatalf("status %d: efforts sent = %v; none was not offered and must not be sent", status, efforts())
			}
		}
	}
}

func TestReadinessProbeDoubleQuotedSupportedListRetriesWithNone(t *testing.T) {
	server, efforts := refuseEffortFake(t, http.StatusBadRequest, `'reasoning_effort' does not support 'minimal'. Supported values are: "none", "low", "medium".`)
	outcome, code := readinessProbeOver(&paramFakeOpenAI{server: server}, "gpt-6-luna")
	wantReady(t, outcome, code)
	got := efforts()
	if len(got) == 0 || got[len(got)-1] != "none" {
		t.Fatalf("efforts sent = %v; the retry must carry none", got)
	}
}
