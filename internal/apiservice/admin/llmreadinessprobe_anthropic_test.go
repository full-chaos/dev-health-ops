package admin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type anthropicDoerFunc func(*http.Request) (*http.Response, error)

func (f anthropicDoerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func anthropicResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}

const anthropicToolUseBody = `{"id":"msg_1","type":"message","role":"assistant","stop_reason":"tool_use","content":[` +
	`{"type":"tool_use","id":"toolu_01","name":"readiness_echo_v1","input":{"nonce":"ready-v1"}}]}`

const anthropicFinalBody = `{"id":"msg_2","type":"message","role":"assistant","stop_reason":"end_turn","content":[` +
	`{"type":"text","text":"{\"kind\":\"final_answer\",\"value\":{\"nonce\":\"ready-v1\"}}"}]}`

type anthropicWireRequest struct {
	Model      string            `json:"model"`
	MaxTokens  int               `json:"max_tokens"`
	Messages   []json.RawMessage `json:"messages"`
	Tools      []map[string]any  `json:"tools"`
	ToolChoice map[string]any    `json:"tool_choice"`
}

// scriptedAnthropic answers round 1 with the first body and round 2 with the second; a request
// whose body carries a tool_result block is round 2. Every request is handed to check.
func scriptedAnthropic(t *testing.T, round1, round2 *http.Response, check func(round int, req *http.Request, body []byte)) (anthropicDoerFunc, *int) {
	t.Helper()
	calls := 0
	return func(req *http.Request) (*http.Response, error) {
		calls++
		raw, _ := io.ReadAll(req.Body)
		req.Body.Close()
		round := 1
		if strings.Contains(string(raw), `"tool_result"`) {
			round = 2
		}
		if check != nil {
			check(round, req, raw)
		}
		if round == 1 {
			return round1, nil
		}
		return round2, nil
	}, &calls
}

func TestAnthropicProbeReady(t *testing.T) {
	doer, calls := scriptedAnthropic(t,
		anthropicResponse(200, anthropicToolUseBody), anthropicResponse(200, anthropicFinalBody),
		func(round int, req *http.Request, body []byte) {
			if req.Method != http.MethodPost || req.URL.String() != "https://example.invalid/v1/messages" {
				t.Errorf("round %d: %s %s, want POST https://example.invalid/v1/messages", round, req.Method, req.URL)
			}
			if got := req.Header.Get("x-api-key"); got != "sk-ant-test" {
				t.Errorf("round %d: x-api-key = %q", round, got)
			}
			if got := req.Header.Get("anthropic-version"); got != "2023-06-01" {
				t.Errorf("round %d: anthropic-version = %q", round, got)
			}
			if got := req.Header.Get("Authorization"); got != "" {
				t.Errorf("round %d: Authorization = %q, want none", round, got)
			}
			var wire anthropicWireRequest
			if err := json.Unmarshal(body, &wire); err != nil {
				t.Fatalf("round %d: body: %v", round, err)
			}
			if wire.Model != "claude-sonnet-test" || wire.MaxTokens != readinessMaxOutputTokens {
				t.Errorf("round %d: model=%q max_tokens=%d", round, wire.Model, wire.MaxTokens)
			}
			if round == 1 {
				if len(wire.Tools) != 1 || wire.Tools[0]["name"] != readinessEchoWireName {
					t.Errorf("round 1 tools = %v", wire.Tools)
				}
				if wire.ToolChoice["type"] != "tool" || wire.ToolChoice["name"] != readinessEchoWireName {
					t.Errorf("round 1 tool_choice = %v", wire.ToolChoice)
				}
			} else if !strings.Contains(string(body), `"tool_use_id":"toolu_01"`) {
				t.Errorf("round 2 does not answer toolu_01: %s", body)
			}
		})
	outcome, code := newAnthropicReadinessProber(doer).probe(context.Background(), "anthropic", "claude-sonnet-test", "https://example.invalid", "sk-ant-test")
	if outcome != readinessOutcomeReady || code != nil {
		t.Fatalf("outcome=%q code=%v, want ready", outcome, code)
	}
	if *calls != 2 {
		t.Fatalf("requests = %d, want 2", *calls)
	}
}

func TestAnthropicMessagesEndpoint(t *testing.T) {
	for base, want := range map[string]string{
		"":                           "https://api.anthropic.com/v1/messages",
		"https://x.example":          "https://x.example/v1/messages",
		"https://x.example/":         "https://x.example/v1/messages",
		"https://x.example/v1":       "https://x.example/v1/messages",
		"https://x.example/v1/":      "https://x.example/v1/messages",
		"https://x.example/proxy/v1": "https://x.example/proxy/v1/messages",
	} {
		if got := anthropicMessagesEndpoint(base); got != want {
			t.Errorf("anthropicMessagesEndpoint(%q) = %q, want %q", base, got, want)
		}
	}
}

func TestAnthropicProbeFailureCodes(t *testing.T) {
	cases := []struct {
		name         string
		status       int
		body         string
		want         string
		wantRequests int
	}{
		{"bad key", 401, `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`, errCodeProviderNotConfigured, 1},
		{"key without permission", 403, `{"type":"error","error":{"type":"permission_error","message":"nope"}}`, errCodeProviderNotConfigured, 1},
		{"unknown model", 404, `{"type":"error","error":{"type":"not_found_error","message":"model: claude-nope"}}`, errCodeModelNotSupported, 1},
		{"model refused", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"model: unsupported"}}`, errCodeInvalidRequest, 1},
		{"no credit", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API."}}`, errCodeProviderNotConfigured, 1},
		{"rate limited", 429, `{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`, errCodeRateLimited, maxProbeAttempts},
		{"overloaded", 529, `{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`, errCodeProviderUnavailable, maxProbeAttempts},
		{"server error", 500, `{"type":"error","error":{"type":"api_error","message":"boom"}}`, errCodeProviderUnavailable, maxProbeAttempts},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			doer := anthropicDoerFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				return anthropicResponse(tc.status, tc.body), nil
			})
			outcome, code := newAnthropicReadinessProber(doer).probe(context.Background(), "anthropic", "claude-sonnet-test", "", "sk-ant-test")
			if outcome != readinessOutcomeFailed || code == nil || *code != tc.want {
				t.Fatalf("outcome=%q code=%v, want failed %q", outcome, code, tc.want)
			}
			if calls != tc.wantRequests {
				t.Fatalf("requests = %d, want %d", calls, tc.wantRequests)
			}
		})
	}
}

func TestAnthropicProbeRetriesATransientFailure(t *testing.T) {
	calls := 0
	doer := anthropicDoerFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		raw, _ := io.ReadAll(req.Body)
		req.Body.Close()
		switch {
		case calls == 1:
			return anthropicResponse(500, `{"type":"error","error":{"type":"api_error","message":"boom"}}`), nil
		case strings.Contains(string(raw), `"tool_result"`):
			return anthropicResponse(200, anthropicFinalBody), nil
		default:
			return anthropicResponse(200, anthropicToolUseBody), nil
		}
	})
	outcome, code := newAnthropicReadinessProber(doer).probe(context.Background(), "anthropic", "claude-sonnet-test", "", "sk-ant-test")
	if outcome != readinessOutcomeReady || code != nil {
		t.Fatalf("outcome=%q code=%v, want ready after one retry", outcome, code)
	}
}

func TestAnthropicProbeRefusesAMalformedExchange(t *testing.T) {
	twoToolUses := `{"stop_reason":"tool_use","content":[` +
		`{"type":"tool_use","id":"a","name":"readiness_echo_v1","input":{"nonce":"ready-v1"}},` +
		`{"type":"tool_use","id":"b","name":"readiness_echo_v1","input":{"nonce":"ready-v1"}}]}`
	cases := []struct {
		name           string
		round1, round2 string
		want           string
	}{
		{"round 1 answers in text", `{"stop_reason":"end_turn","content":[{"type":"text","text":"ready"}]}`, anthropicFinalBody, errCodeInvalidResponse},
		{"round 1 wrong tool", `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"a","name":"other","input":{"nonce":"ready-v1"}}]}`, anthropicFinalBody, errCodeInvalidResponse},
		{"round 1 wrong nonce", `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"a","name":"readiness_echo_v1","input":{"nonce":"x"}}]}`, anthropicFinalBody, errCodeInvalidResponse},
		{"round 1 extra argument", `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"a","name":"readiness_echo_v1","input":{"nonce":"ready-v1","extra":1}}]}`, anthropicFinalBody, errCodeInvalidResponse},
		{"round 1 no tool id", `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"","name":"readiness_echo_v1","input":{"nonce":"ready-v1"}}]}`, anthropicFinalBody, errCodeInvalidResponse},
		{"round 1 two tool uses", twoToolUses, anthropicFinalBody, errCodeProviderContractViolation},
		{"round 1 output exhausted", `{"stop_reason":"max_tokens","content":[{"type":"text","text":"..."}]}`, anthropicFinalBody, errCodeOutputExhausted},
		{"round 1 not json", `not json`, anthropicFinalBody, errCodeInvalidResponse},
		{"round 2 not json text", anthropicToolUseBody, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}`, errCodeInvalidResponse},
		{"round 2 wrong nonce", anthropicToolUseBody, `{"stop_reason":"end_turn","content":[{"type":"text","text":"{\"kind\":\"final_answer\",\"value\":{\"nonce\":\"x\"}}"}]}`, errCodeInvalidResponse},
		{"round 2 wrong kind", anthropicToolUseBody, `{"stop_reason":"end_turn","content":[{"type":"text","text":"{\"kind\":\"refusal\",\"value\":{\"nonce\":\"ready-v1\"}}"}]}`, errCodeInvalidResponse},
		{"round 2 stray field", anthropicToolUseBody, `{"stop_reason":"end_turn","content":[{"type":"text","text":"{\"kind\":\"final_answer\",\"value\":{\"nonce\":\"ready-v1\"},\"stray\":1}"}]}`, errCodeInvalidResponse},
		{"round 2 asks for another tool", anthropicToolUseBody, anthropicToolUseBody, errCodeInvalidResponse},
		{"round 2 empty", anthropicToolUseBody, `{"stop_reason":"end_turn","content":[]}`, errCodeInvalidResponse},
		{"round 2 output exhausted", anthropicToolUseBody, `{"stop_reason":"max_tokens","content":[{"type":"text","text":"{"}]}`, errCodeOutputExhausted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doer, _ := scriptedAnthropic(t, anthropicResponse(200, tc.round1), anthropicResponse(200, tc.round2), nil)
			outcome, code := newAnthropicReadinessProber(doer).probe(context.Background(), "anthropic", "claude-sonnet-test", "", "sk-ant-test")
			if outcome != readinessOutcomeFailed || code == nil || *code != tc.want {
				t.Fatalf("outcome=%q code=%v, want failed %q", outcome, code, tc.want)
			}
		})
	}
}

func TestReadinessProberForDispatchesByProvider(t *testing.T) {
	if p, ok := readinessProberFor("anthropic", nil); !ok {
		t.Fatal("anthropic has no prober")
	} else if _, isAnthropic := p.(*anthropicReadinessProber); !isAnthropic {
		t.Fatalf("anthropic prober is %T", p)
	}
	if p, ok := readinessProberFor("openai", nil); !ok {
		t.Fatal("openai has no prober")
	} else if _, isOpenAI := p.(*openAICompatibleReadinessProber); !isOpenAI {
		t.Fatalf("openai prober is %T", p)
	}
	if _, ok := readinessProberFor("gemini", nil); ok {
		t.Fatal("gemini must have no prober")
	}
}
