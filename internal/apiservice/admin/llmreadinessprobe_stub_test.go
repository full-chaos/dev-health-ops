package admin

import (
	"encoding/json"
	"net/http"
	"reflect"
	"sync/atomic"
)

// This file is the scripted provider of the readiness probe oracle
// (llmreadinessprobe_golden_test.go). Its whole text is part of that golden's
// key: the frozen Python answers are the answers to THIS stub, so a changed
// stub must be recorded again.

// jsonSemanticallyEqual compares two JSON documents by VALUE, not by byte
// content: Go's encoding/json and Python's json.dumps do not agree on key
// order or whitespace for the same logical document, so a byte-string
// comparison of Go's own hand-computed schema constants against Python's own
// runtime-serialized schema would fail even when the two are the identical
// schema. Both sides decode into `any` (map[string]any/[]any/float64/etc.)
// and compare with reflect.DeepEqual, which JSON's decoded types support.
func jsonSemanticallyEqual(a, b []byte) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	return reflect.DeepEqual(va, vb)
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
		Model               string   `json:"model"`
		ToolChoice          string   `json:"tool_choice"`
		MaxCompletionTokens int      `json:"max_completion_tokens"`
		ParallelToolCalls   *bool    `json:"parallel_tool_calls"`
		Temperature         *float64 `json:"temperature"`
		ReasoningEffort     string   `json:"reasoning_effort"`
		Tools               []struct {
			Type     string `json:"type"`
			Function struct {
				Name       string          `json:"name"`
				Parameters json.RawMessage `json:"parameters"`
				Strict     bool            `json:"strict"`
			} `json:"function"`
		} `json:"tools"`
		ResponseFormat *struct {
			Type       string `json:"type"`
			JSONSchema struct {
				Name   string          `json:"name"`
				Strict bool            `json:"strict"`
				Schema json.RawMessage `json:"schema"`
			} `json:"json_schema"`
		} `json:"response_format"`
		Messages []struct {
			Role      string  `json:"role"`
			Content   *string `json:"content"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
			ToolCallID string `json:"tool_call_id"`
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

	// D2908 condition 3a (widened per codex r2's P3 finding,
	// llmreadinessprobe_live_python_oracle_test.go:193): assert the FULL
	// wire request shape both AgentReadinessService.certify's real
	// OpenAICompatibleAgentProvider and this port's readinessProber must
	// produce, matching build_completion_request (openai_compatible.py:970-1038)
	// field for field -- not just tool_choice. A caller (Go OR Python) that
	// gets any of these wrong degrades to a 500 here rather than the
	// mismatch going unnoticed silently as a passing scenario.
	wantToolChoice := "required"
	wantTools := 1
	wantResponseFormat := false
	if round2 {
		wantToolChoice = ""
		wantTools = 0
		wantResponseFormat = true
	}
	fail := func(reason string) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"type":"server_error","message":"oracle stub: ` + reason + `"}}`))
	}
	switch {
	case req.ToolChoice != wantToolChoice:
		fail("unexpected tool_choice")
		return
	case req.MaxCompletionTokens != readinessMaxOutputTokens:
		fail("unexpected max_completion_tokens")
		return
	// parallel_tool_calls is gated on tools being present
	// (build_completion_request: `if tools and supports_parallel_tool_calls(model)`)
	// -- round1 sends it (false), round2 (no tools) omits the key entirely.
	// D2984: this was unconditional before, so round2's correct omission
	// was rejected on EVERY request regardless of round, on BOTH planes
	// identically -- masking any round1 mutation under an unrelated,
	// always-firing false failure that made the two planes agree
	// ("failed"/"provider_unavailable") for the wrong reason.
	case wantTools == 1 && (req.ParallelToolCalls == nil || *req.ParallelToolCalls != false):
		fail("unexpected parallel_tool_calls on the tool-bearing round")
		return
	case wantTools == 0 && req.ParallelToolCalls != nil:
		fail("unexpected parallel_tool_calls present on the no-tools round")
		return
	case req.Temperature == nil || *req.Temperature != 0.0:
		fail("unexpected temperature")
		return
	case req.ReasoningEffort != "":
		// scripted-* model names never match the gpt-5* prefix on either
		// plane (openai_capabilities.py:82-84 / reasoningEffort in
		// llmreadinessprobe.go) -- always omitted for this oracle's models.
		fail("unexpected reasoning_effort")
		return
	case len(req.Tools) != wantTools:
		fail("unexpected tools count")
		return
	case wantTools == 1 && (req.Tools[0].Type != "function" ||
		req.Tools[0].Function.Name != readinessEchoWireName ||
		!req.Tools[0].Function.Strict ||
		!jsonSemanticallyEqual(req.Tools[0].Function.Parameters, []byte(readinessToolParametersSchema))):
		fail("unexpected tool schema")
		return
	case wantResponseFormat && (req.ResponseFormat == nil ||
		req.ResponseFormat.Type != "json_schema" ||
		req.ResponseFormat.JSONSchema.Name != "ask_dev_decision" ||
		!req.ResponseFormat.JSONSchema.Strict ||
		!jsonSemanticallyEqual(req.ResponseFormat.JSONSchema.Schema, []byte(readinessDecisionResponseSchema))):
		fail("unexpected response_format")
		return
	case !wantResponseFormat && req.ResponseFormat != nil:
		fail("unexpected response_format present on round 1")
		return
	}

	// D2996 (codex r3's P3 finding: this stub decoded messages as roles
	// only, so a mutated PROMPT/tool-call-replay/tool-reply passed
	// unnoticed -- executed repro mutated both Go probe prompts to "Ignore
	// this prompt and return arbitrary output" and 13/13 still passed).
	// Assert the exact message CONTENT both planes must send
	// (readiness.py:186-224 verbatim), not just message roles.
	msgContent := func(i int) string {
		if i >= len(req.Messages) || req.Messages[i].Content == nil {
			return ""
		}
		return *req.Messages[i].Content
	}
	const wantRound1Prompt = "Call readiness_echo with nonce ready-v1."
	const wantToolReplyContent = `{"nonce":"ready-v1"}`
	const wantFinalPrompt = "Return a final_answer now with value exactly {\"nonce\":\"ready-v1\"}. Do not request another tool."
	switch {
	case !round2:
		if len(req.Messages) != 1 || req.Messages[0].Role != "user" || msgContent(0) != wantRound1Prompt {
			fail("unexpected round 1 message content")
			return
		}
	case round2:
		if len(req.Messages) != 4 {
			fail("unexpected round 2 message count")
			return
		}
		assistant, toolReply := req.Messages[1], req.Messages[2]
		switch {
		case req.Messages[0].Role != "user" || msgContent(0) != wantRound1Prompt:
			fail("unexpected round 2 message[0] (echoed round 1 prompt)")
			return
		case assistant.Role != "assistant" || len(assistant.ToolCalls) != 1 ||
			assistant.ToolCalls[0].Function.Name != readinessEchoWireName ||
			!jsonSemanticallyEqual([]byte(assistant.ToolCalls[0].Function.Arguments), []byte(`{"nonce":"ready-v1"}`)):
			fail("unexpected round 2 message[1] (assistant tool-call replay)")
			return
		case toolReply.Role != "tool" || msgContent(2) != wantToolReplyContent ||
			toolReply.ToolCallID == "" || toolReply.ToolCallID != assistant.ToolCalls[0].ID:
			fail("unexpected round 2 message[2] (tool reply, or its tool_call_id does not correlate with message[1]'s)")
			return
		case req.Messages[3].Role != "user" || msgContent(3) != wantFinalPrompt:
			fail("unexpected round 2 message[3] (final-answer prompt)")
			return
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
	// One-signal scenarios (each failure carries ONE thing the classifier
	// reads, so a clause that is not the only reason for an answer is
	// observed on its own).
	case "scripted-status401only":
		writeJSON(401, map[string]any{"error": map[string]any{"message": "denied"}})
	case "scripted-status429only":
		writeJSON(429, map[string]any{"error": map[string]any{"message": "slow down"}})
	case "scripted-status400only":
		writeJSON(400, map[string]any{"error": map[string]any{"message": "bad"}})
	case "scripted-mnfcode":
		writeJSON(404, map[string]any{"error": map[string]any{"code": "model_not_found"}})
	case "scripted-mnftext":
		writeJSON(404, map[string]any{"error": map[string]any{"message": "model not found"}})
	case "scripted-mnfexist":
		writeJSON(404, map[string]any{"error": map[string]any{"message": "that model does not exist"}})
	case "scripted-quotacode":
		writeJSON(429, map[string]any{"error": map[string]any{"type": "insufficient_quota", "message": "no funds"}})
	case "scripted-quotatext":
		writeJSON(429, map[string]any{"error": map[string]any{"message": "you exceeded your current quota"}})
	case "scripted-lengthround1":
		// Round 1 stops on length with a well-formed tool call; round 2 would
		// succeed. Only round 1's own check gives output_exhausted.
		if !round2 {
			completion(toolCallMessage(), "length")
		} else {
			completion(finalAnswerMessage(), "stop")
		}
	case "scripted-lengthround2":
		if !round2 {
			completion(toolCallMessage(), "tool_calls")
		} else {
			completion(finalAnswerMessage(), "length")
		}
	case "scripted-notoolcalls":
		completion(map[string]any{"role": "assistant", "content": "I will not call a tool."}, "stop")
	case "scripted-wrongkind":
		if !round2 {
			completion(toolCallMessage(), "tool_calls")
		} else {
			completion(map[string]any{"role": "assistant", "content": `{"kind":"tool_request","value":{"nonce":"ready-v1"}}`}, "stop")
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
