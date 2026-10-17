package categorize

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

type opsRoute struct {
	name    string
	request CompletionRequest
}

func opsRoutes() []opsRoute {
	return []opsRoute{
		{"categorize", CategorizationRequest("p")},
		{"investment_mix_explain", InvestmentMixExplanationRequest("p")},
		{"work_unit_explain", WorkUnitExplanationRequest("p")},
	}
}

// OpenAI strict json_schema answers 400 to a schema that breaks strict mode,
// and since dev-health-go v0.24.3 that 400 is final: every schema ops sends
// as json_schema must be strict-compatible. Every object lists every property
// as required and sets additionalProperties false, and no keyword strict mode
// refuses appears.
func TestEveryOpsSchemaIsOpenAIStrictCompatible(t *testing.T) {
	sent := 0
	for _, route := range opsRoutes() {
		if route.request.JSONSchema == nil {
			continue
		}
		sent++
		if problem := strictProblem(jsonCopy(t, route.request.JSONSchema), "$"); problem != "" {
			t.Errorf("%s: %s", route.name, problem)
		}
	}
	if sent != 2 {
		t.Fatalf("%d schemas checked, want 2 (categorize, investment_mix_explain)", sent)
	}
}

func strictProblem(node any, path string) string {
	switch typed := node.(type) {
	case []any:
		for i, child := range typed {
			if problem := strictProblem(child, fmt.Sprintf("%s[%d]", path, i)); problem != "" {
				return problem
			}
		}
	case map[string]any:
		for _, refused := range []string{"allOf", "oneOf", "not", "if", "then", "else", "dependentRequired", "dependentSchemas", "patternProperties"} {
			if _, present := typed[refused]; present {
				return path + ": keyword " + refused
			}
		}
		if properties, ok := typed["properties"].(map[string]any); ok || typed["type"] == "object" {
			if typed["additionalProperties"] != false {
				return path + ": additionalProperties is not false"
			}
			required := map[string]bool{}
			names, _ := typed["required"].([]any)
			for _, name := range names {
				required[fmt.Sprint(name)] = true
			}
			for name := range properties {
				if !required[name] {
					return path + "." + name + ": optional property"
				}
			}
		}
		for _, key := range sortedMapKeys(typed) {
			if key == "enum" || key == "const" || key == "default" || key == "examples" {
				continue
			}
			if key == "properties" {
				for _, name := range sortedMapKeys(typed[key].(map[string]any)) {
					if problem := strictProblem(typed[key].(map[string]any)[name], path+"."+name); problem != "" {
						return problem
					}
				}
				continue
			}
			if problem := strictProblem(typed[key], path+"."+key); problem != "" {
				return problem
			}
		}
	}
	return ""
}

// Every ops route on every hosted kind sends the format of this table, chosen
// before the call. Where the Anthropic request carries output_config, the
// schema is one structured outputs accept (the documented limits plus the
// measured 2026-10-16 refusal of an enum under a type list); where it carries
// a tool, the tool is the one forced answer. OpenAI's three rows are also pinned
// byte for byte by the 62-case oracle (categorization_floor_2048,
// investment_mix_floor_4096, work_unit_json_object).
func TestOpsWireFormatPerRouteAndProvider(t *testing.T) {
	want := map[string]map[string]string{
		"openai":    {"categorize": "json_schema strict", "investment_mix_explain": "json_schema strict", "work_unit_explain": "json_object"},
		"anthropic": {"categorize": "output_config", "investment_mix_explain": "output_config", "work_unit_explain": "none"},
		"gemini":    {"categorize": "json_schema strict", "investment_mix_explain": "json_schema strict", "work_unit_explain": "json_object"},
		"qwen":      {"categorize": "json_schema strict", "investment_mix_explain": "json_schema strict", "work_unit_explain": "json_object"},
		"local":     {"categorize": "json_schema strict", "investment_mix_explain": "json_schema strict", "work_unit_explain": "none"},
	}
	kinds := map[string]struct {
		kind  ProviderKind
		model string
		reply string
	}{
		"openai":    {ProviderKindOpenAI, "gpt-5-nano", `{"output_text":"{}","usage":{"input_tokens":1,"output_tokens":1,"input_tokens_details":{"cached_tokens":0}}}`},
		"anthropic": {ProviderKindAnthropic, "claude-haiku-5-5", `{"content":[{"type":"text","text":"{}"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`},
		"gemini":    {ProviderKindGemini, "gemini-3.8-flash", `{"choices":[{"message":{"role":"assistant","content":"{}"},"finish_reason":"stop"}]}`},
		"qwen":      {ProviderKindQwen, "qwen-plus", `{"choices":[{"message":{"role":"assistant","content":"{}"},"finish_reason":"stop"}]}`},
		"local":     {ProviderKindLocal, "gemma3:4b", `{"choices":[{"message":{"role":"assistant","content":"{}"},"finish_reason":"stop"}]}`},
	}
	for name, setup := range kinds {
		for _, route := range opsRoutes() {
			t.Run(name+"/"+route.name, func(t *testing.T) {
				server, sent := scriptedSyncServer(t, []sharedSyncStep{{Status: 200, Body: setup.reply}})
				provider, err := NewSharedProvider(SharedProviderConfig{Kind: setup.kind, APIKey: secrets.NewHidden("test-key-value"), BaseURL: server.URL, Model: setup.model})
				if err != nil {
					t.Fatal(err)
				}
				_, _ = provider.Complete(context.Background(), route.request)
				bodies := sent()
				if len(bodies) != 1 {
					t.Fatalf("calls = %d, want 1", len(bodies))
				}
				var body map[string]any
				if err := json.Unmarshal([]byte(bodies[0]), &body); err != nil {
					t.Fatal(err)
				}
				got := wireFormat(name, body)
				if got != want[name][route.name] {
					t.Fatalf("format %q, want %q", got, want[name][route.name])
				}
				if got == "output_config" {
					schema := body["output_config"].(map[string]any)["format"].(map[string]any)["schema"]
					if problem := anthropicStructuredProblem(schema, "$"); problem != "" {
						t.Fatalf("output_config schema refused by structured outputs: %s", problem)
					}
				}
			})
		}
	}
}

func wireFormat(kind string, body map[string]any) string {
	switch kind {
	case "openai":
		format := body["text"].(map[string]any)["format"].(map[string]any)
		if format["type"] == "json_schema" && format["strict"] == true {
			return "json_schema strict"
		}
		return fmt.Sprint(format["type"])
	case "anthropic":
		switch {
		case body["output_config"] != nil && body["tools"] == nil:
			return "output_config"
		case body["tools"] != nil && body["output_config"] == nil:
			return "forced_tool"
		case body["tools"] == nil && body["output_config"] == nil:
			return "none"
		}
		return "output_config and tools"
	default:
		format, named := body["response_format"].(map[string]any)
		if !named {
			return "none"
		}
		if format["type"] == "json_schema" && format["json_schema"].(map[string]any)["strict"] == true {
			return "json_schema strict"
		}
		return fmt.Sprint(format["type"])
	}
}

// anthropicStructuredProblem names the first part of schema that Anthropic
// structured outputs refuse: an enum under a type list (measured 2026-10-16:
// "Enum value 'high' does not match declared type '['string', 'null']'"), a
// numeric/string/array bound, an object whose additionalProperties is not
// false, or a minItems other than 0 or 1.
func anthropicStructuredProblem(node any, path string) string {
	switch typed := node.(type) {
	case []any:
		for i, child := range typed {
			if problem := anthropicStructuredProblem(child, fmt.Sprintf("%s[%d]", path, i)); problem != "" {
				return problem
			}
		}
	case map[string]any:
		if _, list := typed["type"].([]any); list {
			if _, enum := typed["enum"]; enum {
				return path + ": enum under a type list"
			}
		}
		for _, bound := range []string{"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "minLength", "maxLength", "maxItems", "uniqueItems", "minProperties", "maxProperties", "oneOf", "not", "if", "then", "else"} {
			if _, present := typed[bound]; present {
				return path + ": keyword " + bound
			}
		}
		if minItems, present := typed["minItems"]; present && minItems != float64(0) && minItems != float64(1) {
			return path + ": minItems " + fmt.Sprint(minItems)
		}
		if _, ok := typed["properties"]; ok || typed["type"] == "object" {
			if typed["additionalProperties"] != false {
				return path + ": additionalProperties is not false"
			}
		}
		for _, key := range sortedMapKeys(typed) {
			if key == "enum" || key == "const" || key == "default" || key == "examples" {
				continue
			}
			if key == "properties" {
				for _, name := range sortedMapKeys(typed[key].(map[string]any)) {
					if problem := anthropicStructuredProblem(typed[key].(map[string]any)[name], path+"."+name); problem != "" {
						return problem
					}
				}
				continue
			}
			if problem := anthropicStructuredProblem(typed[key], path+"."+key); problem != "" {
				return problem
			}
		}
	}
	return ""
}

func jsonCopy(t *testing.T, value any) any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(encoded, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
