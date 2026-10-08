package categorize

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// The differential oracle of the Batch API port: the Python functions at
// batchPythonBuild (openai.py's _batch_line/_batch_body,
// _parse_openai_batch_lines, _map_openai_batch_status; batch.py's
// BatchItemResult) ran once over batchOracleCorpus, and their answers are
// frozen in testdata/golden. Every case is equal, except the named
// divergences, which must still differ: a divergence the oracle stops
// finding means the comparison broke.

// batchPythonBuild is the last build that held the Python batch provider.
const batchPythonBuild = "9fb387470e4ab360f88bc052694c891d586824ce"

const batchProducerIdentity = "python 3.14.7\nunicodedata 16.0.0"

var batchGoldenPins = map[string]string{
	"batch.golden.json": "5f590784e877992ae5534a1a3e9ab805ee1c777305901ff5fd499283b686eaaa",
}

var batchGoldens = programoracle.Set{
	Package:  "./internal/jobs/investment/categorize/",
	Build:    batchPythonBuild,
	Identity: batchProducerIdentity,
	Pins:     batchGoldenPins,
}

// batchOracleProgram runs the real Python producers over the corpus on stdin.
// The field sets it prints come from the production code: the dataclass
// fields of BatchItemResult and the keys _batch_payload_metadata assigns.
const batchOracleProgram = `
import ast, dataclasses, inspect, json, sys, textwrap
from dev_health_ops.llm.providers import openai as o
from dev_health_ops.llm.providers.batch import BatchItemRequest, BatchItemResult
from dev_health_ops.work_graph.investment.categorization_prompts import build_prompt

def tag(v):
    if v is None:
        return {"t": "null", "v": ""}
    if isinstance(v, bool):
        return {"t": "bool", "v": "true" if v else "false"}
    if isinstance(v, int):
        return {"t": "number", "v": str(v)}
    if isinstance(v, float):
        return {"t": "number", "v": repr(v)}
    if isinstance(v, str):
        return {"t": "str", "v": v}
    if isinstance(v, list):
        return {"t": "array", "v": [tag(x) for x in v]}
    if isinstance(v, dict):
        return {"t": "object", "v": {str(k): tag(x) for k, x in v.items()}}
    raise TypeError(type(v).__name__)

def provider_of(case):
    if case["facade"]:
        return o.OpenAIProvider(api_key="oracle-placeholder", model=case["model"])._impl
    return o.OpenAIGPT5Provider(o.OpenAIProviderConfig(
        api_key="oracle-placeholder", base_url=None, model=case["model"],
        max_output_tokens=case["max_output_tokens"], temperature=0.3))

def prompt_of(case):
    if case["kind"] == "categorization":
        return build_prompt(case["source_block"])
    return case["prompt"]

def metadata_keys():
    tree = ast.parse(textwrap.dedent(inspect.getsource(o._batch_payload_metadata)))
    keys = set()
    for node in ast.walk(tree):
        if (isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Store)
                and isinstance(node.value, ast.Name) and node.value.id == "metadata"):
            keys.add(node.slice.value)
    return sorted(keys)

fields = [f.name for f in dataclasses.fields(BatchItemResult)]
corpus = json.load(sys.stdin)
out = {"lines": {}, "parse": {}, "statuses": {}, "result_fields": fields, "metadata_keys": metadata_keys()}
for case in corpus["lines"]:
    line = provider_of(case)._batch_line(BatchItemRequest(custom_id=case["custom_id"], prompt=prompt_of(case)))
    out["lines"][case["name"]] = {"newline_free": "\n" not in line, "line": tag(json.loads(line))}
for case in corpus["parse"]:
    try:
        results = o._parse_openai_batch_lines(case["text"])
    except Exception as exc:
        out["parse"][case["name"]] = {"error": True, "results": tag(type(exc).__name__)}
        continue
    rows = [{name: getattr(r, name) for name in fields} for r in results]
    out["parse"][case["name"]] = {"error": False, "results": tag(rows)}
for status in corpus["statuses"]:
    out["statuses"][status] = o._map_openai_batch_status(status).value
json.dump(out, sys.stdout, sort_keys=True, ensure_ascii=True)
`

type batchLineCase struct {
	Name            string `json:"name"`
	Model           string `json:"model"`
	MaxOutputTokens int    `json:"max_output_tokens"`
	Facade          bool   `json:"facade"`
	Kind            string `json:"kind"`
	SourceBlock     string `json:"source_block"`
	Prompt          string `json:"prompt"`
	CustomID        string `json:"custom_id"`
}

type batchParseCase struct {
	Name string `json:"name"`
	Text string `json:"text"`
}

type batchCorpus struct {
	Lines    []batchLineCase  `json:"lines"`
	Parse    []batchParseCase `json:"parse"`
	Statuses []string         `json:"statuses"`
}

const explanationPrompt = "DEV_HEALTH_RESPONSE_FORMAT=investment_mix_explanation\nExplain the mix."

// facadeMaxOutputTokens is what openai.py's OpenAIProvider facade configures
// by default (max(4096, max_completion_tokens=4096)).
const facadeMaxOutputTokens = 4096

func batchOracleCorpus() batchCorpus {
	return batchCorpus{
		Lines: []batchLineCase{
			{Name: "categorization_nano", Model: "gpt-5-nano", MaxOutputTokens: 2048, Kind: "categorization",
				SourceBlock: "[pr:1] Fix the login timeout\nRetries now back off.", CustomID: "run1-0"},
			{Name: "categorization_chunked_id", Model: "gpt-5-mini", MaxOutputTokens: 2048, Kind: "categorization",
				SourceBlock: "[issue:7] Upgrade TLS <1.2> & rotate keys \u00e9\u4e2d\U0001F600", CustomID: "run1:chunk:3-12"},
			{Name: "categorization_empty_source", Model: "gpt-5-nano", MaxOutputTokens: 2048, Kind: "categorization",
				SourceBlock: "", CustomID: "run1-1"},
			{Name: "categorization_high_floor", Model: "gpt-5-nano", MaxOutputTokens: 6000, Kind: "categorization",
				SourceBlock: "[commit:a] tidy", CustomID: "run1-2"},
			{Name: "categorization_facade_default", Model: "gpt-5-nano", Facade: true, MaxOutputTokens: facadeMaxOutputTokens,
				Kind: "categorization", SourceBlock: "[pr:2] Add a feature flag", CustomID: "run1-3"},
			{Name: "plain_json_object", Model: "gpt-5-nano", MaxOutputTokens: 4096, Kind: "plain",
				Prompt: "Explain this work unit.\nReturn JSON.", CustomID: "run1-4"},
			// Divergence: openai.py floors every batch body at 2048; the shared
			// builder keeps the synchronous per-format floor (4096 here).
			{Name: "divergence_explanation_floor", Model: "gpt-5-nano", MaxOutputTokens: 2048, Kind: "explanation",
				Prompt: explanationPrompt, CustomID: "run1-5"},
			// Divergence: below gpt-5 openai.py sent Chat Completions lines; this
			// provider has only the Responses API, for its synchronous call too.
			{Name: "divergence_legacy_model", Model: "gpt-4o-mini", Facade: true, MaxOutputTokens: facadeMaxOutputTokens,
				Kind: "categorization", SourceBlock: "[pr:3] Patch", CustomID: "run1-6"},
		},
		Parse: []batchParseCase{
			{Name: "responses_output_chunks", Text: `{"id":"batch_req_1","custom_id":"run1-0","response":{"status_code":200,"request_id":"req_1","body":{"output":[{"type":"message","content":[{"type":"output_text","text":"{\"subcategories\": {\"feature_delivery.roadmap\": 1.0}}"}]}],"usage":{"input_tokens":1060,"output_tokens":787,"input_tokens_details":{"cached_tokens":0}}}},"error":null}`},
			{Name: "responses_output_text_field", Text: `{"id":"batch_req_2","custom_id":"run1-1","response":{"status_code":200,"request_id":"req_2","body":{"output_text":"{\"b\": 1, \"a\": [1, 2]}","usage":{"input_tokens":5,"output_tokens":6}}},"error":null}`},
			{Name: "text_not_json_is_kept", Text: `{"id":"batch_req_3","custom_id":"run1-2","response":{"status_code":200,"request_id":"req_3","body":{"output":[{"content":[{"type":"output_text","text":"not json at all"}]}]}},"error":null}`},
			{Name: "unicode_text", Text: `{"id":"batch_req_4","custom_id":"run1-3","response":{"status_code":200,"request_id":"req_4","body":{"output":[{"content":[{"type":"output_text","text":"{\"q\": \"\u00e9\u4e2d \\u2028\"}"}]}]}},"error":null}`},
			{Name: "empty_body_200", Text: `{"id":"batch_req_5","custom_id":"run1-4","response":{"status_code":200,"request_id":"req_5","body":{}},"error":null}`},
			{Name: "error_object", Text: `{"id":"batch_req_6","custom_id":"run1-5","response":null,"error":{"code":"batch_expired","message":"This request could not be executed before the completion window expired."}}`},
			{Name: "error_type_only", Text: `{"id":"batch_req_7","custom_id":"run1-6","response":null,"error":{"type":"server_error","message":"upstream failed"}}`},
			{Name: "error_code_and_type", Text: `{"id":"batch_req_19","custom_id":"run1-13","response":null,"error":{"code":"invalid_prompt","type":"invalid_request_error","message":"rejected"}}`},
			{Name: "error_message_only", Text: `{"id":"batch_req_8","custom_id":"run1-7","response":null,"error":{"message":"unclassified"}}`},
			{Name: "error_file_http_400", Text: `{"id":"batch_req_9","custom_id":"run1-8","response":{"status_code":400,"request_id":"req_9","body":{"error":{"message":"Invalid schema","type":"invalid_request_error"}}},"error":null}`},
			{Name: "missing_status_code", Text: `{"id":"batch_req_10","custom_id":"run1-9","response":{"request_id":"req_10","body":{}},"error":null}`},
			{Name: "status_code_zero", Text: `{"id":"batch_req_20","custom_id":"run1-14","response":{"status_code":0,"request_id":"req_20","body":{}},"error":null}`},
			{Name: "skipped_and_blank_lines", Text: "\n{\"custom_id\":\"run1-10\"}\n  \n{\"id\":\"batch_req_11\",\"custom_id\":\"run1-11\",\"response\":\"text\",\"error\":\"text\"}\n"},
			{Name: "multi_line_file_order_and_duplicates", Text: strings.Join([]string{
				`{"id":"batch_req_12","custom_id":"run2-1","response":{"status_code":200,"request_id":"req_12","body":{"output_text":"{\"x\": 1}"}},"error":null}`,
				`{"id":"batch_req_13","custom_id":"run2-0","response":{"status_code":500,"request_id":"req_13","body":{"error":{"message":"boom"}}},"error":null}`,
				`{"id":"batch_req_14","custom_id":"run2-1","response":{"status_code":200,"request_id":"req_14","body":{"output_text":"{\"x\": 2}"}},"error":null}`,
			}, "\n") + "\n"},
			{Name: "line_not_json", Text: "{\"custom_id\":\"run1-12\"}\nnot json\n"},
			// Divergence: openai.py joined the text of every output chunk; the
			// shared parser keeps only output_text/text chunks (sync semantics).
			{Name: "divergence_chunk_type_filter", Text: `{"id":"batch_req_15","custom_id":"run3-0","response":{"status_code":200,"request_id":"req_15","body":{"output":[{"type":"reasoning","content":[{"type":"summary_text","text":"thinking "}]},{"type":"message","content":[{"type":"output_text","text":"{\"y\": 1}"}]}]}},"error":null}`},
			// Divergence: openai.py skipped a whitespace-only output_text; the
			// shared parser takes it as the answer.
			{Name: "divergence_whitespace_output_text", Text: `{"id":"batch_req_16","custom_id":"run3-1","response":{"status_code":200,"request_id":"req_16","body":{"output_text":"  ","output":[{"content":[{"type":"output_text","text":"{\"z\": 1}"}]}]}},"error":null}`},
			// Divergence: a Chat Completions body (choices, prompt_tokens) is
			// not a Responses body; this provider never submits one.
			{Name: "divergence_chat_completions_body", Text: `{"id":"batch_req_17","custom_id":"run3-2","response":{"status_code":200,"request_id":"req_17","body":{"choices":[{"message":{"content":"{\"w\": 1}"}}],"usage":{"prompt_tokens":3,"completion_tokens":4}}},"error":null}`},
			// Divergence: with no message, openai.py printed the error dict's
			// Python repr; Go uses the error code.
			{Name: "divergence_error_without_message", Text: `{"id":"batch_req_18","custom_id":"run3-3","response":null,"error":{"code":"rate_limit_exceeded"}}`},
		},
		Statuses: []string{"validating", "in_progress", "finalizing", "completed", "failed", "expired",
			"cancelling", "cancelled", "", "unknown_status", " Completed ", "IN_PROGRESS"},
	}
}

// batchDivergences are the cases the port differs on, on purpose. Each must
// still differ.
var batchDivergences = map[string]bool{
	"lines/divergence_explanation_floor":      true,
	"lines/divergence_legacy_model":           true,
	"parse/divergence_chunk_type_filter":      true,
	"parse/divergence_whitespace_output_text": true,
	"parse/divergence_chat_completions_body":  true,
	"parse/divergence_error_without_message":  true,
}

// batchMetadataFields maps every key _batch_payload_metadata sets to the
// field of BatchItemResult that carries it. The test fails when Python's
// set of keys and this table disagree.
var batchMetadataFields = map[string]func(BatchItemResult) (any, bool){
	"id":            func(r BatchItemResult) (any, bool) { return r.LineID, r.LineID != "" },
	"custom_id":     func(r BatchItemResult) (any, bool) { return r.CustomID, r.CustomID != "" },
	"request_id":    func(r BatchItemResult) (any, bool) { return r.RequestID, r.RequestID != "" },
	"status_code":   func(r BatchItemResult) (any, bool) { return intOrNil(r.StatusCode), r.StatusCode != nil },
	"input_tokens":  func(r BatchItemResult) (any, bool) { return intOrNil(r.InputTokens), r.InputTokens != nil },
	"output_tokens": func(r BatchItemResult) (any, bool) { return intOrNil(r.OutputTokens), r.OutputTokens != nil },
	"error_code":    func(r BatchItemResult) (any, bool) { return r.ProviderErrorCode, r.ProviderErrorCode != "" },
	"error_type":    func(r BatchItemResult) (any, bool) { return r.ProviderErrorType, r.ProviderErrorType != "" },
}

// batchResultFields maps every dataclass field of BatchItemResult.
var batchResultFields = map[string]func(BatchItemResult) any{
	"custom_id":    func(r BatchItemResult) any { return r.CustomID },
	"raw_response": func(r BatchItemResult) any { return stringOrNil(r.Text) },
	"error_code":   func(r BatchItemResult) any { return stringOrNil(r.ErrorCode) },
	"error_message": func(r BatchItemResult) any {
		return stringOrNil(r.ErrorMessage)
	},
	"provider_metadata": func(r BatchItemResult) any {
		metadata := map[string]any{}
		for key, field := range batchMetadataFields {
			if value, ok := field(r); ok {
				metadata[key] = value
			}
		}
		return metadata
	},
}

func intOrNil(value *int) any {
	if value == nil {
		return nil
	}
	return json.Number(fmt.Sprint(*value))
}

func stringOrNil(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type batchOracleAnswer struct {
	Lines map[string]struct {
		NewlineFree bool            `json:"newline_free"`
		Line        json.RawMessage `json:"line"`
	} `json:"lines"`
	Parse map[string]struct {
		Error   bool            `json:"error"`
		Results json.RawMessage `json:"results"`
	} `json:"parse"`
	Statuses     map[string]string `json:"statuses"`
	ResultFields []string          `json:"result_fields"`
	MetadataKeys []string          `json:"metadata_keys"`
}

func frozenBatchAnswer(t *testing.T) (batchCorpus, batchOracleAnswer) {
	t.Helper()
	corpus := batchOracleCorpus()
	stdin, err := json.Marshal(corpus)
	if err != nil {
		t.Fatal(err)
	}
	program := programoracle.Program{Name: "batch provider oracle", Text: batchOracleProgram, Stdin: stdin}
	stdout := batchGoldens.Outputs(t, categorizeRepositoryRoot(t), "batch.golden.json", program)[0]
	var answer batchOracleAnswer
	decoder := json.NewDecoder(strings.NewReader(stdout))
	decoder.UseNumber()
	if err := decoder.Decode(&answer); err != nil {
		t.Fatalf("decode frozen answer: %v", err)
	}
	return corpus, answer
}

// TestBatchProviderMatchesFrozenPython is the oracle.
func TestBatchProviderMatchesFrozenPython(t *testing.T) {
	corpus, answer := frozenBatchAnswer(t)

	if got, want := sortedNames(batchResultFields), sortedCopy(answer.ResultFields); !reflect.DeepEqual(got, want) {
		t.Fatalf("BatchItemResult fields: Python has %v, the comparator maps %v", want, got)
	}
	if got, want := sortedNames(batchMetadataFields), sortedCopy(answer.MetadataKeys); !reflect.DeepEqual(got, want) {
		t.Fatalf("provider_metadata keys: Python sets %v, the comparator maps %v", want, got)
	}

	found := map[string]bool{}
	check := func(name string, equal bool, detail string) {
		t.Helper()
		if batchDivergences[name] {
			if equal {
				t.Errorf("%s: a named divergence is no longer found (the comparison or the case broke)", name)
			} else {
				t.Logf("%s (named divergence): %s", name, detail)
			}
			found[name] = true
			return
		}
		if !equal {
			t.Errorf("%s: Go differs from Python: %s", name, detail)
		}
	}

	for _, lineCase := range corpus.Lines {
		name := "lines/" + lineCase.Name
		python, ok := answer.Lines[lineCase.Name]
		if !ok {
			t.Fatalf("%s: no Python answer", name)
		}
		line, err := goBatchLine(lineCase)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !python.NewlineFree || bytes.ContainsAny(line, "\n\r") {
			t.Errorf("%s: a JSONL line holds a line break", name)
		}
		goTagged, err := tagJSON(line)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		equal, detail := taggedEqual(goTagged, python.Line)
		check(name, equal, detail)
	}

	for _, parseCase := range corpus.Parse {
		name := "parse/" + parseCase.Name
		python, ok := answer.Parse[parseCase.Name]
		if !ok {
			t.Fatalf("%s: no Python answer", name)
		}
		results, err := parseOpenAIBatchLines([]byte(parseCase.Text))
		if python.Error || err != nil {
			check(name, python.Error == (err != nil), fmt.Sprintf("python error=%v go error=%v", python.Error, err))
			continue
		}
		rows := make([]any, 0, len(results))
		for _, result := range results {
			row := map[string]any{}
			for field, value := range batchResultFields {
				row[field] = value(result)
			}
			rows = append(rows, row)
		}
		encoded, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		goTagged, err := tagJSON(encoded)
		if err != nil {
			t.Fatal(err)
		}
		equal, detail := taggedEqual(normalizeRawResponses(goTagged), normalizeRawResponses(python.Results))
		check(name, equal, detail)
	}

	for _, status := range corpus.Statuses {
		want, ok := answer.Statuses[status]
		if !ok {
			t.Fatalf("status %q: no Python answer", status)
		}
		if got := string(mapOpenAIBatchStatus(status)); got != want {
			t.Errorf("status %q: Go maps %q, Python %q", status, got, want)
		}
	}

	for name := range batchDivergences {
		if !found[name] {
			t.Errorf("%s: a named divergence has no case", name)
		}
	}
}

func goBatchLine(lineCase batchLineCase) ([]byte, error) {
	provider := NewOpenAIProvider(OpenAIProviderConfig{
		APIKey: secrets.NewHidden("oracle-placeholder"), Model: lineCase.Model, MaxOutputTokens: lineCase.MaxOutputTokens,
	})
	var request CompletionRequest
	switch lineCase.Kind {
	case "categorization":
		request = CategorizationRequest(BuildPrompt(lineCase.SourceBlock))
	case "explanation":
		request = InvestmentMixExplanationRequest(lineCase.Prompt)
	case "plain":
		request = WorkUnitExplanationRequest(lineCase.Prompt)
	default:
		return nil, fmt.Errorf("unknown kind %q", lineCase.Kind)
	}
	return provider.batchLine(BatchItem{CustomID: lineCase.CustomID, Request: request})
}

// tagJSON tags every leaf of a JSON document with its type, the same way the
// Python program does, so no number is compared through float64.
func tagJSON(document []byte) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(tagValue(value))
}

func tagValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return map[string]any{"t": "null", "v": ""}
	case bool:
		return map[string]any{"t": "bool", "v": fmt.Sprint(typed)}
	case json.Number:
		return map[string]any{"t": "number", "v": typed.String()}
	case string:
		return map[string]any{"t": "str", "v": typed}
	case []any:
		items := make([]any, len(typed))
		for index, item := range typed {
			items[index] = tagValue(item)
		}
		return map[string]any{"t": "array", "v": items}
	case map[string]any:
		object := make(map[string]any, len(typed))
		for key, item := range typed {
			object[key] = tagValue(item)
		}
		return map[string]any{"t": "object", "v": object}
	}
	panic(fmt.Sprintf("untaggable %T", value))
}

type taggedNode struct {
	T string          `json:"t"`
	V json.RawMessage `json:"v"`
}

// normalizeRawResponses replaces each raw_response that is a JSON document
// with its tagged form: openai.py re-encoded valid JSON with json.dumps
// (insertion order, ", " separators), Go compacts it; the document is what
// reaches the validator.
func normalizeRawResponses(tagged json.RawMessage) json.RawMessage {
	decoder := json.NewDecoder(bytes.NewReader(tagged))
	decoder.UseNumber()
	var document map[string]any
	if decoder.Decode(&document) != nil || document["t"] != "array" {
		return tagged
	}
	items, _ := document["v"].([]any)
	for _, item := range items {
		node, _ := item.(map[string]any)
		fields, _ := node["v"].(map[string]any)
		raw, _ := fields["raw_response"].(map[string]any)
		text, isText := raw["v"].(string)
		if raw["t"] != "str" || !isText || validateJSONOrEmpty(text) == "" {
			continue
		}
		inner, err := tagJSON([]byte(text))
		if err != nil {
			continue
		}
		fields["raw_response"] = map[string]any{"t": "json", "v": inner}
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return tagged
	}
	return encoded
}

// taggedEqual compares two tagged documents; numbers compare as exact
// rationals (JSON has one number type: 0 and 0.0 are the same value).
func taggedEqual(left, right json.RawMessage) (bool, string) {
	var a, b taggedNode
	if err := json.Unmarshal(left, &a); err != nil {
		return false, err.Error()
	}
	if err := json.Unmarshal(right, &b); err != nil {
		return false, err.Error()
	}
	return taggedNodeEqual(a, b, "$")
}

func taggedNodeEqual(a, b taggedNode, path string) (bool, string) {
	if a.T != b.T {
		return false, fmt.Sprintf("%s: type go=%s python=%s (go %s, python %s)", path, a.T, b.T, a.V, b.V)
	}
	switch a.T {
	case "object", "json":
		if a.T == "json" {
			var x, y taggedNode
			_ = json.Unmarshal(a.V, &x)
			_ = json.Unmarshal(b.V, &y)
			return taggedNodeEqual(x, y, path)
		}
		var x, y map[string]taggedNode
		_ = json.Unmarshal(a.V, &x)
		_ = json.Unmarshal(b.V, &y)
		keys := map[string]struct{}{}
		for key := range x {
			keys[key] = struct{}{}
		}
		for key := range y {
			keys[key] = struct{}{}
		}
		names := make([]string, 0, len(keys))
		for key := range keys {
			names = append(names, key)
		}
		sort.Strings(names)
		for _, key := range names {
			left, inLeft := x[key]
			right, inRight := y[key]
			if !inLeft || !inRight {
				return false, fmt.Sprintf("%s.%s: present go=%v python=%v", path, key, inLeft, inRight)
			}
			if equal, detail := taggedNodeEqual(left, right, path+"."+key); !equal {
				return false, detail
			}
		}
		return true, ""
	case "array":
		var x, y []taggedNode
		_ = json.Unmarshal(a.V, &x)
		_ = json.Unmarshal(b.V, &y)
		if len(x) != len(y) {
			return false, fmt.Sprintf("%s: length go=%d python=%d", path, len(x), len(y))
		}
		for index := range x {
			if equal, detail := taggedNodeEqual(x[index], y[index], fmt.Sprintf("%s[%d]", path, index)); !equal {
				return false, detail
			}
		}
		return true, ""
	case "number":
		var x, y string
		_ = json.Unmarshal(a.V, &x)
		_ = json.Unmarshal(b.V, &y)
		left, okLeft := new(big.Rat).SetString(x)
		right, okRight := new(big.Rat).SetString(y)
		if !okLeft || !okRight || left.Cmp(right) != 0 {
			return false, fmt.Sprintf("%s: number go=%s python=%s", path, x, y)
		}
		return true, ""
	}
	var x, y string
	if json.Unmarshal(a.V, &x) != nil || json.Unmarshal(b.V, &y) != nil {
		return false, fmt.Sprintf("%s: undecodable leaf", path)
	}
	if x != y {
		return false, fmt.Sprintf("%s: %s", path, firstDifference(x, y))
	}
	return true, ""
}

// firstDifference names where two strings first differ, with a little context.
func firstDifference(goValue, pythonValue string) string {
	index := 0
	for index < len(goValue) && index < len(pythonValue) && goValue[index] == pythonValue[index] {
		index++
	}
	clip := func(value string) string {
		start := max(0, index-20)
		end := min(len(value), index+40)
		return value[start:end]
	}
	return fmt.Sprintf("first difference at byte %d: go=%q python=%q (lengths %d, %d)",
		index, clip(goValue), clip(pythonValue), len(goValue), len(pythonValue))
}

func sortedNames[V any](table map[string]V) []string {
	names := make([]string, 0, len(table))
	for name := range table {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedCopy(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

// TestBatchOracleComparatorSeesEachKindOfDifference keeps the comparator
// honest: a comparator that stops seeing a kind of difference would turn the
// oracle into a test that cannot fail.
func TestBatchOracleComparatorSeesEachKindOfDifference(t *testing.T) {
	cases := []struct {
		name        string
		left, right string
		equal       bool
	}{
		{"same document", `{"a":[1,"x",null,true]}`, `{"a":[1,"x",null,true]}`, true},
		{"0 and 0.0 are one number", `{"a":0}`, `{"a":0.0}`, true},
		{"a missing key", `{"a":1}`, `{"a":1,"b":2}`, false},
		{"another number", `{"a":1}`, `{"a":2}`, false},
		{"another string", `{"a":"x"}`, `{"a":"y"}`, false},
		{"another type", `{"a":"1"}`, `{"a":1}`, false},
		{"another length", `[1]`, `[1,1]`, false},
		{"null is not false", `[null]`, `[false]`, false},
	}
	for _, tc := range cases {
		left, err := tagJSON([]byte(tc.left))
		if err != nil {
			t.Fatal(err)
		}
		right, err := tagJSON([]byte(tc.right))
		if err != nil {
			t.Fatal(err)
		}
		if equal, detail := taggedEqual(left, right); equal != tc.equal {
			t.Errorf("%s: equal = %v (%s)", tc.name, equal, detail)
		}
	}
}
