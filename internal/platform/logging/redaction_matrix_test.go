package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"
)

// protectedSpellings is every spelling family the key classifier must treat
// alike, applied to each protected concept below.
var protectedSpellings = []struct {
	name  string
	spell func(words []string) string
}{
	{"snake", func(words []string) string { return strings.Join(words, "_") }},
	{"kebab", func(words []string) string { return strings.Join(words, "-") }},
	{"camel", func(words []string) string { return camel(words, false) }},
	{"pascal", func(words []string) string { return camel(words, true) }},
	{"upper_snake", func(words []string) string { return strings.ToUpper(strings.Join(words, "_")) }},
	{"dotted", func(words []string) string { return strings.Join(words, ".") }},
	{"run_together", func(words []string) string { return strings.Join(words, "") }},
	{"plural", func(words []string) string { return strings.Join(words, "_") + "s" }},
}

// protectedConcepts are keys a provider or a configuration uses for a
// credential, as words.
var protectedConcepts = [][]string{
	{"token"},
	{"access", "token"},
	{"private", "token"},
	{"api", "key"},
	{"client", "secret"},
	{"password"},
	{"authorization"},
	{"session", "cookie"},
	{"database", "url"},
}

func camel(words []string, upperFirst bool) string {
	var builder strings.Builder
	for index, word := range words {
		if index == 0 && !upperFirst {
			builder.WriteString(word)
			continue
		}
		runes := []rune(word)
		runes[0] = unicode.ToUpper(runes[0])
		builder.WriteString(string(runes))
	}
	return builder.String()
}

// carriers put one protected key/value pair and one neutral pair into text
// the way it reaches a log line.
var carriers = []struct {
	name  string
	build func(key, value, control string) string
}{
	{"json", func(key, value, control string) string {
		return fmt.Sprintf(`{"%s":"%s","note":"%s"}`, key, value, control)
	}},
	{"json_nested_object", func(key, value, control string) string {
		return fmt.Sprintf(`{"%s":{"inner":"%s"},"note":"%s"}`, key, value, control)
	}},
	{"json_quoted_in_text", func(key, value, control string) string {
		return fmt.Sprintf(`provider request failed: status=400 body=%q note=%s`, fmt.Sprintf(`{"%s":"%s"}`, key, value), control)
	}},
	{"json_quoted_twice", func(key, value, control string) string {
		inner := fmt.Sprintf("%q", fmt.Sprintf(`{"%s":"%s"}`, key, value))
		return fmt.Sprintf(`outer=%q note=%s`, inner, control)
	}},
	{"query_string", func(key, value, control string) string {
		return fmt.Sprintf(`GET /api/v4/projects?%s=%s&note=%s failed`, key, value, control)
	}},
	{"header_line", func(key, value, control string) string {
		return fmt.Sprintf("%s: %s\nnote: %s", key, value, control)
	}},
	{"key_value", func(key, value, control string) string {
		return fmt.Sprintf(`%s=%s note=%s`, key, value, control)
	}},
	{"single_quoted", func(key, value, control string) string {
		return fmt.Sprintf(`'%s'='%s' note=%s`, key, value, control)
	}},
	{"percent_query", func(key, value, control string) string {
		return fmt.Sprintf(`GET /api/v4/projects?%s%%3D%s&note=%s failed`, key, value, control)
	}},
	{"percent_json", func(key, value, control string) string {
		return fmt.Sprintf(`body=%%7B%%22%s%%22%%3A%%22%s%%22%%2C%%22note%%22%%3A%%22%s%%22%%7D`, key, value, control)
	}},
	{"path_segment", func(key, value, control string) string {
		return fmt.Sprintf(`GET /download/%s/%s?note=%s failed`, key, value, control)
	}},
	// Malformed or ambiguous shapes. Where a value appears twice, both
	// copies must be hidden.
	{"header_no_space", func(key, value, control string) string {
		return fmt.Sprintf("%s:%s\nnote: %s", key, value, control)
	}},
	{"key_letters_encoded", func(key, value, control string) string {
		return fmt.Sprintf("q=%s%%3D%s&note=%s", encodeEveryByte(key), value, control)
	}},
	{"delimiter_inside_value", func(key, value, control string) string {
		return fmt.Sprintf("GET /x?%s=%s%%26%s%%22%s&note=%s", key, value, value, value, control)
	}},
	{"encoded_twice", func(key, value, control string) string {
		return fmt.Sprintf("q=%s%%253D%s%%2526%s&note=%s", key, value, value, control)
	}},
	{"quote_inside_value", func(key, value, control string) string {
		return fmt.Sprintf(`{"%s":"%s\"%s","note":"%s"}`, key, value, value, control)
	}},
	{"mismatched_brackets", func(key, value, control string) string {
		return fmt.Sprintf(`note=%s %s=[%s}%s`, control, key, value, value)
	}},
	{"unclosed_quote", func(key, value, control string) string {
		return fmt.Sprintf(`note=%s {"%s":"%s %s`, control, key, value, value)
	}},
	{"unclosed_bracket", func(key, value, control string) string {
		return fmt.Sprintf(`note=%s %s={"a":"%s","b":"%s"`, control, key, value, value)
	}},
	{"go_map", func(key, value, control string) string {
		return fmt.Sprintf("%v", map[string]string{key: value, "note": control})
	}},
	{"go_struct", func(key, value, control string) string {
		return fmt.Sprintf("%+v", dynamicStruct(key, value, control))
	}},
}

// neutralHiddenByDesign reports the cells whose neutral value sits after a
// header-named key on the same line: a Cookie or Authorization value runs to
// the end of its line, so the neutral pair after it is hidden too.
func neutralHiddenByDesign(key, carrier string) bool {
	// go_struct names its field from the key's words, singular.
	if !headerKeysToEndOfLine[strings.ToLower(key)] && !(carrier == "go_struct" && headerKeysToEndOfLine[strings.Join(keyWords(key), "")]) {
		return false
	}
	switch carrier {
	case "go_map", "go_struct", "key_value", "query_string", "percent_query",
		"key_letters_encoded", "delimiter_inside_value", "encoded_twice":
		return true
	}
	return false
}

// jsonCarriers build a JSON document; redacting it must leave valid JSON.
var jsonCarriers = map[string]bool{"json": true, "json_nested_object": true, "quote_inside_value": true}

func encodeEveryByte(text string) string {
	var builder strings.Builder
	for index := 0; index < len(text); index++ {
		fmt.Fprintf(&builder, "%%%02X", text[index])
	}
	return builder.String()
}

// dynamicStruct builds a struct whose first field is named after key, so %+v
// prints "{<Key>:<value> Note:<control>}".
func dynamicStruct(key, value, control string) any {
	name := camel(keyWords(key), true)
	if name == "" || !unicode.IsLetter([]rune(name)[0]) {
		name = "X" + name
	}
	typ := reflect.StructOf([]reflect.StructField{
		{Name: name, Type: reflect.TypeOf("")},
		{Name: "Note", Type: reflect.TypeOf("")},
	})
	instance := reflect.New(typ).Elem()
	instance.Field(0).SetString(value)
	instance.Field(1).SetString(control)
	return instance.Interface()
}

type textValuer struct{ text string }

func (v textValuer) LogValue() slog.Value { return slog.StringValue(v.text) }

// positions carry a text to the handler in each way a worker log call can.
var positions = []struct {
	name string
	log  func(logger *slog.Logger, text string)
}{
	{"message", func(logger *slog.Logger, text string) { logger.Warn(text) }},
	{"string_attr", func(logger *slog.Logger, text string) { logger.Warn("m", "cause", text) }},
	{"error_attr", func(logger *slog.Logger, text string) { logger.Warn("m", "error", errors.New(text)) }},
	{"wrapped_error", func(logger *slog.Logger, text string) {
		logger.Warn("m", "error", fmt.Errorf("sync unit: %w", errors.New(text)))
	}},
	{"joined_error", func(logger *slog.Logger, text string) {
		logger.Warn("m", "error", errors.Join(errors.New("first"), errors.New(text)))
	}},
	{"group", func(logger *slog.Logger, text string) { logger.Warn("m", slog.Group("detail", "cause", text)) }},
	{"nested_group", func(logger *slog.Logger, text string) {
		logger.Warn("m", slog.Group("outer", slog.Group("inner", "cause", text)))
	}},
	{"with_attrs", func(logger *slog.Logger, text string) { logger.With("cause", text).Warn("m") }},
	{"with_group", func(logger *slog.Logger, text string) { logger.WithGroup("unit").Warn("m", "cause", text) }},
	{"log_valuer", func(logger *slog.Logger, text string) { logger.Warn("m", "cause", textValuer{text}) }},
	{"any_string", func(logger *slog.Logger, text string) { logger.Warn("m", slog.Any("cause", text)) }},
	{"any_slice", func(logger *slog.Logger, text string) { logger.Warn("m", "causes", []string{"first", text}) }},
	{"any_map", func(logger *slog.Logger, text string) { logger.Warn("m", "result", map[string]any{"cause": text}) }},
	{"log_attrs", func(logger *slog.Logger, text string) {
		logger.LogAttrs(context.Background(), slog.LevelWarn, "m", slog.String("cause", text))
	}},
}

// TestEveryCarrierSpellingAndPositionRedactsTheProtectedValue enumerates
// concepts x spellings x carriers x positions through NewJSON, the handler
// every worker binary logs through. Each cell carries a unique protected
// value that must be absent and a unique neutral value that must be present,
// so a blanket wipe of the attribute fails as surely as a leak.
func TestEveryCarrierSpellingAndPositionRedactsTheProtectedValue(t *testing.T) {
	t.Parallel()
	want := len(protectedConcepts) * len(protectedSpellings) * len(carriers) * len(positions)
	cells := 0
	for conceptIndex, concept := range protectedConcepts {
		for _, spelling := range protectedSpellings {
			key := spelling.spell(concept)
			for _, carrier := range carriers {
				for _, position := range positions {
					cells++
					value := fmt.Sprintf("canary%dx%d", conceptIndex, cells)
					control := fmt.Sprintf("control%dx%d", conceptIndex, cells)
					text := carrier.build(key, value, control)
					if jsonCarriers[carrier.name] && !json.Valid([]byte(RedactText(text))) {
						t.Errorf("%s/%s/%s: redacted JSON is invalid: %s", key, spelling.name, carrier.name, RedactText(text))
					}
					var output bytes.Buffer
					position.log(NewJSON(&output, slog.LevelInfo), text)
					line := output.String()
					if strings.Contains(line, value) {
						t.Errorf("%s/%s/%s/%s leaked: %s", key, spelling.name, carrier.name, position.name, line)
						continue
					}
					if !strings.Contains(line, control) && !neutralHiddenByDesign(key, carrier.name) {
						t.Errorf("%s/%s/%s/%s lost the neutral value: %s", key, spelling.name, carrier.name, position.name, line)
					}
					if !json.Valid(bytes.TrimSpace(output.Bytes())) {
						t.Errorf("%s/%s/%s/%s is not valid JSON: %s", key, spelling.name, carrier.name, position.name, line)
					}
				}
			}
		}
	}
	if cells != want || cells != 9*8*21*14 {
		t.Fatalf("enumerated %d cells, want %d", cells, want)
	}
}

// TestProtectedAttributeKeyHidesEveryKind: an attribute or group named like a
// credential hides its value whatever slog.Kind carries it.
func TestProtectedAttributeKeyHidesEveryKind(t *testing.T) {
	t.Parallel()
	values := []slog.Value{
		slog.StringValue("canary-string"),
		slog.Int64Value(981234567),
		slog.Uint64Value(981234568),
		slog.Float64Value(98123.4569),
		slog.BoolValue(true),
		slog.DurationValue(981234570 * time.Nanosecond),
		slog.TimeValue(time.Date(2031, 1, 2, 3, 4, 5, 0, time.UTC)),
		slog.AnyValue(errors.New("canary-error")),
		slog.AnyValue(map[string]string{"inner": "canary-map"}),
		slog.AnyValue(struct{ Inner string }{"canary-struct"}),
		slog.AnyValue([]byte("canary-bytes")),
		slog.GroupValue(slog.String("inner", "canary-group")),
		slog.AnyValue(textValuer{"canary-valuer"}),
	}
	kinds := map[slog.Kind]bool{}
	for _, value := range values {
		kinds[value.Kind()] = true
	}
	for kind := slog.KindAny; kind <= slog.KindLogValuer; kind++ {
		if !kinds[kind] {
			t.Fatalf("slog.Kind %s has no cell", kind)
		}
	}
	for _, key := range []string{"token", "apiKey", "client-secret", "Authorization", "database_url"} {
		for _, value := range values {
			for _, carrier := range []string{"attr", "group_name", "outer_group_name", "with_group_name"} {
				var output bytes.Buffer
				logger := NewJSON(&output, slog.LevelInfo)
				switch carrier {
				case "attr":
					logger.Info("m", slog.Attr{Key: key, Value: value}, "note", "visible")
				case "group_name":
					logger.Info("m", slog.Group(key, slog.Attr{Key: "inner_value", Value: value}), "note", "visible")
				case "outer_group_name":
					logger.Info("m", slog.Group(key, slog.Group("middle", slog.Attr{Key: "inner_value", Value: value})), "note", "visible")
				case "with_group_name":
					logger.WithGroup(key).WithGroup("middle").Info("m", slog.Attr{Key: "inner_value", Value: value})
					logger.Info("m", "note", "visible")
				}
				line := output.String()
				for _, fragment := range []string{"canary", "98123", "2031-01-02", "Y2FuYXJ5"} {
					if strings.Contains(line, fragment) {
						t.Errorf("%s %s kind=%s leaked %q: %s", carrier, key, value.Kind(), fragment, line)
					}
				}
				if value.Kind() == slog.KindBool && strings.Contains(line, "true") {
					t.Errorf("%s %s bool leaked: %s", carrier, key, line)
				}
				if !strings.Contains(line, `"note":"visible"`) || !strings.Contains(line, redacted) {
					t.Errorf("%s %s kind=%s: %s", carrier, key, value.Kind(), line)
				}
			}
		}
	}
}

type sensitiveRecord struct {
	Name     string            `json:"name"`
	APIToken string            `json:"apiToken"`
	Nested   *sensitiveNested  `json:"nested"`
	Headers  map[string]string `json:"labels"`
}

type sensitiveNested struct {
	ClientSecret string `json:"client_secret"`
	Detail       string `json:"detail"`
}

type marshalerValue struct{}

func (marshalerValue) MarshalJSON() ([]byte, error) {
	return []byte(`{"private_token":"canary-marshaler","note":"visible-marshaler"}`), nil
}

// TestStructuredValuesAreRedactedAtEveryDepth covers values the handler
// marshals itself: typed structs, pointers, maps, slices, json.RawMessage and
// types with their own MarshalJSON.
func TestStructuredValuesAreRedactedAtEveryDepth(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		value   any
		visible []string
	}{
		{"struct", sensitiveRecord{Name: "visible-name", APIToken: "canary-a", Nested: &sensitiveNested{ClientSecret: "canary-b", Detail: "visible-detail"}, Headers: map[string]string{"Authorization": "canary-c", "Accept": "visible-accept"}}, []string{"visible-name", "visible-detail", "visible-accept"}},
		{"pointer", &sensitiveNested{ClientSecret: "canary-d", Detail: "visible-ptr"}, []string{"visible-ptr"}},
		{"map_any", map[string]any{"rows": []any{map[string]any{"refresh_token": "canary-e", "id": "visible-id"}}}, []string{"visible-id"}},
		{"raw_message", json.RawMessage(`{"x-api-key":"canary-f","status":"visible-status"}`), []string{"visible-status"}},
		{"string_inside_map", map[string]string{"error": `upstream said {"password":"canary-g"}`, "state": "visible-state"}, []string{"visible-state"}},
		{"marshaler", marshalerValue{}, []string{"visible-marshaler"}},
		{"slice_of_text", []string{"token=canary-h", "visible-slice"}, []string{"visible-slice"}},
		{"bytes", []byte(`{"token":"canary-i","note":"visible-bytes"}`), []string{"visible-bytes"}},
	}
	for _, testCase := range cases {
		var output bytes.Buffer
		NewJSON(&output, slog.LevelInfo).Info("m", "value", testCase.value)
		line := output.String()
		if strings.Contains(line, "canary") {
			t.Errorf("%s leaked: %s", testCase.name, line)
		}
		for _, visible := range testCase.visible {
			if !strings.Contains(line, visible) {
				t.Errorf("%s lost %q: %s", testCase.name, visible, line)
			}
		}
		if !json.Valid(bytes.TrimSpace(output.Bytes())) {
			t.Errorf("%s: invalid JSON: %s", testCase.name, line)
		}
	}
}

type panickingError struct{}

func (panickingError) Error() string { panic("token=canary-panic") }

type panickingMarshaler struct{}

func (panickingMarshaler) MarshalJSON() ([]byte, error) { panic("token=canary-panic") }

type panickingValuer struct{}

func (panickingValuer) LogValue() slog.Value { panic("token=canary-panic") }

// TestRedactorFailuresLogAFixedMarker: a panic inside an error's Error, a
// value's MarshalJSON or LogValue, or an unmarshallable value, logs a fixed
// marker for that attribute and keeps the rest of the line.
func TestRedactorFailuresLogAFixedMarker(t *testing.T) {
	t.Parallel()
	for name, value := range map[string]any{
		"error_panics":     panickingError{},
		"marshaler_panics": panickingMarshaler{},
		"valuer_panics":    panickingValuer{},
		"unmarshallable":   make(chan int),
	} {
		var output bytes.Buffer
		NewJSON(&output, slog.LevelInfo).Info("m", "value", value, "note", "visible")
		line := output.String()
		if strings.Contains(line, "canary") {
			t.Errorf("%s leaked: %s", name, line)
		}
		if !strings.Contains(line, `"note":"visible"`) {
			t.Errorf("%s lost the rest of the line: %s", name, line)
		}
		// slog itself recovers a LogValue panic and logs its own fixed
		// "LogValue panicked" text with a stack, never the panic value.
		if !strings.Contains(line, redactionFailed) && !strings.Contains(line, unloggable) && !strings.Contains(line, "LogValue panicked") {
			t.Errorf("%s has no fixed marker: %s", name, line)
		}
	}
}

// TestLargeAndInvalidUTF8TextIsBoundedAndRedacted: a value larger than the
// bound is cut on a rune boundary after redaction; invalid UTF-8 does not
// hide a protected pair.
func TestLargeAndInvalidUTF8TextIsBoundedAndRedacted(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	for offset := 0; offset < 4; offset++ {
		output.Reset()
		large := strings.Repeat("a", offset) + strings.Repeat("€", maxLoggedValueBytes) + ` "token":"canary-tail"`
		NewJSON(&output, slog.LevelInfo).Info("m", "cause", `{"token":"canary-head"} `+large)
		line := output.String()
		if strings.Contains(line, "canary") || !strings.Contains(line, truncatedSuffix) || !json.Valid(bytes.TrimSpace(output.Bytes())) {
			t.Fatalf("offset %d: large value not bounded and redacted (%d bytes)", offset, len(line))
		}
		if len(line) > maxLoggedValueBytes+512 {
			t.Fatalf("offset %d: line is %d bytes, want at most the bound plus framing", offset, len(line))
		}
		var decoded struct{ Cause string }
		if err := json.Unmarshal(output.Bytes(), &decoded); err != nil || !utf8.ValidString(decoded.Cause) || strings.ContainsRune(decoded.Cause, utf8.RuneError) {
			t.Fatalf("offset %d: truncation split a rune: %v", offset, err)
		}
	}
	output.Reset()
	NewJSON(&output, slog.LevelInfo).Info("m", "result", map[string]string{"note": strings.Repeat("v", 2*maxLoggedValueBytes)})
	if !strings.Contains(output.String(), truncatedSuffix) || !json.Valid(bytes.TrimSpace(output.Bytes())) || output.Len() > maxLoggedValueBytes+512 {
		t.Fatalf("a large structured value is not bounded (%d bytes)", output.Len())
	}
	output.Reset()
	NewJSON(&output, slog.LevelInfo).Info("m", "cause", "\xff\xfe token=canary-utf8 \xc3")
	if strings.Contains(output.String(), "canary") {
		t.Fatalf("invalid UTF-8 hid a protected pair: %s", output.String())
	}
}

// TestKeptVisible pins what debugging needs: error class, status, path,
// request ids, sync ids, provider and dataset stay in the line.
func TestKeptVisible(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	NewJSON(&output, slog.LevelInfo).Warn("provider_sync.unit_failed",
		"error", errors.New("provider request failed: permanent status=400 path=/repos/octo/hello/deployments"),
		"request_id", "req-visible-1", "x-request-id", "req-visible-2", "provider_request_id", "req-visible-3",
		"sync_run_id", "run-visible", "sync_unit_id", "unit-visible", "provider", "github", "dataset", "deployments",
		"error_class", "permanent", "status_code", 400,
		"prompt_tokens", 1201, "completionTokens", 87,
	)
	line := output.String()
	for _, want := range []string{"permanent status=400 path=/repos/octo/hello/deployments", "req-visible-1", "req-visible-2", "req-visible-3", "run-visible", "unit-visible", `"provider":"github"`, `"dataset":"deployments"`, `"error_class":"permanent"`, `"status_code":400`, `"prompt_tokens":1201`, `"completionTokens":87`} {
		if !strings.Contains(line, want) {
			t.Errorf("missing %q: %s", want, line)
		}
	}
	if strings.Contains(line, redacted) {
		t.Errorf("a debugging value was redacted: %s", line)
	}
}

type snippetError struct{ body string }

func (e snippetError) Error() string               { return "provider request failed: permanent status=400" }
func (e snippetError) ResponseBodySnippet() string { return e.body }

// TestNoLogLevelAppendsAResponseBody: an error that carries a response body
// behind an accessor logs its Error() text only, at every configured level.
func TestNoLogLevelAppendsAResponseBody(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("lookup: %w", snippetError{body: `{"message":"canary-reason"}`})
	for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn} {
		var output bytes.Buffer
		NewJSON(&output, level).Error("m", "error", err)
		if strings.Contains(output.String(), "canary") || !strings.Contains(output.String(), "lookup: provider request failed: permanent status=400") {
			t.Fatalf("level %s: %s", level, output.String())
		}
	}
}

// TestProviderAssignedIDShape: 1-64 bytes of letters, digits, "_" and "-"
// pass; anything else is dropped; empty is no id.
func TestProviderAssignedIDShape(t *testing.T) {
	t.Parallel()
	for raw, want := range map[string]struct {
		id      string
		dropped bool
	}{
		"":                                     {"", false},
		"4ef9a1c2-8b3d-4c1e-9f00-1a2b3c4d5e6f": {"4ef9a1c2-8b3d-4c1e-9f00-1a2b3c4d5e6f", false},
		"msg_ABC-123":                          {"msg_ABC-123", false},
		strings.Repeat("a", 64):                {strings.Repeat("a", 64), false},
		strings.Repeat("a", 65):                {"", true},
		"id with space":                        {"", true},
		"token=abc":                            {"", true},
		`{"id":"x"}`:                           {"", true},
		"id\n":                                 {"", true},
		"é":                                    {"", true},
		"a.b":                                  {"", true},
	} {
		id, dropped := ProviderAssignedID(raw)
		if id != want.id || dropped != want.dropped {
			t.Errorf("ProviderAssignedID(%q) = %q, %v; want %q, %v", raw, id, dropped, want.id, want.dropped)
		}
	}
}

// TestTokenCountKeysStayVisibleButTokenKeysDoNot: a language-model token
// count is a number worth logging; every other spelling of a token key stays
// protected, including a count word that is not directly before "tokens".
func TestTokenCountKeysStayVisibleButTokenKeysDoNot(t *testing.T) {
	t.Parallel()
	for key, want := range map[string]bool{
		"prompt_tokens": false, "completionTokens": false, "total-tokens": false, "max_tokens": false,
		"tokens": true, "access_tokens": true, "prompt_token": true, "prompt_access_tokens": true,
		"api_key_prompt_tokens": true, "prompttokens": true, "tokens_prompt": true,
	} {
		if got := ProtectedKey(key); got != want {
			t.Errorf("ProtectedKey(%q) = %v, want %v", key, got, want)
		}
	}
}

// TestCallerAttributesNamedLikeBuiltInsAreRedacted: an attribute that reuses
// the handler's own "time", "level", "source" or "msg" key is still redacted;
// the handler's own time, level and source stay intact.
func TestCallerAttributesNamedLikeBuiltInsAreRedacted(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{AddSource: true, ReplaceAttr: attrRedactor{}.replace}))
	logger.Info("m", "time", "token=canary-time", "level", `{"password":"canary-level"}`, "source", "secret=canary-source", "msg", "api_key=canary-msg")
	line := output.String()
	if strings.Contains(line, "canary") {
		t.Fatalf("a caller attribute named like a built-in skipped redaction: %s", line)
	}
	for _, want := range []string{`"level":"INFO"`, `"source":{"function"`, `"time":"`} {
		if !strings.Contains(line, want) {
			t.Fatalf("built-in %s changed: %s", want, line)
		}
	}
}

// TestProviderTokenPrefixesAreRedactedInProse: a provider token with no key
// in front of it is caught by its documented prefix.
func TestProviderTokenPrefixesAreRedactedInProse(t *testing.T) {
	t.Parallel()
	for _, prefix := range []string{"ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_", "glpat-", "gloas-", "glrt-", "gldt-", "glptt-", "glsoat-", "glft-", "glimt-", "glagent-", "glcbt-", "glffct-", "lin_api_", "lin_oauth_", "ATATT", "ATCTT", "pdus+_"} {
		token := prefix + strings.Repeat("A1b2", 6)
		got := RedactText("token " + token + " is invalid")
		if strings.Contains(got, token) || !strings.Contains(got, "is invalid") {
			t.Errorf("RedactText kept %q: %q", prefix, got)
		}
	}
}

// TestLaunchDarklyKeysAreRedactedInProse: LaunchDarkly keys are a kind
// prefix and a UUID; a bare UUID (a sync id) stays visible.
func TestLaunchDarklyKeysAreRedactedInProse(t *testing.T) {
	t.Parallel()
	const uuid = "3f1c1f7e-9a51-4b8e-9d7e-6f0a3c2b1d10"
	for _, prefix := range []string{"api-", "sdk-", "mob-"} {
		if got := RedactText("launchdarkly rejected " + prefix + uuid); strings.Contains(got, uuid) {
			t.Errorf("RedactText kept %s key: %q", prefix, got)
		}
	}
	if got := RedactText("sync_run " + uuid); got != "sync_run "+uuid {
		t.Errorf("a bare UUID changed: %q", got)
	}
}

// TestHeaderValuesRunToEndOfLine: a Cookie or Authorization header value
// carries its own separators and is redacted whole; the next line is kept.
func TestHeaderValuesRunToEndOfLine(t *testing.T) {
	t.Parallel()
	for _, text := range []string{
		"Cookie: a=canary-one; b=canary-two\nnote: visible",
		"Set-Cookie: sid=canary-one; Path=/; canary-two\r\nnote: visible",
		"Proxy-Authorization: Negotiate canary-one canary-two\nnote: visible",
	} {
		got := RedactText(text)
		if strings.Contains(got, "canary") || !strings.Contains(got, "note: visible") {
			t.Errorf("RedactText(%q) = %q", text, got)
		}
	}
}

// TestKeyStartsAtALetterOrUnderscore: a key glued to leading digits is read
// from its first letter, so "1token=v" still hides v.
func TestKeyStartsAtALetterOrUnderscore(t *testing.T) {
	t.Parallel()
	for _, text := range []string{"1token=canary", "x9.-_token=canary", "--api-key=canary", "..secret: canary", "9api_key=canary", "-.api.key=canary", "9raw_payload=canary", "4response_body: canary"} {
		if got := RedactText(text); strings.Contains(got, "canary") {
			t.Errorf("RedactText(%q) = %q", text, got)
		}
	}
	if got := RedactText("12:30 status=200"); got != "12:30 status=200" {
		t.Errorf("text with no protected key changed: %q", got)
	}
}

// TestUnclosedValuesRunToTheEnd: a quoted or bracketed value that never
// closes is redacted to the end of the text.
func TestUnclosedValuesRunToTheEnd(t *testing.T) {
	t.Parallel()
	for _, text := range []string{`{"token":"a\"canary-a\"b","note":"x"}`, `\"token\":\"a\\\"canary-a\\\"b\"`, `{"token":"canary-a canary-b`, `token={"inner":"canary-a"`, `token=[canary-a, canary-b`, `\"token\":\"canary-a`} {
		if got := RedactText(text); strings.Contains(got, "canary") {
			t.Errorf("RedactText(%q) = %q", text, got)
		}
	}
}

// TestInstallDefaultRoutesAndRestores: while installed, slog.Default() and the
// standard log package write through the redacting logger; the returned
// function restores the previous default and the log package's writer.
// Not parallel: it swaps the process default.
func TestInstallDefaultRoutesAndRestores(t *testing.T) {
	var redactingOutput, previousOutput bytes.Buffer
	previous := slog.New(slog.NewJSONHandler(&previousOutput, nil))
	restoreOuter := InstallDefault(previous)
	defer restoreOuter()
	previousWriter := log.Writer()

	restore := InstallDefault(NewJSON(&redactingOutput, slog.LevelInfo))
	slog.Warn("m", "cause", "token=canary-slog")
	log.Printf("secret=canary-log")
	if strings.Contains(redactingOutput.String(), "canary") || !strings.Contains(redactingOutput.String(), `"msg":"secret=[REDACTED]"`) {
		t.Fatalf("installed default did not redact: %s", redactingOutput.String())
	}
	restore()
	if slog.Default() != previous || log.Writer() != previousWriter {
		t.Fatal("restore did not put back the previous default and log writer")
	}
	slog.Info("after")
	if !strings.Contains(previousOutput.String(), `"msg":"after"`) {
		t.Fatalf("restored default does not receive lines: %s", previousOutput.String())
	}
}

// TestEveryVocabularyEntryIsProtected is generated from the vocabulary
// itself: every listed word, every listed pair in snake_case and every
// fragment is protected, and the first word of a pair alone is not.
func TestEveryVocabularyEntryIsProtected(t *testing.T) {
	t.Parallel()
	if len(protectedKeyWords) == 0 || len(protectedKeyWordPairs) == 0 {
		t.Fatal("empty vocabulary")
	}
	for word := range protectedKeyWords {
		if !ProtectedKey(word) || !ProtectedKey("x_"+word+"_y") {
			t.Errorf("word %q is not protected", word)
		}
	}
	for pair := range protectedKeyWordPairs {
		if !ProtectedKey(pair[0] + "_" + pair[1]) {
			t.Errorf("pair %v is not protected", pair)
		}
		if ProtectedKey(pair[0]) && !protectedKeyWords[pair[0]] {
			t.Errorf("first word of pair %v alone is protected", pair)
		}
	}
	for _, fragment := range protectedKeyFragments {
		if !ProtectedKey("x" + fragment + "y") {
			t.Errorf("fragment %q is not protected", fragment)
		}
	}
}

// TestCredentialShapedValuesAfterACredentialWordInProse: a provider body
// written as prose hides a credential-shaped value after a credential word,
// in every spelling of the word; ordinary prose after the word stays.
func TestCredentialShapedValuesAfterACredentialWordInProse(t *testing.T) {
	t.Parallel()
	for _, word := range []string{"token", "Tokens", "secret", "password", "passwd", "apikey", "api key", "API-Key", "client_secret", "access token", "private-token", "credential"} {
		for _, value := range []string{"canary-prose-1", "canary_prose_2", "canary.prose.3", "canaryprose4x"} {
			got := RedactText("rejected: " + word + " " + value + " is invalid")
			if strings.Contains(got, "canary") || !strings.Contains(got, "is invalid") {
				t.Errorf("%q / %q -> %q", word, value, got)
			}
		}
	}
	for _, text := range []string{"token is invalid", "the password expired yesterday", "secret rotation scheduled", "token abc", "token v1-2", "api key a.b"} {
		if got := RedactText(text); got != text {
			t.Errorf("ordinary prose changed: %q -> %q", text, got)
		}
	}
}

// TestPathSegmentsNamedLikeCredentials: "/token/<value>" hides the value; a
// segment holding a protected word hides itself; other segments stay.
func TestPathSegmentsNamedLikeCredentials(t *testing.T) {
	t.Parallel()
	for text, want := range map[string]string{
		"path=/download/token-canary-1/file":    "path=/download/[REDACTED]/file",
		"path=/v1/token/canary-2/refresh":       "path=/v1/token/[REDACTED]/refresh",
		"path=/v1/apikeys/canary-3":             "path=/v1/apikeys/[REDACTED]",
		"path=/repos/octo/hello/pulls/7":        "path=/repos/octo/hello/pulls/7",
		"GET /api/v4/projects/12/issues failed": "GET /api/v4/projects/12/issues failed",
	} {
		if got := RedactText(text); got != want {
			t.Errorf("RedactText(%q) = %q, want %q", text, got, want)
		}
	}
}

// TestPercentEncodedTextIsReadDecoded: percent-encoded text is decoded
// (to a fixed point) before redaction and logged decoded, so an encoded key,
// separator or quote is read as what it stands for; a delimiter that was
// encoded is part of the value, and only a raw one ends it.
func TestPercentEncodedTextIsReadDecoded(t *testing.T) {
	t.Parallel()
	for text, want := range map[string]string{
		"q=token%3Dcanary%26note%3Dvisible":             "q=token=[REDACTED]",
		"q=api%5Fkey%3Dcanary%20note%3Dvisible&page=2":  "q=api_key=[REDACTED]&page=2",
		"body=%7B%22token%22%3A%22canary%22%7D":         `body={"token":"[REDACTED]"}`,
		"q=token%253Dcanary%2526more":                   "q=token=[REDACTED]",
		"GET /x?token=canary%2522still%2526secret&n=1":  "GET /x?token=[REDACTED]&n=1",
		"progress 50% done, 100%":                       "progress 50% done, 100%",
		"path=/rest/api/3/search/jql?jql=project+%3D+x": "path=/rest/api/3/search/jql?jql=project+=+x",
	} {
		if got := RedactText(text); got != want {
			t.Errorf("RedactText(%q) = %q, want %q", text, got, want)
		}
	}
}

// TestBracketedAndLiteralValuesKeepTheirBoundaries: a well-formed object or
// array value is hidden exactly, skipping brackets inside strings at the
// value's own escape depth; a JSON literal after a quoted key ends before
// the closing brace; brackets of the other type closing first hide the rest.
func TestBracketedAndLiteralValuesKeepTheirBoundaries(t *testing.T) {
	t.Parallel()
	for text, want := range map[string]string{
		`token={"a":"}","b":"canary"} note=visible`:              `token=[REDACTED] note=visible`,
		`token=["]","canary"] note=visible`:                      `token=[REDACTED] note=visible`,
		`token={\"a\":\"x\\\"}\",\"b\":\"canary\"} note=visible`: `token=[REDACTED] note=visible`,
		`{"note":"x","pin_password":1234}`:                       `{"note":"x","pin_password":"[REDACTED]"}`,
		`{"note":"x","secrets":[1,2]}`:                           `{"note":"x","secrets":"[REDACTED]"}`,
		`token=[a}b] canary-tail`:                                `token=[REDACTED]`,
	} {
		if got := RedactText(text); got != want {
			t.Errorf("RedactText(%q) = %q, want %q", text, got, want)
		}
	}
}

// TestStructuredKeysAreDecodedBeforeTheyAreClassified: a structured value is
// always decoded and every key classified, whatever its serialized text looks
// like (escaped key letters, a key split by separators).
func TestStructuredKeysAreDecodedBeforeTheyAreClassified(t *testing.T) {
	t.Parallel()
	for name, value := range map[string]any{
		"separated key":      map[string]string{"t_o_k_e_n": "canary-1", "note": "visible"},
		"escaped key":        json.RawMessage(`{"token":"canary-2","note":"visible"}`),
		"escaped deep":       json.RawMessage(`{"a":[{"secret":"canary-3"}],"note":"visible"}`),
		"bytes holding JSON": []byte(`{"\u0074oken":"canary-4","note":"visible"}`),
	} {
		var output bytes.Buffer
		NewJSON(&output, slog.LevelInfo).Info("m", "detail", value)
		if strings.Contains(output.String(), "canary") || !strings.Contains(output.String(), "visible") {
			t.Errorf("%s: %s", name, output.String())
		}
	}
}

// TestProviderIDAttrs: a well-shaped id is logged under its key; any other
// value is replaced by <key>_dropped=true, or by "[id_dropped]" in a list.
func TestProviderIDAttrs(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	NewJSON(&output, slog.LevelInfo).Info("m",
		ProviderIDAttr("deployment_id", "4ef9a1c2-8b3d"),
		ProviderIDAttr("event_id", "canary id"),
		ProviderIDAttr("board_id", strings.Repeat("9", 65)),
		ProviderIDsAttr("team_ids", []string{"team-a", "canary/team", "team_b"}),
	)
	line := output.String()
	for _, want := range []string{`"deployment_id":"4ef9a1c2-8b3d"`, `"event_id_dropped":true`, `"board_id_dropped":true`, `"team_ids":["team-a","[id_dropped]","team_b"]`} {
		if !strings.Contains(line, want) {
			t.Errorf("missing %s: %s", want, line)
		}
	}
	if strings.Contains(line, "canary") || strings.Contains(line, strings.Repeat("9", 65)) {
		t.Fatalf("a malformed id reached the line: %s", line)
	}
}
