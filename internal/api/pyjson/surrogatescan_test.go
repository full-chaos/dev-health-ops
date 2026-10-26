package pyjson_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func TestHasLoneSurrogateEscape(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want bool
	}{
		{"lone high", `{"a":"x\ud800y"}`, true},
		{"lone high upper case", `{"a":"\uD83D"}`, true},
		{"lone low", `{"a":"\udc00"}`, true},
		{"last code point of each range", `["\udbff"]`, true},
		{"last low", `["\udfff"]`, true},
		{"pair", `{"a":"😀"}`, false},
		{"pair upper case", `{"a":"😀"}`, false},
		{"reversed pair", `{"a":"\ude00\ud83d"}`, true},
		{"high then another high", `{"a":"\ud83d\ud83d"}`, true},
		{"high then a plain escape", `{"a":"\ud83dA"}`, true},
		{"high then a plain character", `{"a":"\ud83dA"}`, true},
		{"high, text, low", `{"a":"\ud83dx\ude00"}`, true},
		{"pair then lone", `{"a":"😀\ud800"}`, true},
		{"escaped backslash is not an escape", `{"a":"\\ud800"}`, false},
		{"escaped backslash then a real one", `{"a":"\\\ud800"}`, true},
		{"key", `{"\ud800":1}`, true},
		{"nested array", `{"a":[1,{"b":["ok","\udc00"]}]}`, true},
		{"literal emoji", "{\"a\":\"😀\"}", false},
		{"no escapes", `{"a":"plain","b":1,"c":[true,null]}`, false},
		{"escape outside a string is not scanned", `{"a":1}`, false},
		{"non surrogate escapes", `{"a":"Aé퟿￿"}`, false},
		{"truncated escape", `{"a":"\ud80`, false},
		{"truncated after the high", `{"a":"\ud800`, true},
		{"bad hex", `{"a":"\ud8zz"}`, false},
		{"empty", ``, false},
		{"raw bytes of a surrogate", "{\"a\":\"x\xed\xa0\x80y\"}", true},
		{"raw bytes of the last surrogate", "[\"\xed\xbf\xbf\"]", true},
		{"raw bytes of a surrogate in a key", "{\"\xed\xb0\x80\":1}", true},
		{"two raw halves are two lone surrogates", "[\"\xed\xa0\xbd\xed\xb8\x80\"]", true},
		{"the code point before the range", "[\"\xed\x9f\xbf\"]", false},
		{"the code point after the range", "[\"\xee\x80\x80\"]", false},
		{"a truncated raw surrogate", "[\"\xed\xa0\"]", false},
		{"raw valid multibyte text", "{\"a\":\"é名😀\"}", false},
		{"raw surrogate bytes outside a string are not scanned", "[1,\xed\xa0\x80]", false},
	}
	for _, tc := range cases {
		if got := pyjson.HasLoneSurrogateEscape([]byte(tc.doc)); got != tc.want {
			t.Errorf("%s: %q = %v, want %v", tc.name, tc.doc, got, tc.want)
		}
	}
}

// walkHasSurrogate is the reference: json.loads (pyjson.DecodeString) keeps a lone
// escape as a lone surrogate, so a string or key that holds one is exactly
// what the scanner must report.
func walkHasSurrogate(value pyjson.Value) bool {
	switch typed := value.(type) {
	case string:
		return pyjson.HasSurrogate(typed)
	case []pyjson.Value:
		for _, item := range typed {
			if walkHasSurrogate(item) {
				return true
			}
		}
	case *pyjson.Object:
		for _, key := range typed.Keys() {
			item, _ := typed.Get(key)
			if pyjson.HasSurrogate(key) || walkHasSurrogate(item) {
				return true
			}
		}
	}
	return false
}

// differential compares the scanner with the reference on one document that
// decodes. A document that does not decode is outside the contract: the
// callers scan only a body their decoder already accepted.
func differential(t testing.TB, doc []byte) {
	t.Helper()
	// DecodeBody sniffs a BOM or a NUL byte at the start as UTF-16 or UTF-32,
	// which encoding/json refuses outright, so no handler reads such a body as
	// text: it is outside the scanner's contract.
	if len(doc) > 0 && (doc[0] == 0 || doc[0] == 0xff || doc[0] == 0xfe || doc[0] == 0xef) || len(doc) > 1 && doc[1] == 0 {
		return
	}
	// The reference is the api plane's own reading of the bytes: DecodeBody
	// (json.loads on bytes, raw surrogate bytes kept) then DecodeString.
	text, err := pyjson.DecodeBody(doc)
	if err != nil {
		return
	}
	value, err := pyjson.DecodeString(text)
	if err != nil {
		return
	}
	if got, want := pyjson.HasLoneSurrogateEscape(doc), walkHasSurrogate(value); got != want {
		t.Fatalf("%q: scanner %v, reference %v", doc, got, want)
	}
}

func TestHasLoneSurrogateEscapeAgreesWithTheDecoderOnEveryCombination(t *testing.T) {
	pieces := []string{`\ud800`, `\udbff`, `\udc00`, `\udfff`, `\ud83d`, `\ude00`, `\uD83D`, `\\`, `\\u`, `\\ud800`, `A`, "a", "😀", `\"`, `\n`}
	seen := 0
	var build func(parts []string, depth int)
	build = func(parts []string, depth int) {
		if depth == 0 {
			body := strings.Join(parts, "")
			for _, shape := range []string{`{"k":"%s"}`, `["%s"]`, `{"%s":0}`, `"%s"`} {
				differential(t, []byte(strings.Replace(shape, "%s", body, 1)))
				seen++
			}
			return
		}
		for _, piece := range pieces {
			build(append(parts, piece), depth-1)
		}
	}
	for depth := 0; depth <= 3; depth++ {
		build(nil, depth)
	}
	if seen < 13000 {
		t.Fatalf("only %d documents compared", seen)
	}
}

func FuzzHasLoneSurrogateEscape(f *testing.F) {
	for _, seed := range []string{`{"a":"\ud800"}`, `{"a":"😀"}`, `["\\ud800"]`, `{"\udc00":"x"}`, `"\ud83d😀"`, `{"a":"é😀"}`, `[]`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, doc []byte) { differential(t, doc) })
}

func TestMemberHasLoneSurrogateEscapeReadsOnlyTheNamedMember(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		fold bool
		want bool
	}{
		{"named member lone", `{"relay_state":"\ud800"}`, false, true},
		{"named member pair", `{"relay_state":"😀"}`, false, false},
		{"other member lone is ignored", `{"relay_state":"ok","note":"\ud800"}`, false, false},
		{"key of another member is ignored", `{"\ud800":1,"relay_state":"ok"}`, false, false},
		{"exact case only", `{"RELAY_STATE":"\ud800"}`, false, false},
		{"folded case", `{"RELAY_STATE":"\ud800"}`, true, true},
		{"folded case, both spellings, the second lone", `{"relay_state":"ok","Relay_State":"\udc00"}`, true, true},
		{"null", `{"relay_state":null}`, true, false},
		{"not an object", `["\ud800"]`, true, false},
		{"does not decode", `{"relay_state":"\ud800"`, true, false},
		{"object value is not the string", `{"relay_state":{"a":"\ud800"}}`, true, false},
		{"array value is not the string", `{"relay_state":["\ud800"]}`, true, false},
		{"number value", `{"relay_state":1}`, true, false},
		{"the last member wins: first lone, second clean", `{"RELAY_STATE":"\ud800","relay_state":"ok"}`, true, false},
		{"the last member wins: first clean, second lone", `{"relay_state":"ok","RELAY_STATE":"\ud800"}`, true, true},
		{"exact duplicates: last wins, clean", `{"relay_state":"\ud800","relay_state":"ok"}`, false, false},
		{"exact duplicates: last wins, lone", `{"relay_state":"ok","relay_state":"\ud800"}`, false, true},
		{"a null after a string resets a pointer or interface target", `{"relay_state":"\ud800","relay_state":null}`, true, false},
		{"a string after a null", `{"relay_state":null,"relay_state":"\ud800"}`, true, true},
		{"the long s folds to s", `{"relay_\u017ftate":"\ud800"}`, true, true},
		{"the long s is not the name when folding is off", `{"relay_\u017ftate":"\ud800"}`, false, false},
		{"data after the object", `{"relay_state":"\ud800"} x`, true, false},
		{"a second object after the object", `{"relay_state":"\ud800"}{}`, true, false},
		{"a second object after the object, exact key", `{"relay_state":"\ud800"}{}`, false, false},
		{"white space after the object is fine", "{\"relay_state\":\"\\ud800\"} \n\t", true, true},
		{"the object is not closed", `{"relay_state":"\ud800"`, true, false},
		{"raw surrogate bytes in the kept value", "{\"relay_state\":\"a\xed\xa0\x80\"}", true, true},
		{"raw surrogate bytes in another member", "{\"relay_state\":\"ok\",\"note\":\"\xed\xa0\x80\"}", true, false},
		{"raw surrogate bytes in an overwritten member", "{\"RELAY_STATE\":\"\xed\xa0\x80\",\"relay_state\":\"ok\"}", true, false},
		{"raw valid text in the kept value", "{\"relay_state\":\"é😀\"}", true, false},
		{"an escaped key is the same key", `{"relay\u005fstate":"\ud800"}`, false, true},
	}
	for _, tc := range cases {
		if got := pyjson.MemberHasLoneSurrogateEscape([]byte(tc.doc), "relay_state", tc.fold); got != tc.want {
			t.Errorf("%s: %q fold=%v = %v, want %v", tc.name, tc.doc, tc.fold, got, tc.want)
		}
	}
}

// The oracle for the member scan is encoding/json itself, the decoder both
// handlers use: a lone surrogate escape in the value it keeps comes out as
// U+FFFD, and no value in the generated bodies holds a literal U+FFFD. The
// route decodes into a *string (SAML) or an interface (OIDC), where a later
// null replaces an earlier value with nil.
func TestMemberHasLoneSurrogateEscapeAgreesWithEncodingJSONOnEveryBody(t *testing.T) {
	keys := []string{"relay_state", "RELAY_STATE", "Relay_State", "relay_\u017ftate", "relay\u005fstate", "other"}
	values := []string{"\"x\xed\xa0\x80\"", "\"é😀\"", `"ok"`, `"\ud800"`, `"a\ud83d\ude00b"`, `"\\ud800"`, `"\ud83d"`, `null`, `{"x":"\ud800"}`, `["\ud800"]`, `1`, `true`}
	compared, refusedByDecoder := 0, 0
	var build func(members []string, depth int)
	build = func(members []string, depth int) {
		if depth == 0 {
			body := []byte("{" + strings.Join(members, ",") + "}")
			var typed struct {
				RelayState *string `json:"relay_state"`
			}
			if err := json.Unmarshal(body, &typed); err != nil {
				refusedByDecoder++
				return
			}
			want := typed.RelayState != nil && strings.Contains(*typed.RelayState, "\ufffd")
			if got := pyjson.MemberHasLoneSurrogateEscape(body, "relay_state", true); got != want {
				t.Fatalf("%s: scan %v, encoding/json keeps %v (typed target)", body, got, want)
			}
			var generic map[string]any
			if err := json.Unmarshal(body, &generic); err != nil {
				t.Fatalf("%s: %v", body, err)
			}
			text, isString := generic["relay_state"].(string)
			if got, want := pyjson.MemberHasLoneSurrogateEscape(body, "relay_state", false), isString && strings.Contains(text, "\ufffd"); got != want {
				t.Fatalf("%s: exact-key scan %v, encoding/json keeps %v (map target)", body, got, want)
			}
			compared++
			return
		}
		for _, key := range keys {
			for _, value := range values {
				build(append(members[:len(members):len(members)], `"`+key+`":`+value), depth-1)
			}
		}
	}
	for depth := 1; depth <= 3; depth++ {
		build(nil, depth)
	}
	if compared < 10000 || refusedByDecoder == 0 {
		t.Fatalf("compared %d bodies, %d skipped as not decodable", compared, refusedByDecoder)
	}
}

// Byte soups: every document the api plane decodes is compared, whatever the
// source of the surrogate (an escape or raw bytes); a document it refuses
// (invalid bytes) is outside the scanner's contract.
func TestHasLoneSurrogateEscapeAgreesWithTheApiPlaneOnByteSoups(t *testing.T) {
	pieces := []string{"a", "é", "😀", "\xff", "\xc0", "\xe2\x82", "\xed\xa0\x80", "\xed\xbf\xbf", "\xed\xa0\xbd", "\xed\xb8\x80", "\xed\x9f\xbf", "\xee\x80\x80",
		`\ud800`, `\ude00`, `\ud83d\ude00`, `\\`, `\"`}
	seen, refused := 0, 0
	var build func(parts []string, depth int)
	build = func(parts []string, depth int) {
		if depth == 0 {
			body := strings.Join(parts, "")
			for _, shape := range []string{`{"k":"%s"}`, `["%s"]`, `{"%s":0}`} {
				doc := []byte(strings.Replace(shape, "%s", body, 1))
				if _, err := pyjson.DecodeBody(doc); err != nil {
					refused++
				} else {
					seen++
				}
				differential(t, doc)
			}
			return
		}
		for _, piece := range pieces {
			build(append(parts[:len(parts):len(parts)], piece), depth-1)
		}
	}
	for depth := 0; depth <= 3; depth++ {
		build(nil, depth)
	}
	if seen < 5000 || refused < 5000 {
		t.Fatalf("%d decodable and %d refused documents compared", seen, refused)
	}
}

func FuzzHasLoneSurrogateEscapeOnBytes(f *testing.F) {
	for _, seed := range []string{"{\"a\":\"\xed\xa0\x80\"}", "[\"\xff\"]", "{\"a\":\"\xed\xb8\x80\\ud800\"}", "[\"é\xed\x9f\xbf\"]"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, doc []byte) { differential(t, doc) })
}
