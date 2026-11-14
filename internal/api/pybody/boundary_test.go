package pybody_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func checked(t *testing.T, body string) (pybody.Outcome, []pybody.Error) {
	t.Helper()
	req := httptest.NewRequest("POST", "/x", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	_, outcome, errs, err := pybody.ReadChecked(req)
	if err != nil {
		t.Fatal(err)
	}
	return outcome, errs
}

func locs(errs []pybody.Error) []string {
	out := make([]string, len(errs))
	for i, e := range errs {
		raw, _ := json.Marshal(e.Loc)
		out[i] = string(raw)
	}
	return out
}

func TestReadCheckedRefusesALoneSurrogateAtEveryPosition(t *testing.T) {
	cases := []struct {
		name, body string
		want       []string
	}{
		{"str", `{"v":"\ud800"}`, []string{`["body","v"]`}},
		{"low half in text", `{"v":"a\udc00b"}`, []string{`["body","v"]`}},
		{"list item", `{"v":["a","\ud800"]}`, []string{`["body","v",1]`}},
		{"dict value", `{"v":{"k":"\ud800"}}`, []string{`["body","v","k"]`}},
		{"dict key", `{"v":{"k\ud800":["x"]}}`, []string{`["body","v","[key]"]`}},
		{"dict key and its value", `{"v":{"\ud800":"\udc00"}}`, []string{`["body","v","[key]"]`, `["body","v","[key]"]`}},
		{"top-level key", `{"\ud800":1}`, []string{`["body","[key]"]`}},
		{"nested model", `{"v":{"s":{"t":"\ud800"}}}`, []string{`["body","v","s","t"]`}},
		{"list of dicts", `{"v":[{"a":"x"},{"a":"\ud800"}]}`, []string{`["body","v",1,"a"]`}},
		{"top-level array", `["ok","\ud800"]`, []string{`["body",1]`}},
		{"top-level string", `"\ud800"`, []string{`["body"]`}},
		{"in order across fields", `{"a":"\ud800","b":"fine","c":["\udc00"]}`, []string{`["body","a"]`, `["body","c",0]`}},
		{"non-string typed field", `{"n":"\ud800","u":"\ud800","e":"\ud800"}`, []string{`["body","n"]`, `["body","u"]`, `["body","e"]`}},
		{"with another error: only the surrogate errors", `{"n":"not-an-int","v":"\ud800"}`, []string{`["body","v"]`}},
	}
	for _, c := range cases {
		outcome, errs := checked(t, c.body)
		if outcome != pybody.DecodeFailed {
			t.Errorf("%s: outcome %v, want DecodeFailed", c.name, outcome)
			continue
		}
		got := locs(errs)
		if strings.Join(got, ";") != strings.Join(c.want, ";") {
			t.Errorf("%s: loc %v, want %v", c.name, got, c.want)
		}
		for _, e := range errs {
			if e.Type != "string_unicode" || e.Input != nil || e.Ctx != nil {
				t.Errorf("%s: error %+v must be string_unicode with no input and no ctx", c.name, e)
			}
		}
	}
}

// escapedPair is the JSON text of one astral code point as a Python json.dumps
// (ensure_ascii) client sends it: two escapes, not raw UTF-8.
const escapedPair = "\\ud83d" + "\\ude00"

func TestReadCheckedKeepsWhatIsNotALoneSurrogate(t *testing.T) {
	for _, body := range []string{
		`{"v":"` + escapedPair + `"}`,
		`{"v":["a","` + escapedPair + `"]}`,
		`{"v":{"` + escapedPair + `":"x"}}`,
		`{"v":{"k":"` + escapedPair + `"}}`,
		`{"v":"a` + escapedPair + `b` + escapedPair + `"}`,
		`{"v":"` + "\U0001F600" + `"}`,
		`{"v":"\u00e9"}`, `{"v":"é"}`, `{"v":["a"],"w":{"k":"v"}}`, `{"v":"` + "\\" + `ufffd"}`, `{"v":null}`, `[]`, `5`,
	} {
		if outcome, errs := checked(t, body); outcome != pybody.Ready || len(errs) != 0 {
			t.Errorf("%s: outcome %v errs %v, want Ready", body, outcome, errs)
		}
	}
}

func TestReadCheckedDecodesAnEscapedPairToOneCodePoint(t *testing.T) {
	req := httptest.NewRequest("POST", "/x", strings.NewReader(`{"v":"`+escapedPair+`"}`))
	req.Header.Set("Content-Type", "application/json")
	body, outcome, _, err := pybody.ReadChecked(req)
	if err != nil || outcome != pybody.Ready {
		t.Fatalf("outcome %v err %v", outcome, err)
	}
	object, _ := body.Value.(*pyjson.Object)
	value, _ := object.Get("v")
	if text, _ := value.(string); text != "\U0001F600" {
		t.Errorf("an escaped pair decodes to %q, want one U+1F600", text)
	}
}

func TestReadCheckedLeavesDecodeFailuresAndNonJSONBodiesAlone(t *testing.T) {
	if outcome, errs := checked(t, `{"v":`); outcome != pybody.DecodeFailed || len(errs) != 1 || errs[0].Type != "json_invalid" {
		t.Errorf("syntax error: %v %v", outcome, errs)
	}
	req := httptest.NewRequest("POST", "/x", strings.NewReader(`plain \ud800 text`))
	req.Header.Set("Content-Type", "text/plain")
	if _, outcome, errs, _ := pybody.ReadChecked(req); outcome != pybody.Ready || len(errs) != 0 {
		t.Errorf("non-JSON body: %v %v", outcome, errs)
	}
}

func TestReadCheckedCapsTheErrorsAtOneHundredInDocumentOrder(t *testing.T) {
	var items []string
	for i := 0; i < 101; i++ {
		items = append(items, `"\ud800"`)
	}
	outcome, errs := checked(t, `{"v":[`+strings.Join(items, ",")+`]}`)
	if outcome != pybody.DecodeFailed || len(errs) != 100 {
		t.Fatalf("outcome %v, %d errors, want exactly 100", outcome, len(errs))
	}
	got := locs(errs)
	if got[0] != `["body","v",0]` || got[99] != `["body","v",99]` {
		t.Errorf("order: first %s last %s", got[0], got[99])
	}
}

func TestTheRefusalBodyIsValidUTF8JSONAndNeverEchoesTheSurrogate(t *testing.T) {
	_, errs := checked(t, `{"s\ud800":"x","v":"\ud800","w":{"k\udc00":["\ud800"]}}`)
	raw, err := pyjson.Dumps(pybody.Detail(errs))
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(raw)
	if !utf8.ValidString(raw) || !json.Valid([]byte(raw)) {
		t.Fatalf("body is not valid UTF-8 JSON: %q", raw)
	}
	for _, bad := range []string{"ud800", "udc00", "udfff", `"input"`, `"ctx"`, "\xed\xa0\x80"} {
		if strings.Contains(lower, bad) {
			t.Errorf("body carries %q: %s", bad, raw)
		}
	}
	if !strings.Contains(raw, `"[key]"`) {
		t.Errorf("a surrogate key must show as [key]: %s", raw)
	}
}
