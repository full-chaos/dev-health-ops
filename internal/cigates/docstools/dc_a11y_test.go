package docstools

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// The corpora under testdata/helpers for check-docs-accessibility were recorded once from Python 3.14.7 (see
// testdata/README.md): the event stream of html.parser.HTMLParser(convert_charrefs=True).feed(text), the findings of
// _audit_html on a page, int(text) > 0 for a tabindex, and str(UnicodeDecodeError) for bytes.

func a11yRows[T any](t *testing.T, name string, min int) []T {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "helpers", name+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rows []T
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<24), 1<<24)
	for sc.Scan() {
		var r T
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		rows = append(rows, r)
	}
	if len(rows) < min {
		t.Fatalf("%s: %d rows, want at least %d", name, len(rows), min)
	}
	return rows
}

type recordedEvents struct {
	In  string              `json:"in"`
	Out [][]json.RawMessage `json:"out"`
	Err *string             `json:"err"`
}

type eventRecorder struct{ ev [][]any }

func (r *eventRecorder) starttag(tag string, attrs []dcHTMLAttr) {
	list := make([][]any, 0, len(attrs))
	for _, a := range attrs {
		if a.hasNone {
			list = append(list, []any{a.name, nil})
		} else {
			list = append(list, []any{a.name, a.value})
		}
	}
	r.ev = append(r.ev, []any{"s", tag, list})
}
func (r *eventRecorder) endtag(tag string) { r.ev = append(r.ev, []any{"e", tag}) }
func (r *eventRecorder) data(text string)  { r.ev = append(r.ev, []any{"d", text}) }

// normalise through JSON so []any / [][]any compare with what json.Unmarshal gave
func a11yJSON(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestHTMLParserEventsMatchPython(t *testing.T) {
	rows := a11yRows[struct {
		In  string `json:"in"`
		Out []any  `json:"out"`
		Err string `json:"err"`
	}](t, "htmlevents", 2500)
	for _, r := range rows {
		rec := &eventRecorder{}
		err := dcParseHTML(r.In, rec)
		if r.Err != "" {
			if err == nil || err.Error() != r.Err {
				t.Errorf("%q: Python raised %q, Go gave %v", r.In, r.Err, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: Go raised %v, Python gave events", r.In, err)
			continue
		}
		if len(rec.ev) == 0 && len(r.Out) == 0 {
			continue
		}
		got := a11yJSON(t, rec.ev)
		want := any(r.Out)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%q:\n got %v\nwant %v", r.In, got, want)
		}
	}
}

func TestAuditHTMLMatchesPython(t *testing.T) {
	rows := a11yRows[struct {
		In   string   `json:"in"`
		Name string   `json:"name"`
		Out  []string `json:"out"`
		Err  string   `json:"err"`
	}](t, "a11yaudit", 2500)
	failing := 0
	for _, r := range rows {
		got, err := dcAuditHTMLText(r.In, r.Name)
		if r.Err != "" {
			if err == nil || err.Error() != r.Err {
				t.Errorf("%q: Python raised %q, Go gave %v", r.In, r.Err, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: Go raised %v", r.In, err)
			continue
		}
		if len(got) == 0 && len(r.Out) == 0 {
			continue
		}
		failing++
		if !reflect.DeepEqual(got, r.Out) {
			t.Errorf("%q (%s):\n got %q\nwant %q", r.In, r.Name, got, r.Out)
		}
	}
	if failing < 2000 {
		t.Errorf("only %d corpus rows have findings", failing)
	}
}

func TestTabindexMatchesPython(t *testing.T) {
	counts := map[string]int{}
	for _, r := range a11yRows[struct {
		In  string `json:"in"`
		Out string `json:"out"`
	}](t, "tabindex", 3000) {
		pos, ok := dcPyIntPositive(r.In)
		got := "nonpos"
		switch {
		case !ok:
			got = "err"
		case pos:
			got = "pos"
		}
		counts[r.Out]++
		if got != r.Out {
			t.Errorf("int(%q) > 0: Go %s, Python %s", r.In, got, r.Out)
		}
	}
	if counts["pos"] == 0 || counts["nonpos"] == 0 || counts["err"] == 0 {
		t.Errorf("corpus lacks a class: %v", counts)
	}
}

func TestUTF8DecodeMessageMatchesPython(t *testing.T) {
	bad := 0
	for _, r := range a11yRows[struct {
		In  string  `json:"in"`
		Out *string `json:"out"`
	}](t, "utf8err", 2500) {
		raw, err := hex.DecodeString(r.In)
		if err != nil {
			t.Fatal(err)
		}
		msg, isBad := dcUTF8DecodeMessage(raw)
		switch {
		case r.Out == nil && isBad:
			t.Errorf("%x: Go refused (%s), Python decoded", raw, msg)
		case r.Out != nil && (!isBad || msg != *r.Out):
			t.Errorf("%x: Go %q (%v), Python %q", raw, msg, isBad, *r.Out)
		}
		if isBad {
			bad++
		}
	}
	if bad < 2500 {
		t.Errorf("only %d invalid rows", bad)
	}
}

func TestDecimalTableIsPython16(t *testing.T) {
	if len(dcDecimalTable) != 750 {
		t.Fatalf("decimal table has %d rows, recorded 750", len(dcDecimalTable))
	}
	_ = fmt.Sprint
}

func TestTabindexRefusesUnicode16Digits(t *testing.T) {
	// Cloudflare Workers Builds ran the former script on Python 3.13 (Unicode 15.1), whose int() refuses these digits; 3.14.7 reads them.
	rows := strings.Fields(strings.ReplaceAll(dcDecimalUnicode16, "\n", " "))
	if len(rows) != 80 {
		t.Fatalf("%d Unicode 16 decimal digits, recorded 80", len(rows))
	}
	for _, h := range rows {
		n, err := strconv.ParseInt(h, 16, 32)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := dcPyIntPositive("1" + string(rune(n))); ok {
			t.Errorf("U+%X is accepted by int()", n)
		}
	}
}
