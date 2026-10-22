package docstools

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func helperRowsTyped[T any](t *testing.T, name string) []T {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "helpers", name+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rows []T
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var r T
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		rows = append(rows, r)
	}
	if len(rows) == 0 {
		t.Fatalf("%s: no rows", name)
	}
	return rows
}

// The YAML loader of the publication validator is a port of PyYAML 6.0.3 (pure-Python SafeLoader). Each row of
// testdata/helpers/yamlsafeload.jsonl was recorded from yaml.safe_load on Python 3.14.7: the repr and str of the result,
// or the first line of the exception. Go gives the same, or refuses a place the README names STRICTER.

type yamlRow struct {
	In      string  `json:"in"`
	Repr    *string `json:"repr"`
	Str     *string `json:"str"`
	Err     *string `json:"err"`
	ReprErr *string `json:"reprerr"`
	Nodet   bool    `json:"nodet"`
}

const yamlSafeLoadRows = 2770

func TestHelperYAMLSafeLoadMatchesPython(t *testing.T) {
	rows := helperRowsTyped[yamlRow](t, "yamlsafeload")
	if len(rows) != yamlSafeLoadRows {
		t.Fatalf("%d rows, pinned %d", len(rows), yamlSafeLoadRows)
	}
	strict, same := 0, 0
	for _, r := range rows {
		v, err := dcYAMLSafeLoad(r.In)
		if err != nil {
			if strings.Contains(err.msg, "STRICTER") {
				strict++
				continue
			}
			if r.Err == nil {
				t.Errorf("%q: Go refused (%s), Python loaded %v", r.In, err, deref(r.Repr))
				continue
			}
			if got := err.kind + ": " + err.msg; got != *r.Err {
				t.Errorf("%q: error %q, Python %q", r.In, got, *r.Err)
				continue
			}
			same++
			continue
		}
		if r.Err != nil {
			t.Errorf("%q: Go loaded a value, Python refused: %s", r.In, *r.Err)
			continue
		}
		if r.Nodet {
			continue
		}
		rep, rerr := pyRepr(v, nil)
		if rerr != nil {
			if r.ReprErr == nil {
				t.Errorf("%q: repr refused (%v), Python repr %v", r.In, rerr, deref(r.Repr))
			}
			continue
		}
		if r.Repr == nil || rep != *r.Repr {
			t.Errorf("%q: repr %q, Python %v (%v)", r.In, rep, deref(r.Repr), deref(r.ReprErr))
			continue
		}
		if s, _ := pyStr(v); r.Str != nil && s != *r.Str {
			t.Errorf("%q: str %q, Python %q", r.In, s, *r.Str)
		}
		same++
	}
	t.Logf("%d rows equal, %d STRICTER refusals", same, strict)
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

type decRow struct {
	In  string  `json:"in"`
	Out *string `json:"out"`
	Err *string `json:"err"`
}

func TestHelperBase64DecodeMatchesPython(t *testing.T) {
	rows := helperRowsTyped[decRow](t, "b64decode")
	if len(rows) != 3000 {
		t.Fatalf("%d rows, pinned 3000", len(rows))
	}
	for _, r := range rows {
		got, msg := dcB64DecodeBytes([]byte(r.In))
		switch {
		case r.Err != nil && msg != *r.Err:
			t.Errorf("%q: error %q, Python %q", r.In, msg, *r.Err)
		case r.Err == nil && msg != "":
			t.Errorf("%q: error %q, Python decoded %s", r.In, msg, deref(r.Out))
		case r.Err == nil && fmt.Sprintf("%x", got) != *r.Out:
			t.Errorf("%q: %x, Python %s", r.In, got, *r.Out)
		}
	}
}

func TestHelperTitleMatchesPython(t *testing.T) {
	rows := helperRowsTyped[decRow](t, "title")
	if len(rows) != 2446 {
		t.Fatalf("%d rows, pinned 2446", len(rows))
	}
	for _, r := range rows {
		if got := dcTitle(r.In); got != *r.Out {
			t.Errorf("title(%q) = %q, Python %q", r.In, got, *r.Out)
		}
	}
}

func TestHelperCSVRowsMatchPython(t *testing.T) {
	type row struct {
		In  string           `json:"in"`
		Out []map[string]any `json:"out"`
		Err *string          `json:"err"`
	}
	rows := helperRowsTyped[row](t, "csvrows")
	if len(rows) != 1500 {
		t.Fatalf("%d rows, pinned 1500", len(rows))
	}
	for _, r := range rows {
		got, err := dcCSVRows(r.In)
		if r.Err != nil {
			var ye *dcYErr
			if !errors.As(err, &ye) || ye.kind+": "+ye.msg != *r.Err {
				t.Errorf("%q: error %v, Python %q", r.In, err, *r.Err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: error %v, Python %v", r.In, err, r.Out)
			continue
		}
		if len(got) != len(r.Out) {
			t.Errorf("%q: %d rows, Python %d", r.In, len(got), len(r.Out))
			continue
		}
		for i := range got {
			for k, wantAny := range r.Out[i] {
				if k == "\x00None" {
					continue
				}
				var want *string
				if w, ok := wantAny.(string); ok {
					want = &w
				}
				g, ok := got[i][k]
				if !ok || (g == nil) != (want == nil) || (g != nil && *g != *want) {
					t.Errorf("%q: row %d column %q = %v, Python %v", r.In, i, k, g, want)
				}
			}
			n := 0
			for k := range r.Out[i] {
				if k != "\x00None" {
					n++
				}
			}
			if len(got[i]) != n {
				t.Errorf("%q: row %d has %d columns, Python %d", r.In, i, len(got[i]), n)
			}
		}
	}
}

func TestHelperCSVWriteMatchesPython(t *testing.T) {
	type row struct {
		In  []string `json:"in"`
		Out string   `json:"out"`
	}
	for _, r := range helperRowsTyped[row](t, "csvwrite") {
		if got := dcPubCSVRow(r.In); got != r.Out {
			t.Errorf("%q: %q, Python %q", r.In, got, r.Out)
		}
	}
}

func TestHelperCanonicalURLMatchesPython(t *testing.T) {
	for _, r := range helperRowsTyped[decRow](t, "canonurl") {
		if got := dcPubCanonicalURL(r.In); got != *r.Out {
			t.Errorf("_canonical_url(%q) = %q, Python %q", r.In, got, *r.Out)
		}
	}
}

func TestHelperStemTitleMatchesPython(t *testing.T) {
	type row struct {
		In   string `json:"in"`
		Out  string `json:"out"`
		Stem string `json:"stem"`
	}
	for _, r := range helperRowsTyped[row](t, "stemtitle") {
		stem := dcPyStem(r.In)
		if stem != r.Stem {
			t.Errorf("stem(%q) = %q, Python %q", r.In, stem, r.Stem)
		}
		if got := dcTitle(strings.ReplaceAll(stem, "-", " ")); got != r.Out {
			t.Errorf("title of stem(%q) = %q, Python %q", r.In, got, r.Out)
		}
	}
}

func TestHelperFlattenNavMatchesPython(t *testing.T) {
	type row struct {
		In  string      `json:"in"`
		Out [][3]string `json:"out"`
		Err *string     `json:"err"`
	}
	rows := helperRowsTyped[row](t, "flattennav")
	if len(rows) != 327 {
		t.Fatalf("%d rows, pinned 327", len(rows))
	}
	for _, r := range rows {
		node, yerr := dcYAMLSafeLoad(r.In)
		if yerr != nil {
			if r.Err == nil {
				t.Errorf("%q: loader error %v", r.In, yerr)
			}
			continue
		}
		recs, err := dcPubFlattenNav(node, nil)
		if r.Err != nil {
			if err == nil {
				t.Errorf("%q: Go flattened, Python raised %s", r.In, *r.Err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: Go raised %v", r.In, err)
			continue
		}
		var got [][3]string
		for _, x := range recs {
			got = append(got, [3]string{x.sourcePath, x.label, x.navPath})
		}
		if fmt.Sprint(got) != fmt.Sprint(r.Out) {
			t.Errorf("%q: %v, Python %v", r.In, got, r.Out)
		}
	}
}

func TestHelperLinksMatchPython(t *testing.T) {
	type row struct {
		In  string      `json:"in"`
		Out [][2]string `json:"out"`
		Err *string     `json:"err"`
	}
	for _, r := range helperRowsTyped[row](t, "links") {
		got, err := dcPubLinks(r.In)
		if r.Err != nil {
			if err == nil {
				t.Errorf("%q: Go found links, Python raised %s", r.In, *r.Err)
			}
			continue
		}
		if err != nil || fmt.Sprint(got) != fmt.Sprint(r.Out) {
			t.Errorf("%q: %v (%v), Python %v", r.In, got, err, r.Out)
		}
	}
}

func TestHelperFrontMatterMatchesPython(t *testing.T) {
	type row struct {
		In  string  `json:"in"`
		Out *string `json:"out"`
	}
	for _, r := range helperRowsTyped[row](t, "frontmatter") {
		got, ok := dcPubFrontMatter(r.In)
		if ok != (r.Out != nil) || (ok && got != *r.Out) {
			t.Errorf("%q: %q %v, Python %v", r.In, got, ok, deref(r.Out))
		}
	}
}

func TestHelperURLParseMatchesPython(t *testing.T) {
	type row struct {
		In  string    `json:"in"`
		Out [3]string `json:"out"`
		Err *string   `json:"err"`
	}
	for _, r := range helperRowsTyped[row](t, "urlparse") {
		scheme, netloc, path, err := dcPubURLParse(r.In)
		if r.Err != nil {
			if err == nil {
				t.Errorf("%q: Go parsed, Python raised %s", r.In, *r.Err)
			}
			continue
		}
		if err != nil {
			if !strings.Contains(err.Error(), "STRICTER") {
				t.Errorf("%q: Go raised %v, Python %v", r.In, err, r.Out)
			}
			continue
		}
		if [3]string{scheme, netloc, path} != r.Out {
			t.Errorf("%q: %q %q %q, Python %q", r.In, scheme, netloc, path, r.Out)
		}
	}
}

func TestHelperSortKeyTypeErrorMatchesPython(t *testing.T) {
	type row struct {
		In  []*string `json:"in"`
		Out string    `json:"out"`
	}
	rows := helperRowsTyped[row](t, "sortkeys")
	if len(rows) != 544 {
		t.Fatalf("%d rows, pinned 544", len(rows))
	}
	same, differ := 0, 0
	for _, r := range rows {
		got := dcSortKeyTypeError(r.In)
		if got == r.Out {
			same++
			continue
		}
		if (got == "") != (r.Out == "") || len(r.In) < 64 {
			t.Errorf("%v: %q, Python %q", r.In, got, r.Out)
			continue
		}
		differ++
	}
	t.Logf("%d equal, %d text differences at 64 items or more", same, differ)
}
