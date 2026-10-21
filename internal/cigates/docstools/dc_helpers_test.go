package docstools

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The helpers port Python standard-library calls. Each corpus under testdata/helpers was recorded once from Python
// 3.12.3 (see testdata/README.md); the Go helper must give the same result on every row, or refuse where the README
// names a STRICTER place.

type helperRow struct {
	In  json.RawMessage `json:"in"`
	Out json.RawMessage `json:"out"`
	Err *string         `json:"err"`
}

func helperRows(t *testing.T, name string) []helperRow {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "helpers", name+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rows []helperRow
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r helperRow
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

func (r helperRow) str(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestHelperURLSplitMatchesPython(t *testing.T) {
	strict := 0
	for _, r := range helperRows(t, "urlsplit") {
		in := r.str(t, r.In)
		got, err := dcURLSplit(in)
		if r.Err != nil {
			if err == nil {
				t.Errorf("urlsplit(%q): Python raised %q, Go accepted", in, *r.Err)
			} else if ve, ok := err.(dcValueError); ok && !strings.Contains(ve.msg, "STRICTER") && ve.msg != *r.Err && !strings.HasPrefix(*r.Err, "netloc ") {
				t.Errorf("urlsplit(%q): message %q, Python %q", in, ve.msg, *r.Err)
			}
			continue
		}
		var want []string
		_ = json.Unmarshal(r.Out, &want)
		if strings.ContainsAny(want[1], "[]") {
			if err == nil {
				t.Errorf("urlsplit(%q): bracketed netloc accepted", in)
			}
			strict++
			continue
		}
		if err != nil {
			t.Errorf("urlsplit(%q): Go refused (%v), Python gave %q", in, err, want)
			continue
		}
		if g := []string{got.scheme, got.netloc, got.path, got.query, got.fragment}; strings.Join(g, "\x00") != strings.Join(want, "\x00") {
			t.Errorf("urlsplit(%q) = %q, Python %q", in, g, want)
		}
	}
	if strict == 0 {
		t.Error("the corpus has no bracketed netloc that Python accepted")
	}
}

func TestHelperUnquoteMatchesPython(t *testing.T) {
	for _, r := range helperRows(t, "unquote") {
		in, want := r.str(t, r.In), r.str(t, r.Out)
		if got := dcUnquote(in); got != want {
			t.Errorf("unquote(%q) = %q, Python %q", in, got, want)
		}
	}
}

func TestHelperUnescapeMatchesPython(t *testing.T) {
	for _, r := range helperRows(t, "unescape") {
		in, want := r.str(t, r.In), r.str(t, r.Out)
		got, err := dcUnescape(in)
		if err != nil || got != want {
			t.Errorf("unescape(%q) = %q (%v), Python %q", in, got, err, want)
		}
	}
}

func TestHelperLowerMatchesPython(t *testing.T) {
	for _, r := range helperRows(t, "lower") {
		in, want := r.str(t, r.In), r.str(t, r.Out)
		if got := dcLower(in); got != want {
			t.Errorf("lower(%q) = %q, Python %q", in, got, want)
		}
	}
}

func TestHelperLowerTableCoversEveryCodePoint(t *testing.T) {
	// the recorded table holds each code point whose lower() differs; every other one lowers to itself
	for cp := rune(0); cp < 0x110000; cp++ {
		if cp >= 0xd800 && cp <= 0xdfff || cp == 0x3a3 {
			continue
		}
		want, ok := dcLowerTable[cp]
		if !ok {
			want = string(cp)
		}
		if got := dcLower("é" + string(cp)); got != "é"+want {
			t.Fatalf("lower(U+%04X) = %q, table %q", cp, got, want)
		}
	}
}

func TestHelperFloatReprMatchesPython(t *testing.T) {
	for _, r := range helperRows(t, "floatrepr") {
		in, want := r.str(t, r.In), r.str(t, r.Out)
		f, err := strconv.ParseFloat(in, 64)
		if err != nil {
			t.Fatal(err)
		}
		if got := dcPyFloatRepr(f); got != want {
			t.Errorf("repr(%s) = %q, Python %q", in, got, want)
		}
	}
}

func TestHelperCSVRowMatchesPython(t *testing.T) {
	for _, r := range helperRows(t, "csvrow") {
		var in []string
		_ = json.Unmarshal(r.In, &in)
		if got, want := dcCSVRow(in), r.str(t, r.Out); got != want {
			t.Errorf("csv row %q = %q, Python %q", in, got, want)
		}
	}
}

func TestHelperNormalizeURLMatchesPython(t *testing.T) {
	for _, r := range helperRows(t, "normalize") {
		in := r.str(t, r.In)
		got, err := dcNormalizeURL(in)
		if r.Err != nil {
			if err == nil {
				t.Errorf("normalize(%q): Python raised, Go gave %q", in, got)
			}
			continue
		}
		if want := r.str(t, r.Out); err != nil || got != want {
			t.Errorf("normalize(%q) = %q (%v), Python %q", in, got, err, want)
		}
	}
}
