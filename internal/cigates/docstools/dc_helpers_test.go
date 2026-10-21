package docstools

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The helpers port Python standard-library calls. Each corpus under testdata/helpers was recorded once from Python
// 3.14.7 (see testdata/README.md); the Go helper must give the same result on every row, or refuse where the README
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
		in := r.str(t, r.In)
		got, err := dcUnescape(in)
		if r.Err != nil {
			if err == nil {
				t.Errorf("unescape(%.40q): Python raised %q, Go gave %.40q", in, *r.Err, got)
			}
			continue
		}
		want := r.str(t, r.Out)
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

var dcSurrogateRepr = regexp.MustCompile(`\\\\ud[89a-f][0-9a-f]{2}`)

func TestHelperJSONLoadsMatchesPython(t *testing.T) {
	for _, r := range helperRows(t, "jsonloads") {
		in := r.str(t, r.In)
		v, err := dcParseJSON(in)
		// Refused on purpose (README, "Stricter than Python"): a lone surrogate escape and nesting deeper than 500.
		stricter := dcSurrogateRepr.MatchString(string(r.Out)) || strings.Count(in, "[") > dcJSONMaxDepth
		switch {
		case r.Err != nil:
			if err == nil {
				t.Errorf("json %.60q: Python raised %s, Go gave %s", in, *r.Err, v.repr())
			}
		case stricter && err != nil:
		case err != nil:
			t.Errorf("json %.60q: Go refused (%v), Python gave %.60s", in, err, r.str(t, r.Out))
		case v.repr() != r.str(t, r.Out):
			t.Errorf("json %.60q: Go %.60s, Python %.60s", in, v.repr(), r.str(t, r.Out))
		}
	}
}

func TestHelperRealpathMatchesPython(t *testing.T) {
	rows := helperRows(t, "realpath")
	var spec struct {
		Links [][2]string `json:"links"`
		Dirs  []string    `json:"dirs"`
		Files []string    `json:"files"`
	}
	if err := json.Unmarshal(rows[0].In, &spec); err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		In   []string `json:"in"`
		Out  string   `json:"out"`
		Form string   `json:"form"`
	}
	if err := json.Unmarshal(rows[0].Out, &cases); err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range spec.Dirs {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range spec.Files {
		if err := os.WriteFile(filepath.Join(root, f), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, l := range spec.Links {
		if err := os.Symlink(strings.ReplaceAll(l[1], "{ROOT}", root), filepath.Join(root, l[0])); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)
	for _, c := range cases {
		arg := strings.ReplaceAll(c.In[0], "{ROOT}", root)
		got, err := dcRealpath(arg)
		if err != nil {
			t.Errorf("realpath(%q): %v", c.In[0], err)
			continue
		}
		if want := strings.ReplaceAll(c.Out, "{ROOT}", root); got != want {
			t.Errorf("realpath(%q) = %q, Python %q", c.In[0], strings.ReplaceAll(got, root, "{ROOT}"), c.Out)
		}
	}
}

const dcPathOpRows = 125

// The string forms the ported checks use for paths, against pathlib's (str, parent, suffix, "/", relative_to): the table
// of "path string -> Python result" that guards the class of finding "Go cleaned a path that Python keeps". Unique row
// names, pinned count.
func TestHelperPathOpsMatchPython(t *testing.T) {
	f, err := os.Open(filepath.Join("testdata", "helpers", "pathops.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	seen := map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var row struct {
			Name string          `json:"name"`
			Op   string          `json:"op"`
			In   []string        `json:"in"`
			Out  json.RawMessage `json:"out"`
		}
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		if seen[row.Name] {
			t.Fatalf("repeated row name %q", row.Name)
		}
		seen[row.Name] = true
		var got, want string
		switch row.Op {
		case "str":
			got = a1PathStr(row.In[0])
		case "parent":
			got = dcParent(row.In[0])
		case "suffix":
			got = dcPathSuffix(row.In[0])
		case "join":
			got = dcJoin(row.In[0], row.In[1])
		case "relative_to":
			got = fmt.Sprint(dcRelativeTo(row.In[0], row.In[1]))
		default:
			t.Fatalf("row %s: unknown op %q", row.Name, row.Op)
		}
		if row.Op == "relative_to" {
			want = strings.TrimSpace(string(row.Out))
		} else if err := json.Unmarshal(row.Out, &want); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s: %s(%q) = %q, Python %q", row.Name, row.Op, row.In, got, want)
		}
	}
	if len(seen) != dcPathOpRows {
		t.Fatalf("the table has %d rows, pinned %d", len(seen), dcPathOpRows)
	}
}
