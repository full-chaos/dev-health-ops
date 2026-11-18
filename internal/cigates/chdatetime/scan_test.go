package chdatetime_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type class string

const (
	classSafe       class = "safe"
	classSkip       class = "skip"
	classFlag       class = "flag"
	classUnmeasured class = "unmeasured"
)

// text is one SQL text: a Go string constant or a .sql file.
type text struct {
	File string
	Line int
	Body string
}

type hit struct {
	File   string
	Line   int
	Column string
	Op     string
	Bound  string
	Class  class
}

var (
	funcRe    = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*\(`)
	paramRe   = regexp.MustCompile(`^\{\w+:([A-Za-z0-9]+)`)
	identPath = regexp.MustCompile(`^(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)`)
	numberRe  = regexp.MustCompile(`^-?[0-9]`)
)

// A DateTime bound against a DateTime64 column is the unsafe pair: ClickHouse analyses a strict comparison of the two
// types wrongly in a key, partition or skip index. A Date bound and every DateTime64 bound are measured safe (the
// executed cells of CHAOS-7290).
var (
	safeFuncs = map[string]bool{
		"todatetime64": true, "now64": true, "fromunixtimestamp64milli": true, "fromunixtimestamp64micro": true,
		"fromunixtimestamp64nano": true, "parsedatetime64besteffort": true, "todate": true, "todate32": true,
		"today": true, "yesterday": true,
	}
	flagFuncs = map[string]bool{"todatetime": true, "now": true, "parsedatetimebesteffort": true}
)

func classifyBound(bound string, names map[string]bool) class {
	bound = strings.TrimSpace(bound)
	switch {
	case bound == "":
		return classUnmeasured
	case bound[0] == '?':
		return classFlag
	case bound[0] == '$' && len(bound) > 1 && bound[1] >= '0' && bound[1] <= '9':
		return classSkip
	case bound[0] == '{':
		m := paramRe.FindStringSubmatch(bound)
		switch {
		case m == nil:
			return classUnmeasured
		case m[1] == "DateTime64" || m[1] == "Date" || m[1] == "Date32":
			return classSafe
		case m[1] == "DateTime":
			return classFlag
		}
		return classUnmeasured
	case numberRe.MatchString(bound):
		return classSkip
	case bound[0] == '\'':
		return classSafe
	}
	if m := funcRe.FindStringSubmatch(bound); m != nil {
		switch name := strings.ToLower(m[1]); {
		case safeFuncs[name]:
			return classSafe
		case flagFuncs[name]:
			return classFlag
		}
		return classUnmeasured
	}
	if m := identPath.FindStringSubmatch(bound); m != nil && names[m[1]] {
		return classSafe
	}
	return classUnmeasured
}

func isIdentByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// identBefore is the identifier that ends just before index end (spaces skipped), without its alias.
func identBefore(body string, end int) (string, bool) {
	for end > 0 && (body[end-1] == ' ' || body[end-1] == '\t' || body[end-1] == '\n') {
		end--
	}
	start := end
	for start > 0 && isIdentByte(body[start-1]) {
		start--
	}
	if start == end || body[start] >= '0' && body[start] <= '9' {
		return "", false
	}
	if start > 0 && body[start-1] == '`' {
		return "", false
	}
	return body[start:end], true
}

// boundBefore classifies the expression that ends just before the operator, for `bound < column`.
func boundBefore(body string, end int, names map[string]bool) class {
	for end > 0 && (body[end-1] == ' ' || body[end-1] == '\t' || body[end-1] == '\n') {
		end--
	}
	if end == 0 {
		return classUnmeasured
	}
	switch last := body[end-1]; {
	case last == '?':
		return classFlag
	case last == '}':
		if open := strings.LastIndex(body[:end], "{"); open >= 0 {
			return classifyBound(body[open:end], names)
		}
	case last == ')':
		depth := 0
		for i := end - 1; i >= 0; i-- {
			if body[i] == ')' {
				depth++
			} else if body[i] == '(' {
				if depth--; depth == 0 {
					start := i
					for start > 0 && isIdentByte(body[start-1]) {
						start--
					}
					return classifyBound(body[start:i+1], names)
				}
			}
		}
	}
	if id, ok := identBefore(body, end); ok {
		if names[id] {
			return classSafe
		}
		if numberRe.MatchString(id) {
			return classSkip
		}
	}
	return classUnmeasured
}

// strictHits finds every strict `<` or `>` comparison of a DateTime64 column (a name in names) in one SQL text.
func strictHits(t text, names map[string]bool) []hit {
	var out []hit
	body := t.Body
	for i := 0; i < len(body); i++ {
		op := body[i]
		if op != '<' && op != '>' {
			continue
		}
		next := byte(0)
		if i+1 < len(body) {
			next = body[i+1]
		}
		prev := byte(0)
		if i > 0 {
			prev = body[i-1]
		}
		if next == '=' || next == '>' || op == '<' && next == '<' || prev == '-' || prev == '=' || prev == '<' || prev == '>' {
			continue
		}
		line := t.Line + strings.Count(body[:i], "\n")
		rest := strings.TrimSpace(body[i+1:])
		if id, ok := identBefore(body, i); ok && names[id] {
			out = append(out, hit{t.File, line, id, string(op), snippet(rest), classifyBound(rest, names)})
			continue
		}
		if m := identPath.FindStringSubmatch(rest); m != nil && names[m[1]] && !strings.HasPrefix(strings.TrimSpace(rest[len(m[0]):]), "(") {
			out = append(out, hit{t.File, line, m[1], string(op), "(bound before the operator)", boundBefore(body, i, names)})
		}
	}
	return out
}

func snippet(s string) string {
	if len(s) > 40 {
		s = s[:40]
	}
	return strings.Join(strings.Fields(s), " ")
}

var skipDirs = map[string]bool{".git": true, "node_modules": true, "third_party": true, "testdata": true, "vendor": true, ".codegraph": true}

// sqlTexts reads every Go string constant of the non-test Go files and every .sql file under root, apart from the
// migration history and the test data.
func sqlTexts(root string) (texts []text, goFiles int, err error) {
	skipPrefix := []string{
		filepath.Join("internal", "chmigrate", "history"), filepath.Join("internal", "chmigrate", "archive"),
		filepath.Join("internal", "chmigrate", "rollback"), filepath.Join("internal", "chmigrate", "baseline"),
	}
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, _ := filepath.Rel(root, path)
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			for _, prefix := range skipPrefix {
				if rel == prefix {
					return filepath.SkipDir
				}
			}
			return nil
		}
		switch {
		case strings.HasSuffix(path, ".sql"):
			body, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			texts = append(texts, text{File: filepath.ToSlash(rel), Line: 1, Body: string(body)})
		case strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go"):
			goFiles++
			fileTexts, parseErr := goTexts(path, filepath.ToSlash(rel))
			if parseErr != nil {
				return parseErr
			}
			texts = append(texts, fileTexts...)
		}
		return nil
	})
	return texts, goFiles, err
}

func goTexts(path, rel string) ([]text, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", rel, err)
	}
	var out []text
	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.BinaryExpr:
			if body, ok := foldLiterals(node); ok {
				out = append(out, text{rel, fset.Position(node.Pos()).Line, body})
				return false
			}
		case *ast.BasicLit:
			if node.Kind == token.STRING {
				if body, unquoteErr := strconv.Unquote(node.Value); unquoteErr == nil {
					out = append(out, text{rel, fset.Position(node.Pos()).Line, body})
				}
			}
		}
		return true
	})
	return out, nil
}

// foldLiterals joins `"a" + "b"` chains of string literals into one text.
func foldLiterals(expr ast.Expr) (string, bool) {
	switch node := expr.(type) {
	case *ast.BasicLit:
		if node.Kind != token.STRING {
			return "", false
		}
		body, err := strconv.Unquote(node.Value)
		return body, err == nil
	case *ast.ParenExpr:
		return foldLiterals(node.X)
	case *ast.BinaryExpr:
		if node.Op != token.ADD {
			return "", false
		}
		left, okLeft := foldLiterals(node.X)
		right, okRight := foldLiterals(node.Y)
		return left + right, okLeft && okRight
	}
	return "", false
}

type allowRow struct{ File, Column, Bound, Reason string }

func loadAllowlist(path string) ([]allowRow, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rows []allowRow
	for n, line := range strings.Split(string(body), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cells := strings.Split(line, "\t")
		if len(cells) != 4 || cells[3] == "" {
			return nil, fmt.Errorf("%s:%d: want file<TAB>column<TAB>bound<TAB>reason", path, n+1)
		}
		rows = append(rows, allowRow{cells[0], cells[1], cells[2], cells[3]})
	}
	return rows, nil
}

type result struct {
	Violations []hit
	StaleRows  []allowRow
	Strict     int
	OnKeyed    int
	Flagged    map[string]int
}

// check applies the policy: a strict comparison of a keyed DateTime64 column against a DateTime bound (or a bound the
// scan cannot classify) is a violation unless an allowlist row names it. A column no table keeps is only counted.
func check(cat catalog, texts []text, allow []allowRow) result {
	names := cat.names()
	keyed := cat.keyNames()
	res := result{Flagged: map[string]int{}}
	used := make([]bool, len(allow))
	for _, t := range texts {
		for _, h := range strictHits(t, names) {
			res.Strict++
			if _, ok := keyed[h.Column]; !ok {
				if h.Class == classFlag || h.Class == classUnmeasured {
					res.Flagged[h.Column]++
				}
				continue
			}
			res.OnKeyed++
			if h.Class != classFlag && h.Class != classUnmeasured {
				continue
			}
			allowed := false
			for i, row := range allow {
				if row.File == h.File && row.Column == h.Column && row.Bound == h.Bound {
					used[i], allowed = true, true
				}
			}
			if !allowed {
				res.Violations = append(res.Violations, h)
			}
		}
	}
	for i, row := range allow {
		if !used[i] {
			res.StaleRows = append(res.StaleRows, row)
		}
	}
	sort.Slice(res.Violations, func(i, j int) bool {
		a, b := res.Violations[i], res.Violations[j]
		return a.File < b.File || a.File == b.File && a.Line < b.Line
	})
	return res
}
