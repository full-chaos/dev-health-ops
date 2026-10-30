package platformrole

import (
	"bufio"
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
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

var (
	usersWrite   = regexp.MustCompile(`(?is)\b(?:insert\s+into|update)\s+(?:public\.)?users\b`)
	usersInsert  = regexp.MustCompile(`(?is)\binsert\s+into\s+(?:public\.)?users\s*\(([^)]*)\)\s*values\s*\((.*)\)`)
	superuserSet = regexp.MustCompile(`(?i)\bis_superuser\s*=\s*([^,\s]+)`)
)

// superuserWriteValue is the value a users INSERT or UPDATE statement gives
// is_superuser, lowered, or "" when the statement does not write it.
func superuserWriteValue(sql string) string {
	if m := usersInsert.FindStringSubmatch(sql); m != nil {
		columns := splitTopLevel(m[1])
		values := splitTopLevel(m[2])
		for i, column := range columns {
			if strings.EqualFold(column, "is_superuser") && i < len(values) {
				return strings.ToLower(values[i])
			}
		}
		return ""
	}
	if m := superuserSet.FindStringSubmatch(sql); m != nil {
		return strings.ToLower(m[1])
	}
	return ""
}

// splitTopLevel splits on the commas outside parentheses and trims each part.
func splitTopLevel(list string) []string {
	var parts []string
	depth, start := 0, 0
	for i, r := range list {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(list[start:i]))
				start = i + 1
			}
		}
	}
	return append(parts, strings.TrimSpace(list[start:]))
}

// superuserWrites lists, per function, the values of the users statements in
// file that write is_superuser.
func superuserWrites(file *ast.File) map[string][]string {
	writes := map[string][]string{}
	collect := func(scope string, node ast.Node) {
		ast.Inspect(node, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING || !strings.Contains(lit.Value, "is_superuser") {
				return true
			}
			sql, err := strconv.Unquote(lit.Value)
			if err != nil || !usersWrite.MatchString(sql) {
				return true
			}
			if value := superuserWriteValue(sql); value != "" {
				writes[scope] = append(writes[scope], value)
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			collect(funcName(decl), decl)
		case *ast.GenDecl:
			if decl.Tok != token.IMPORT {
				collect("-", decl)
			}
		}
	}
	return writes
}

// TestEverySuperuserWriteIsProvenWhileOwned is the census of the writes of
// public users.is_superuser: every non-test users INSERT or UPDATE statement
// that writes the column either writes the constant false, or is listed in
// testdata/superuser_writes.tsv with the test that proves what it does while
// the platform roles are owned (refused, or the column left out), so no write
// carries a switched read back into the public column. A fixture under
// internal/testsupport is listed with the proof "fixture".
func TestEverySuperuserWriteIsProvenWhileOwned(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	listed := readWriteRows(t)
	found := map[string][]string{}
	fset := token.NewFileSet()
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			rel := filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator)))
			for function, values := range superuserWrites(file) {
				for _, value := range values {
					if value != "false" {
						found[rel+"\t"+function] = append(found[rel+"\t"+function], value)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var unlisted []string
	for key, values := range found {
		row, ok := listed[key]
		if !ok {
			unlisted = append(unlisted, strings.ReplaceAll(key, "\t", " ")+" writes "+strings.Join(values, ", "))
			continue
		}
		if got := strings.Join(values, ","); got != row.values {
			t.Errorf("%s writes is_superuser = %s, listed %s: update the census", strings.ReplaceAll(key, "\t", " "), got, row.values)
		}
		file := strings.SplitN(key, "\t", 2)[0]
		if row.proof == "fixture" {
			if !strings.HasPrefix(file, "internal/testsupport/") {
				t.Errorf("%s: only internal/testsupport may be proven by \"fixture\"", key)
			}
			continue
		}
		for _, test := range strings.Split(row.proof, ",") {
			if !testExists(t, filepath.Join(root, filepath.Dir(file)), test) {
				t.Errorf("%s: proof %s is not a test in %s", strings.ReplaceAll(key, "\t", " "), test, filepath.Dir(file))
			}
		}
	}
	for key := range listed {
		if _, ok := found[key]; !ok {
			t.Errorf("%s is listed but writes no non-false is_superuser: drop the row", strings.ReplaceAll(key, "\t", " "))
		}
	}
	sort.Strings(unlisted)
	for _, row := range unlisted {
		t.Errorf("unproven is_superuser write (a constant false, or list it in testdata/superuser_writes.tsv with the test of its owned behaviour): %s", row)
	}
}

// testExists reports whether a _test.go file in dir declares func name.
func testExists(t *testing.T, dir, name string) bool {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, path := range matches {
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
				return true
			}
		}
	}
	return false
}

type writeRow struct{ values, proof string }

func readWriteRows(t *testing.T) map[string]writeRow {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "superuser_writes.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	rows := map[string]writeRow{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 4 || fields[2] == "" || fields[3] == "" {
			t.Fatalf("superuser_writes.tsv: %q is not file, function, values, proof", line)
		}
		key := fields[0] + "\t" + fields[1]
		if _, dup := rows[key]; dup {
			t.Fatalf("superuser_writes.tsv: %s listed twice", key)
		}
		rows[key] = writeRow{values: fields[2], proof: fields[3]}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// The write probe reads the value each statement shape gives is_superuser.
func TestTheSuperuserWriteProbeSeesItsShapes(t *testing.T) {
	for sql, want := range map[string]string{
		"INSERT INTO users\n(id, email, is_active, is_superuser, token_version)\nVALUES ($1, NULLIF($2, ''), true, false, 0)": "false",
		"INSERT INTO public.users (id, is_superuser, created_at) VALUES ($1, $2, now())":                                      "$2",
		"\nUPDATE users SET email = $2, is_superuser = $8, updated_at = $9\nWHERE id = $1":                                    "$8",
		"UPDATE users SET is_superuser=TRUE WHERE id = $1":                                                                    "true",
		"UPDATE users SET email = $2, updated_at = $3 WHERE id = $1":                                                          "",
		"SELECT is_superuser FROM users WHERE id = $1":                                                                        "",
	} {
		if !usersWrite.MatchString(sql) && want != "" {
			t.Errorf("%q: not seen as a users write", sql)
		}
		if got := superuserWriteValue(sql); usersWrite.MatchString(sql) && got != want {
			t.Errorf("%q: value %q, want %q", sql, got, want)
		}
	}
}
