package platformrole

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// TestEveryPublicSuperuserLiteralIsListed is the census of the code that
// names the public column: every non-test string literal containing
// is_superuser, by file and function, with its count and its kind
// (testdata/superuser_literals.tsv). A read of the column that decides
// platform power must go through policy.PlatformRoleRead (or authapi's,
// policy's or the minter's switched statements); a new literal fails here
// until it is classified.
func TestEveryPublicSuperuserLiteralIsListed(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	listed := readLiteralRows(t)
	found := map[string]int{}
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
			for function, n := range superuserLiterals(file) {
				found[rel+"\t"+function] += n
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for key, row := range listed {
		if got := found[key]; got != row.count {
			t.Errorf("%s: %d is_superuser literals, listed %d (%s): update the census", strings.ReplaceAll(key, "\t", " "), got, row.count, row.kind)
		}
	}
	var unlisted []string
	for key, n := range found {
		if _, ok := listed[key]; !ok {
			unlisted = append(unlisted, fmt.Sprintf("%s\t%d", key, n))
		}
	}
	sort.Strings(unlisted)
	for _, row := range unlisted {
		t.Errorf("unlisted is_superuser literal (classify it in testdata/superuser_literals.tsv; a read that decides platform power goes through policy.PlatformRoleRead): %s", row)
	}
}

// superuserLiterals counts, per function, the string literals that contain
// is_superuser. Struct tags and import paths are not counted.
func superuserLiterals(file *ast.File) map[string]int {
	counts := map[string]int{}
	tags := map[*ast.BasicLit]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		if field, ok := n.(*ast.Field); ok && field.Tag != nil {
			tags[field.Tag] = true
		}
		return true
	})
	count := func(scope string, node ast.Node) {
		ast.Inspect(node, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING && !tags[lit] && strings.Contains(lit.Value, "is_superuser") {
				counts[scope]++
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			count(funcName(decl), decl)
		case *ast.GenDecl:
			if decl.Tok != token.IMPORT {
				count("-", decl)
			}
		}
	}
	return counts
}

type literalRow struct {
	count int
	kind  string
}

func readLiteralRows(t *testing.T) map[string]literalRow {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "superuser_literals.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	rows := map[string]literalRow{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 4 || fields[3] == "" {
			t.Fatalf("superuser_literals.tsv: %q is not file, function, count, kind", line)
		}
		n, err := strconv.Atoi(fields[2])
		if err != nil || n < 1 {
			t.Fatalf("superuser_literals.tsv: %q: count must be a positive number", line)
		}
		key := fields[0] + "\t" + fields[1]
		if _, dup := rows[key]; dup {
			t.Fatalf("superuser_literals.tsv: %s listed twice", key)
		}
		rows[key] = literalRow{count: n, kind: fields[3]}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}
