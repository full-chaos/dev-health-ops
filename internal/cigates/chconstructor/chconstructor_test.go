// Package chconstructor is the CI guard that every ClickHouse client of production code is built by a shared
// constructor, so every client keeps warm pooled connections sized for a ~55 ms RTT to ClickHouse Cloud (CHAOS-10713).
//
// Constructors: internal/storage/clickhouse.Open (native driver, pool + warm-up) and internal/queryapi/chclient
// (the query client's options). Sites:
//
//	driver-open   a call of clickhouse-go's Open or OpenDB: allowed only with a row in allow.tsv
//	query-client  a call of dev-health-go clickhouse.NewClickHouseQueryClientWithOptions: allowed when the enclosing
//	              function (or the call's arguments) builds its options with chclient.Options or
//	              newUnrestrictedReadClickHouseOptions, else it needs a row
//
// Test files are not walked. An unlisted site, a count that differs and a row that matches nothing FAIL.
package chconstructor

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
)

const (
	driverPath = "github.com/ClickHouse/clickhouse-go/v2"
	queryPath  = "github.com/full-chaos/dev-health-go/clickhouse"
	allowFile  = "allow.tsv"
)

type site struct{ file, symbol, kind string }

type allowRow struct {
	count  int
	reason string
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func TestEveryClickHouseClientUsesASharedConstructor(t *testing.T) {
	root := moduleRoot(t)
	found := map[site]int{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			if name == ".git" || name == "testdata" || name == "node_modules" || name == "third_party" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(source), driverPath) && !strings.Contains(string(source), queryPath) {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		sites, err := scanSource(filepath.ToSlash(rel), source)
		if err != nil {
			return err
		}
		for s, n := range sites {
			found[s] += n
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) == 0 {
		t.Fatal("the walk found no site: the shared constructors alone are sites, so the walk did not run")
	}
	problems := compare(found, readAllow(t, allowFile))
	for _, problem := range problems {
		t.Error(problem)
	}
	if len(problems) > 0 {
		t.Log("fix: build the client with internal/storage/clickhouse.Open (native) or chclient.Options (query client); a deliberate exception gets an allow.tsv row with its reason")
	}
}

func TestPlantedSitesAreFound(t *testing.T) {
	cases := map[string]struct {
		source string
		want   map[site]int
	}{
		"driver open": {
			source: `package p
import ch "github.com/ClickHouse/clickhouse-go/v2"
func f() { ch.Open(nil) }`,
			want: map[site]int{{"p.go", "f", "driver-open"}: 1},
		},
		"driver opendb aliased": {
			source: `package p
import drv "github.com/ClickHouse/clickhouse-go/v2"
func f() { drv.OpenDB(nil) }`,
			want: map[site]int{{"p.go", "f", "driver-open"}: 1},
		},
		"query client hand-rolled options": {
			source: `package p
import dh "github.com/full-chaos/dev-health-go/clickhouse"
func f() { dh.NewClickHouseQueryClientWithOptions(dh.Options{DSN: "x"}) }`,
			want: map[site]int{{"p.go", "f", "query-client"}: 1},
		},
		"query client shared options": {
			source: `package p
import dh "github.com/full-chaos/dev-health-go/clickhouse"
func f() { opts := chclient.Options("x"); dh.NewClickHouseQueryClientWithOptions(opts) }`,
			want: map[site]int{},
		},
		"unrelated Open": {
			source: `package p
import "os"
func f() { os.Open("x") }`,
			want: map[site]int{},
		},
	}
	for name, tc := range cases {
		got, err := scanSource("p.go", []byte(tc.source))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("%s: got %v, want %v", name, got, tc.want)
		}
	}
}

func scanSource(file string, source []byte) (map[site]int, error) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, source, 0)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	names := map[string]string{} // local import name -> import path
	for _, spec := range parsed.Imports {
		path, _ := strconv.Unquote(spec.Path.Value)
		if path != driverPath && path != queryPath {
			continue
		}
		local := "clickhouse"
		if spec.Name != nil {
			local = spec.Name.Name
		}
		names[local] = path
	}
	found := map[site]int{}
	if len(names) == 0 {
		return found, nil
	}
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		symbol := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) > 0 {
			symbol = receiverName(fn.Recv.List[0].Type) + "." + symbol
		}
		bodyText := string(source[fset.Position(fn.Body.Pos()).Offset:fset.Position(fn.Body.End()).Offset])
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			qualifier, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			switch names[qualifier.Name] {
			case driverPath:
				if selector.Sel.Name == "Open" || selector.Sel.Name == "OpenDB" {
					found[site{file, symbol, "driver-open"}]++
				}
			case queryPath:
				if selector.Sel.Name == "NewClickHouseQueryClientWithOptions" || selector.Sel.Name == "NewClickHouseQueryClient" {
					if !usesSharedOptions(bodyText) {
						found[site{file, symbol, "query-client"}]++
					}
				}
			}
			return true
		})
	}
	return found, nil
}

func usesSharedOptions(body string) bool {
	return strings.Contains(body, "chclient.Options(") || strings.Contains(body, "newUnrestrictedReadClickHouseOptions(")
}

func receiverName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.StarExpr:
		return receiverName(typed.X)
	case *ast.Ident:
		return typed.Name
	case *ast.IndexExpr:
		return receiverName(typed.X)
	}
	return "?"
}

func readAllow(t *testing.T, name string) map[site]allowRow {
	t.Helper()
	file, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows := map[site]allowRow{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 5 {
			t.Fatalf("%s: want 5 tab-separated fields (file, symbol, kind, count, reason), got %d in %q", name, len(fields), line)
		}
		count, err := strconv.Atoi(fields[3])
		if err != nil || count < 1 {
			t.Fatalf("%s: bad count in %q", name, line)
		}
		if strings.TrimSpace(fields[4]) == "" {
			t.Fatalf("%s: row without a reason: %q", name, line)
		}
		key := site{fields[0], fields[1], fields[2]}
		if _, dup := rows[key]; dup {
			t.Fatalf("%s: duplicate row %v", name, key)
		}
		rows[key] = allowRow{count, fields[4]}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func compare(found map[site]int, allow map[site]allowRow) []string {
	var problems []string
	for s, n := range found {
		row, ok := allow[s]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("%s %s: %d unlisted %s site(s) (ClickHouse client built outside the shared constructor)", s.file, s.symbol, n, s.kind))
		case row.count != n:
			problems = append(problems, fmt.Sprintf("%s %s %s: %d site(s), allow.tsv says %d", s.file, s.symbol, s.kind, n, row.count))
		}
	}
	for s := range allow {
		if _, ok := found[s]; !ok {
			problems = append(problems, fmt.Sprintf("allow.tsv row matches nothing: %s %s %s", s.file, s.symbol, s.kind))
		}
	}
	sort.Strings(problems)
	return problems
}
