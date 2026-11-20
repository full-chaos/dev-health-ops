package clickhouse

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// clickhouse-go v2.51.0 conn_batch.go:76 and :306 pass the closeQuery error to
// the connection release and return success, so batchctx.go cannot name the
// bound a stalled server broke. batch.Close returns that error. Allowlist key:
// "<slash path>:<identifier>".
var allowedCloseQueryBypassUses = map[string]string{}

var closeQueryBypassIdents = map[string]bool{
	"WithReleaseConnection": true,
	"WithCloseOnFlush":      true,
	"ReleaseConnection":     true,
	"CloseOnFlush":          true,
}

func isProductionGoFile(path string) bool {
	return strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go")
}

type bypassHit struct{ file, ident, where string }

func scanForCloseQueryBypass(name string, src any) ([]bypassHit, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var hits []bypassHit
	ast.Inspect(f, func(n ast.Node) bool {
		id, ok := n.(*ast.Ident)
		if !ok {
			return true
		}
		if closeQueryBypassIdents[id.Name] {
			hits = append(hits, bypassHit{file: name, ident: id.Name, where: fmt.Sprintf("%s:%d", name, fset.PositionFor(id.Pos(), false).Line)})
		}
		return true
	})
	return hits, nil
}

func TestNoProductionUseOfCloseQueryBypassOptions(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	var files int
	used := map[string]bool{}
	var bad []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor", "testdata":
				return filepath.SkipDir
			}
			if path != root {
				if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !isProductionGoFile(path) {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		hits, err := scanForCloseQueryBypass(rel, src)
		if err != nil {
			return fmt.Errorf("parse %s: %w", rel, err)
		}
		files++
		for _, h := range hits {
			key := h.file + ":" + h.ident
			if _, ok := allowedCloseQueryBypassUses[key]; ok {
				used[key] = true
				continue
			}
			bad = append(bad, h.where+" "+h.ident)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files == 0 {
		t.Error("scan did not run over production code: 0 files")
	}
	sort.Strings(bad)
	for _, b := range bad {
		t.Errorf("%s: driver option drops the closeQuery error on release (clickhouse-go v2.51.0 conn_batch.go:76,306); use batch.Close()", b)
	}
	for key := range allowedCloseQueryBypassUses {
		if !used[key] {
			t.Errorf("allowlist key %q matches no production use: remove it", key)
		}
	}
}

func TestCloseQueryBypassScanRefusesPlantedUse(t *testing.T) {
	cases := []struct {
		name, src string
		want      []string
	}{
		{"flush call", "package p\nfunc f(c C){ c.PrepareBatch(ctx, q, driver.WithCloseOnFlush()) }", []string{"WithCloseOnFlush"}},
		{"release call", "package p\nfunc f(c C){ c.PrepareBatch(ctx, q, driver.WithReleaseConnection()) }", []string{"WithReleaseConnection"}},
		{"func value", "package p\nvar o = driver.WithCloseOnFlush", []string{"WithCloseOnFlush"}},
		{"hand-written option", "package p\nvar o = func(x *driver.PrepareBatchOptions){ x.CloseOnFlush = true; x.ReleaseConnection = true }", []string{"CloseOnFlush", "ReleaseConnection"}},
		{"literal", "package p\nvar o = driver.PrepareBatchOptions{ReleaseConnection: true}", []string{"ReleaseConnection"}},
		{"clean", "package p\nfunc f(c C){ b, _ := c.PrepareBatch(ctx, q); defer b.Close() }", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hits, err := scanForCloseQueryBypass("planted.go", tc.src)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, h := range hits {
				got = append(got, h.ident)
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("hits = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCloseQueryBypassScanReportsPhysicalLine(t *testing.T) {
	src := "package p\n//line options.tmpl:100\nvar o = driver.WithCloseOnFlush\n"
	hits, err := scanForCloseQueryBypass("planted.go", src)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].where != "planted.go:3" {
		t.Fatalf("hits = %+v, want one at planted.go:3", hits)
	}
}

func TestProductionGoFileFilter(t *testing.T) {
	for path, want := range map[string]bool{"a.go": true, "dir/a.go": true, "a_test.go": false, "dir/a_test.go": false, "a.txt": false, "go.mod": false} {
		if got := isProductionGoFile(path); got != want {
			t.Errorf("isProductionGoFile(%q) = %v, want %v", path, got, want)
		}
	}
}
