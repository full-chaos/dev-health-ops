package clickhouse

import (
	"bufio"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type batchCtxKey struct{}

type recordingBatchConn struct {
	driver.Conn
	prepares int
	ctx      context.Context
	batch    *recordingBatch
}

func (c *recordingBatchConn) PrepareBatch(ctx context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.prepares++
	c.ctx = ctx
	c.batch = &recordingBatch{}
	return c.batch, nil
}

type recordingBatch struct {
	driver.Batch
	sends, aborts int
}

func (b *recordingBatch) Send() error  { b.sends++; return nil }
func (b *recordingBatch) Abort() error { b.aborts++; return nil }

func TestTheDriverBatchContextIsNeverCancelledAndKeepsTheCallersValues(t *testing.T) {
	inner := &recordingBatchConn{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), batchCtxKey{}, "kept"))
	defer cancel()
	batch, err := wrapConn(inner).PrepareBatch(ctx, "INSERT INTO t")
	if err != nil {
		t.Fatal(err)
	}
	if inner.ctx.Done() != nil {
		t.Fatal("the driver's batch context can be cancelled: its Send watchdog can close a pooled connection")
	}
	if got := inner.ctx.Value(batchCtxKey{}); got != "kept" {
		t.Fatalf("the driver's batch context lost the caller's values: %v", got)
	}
	if err := batch.Send(); err != nil || inner.batch.sends != 1 || inner.batch.aborts != 0 {
		t.Fatalf("send = %v, sends %d aborts %d; want one send", err, inner.batch.sends, inner.batch.aborts)
	}
}

func TestADoneCallerContextRefusesTheBatchBeforeTheDriver(t *testing.T) {
	inner := &recordingBatchConn{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := wrapConn(inner).PrepareBatch(ctx, "INSERT INTO t"); !errors.Is(err, context.Canceled) || inner.prepares != 0 {
		t.Fatalf("err = %v, driver prepares %d; want context.Canceled and no prepare", err, inner.prepares)
	}
}

func TestACallerCancelledBeforeSendAbortsTheBatchAndSendsNothing(t *testing.T) {
	inner := &recordingBatchConn{}
	ctx, cancel := context.WithCancel(context.Background())
	batch, err := wrapConn(inner).PrepareBatch(ctx, "INSERT INTO t")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := batch.Send(); !errors.Is(err, context.Canceled) || inner.batch.sends != 0 || inner.batch.aborts != 1 {
		t.Fatalf("send = %v, sends %d aborts %d; want context.Canceled, no send, one abort", err, inner.batch.sends, inner.batch.aborts)
	}
}

// Census (CHAOS-14054): every production native connection is built by OpenWith
// (internal/cigates/chconstructor holds that; its allow.tsv lists every other
// driver-open site), OpenWith returns wrapConn, and no other driver-open site
// of non-test code prepares a batch.
func TestEveryProductionBatchGoesThroughTheDetachedBatchWrapper(t *testing.T) {
	root := moduleRootDir(t)
	allow, err := os.Open(filepath.Join(root, "internal/cigates/chconstructor/allow.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer allow.Close()
	others := 0
	scanner := bufio.NewScanner(allow)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := strings.Split(line, "\t")
		if len(cols) < 3 || cols[2] != "driver-open" {
			continue
		}
		file, symbol := cols[0], cols[1]
		parsed := parseGoFile(t, filepath.Join(root, file))
		if file == "internal/storage/clickhouse/factory.go" {
			if symbol != "OpenWith" || !funcCalls(parsed, "OpenWith", "wrapConn") {
				t.Fatalf("%s %s: the shared constructor no longer returns wrapConn", file, symbol)
			}
			continue
		}
		others++
		ast.Inspect(parsed, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "PrepareBatch" {
				t.Errorf("%s: PrepareBatch on a connection opened outside the shared constructor (allow.tsv %s)", file, symbol)
			}
			return true
		})
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if others == 0 {
		t.Fatal("allow.tsv lists no other driver-open site: the census read nothing")
	}
}

func moduleRootDir(t *testing.T) string {
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

func parseGoFile(t *testing.T, path string) *ast.File {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func funcCalls(file *ast.File, funcName, callee string) bool {
	found := false
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != funcName || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == callee {
					found = true
				}
			}
			return true
		})
	}
	return found
}
