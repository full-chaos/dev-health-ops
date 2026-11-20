package clickhouse

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type batchCtxKey struct{}

type recordingBatchConn struct {
	driver.Conn
	prepares int
	ctx      context.Context
	batch    *recordingBatch
	block    bool
}

func (c *recordingBatchConn) PrepareBatch(ctx context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.prepares++
	c.ctx = ctx
	c.batch = &recordingBatch{ctx: ctx, block: c.block}
	return c.batch, nil
}

// recordingBatch: with block, Send waits like a stalled server until its driver
// context ends and returns the driver's deadline error.
type recordingBatch struct {
	driver.Batch
	ctx                   context.Context
	block                 bool
	sends, aborts, closes int
}

func (b *recordingBatch) Send() error {
	b.sends++
	if b.block {
		select {
		case <-b.ctx.Done():
			return b.ctx.Err()
		case <-time.After(10 * time.Second):
			return errors.New("the driver context never ended: the batch has no bound")
		}
	}
	return nil
}
func (b *recordingBatch) Abort() error { b.aborts++; return nil }
func (b *recordingBatch) Close() error { b.closes++; return nil }

func TestACallerCancelNeverReachesTheDriverBatchContext(t *testing.T) {
	inner := &recordingBatchConn{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), batchCtxKey{}, "kept"))
	batch, err := wrapConn(inner, time.Minute).PrepareBatch(ctx, "INSERT INTO t")
	if err != nil {
		t.Fatal(err)
	}
	if got := inner.ctx.Value(batchCtxKey{}); got != "kept" {
		t.Fatalf("the driver's batch context lost the caller's values: %v", got)
	}
	if err := batch.Send(); err != nil || inner.batch.sends != 1 || inner.batch.aborts != 0 {
		t.Fatalf("send = %v, sends %d aborts %d; want one send", err, inner.batch.sends, inner.batch.aborts)
	}
	cancel()
	if err := inner.ctx.Err(); err != nil {
		t.Fatalf("the driver's batch context ended right after Send and the caller's cancel (%v): its late Send watchdog can close a pooled connection", err)
	}
}

func TestTheDriverBatchContextCarriesTheBoundOrTheEarlierCallerDeadline(t *testing.T) {
	inner := &recordingBatchConn{}
	before := time.Now()
	if _, err := wrapConn(inner, time.Minute).PrepareBatch(context.Background(), "INSERT INTO t"); err != nil {
		t.Fatal(err)
	}
	deadline, ok := inner.ctx.Deadline()
	if !ok || deadline.Before(before.Add(time.Minute)) || deadline.After(time.Now().Add(time.Minute)) {
		t.Fatalf("driver deadline = %v (set %v); want the bound, one minute after PrepareBatch", deadline, ok)
	}
	callerDeadline := time.Now().Add(5 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()
	if _, err := wrapConn(inner, time.Minute).PrepareBatch(ctx, "INSERT INTO t"); err != nil {
		t.Fatal(err)
	}
	if deadline, ok := inner.ctx.Deadline(); !ok || !deadline.Equal(callerDeadline) {
		t.Fatalf("driver deadline = %v (set %v); want the earlier caller deadline %v", deadline, ok, callerDeadline)
	}
	if _, err := wrapConn(inner, 0).PrepareBatch(context.Background(), "INSERT INTO t"); err != nil {
		t.Fatal(err)
	}
	if deadline, ok := inner.ctx.Deadline(); !ok || deadline.Before(before.Add(DefaultBatchWriteBound)) {
		t.Fatalf("zero bound: driver deadline = %v (set %v); want DefaultBatchWriteBound", deadline, ok)
	}
}

func TestTheBatchBoundIsTheConfiguredOneElseTheLongerOfTheFloorAndReadTimeout(t *testing.T) {
	for _, tc := range []struct {
		configured, readTimeout, want time.Duration
	}{
		{time.Minute, 30 * time.Second, time.Minute},
		{0, DefaultConfig("clickhouse://x").ReadTimeout, 30 * time.Second},
		{0, 5 * time.Second, DefaultBatchWriteBound},
		{0, 10 * time.Minute, 10 * time.Minute},
	} {
		if got := batchWriteBound(tc.configured, tc.readTimeout); got != tc.want {
			t.Errorf("batchWriteBound(%s, %s) = %s, want %s", tc.configured, tc.readTimeout, got, tc.want)
		}
	}
}

func TestABatchPastItsBoundEndsWithErrBatchWriteBound(t *testing.T) {
	inner := &recordingBatchConn{block: true}
	batch, err := wrapConn(inner, 50*time.Millisecond).PrepareBatch(context.Background(), "INSERT INTO t")
	if err != nil {
		t.Fatal(err)
	}
	err = batch.Send()
	if !errors.Is(err, ErrBatchWriteBound) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("send = %v; want ErrBatchWriteBound wrapping the driver's deadline error", err)
	}
	if !errors.Is(context.Cause(inner.ctx), ErrBatchWriteBound) {
		t.Fatalf("driver context cause = %v; want ErrBatchWriteBound", context.Cause(inner.ctx))
	}
}

func TestABatchPastTheCallerDeadlineEndsWithTheCallerDeadlineNotTheBound(t *testing.T) {
	inner := &recordingBatchConn{block: true}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	batch, err := wrapConn(inner, time.Minute).PrepareBatch(ctx, "INSERT INTO t")
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrBatchWriteBound) {
		t.Fatalf("send = %v; want the caller's deadline, not ErrBatchWriteBound", err)
	}
}

func TestADoneCallerContextRefusesTheBatchBeforeTheDriver(t *testing.T) {
	inner := &recordingBatchConn{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := wrapConn(inner, time.Minute).PrepareBatch(ctx, "INSERT INTO t"); !errors.Is(err, context.Canceled) || inner.prepares != 0 {
		t.Fatalf("err = %v, driver prepares %d; want context.Canceled and no prepare", err, inner.prepares)
	}
}

func TestACallerCancelledBeforeSendAbortsTheBatchAndSendsNothing(t *testing.T) {
	inner := &recordingBatchConn{}
	ctx, cancel := context.WithCancel(context.Background())
	batch, err := wrapConn(inner, time.Minute).PrepareBatch(ctx, "INSERT INTO t")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := batch.Flush(); !errors.Is(err, context.Canceled) {
		t.Fatalf("flush = %v; want context.Canceled", err)
	}
	if err := batch.Send(); !errors.Is(err, context.Canceled) || inner.batch.sends != 0 || inner.batch.aborts != 1 {
		t.Fatalf("send = %v, sends %d aborts %d; want context.Canceled, no send, one abort", err, inner.batch.sends, inner.batch.aborts)
	}
	if !errors.Is(inner.ctx.Err(), context.Canceled) {
		t.Fatalf("driver context after an abort with no Send = %v; want cancelled at once", inner.ctx.Err())
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

// socketDeadlineBatch models clickhouse-go's Send on a stalled server: the driver
// sets the socket deadline to the batch context's deadline, so the read can end
// with the net deadline error before the context's own timer has fired.
type socketDeadlineBatch struct {
	driver.Batch
	ctx            context.Context
	early          bool
	ctxDoneAtError bool
}

func (b *socketDeadlineBatch) Send() error {
	deadline, _ := b.ctx.Deadline()
	for !b.early && time.Now().Before(deadline) {
	}
	b.ctxDoneAtError = b.ctx.Err() != nil
	return fmt.Errorf("query processing: failed to read packet: %w", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded})
}
func (b *socketDeadlineBatch) Abort() error { return nil }

type socketDeadlineConn struct {
	driver.Conn
	early bool
	batch *socketDeadlineBatch
}

func (c *socketDeadlineConn) PrepareBatch(ctx context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.batch = &socketDeadlineBatch{ctx: ctx, early: c.early}
	return c.batch, nil
}

// CHAOS-14255: the socket deadline the driver takes from the bound can end Send
// before the bound's context timer fires; the error still names the bound.
func TestASocketDeadlineAtTheBoundEndsWithErrBatchWriteBound(t *testing.T) {
	for range 20 {
		inner := &socketDeadlineConn{}
		batch, err := wrapConn(inner, 5*time.Millisecond).PrepareBatch(context.Background(), "INSERT INTO t")
		if err != nil {
			t.Fatal(err)
		}
		err = batch.Send()
		if !errors.Is(err, ErrBatchWriteBound) || !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("send = %v (context done at the socket error: %v); want ErrBatchWriteBound wrapping the socket deadline error", err, inner.batch.ctxDoneAtError)
		}
	}
}

// timerNotFiredCtx is a driver context whose deadline is set but whose timer has
// not fired: the window where the socket deadline wins the race.
type timerNotFiredCtx struct {
	context.Context
	deadline time.Time
	hasDL    bool
}

func (c timerNotFiredCtx) Deadline() (time.Time, bool) { return c.deadline, c.hasDL }
func (c timerNotFiredCtx) Err() error                  { return nil }

func TestTheBoundEndedACallOnlyOnASocketDeadlineErrorAtOrAfterTheDeadline(t *testing.T) {
	socketErr := fmt.Errorf("query processing: %w", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded})
	past, future := time.Now().Add(-time.Millisecond), time.Now().Add(time.Hour)
	for _, tc := range []struct {
		name string
		ctx  timerNotFiredCtx
		err  error
		want bool
	}{
		{"socket deadline error past the deadline", timerNotFiredCtx{context.Background(), past, true}, socketErr, true},
		{"other error past the deadline", timerNotFiredCtx{context.Background(), past, true}, errors.New("server exception"), false},
		{"socket deadline error before the deadline", timerNotFiredCtx{context.Background(), future, true}, socketErr, false},
		{"socket deadline error with no deadline", timerNotFiredCtx{Context: context.Background()}, socketErr, false},
	} {
		if got := deadlineEnded(tc.ctx, tc.err); got != tc.want {
			t.Errorf("%s: deadlineEnded = %v, want %v", tc.name, got, tc.want)
		}
		got := boundErr(tc.ctx, ErrBatchWriteBound, tc.err)
		if errors.Is(got, ErrBatchWriteBound) != tc.want || !errors.Is(got, tc.err) {
			t.Errorf("%s: boundErr = %v; want ErrBatchWriteBound %v and the driver error kept", tc.name, got, tc.want)
		}
	}
}

// A socket deadline under a caller deadline earlier than the bound names the
// caller's deadline, not the bound.
func TestASocketDeadlineAtTheCallerDeadlineDoesNotNameTheBound(t *testing.T) {
	inner := &socketDeadlineConn{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	batch, err := wrapConn(inner, time.Minute).PrepareBatch(ctx, "INSERT INTO t")
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); errors.Is(err, ErrBatchWriteBound) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("send = %v; want the caller's deadline wrapping the socket deadline error, not ErrBatchWriteBound", err)
	}
}

// A socket deadline error before the driver context's deadline (a dial or
// handshake timeout of a re-acquired connection) is not the bound.
func TestASocketDeadlineBeforeTheBoundStaysTheDriverError(t *testing.T) {
	inner := &socketDeadlineConn{early: true}
	batch, err := wrapConn(inner, time.Minute).PrepareBatch(context.Background(), "INSERT INTO t")
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); errors.Is(err, ErrBatchWriteBound) || errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("send = %v; want the raw socket deadline error, not a bound", err)
	}
}
