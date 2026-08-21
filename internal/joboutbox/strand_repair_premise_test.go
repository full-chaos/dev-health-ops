package joboutbox

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestStrandRepairLeaseProxyPremise pins the argument that lets StrandRepair
// refuse a live idempotency lease without ever reading worker_job_runs.
//
// The repair asserts that an expired domain lease implies an expired
// idempotency lease. That holds only while three things stay true: the lease
// durations are equal, every lease renews on the same divisor, and the
// idempotency claim is taken before the domain lease. If any of them drifts,
// the repair starts re-arming rows whose idempotency claim is still live; each
// such job is ACKed as a duplicate success without reaching its handler
// (CHAOS-3998), which manufactures a fresh strand instead of clearing one.
// Nothing else in the tree would catch that, because the drift would be in a
// package this one cannot import.
//
// The constants are unexported and live in three separate packages, so this
// test reads them from source rather than importing them. Every failure to
// find or interpret a value is a test FAILURE, never a skip: a premise that
// cannot be checked is not a premise that has been met. The checks themselves
// are proved to fire by TestStrandRepairPremiseChecksDetectDrift.
func TestStrandRepairLeaseProxyPremise(t *testing.T) {
	root := moduleRoot(t)

	idempotencyLease := mustConstDuration(t, filepath.Join(root,
		"internal/jobruntime/idempotency_postgres.go"), "defaultIdempotencyLease")
	dailyLease := mustConstDuration(t, filepath.Join(root,
		"internal/jobs/metrics/daily/postgres.go"), "defaultLease")
	workGraphLease := mustConstDuration(t, filepath.Join(root,
		"internal/jobs/workgraph/postgres.go"), "defaultLease")

	// The safety condition is idempotency <= every domain lease this repair
	// covers. Equality is the current state and is pinned as well, so that a
	// change in either direction is loud rather than silently absorbed.
	for _, domain := range []struct {
		name  string
		lease time.Duration
	}{{"daily", dailyLease}, {"workgraph", workGraphLease}} {
		if idempotencyLease > domain.lease {
			t.Fatalf("PREMISE BROKEN: idempotency lease %s exceeds the %s domain lease %s; "+
				"an expired domain lease no longer implies an expired idempotency lease, "+
				"and StrandRepair would re-arm into a guaranteed duplicate success",
				idempotencyLease, domain.name, domain.lease)
		}
		if idempotencyLease != domain.lease {
			t.Fatalf("PREMISE DRIFT: idempotency lease %s and %s domain lease %s are no longer equal; "+
				"re-derive the argument in strand_repair.go before changing this test",
				idempotencyLease, domain.name, domain.lease)
		}
	}

	// The repair's own copy of the idempotency lease bounds how long a
	// terminal delivery must have been finalized before an unclaimed domain
	// row may be re-armed. A copy that drifts below the real lease reopens
	// exactly the window it exists to close.
	if strandIdempotencyLease != idempotencyLease {
		t.Fatalf("PREMISE BROKEN: strandIdempotencyLease is %s but defaultIdempotencyLease is %s; "+
			"the finalized_at grace no longer covers one idempotency lease",
			strandIdempotencyLease, idempotencyLease)
	}

	// All three leases renew on the same divisor. A renewal that fired less
	// often than the others would let its lease lapse while a live process
	// still held the work.
	for _, renewal := range []struct{ name, path string }{
		{"idempotency", "internal/jobruntime/idempotency_postgres.go"},
		{"daily", "internal/jobs/metrics/daily/daily.go"},
		{"workgraph", "internal/jobs/workgraph/handler.go"},
	} {
		divisor, err := tickerDivisor(filepath.Join(root, renewal.path))
		if err != nil {
			t.Fatalf("cannot check the %s renewal cadence: %v", renewal.name, err)
		}
		if divisor != 3 {
			t.Fatalf("PREMISE BROKEN: the %s lease renews on lease/%d, not lease/3; "+
				"the renewal cadences no longer agree", renewal.name, divisor)
		}
	}

	// The idempotency claim must be taken before the handler that acquires the
	// domain lease. Reversing them would make the idempotency lease expire
	// LATER than the domain lease, inverting the implication the repair rests
	// on.
	if err := checkClaimPrecedesHandler(filepath.Join(root, "internal/jobruntime/adapter.go")); err != nil {
		t.Fatalf("PREMISE BROKEN: %v", err)
	}
}

// TestStrandRepairPremiseChecksDetectDrift is the negative control for the
// test above. A premise check that cannot fail is worth nothing, and every
// check here is the only thing standing between a constant edit in another
// package and a repair that manufactures strands. Each case feeds the checker
// a source file carrying exactly one broken premise and requires it to say so.
func TestStrandRepairPremiseChecksDetectDrift(t *testing.T) {
	t.Run("duration", func(t *testing.T) {
		for _, testCase := range []struct {
			name     string
			source   string
			expected time.Duration
			wantErr  string
		}{
			{
				name:     "positive control reads the pinned value",
				source:   "package p\nimport \"time\"\nconst defaultIdempotencyLease = 10 * time.Minute\n",
				expected: 10 * time.Minute,
			},
			{
				name:     "a widened lease is read as widened",
				source:   "package p\nimport \"time\"\nconst defaultIdempotencyLease = 20 * time.Minute\n",
				expected: 20 * time.Minute,
			},
			{
				name:     "a bare unit is understood",
				source:   "package p\nimport \"time\"\nconst defaultIdempotencyLease = time.Hour\n",
				expected: time.Hour,
			},
			{
				name:    "a computed lease is refused rather than assumed",
				source:  "package p\nconst defaultIdempotencyLease = leaseFromConfig()\n",
				wantErr: "cannot interpret",
			},
			{
				name:    "a deleted constant is refused rather than defaulted",
				source:  "package p\nconst somethingElse = 1\n",
				wantErr: "not found",
			},
			{
				name:    "a variable is not mistaken for the constant",
				source:  "package p\nimport \"time\"\nvar defaultIdempotencyLease = 10 * time.Minute\n",
				wantErr: "not found",
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				path := writeFixture(t, testCase.source)
				actual, err := constDuration(path, "defaultIdempotencyLease")
				assertOutcome(t, testCase.wantErr, err)
				if testCase.wantErr == "" && actual != testCase.expected {
					t.Fatalf("read %s, want %s", actual, testCase.expected)
				}
			})
		}
	})

	t.Run("renewal cadence", func(t *testing.T) {
		for _, testCase := range []struct {
			name     string
			source   string
			expected int64
			wantErr  string
		}{
			{
				name:     "positive control reads the direct divisor",
				source:   "package p\nimport \"time\"\nfunc f(lease time.Duration) { _ = time.NewTicker(lease / 3) }\n",
				expected: 3,
			},
			{
				name: "positive control follows one level of indirection",
				source: "package p\nimport \"time\"\nfunc f(lease time.Duration) {\n" +
					"interval := lease / 3\nif interval < 100*time.Millisecond { interval = 100 * time.Millisecond }\n" +
					"_ = time.NewTicker(interval)\n}\n",
				expected: 3,
			},
			{
				name:     "a slowed renewal is read as slowed",
				source:   "package p\nimport \"time\"\nfunc f(lease time.Duration) { _ = time.NewTicker(lease / 4) }\n",
				expected: 4,
			},
			{
				name:    "a constant interval is refused rather than assumed",
				source:  "package p\nimport \"time\"\nfunc f() { _ = time.NewTicker(time.Minute) }\n",
				wantErr: "no longer built as",
			},
			{
				name:    "a removed ticker is refused rather than passed",
				source:  "package p\nfunc f() {}\n",
				wantErr: "found 0",
			},
			{
				name: "an ambiguous second ticker is refused",
				source: "package p\nimport \"time\"\nfunc f(lease time.Duration) {\n" +
					"_ = time.NewTicker(lease / 3)\n_ = time.NewTicker(lease / 9)\n}\n",
				wantErr: "found 2",
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				path := writeFixture(t, testCase.source)
				actual, err := tickerDivisor(path)
				assertOutcome(t, testCase.wantErr, err)
				if testCase.wantErr == "" && actual != testCase.expected {
					t.Fatalf("read divisor %d, want %d", actual, testCase.expected)
				}
			})
		}
	})

	t.Run("claim ordering", func(t *testing.T) {
		claimFirst := "package p\nfunc f() {\n" +
			"claim, _ := adapter.idempotency.Begin(ctx, request)\n" +
			"_ = adapter.handler.Work(ctx, execution)\n_ = claim\n}\n"
		handlerFirst := "package p\nfunc f() {\n" +
			"_ = adapter.handler.Work(ctx, execution)\n" +
			"claim, _ := adapter.idempotency.Begin(ctx, request)\n_ = claim\n}\n"
		neither := "package p\nfunc f() {}\n"

		if err := checkClaimPrecedesHandler(writeFixture(t, claimFirst)); err != nil {
			t.Fatalf("positive control rejected a correctly ordered file: %v", err)
		}
		assertOutcome(t, "no longer precedes", checkClaimPrecedesHandler(writeFixture(t, handlerFirst)))
		assertOutcome(t, "cannot find", checkClaimPrecedesHandler(writeFixture(t, neither)))
	})
}

func writeFixture(t *testing.T, source string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.go")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("cannot write fixture: %v", err)
	}
	return path
}

func assertOutcome(t *testing.T, wantErr string, err error) {
	t.Helper()
	switch {
	case wantErr == "" && err != nil:
		t.Fatalf("unexpected error: %v", err)
	case wantErr == "":
	case err == nil:
		t.Fatalf("expected an error containing %q, got none -- this check cannot fail and "+
			"therefore protects nothing", wantErr)
	case !strings.Contains(err.Error(), wantErr):
		t.Fatalf("expected an error containing %q, got %v", wantErr, err)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatalf("cannot resolve working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("cannot locate the module root: no go.mod above the test's working directory")
		}
		directory = parent
	}
}

func mustConstDuration(t *testing.T, path string, name string) time.Duration {
	t.Helper()
	value, err := constDuration(path, name)
	if err != nil {
		t.Fatalf("cannot read %s: %v", name, err)
	}
	return value
}

// constDuration reads one named duration constant out of a file. Anything it
// cannot interpret is an error rather than a zero value, so a rewrite into a
// form this cannot read is surfaced instead of silently passing.
func constDuration(path string, name string) (time.Duration, error) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %s: %w", path, err)
	}
	for _, declaration := range file.Decls {
		general, isGeneral := declaration.(*ast.GenDecl)
		if !isGeneral || general.Tok != token.CONST {
			continue
		}
		for _, specification := range general.Specs {
			value, isValue := specification.(*ast.ValueSpec)
			if !isValue {
				continue
			}
			for index, identifier := range value.Names {
				if identifier.Name != name || index >= len(value.Values) {
					continue
				}
				return evaluateDuration(path, name, value.Values[index])
			}
		}
	}
	return 0, fmt.Errorf("constant %s not found in %s; the lease-proxy premise can no longer be checked",
		name, path)
}

// evaluateDuration understands `<int> * time.<Unit>` and a bare `time.<Unit>`.
// Every other shape is an error: an unrecognised expression means the premise
// is unverified, which must never read as verified.
func evaluateDuration(path string, name string, expression ast.Expr) (time.Duration, error) {
	switch typed := expression.(type) {
	case *ast.BinaryExpr:
		if typed.Op != token.MUL {
			break
		}
		multiplier, ok := literalInt(typed.X)
		if !ok {
			break
		}
		unit, ok := durationUnit(typed.Y)
		if !ok {
			break
		}
		return time.Duration(multiplier) * unit, nil
	case *ast.SelectorExpr:
		if unit, ok := durationUnit(typed); ok {
			return unit, nil
		}
	}
	return 0, fmt.Errorf("cannot interpret %s in %s as a duration; extend this check to cover its "+
		"new form rather than deleting it", name, path)
}

func literalInt(expression ast.Expr) (int64, bool) {
	literal, isLiteral := expression.(*ast.BasicLit)
	if !isLiteral || literal.Kind != token.INT {
		return 0, false
	}
	var value int64
	for _, digit := range literal.Value {
		if digit < '0' || digit > '9' {
			return 0, false
		}
		value = value*10 + int64(digit-'0')
	}
	return value, true
}

func durationUnit(expression ast.Expr) (time.Duration, bool) {
	selector, isSelector := expression.(*ast.SelectorExpr)
	if !isSelector {
		return 0, false
	}
	packageName, isIdentifier := selector.X.(*ast.Ident)
	if !isIdentifier || packageName.Name != "time" {
		return 0, false
	}
	switch selector.Sel.Name {
	case "Nanosecond":
		return time.Nanosecond, true
	case "Microsecond":
		return time.Microsecond, true
	case "Millisecond":
		return time.Millisecond, true
	case "Second":
		return time.Second, true
	case "Minute":
		return time.Minute, true
	case "Hour":
		return time.Hour, true
	}
	return 0, false
}

// tickerDivisor finds the lease-renewal ticker in a file and returns the
// divisor it is built from. The renewal loops are written either as
// time.NewTicker(<lease> / <divisor>) or as an interval variable assigned from
// that same quotient and then passed to the ticker, so one level of local
// indirection is resolved. Anything else is an error.
func tickerDivisor(path string) (int64, error) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %s: %w", path, err)
	}
	arguments := make([]ast.Expr, 0, 2)
	ast.Inspect(file, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall || len(call.Args) != 1 {
			return true
		}
		selector, isSelector := call.Fun.(*ast.SelectorExpr)
		if !isSelector || selector.Sel.Name != "NewTicker" {
			return true
		}
		if packageName, isIdentifier := selector.X.(*ast.Ident); !isIdentifier || packageName.Name != "time" {
			return true
		}
		arguments = append(arguments, call.Args[0])
		return true
	})
	if len(arguments) != 1 {
		return 0, fmt.Errorf("expected exactly one lease-renewal ticker in %s, found %d; "+
			"the renewal-cadence premise can no longer be checked", path, len(arguments))
	}
	return quotientDivisor(path, file, arguments[0])
}

// quotientDivisor reads the divisor out of a `<lease> / <divisor>` expression,
// following a local variable at most one step to reach it.
func quotientDivisor(path string, file *ast.File, expression ast.Expr) (int64, error) {
	if identifier, isIdentifier := expression.(*ast.Ident); isIdentifier {
		resolved := make([]ast.Expr, 0, 2)
		ast.Inspect(file, func(node ast.Node) bool {
			assignment, isAssignment := node.(*ast.AssignStmt)
			if !isAssignment || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
				return true
			}
			target, isTarget := assignment.Lhs[0].(*ast.Ident)
			if !isTarget || target.Name != identifier.Name {
				return true
			}
			// Only quotients are candidates. A renewal loop may also clamp its
			// interval to a floor (`interval = 100 * time.Millisecond`); that
			// assignment is not the cadence, and at the pinned ten-minute
			// lease such a floor is inert -- lease/3 is three orders of
			// magnitude above it.
			binary, isBinary := assignment.Rhs[0].(*ast.BinaryExpr)
			if !isBinary || binary.Op != token.QUO {
				return true
			}
			resolved = append(resolved, assignment.Rhs[0])
			return true
		})
		if len(resolved) != 1 {
			return 0, fmt.Errorf("the renewal interval %q in %s is not assigned from exactly one "+
				"quotient (found %d); the renewal-cadence premise can no longer be checked",
				identifier.Name, path, len(resolved))
		}
		expression = resolved[0]
	}
	binary, isBinary := expression.(*ast.BinaryExpr)
	if !isBinary || binary.Op != token.QUO {
		return 0, fmt.Errorf("the renewal ticker in %s is no longer built as <lease>/<divisor>; "+
			"the renewal-cadence premise can no longer be checked", path)
	}
	divisor, ok := literalInt(binary.Y)
	if !ok {
		return 0, fmt.Errorf("the renewal ticker divisor in %s is not an integer literal; "+
			"the renewal-cadence premise can no longer be checked", path)
	}
	return divisor, nil
}

// checkClaimPrecedesHandler proves the idempotency claim is taken before the
// handler runs. The handler is what acquires the domain lease, so this
// ordering is what makes the idempotency lease expire no later than the domain
// lease.
func checkClaimPrecedesHandler(path string) error {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		return fmt.Errorf("cannot parse %s: %w", path, err)
	}
	claimAt, handlerAt := token.NoPos, token.NoPos
	ast.Inspect(file, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		outer, isSelector := call.Fun.(*ast.SelectorExpr)
		if !isSelector {
			return true
		}
		inner, isInner := outer.X.(*ast.SelectorExpr)
		if !isInner {
			return true
		}
		switch {
		case inner.Sel.Name == "idempotency" && outer.Sel.Name == "Begin" && !claimAt.IsValid():
			claimAt = call.Pos()
		case inner.Sel.Name == "handler" && outer.Sel.Name == "Work" && !handlerAt.IsValid():
			handlerAt = call.Pos()
		}
		return true
	})
	if !claimAt.IsValid() || !handlerAt.IsValid() {
		return fmt.Errorf("cannot find both the idempotency claim and the handler call in %s; "+
			"the claim-ordering premise can no longer be checked", path)
	}
	if claimAt >= handlerAt {
		return fmt.Errorf("the idempotency claim at %s no longer precedes the handler at %s; "+
			"the idempotency lease may now outlive the domain lease",
			fileSet.Position(claimAt), fileSet.Position(handlerAt))
	}
	return nil
}
