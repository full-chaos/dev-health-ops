package joboutbox

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestStrandRepairClaimLivenessPremise pins the ONE cross-package fact this
// repair still depends on now that the execution-state read has replaced the
// lease proxy.
//
// RETIRED WITH THE PROXY, deliberately: the assertions that the idempotency and
// domain leases were equal, and that the claim was taken before the domain
// lease. StrandRepair reads worker_job_runs directly, so it depends on neither.
// A test pinning a premise nothing depends on is future confusion rather than
// safety -- it would tell the next reader this repair rests on a relationship
// it does not.
//
// What survives is narrower and real. claimStateSQL treats an EXPIRED claim
// lease as proof the claimant is gone. That is sound only while a live claimant
// keeps its lease fresh, which is the renewer's job. If the renewal interval
// ever crept up to or past the lease, a healthy worker would intermittently
// show an expired lease and this repair would rearm underneath it, into exactly
// the duplicate-success no-op the design exists to avoid (CHAOS-3998).
//
// The constant and the renewal loop are unexported and in another package, so
// they are read from source. Anything unreadable is a FAILURE, never a skip.
// The checks are proved to fire by TestStrandRepairPremiseChecksDetectDrift.
func TestStrandRepairClaimLivenessPremise(t *testing.T) {
	root := moduleRoot(t)
	const idempotencySource = "internal/jobruntime/idempotency_postgres.go"

	lease := mustConstDuration(t, filepath.Join(root, idempotencySource), "defaultIdempotencyLease")
	divisor, err := tickerDivisor(filepath.Join(root, idempotencySource))
	if err != nil {
		t.Fatalf("cannot check the claim renewal cadence: %v", err)
	}

	// The renewal must fire strictly more often than the lease expires, with
	// margin for a missed tick. A divisor of 1 renews exactly as the lease
	// lapses; below 2 there is no room for a single failed attempt.
	if divisor < 2 {
		t.Fatalf("PREMISE BROKEN: the idempotency claim renews on lease/%d, leaving no margin "+
			"before expiry; an expired lease would no longer prove the claimant is gone, and "+
			"StrandRepair would rearm underneath a live worker", divisor)
	}
	if divisor != 3 {
		t.Fatalf("PREMISE DRIFT: the idempotency claim renewal divisor changed from 3 to %d; "+
			"re-derive the expired-lease-implies-dead-claimant argument in strand_repair.go "+
			"before changing this test", divisor)
	}
	if interval := lease / time.Duration(divisor); interval >= lease {
		t.Fatalf("PREMISE BROKEN: renewal interval %s is not shorter than the lease %s", interval, lease)
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
				// codex review 2026-08-20: the divisor alone is not the
				// cadence. Dividing something that is not a lease reports a
				// correct-looking 3 for an unrelated interval.
				name:    "a divisor on a non-lease numerator is refused",
				source:  "package p\nimport \"time\"\nfunc f(pollInterval time.Duration) { _ = time.NewTicker(pollInterval / 3) }\n",
				wantErr: "does not name a lease",
			},
			{
				name:     "a lease-named selector numerator is accepted",
				source:   "package p\nimport \"time\"\nfunc f(s struct{ leaseDuration time.Duration }) { _ = time.NewTicker(s.leaseDuration / 3) }\n",
				expected: 3,
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
// leaseNumeratorPattern matches the identifiers the renewal loops legitimately
// divide: a lease duration, by any of its spellings in the three packages.
// Requiring the numerator to name a lease is what stops `somethingElse / 3`
// from satisfying the cadence check.
var leaseNumeratorPattern = regexp.MustCompile(`(?i)lease|ttl`)

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
	// The numerator must actually be a lease. Checking only the divisor would
	// accept `unrelatedInterval / 3` and report a cadence that has nothing to
	// do with the lease this repair reasons about.
	numerator := renderExpr(binary.X)
	if !leaseNumeratorPattern.MatchString(numerator) {
		return 0, fmt.Errorf("the renewal ticker in %s divides %q, which does not name a lease; "+
			"the renewal-cadence premise can no longer be checked", path, numerator)
	}
	divisor, ok := literalInt(binary.Y)
	if !ok {
		return 0, fmt.Errorf("the renewal ticker divisor in %s is not an integer literal; "+
			"the renewal-cadence premise can no longer be checked", path)
	}
	return divisor, nil
}

// renderExpr flattens an expression back to source-ish text for identity
// checks. Only the shapes the renewal loops actually use are rendered; anything
// else yields the empty string and therefore fails the numerator check closed.
func renderExpr(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return renderExpr(typed.X) + "." + typed.Sel.Name
	}
	return ""
}
