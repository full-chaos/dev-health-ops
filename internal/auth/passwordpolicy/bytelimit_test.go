package passwordpolicy_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
)

// The auth service and the go-api routes read one byte limit: the auth
// service's constants are the policy's, and the sign-in byte constants of both
// services equal it.
func TestByteLimitIsOneConstantInBothServices(t *testing.T) {
	if local.MaxPasswordBytes != passwordpolicy.MaxBytes || local.ViolationTooLong != passwordpolicy.ViolationTooLong {
		t.Fatalf("auth-service byte limit %d / %q differs from the policy's %d / %q",
			local.MaxPasswordBytes, local.ViolationTooLong, passwordpolicy.MaxBytes, passwordpolicy.ViolationTooLong)
	}
	for _, file := range []string{"../../api/session/login.go", "../identity/local/login.go"} {
		if got := intConst(t, file, "bcryptMaxPasswordBytes"); got != passwordpolicy.MaxBytes {
			t.Errorf("%s: bcryptMaxPasswordBytes = %d, want the policy's %d", file, got, passwordpolicy.MaxBytes)
		}
	}
	edge := "A1" + strings.Repeat("b", passwordpolicy.MaxBytes-2)
	if v := passwordpolicy.ValidateForHash(edge); len(v) != 0 {
		t.Errorf("%d bytes refused: %q", len(edge), v)
	}
	if v := passwordpolicy.ValidateForHash(edge + "b"); len(v) != 1 || v[0] != passwordpolicy.ViolationTooLong {
		t.Errorf("%d bytes = %q, want the byte violation", len(edge)+1, v)
	}
}

func intConst(t *testing.T, file, name string) int {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := -1
	ast.Inspect(parsed, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, id := range spec.Names {
			if id.Name != name || i >= len(spec.Values) {
				continue
			}
			if lit, ok := spec.Values[i].(*ast.BasicLit); ok {
				found, _ = strconv.Atoi(lit.Value)
			}
		}
		return true
	})
	if found < 0 {
		t.Fatalf("%s: constant %s not found", file, name)
	}
	return found
}
