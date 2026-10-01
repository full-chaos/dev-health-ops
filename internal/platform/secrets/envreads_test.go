package secrets_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// dynamicEnvReads is the closed list of the files that read an environment variable
// by a name the scan cannot see (a parameter), with why none of them reads a secret.
// A new such read fails the guard until it is classified here, or reads through
// GetenvNamed / GetenvSecret / ResolveSecret.
var dynamicEnvReads = map[string]string{
	"internal/apiservice/customerpush/schemas.go":                "customer push options (limits, flags)",
	"internal/edgetokenmint/edgetokenmint.go":                    "issuer and audience names (the signing key reads through GetenvSecret)",
	"internal/envelopemint/envelopemint.go":                      "issuer, audience and key id (the private key reads through GetenvSecret)",
	"internal/externalrecompute/plan.go":                         "integer tuning knobs",
	"internal/jobs/metrics/daily/wellbeing_native_executor.go":   "timezone and tuning names",
	"internal/mail/sender.go":                                    "provider, host, port, TLS and sender names (the API keys and the SMTP password read through GetenvSecret)",
	"internal/platform/tracing/tracing.go":                       "OpenTelemetry settings",
	"internal/queryapi/investmentexplain/provider.go":            "model names per provider",
	"internal/queryapi/investmentexplain/provider_org.go":        "model names per provider",
	"internal/scheduler/fixed/producers.go":                      "cron intervals",
	"internal/scheduler/sync/materializer.go":                    "feature switches",
	"internal/syncdispatchruntime/budget_unit.go":                "integer budgets",
	"internal/syncdispatchruntime/native_reference_discovery.go": "integer limits",
	"internal/platform/secrets/registry.go":                      "the registering readers themselves",
}

var secretEnvName = regexp.MustCompile(`^[A-Z][A-Z0-9_]*(TOKEN|PASSWORD|PASSWD|SECRET|PRIVATE_KEY|API_KEY|_KEY|_PASS|DSN|_URI)[A-Z0-9_]*$`)

func TestNoSecretIsReadFromTheEnvironmentWithoutRegistration(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	fset := token.NewFileSet()
	var literal, dynamic []string
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") ||
			strings.Contains(path, "/testsupport/") || strings.Contains(filepath.ToSlash(path), "../../../tests/") || strings.Contains(path, "/.git/") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Errorf("parse %s: %v", path, perr)
			return nil
		}
		consts := map[string]string{}
		ast.Inspect(file, func(n ast.Node) bool {
			if spec, ok := n.(*ast.ValueSpec); ok {
				for i, name := range spec.Names {
					if i < len(spec.Values) {
						if lit, ok := spec.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
							v, _ := strconv.Unquote(lit.Value)
							consts[name.Name] = v
						}
					}
				}
			}
			return true
		})
		rel := filepath.ToSlash(strings.TrimPrefix(filepath.ToSlash(path), "../../../"))
		called := map[ast.Node]bool{}
		ast.Inspect(file, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				called[call.Fun] = true
			}
			return true
		})
		ast.Inspect(file, func(n ast.Node) bool {
			// os.LookupEnv or os.Getenv handed on as a function value is the process's
			// own lookup: it must be secrets.ProcessLookup, which registers what it reads.
			if sel, ok := n.(*ast.SelectorExpr); ok && !called[n] && rel != "internal/platform/secrets/registry.go" && rel != "internal/platform/secrets/source.go" {
				if owner, _ := sel.X.(*ast.Ident); owner != nil && owner.Name == "os" && (sel.Sel.Name == "LookupEnv" || sel.Sel.Name == "Getenv") {
					dynamic = append(dynamic, fmt.Sprintf("%s hands os.%s on as a lookup function (use secrets.ProcessLookup)", rel, sel.Sel.Name))
				}
			}
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			owner, _ := sel.X.(*ast.Ident)
			if owner == nil || owner.Name != "os" || (sel.Sel.Name != "Getenv" && sel.Sel.Name != "LookupEnv") {
				return true
			}
			switch arg := call.Args[0].(type) {
			case *ast.BasicLit:
				name, _ := strconv.Unquote(arg.Value)
				if secretEnvName.MatchString(name) {
					literal = append(literal, fmt.Sprintf("%s reads %s", rel, name))
				}
			case *ast.Ident:
				if name, ok := consts[arg.Name]; ok {
					if secretEnvName.MatchString(name) {
						literal = append(literal, fmt.Sprintf("%s reads %s", rel, name))
					}
				} else if _, classified := dynamicEnvReads[rel]; !classified {
					dynamic = append(dynamic, fmt.Sprintf("%s reads a variable chosen at run time", rel))
				}
			}
			return true
		})
		return nil
	})
	for _, line := range literal {
		t.Errorf("%s directly: a secret read must go through secrets.GetenvSecret / GetenvDSN / GetenvNamed / ResolveSecret so the process logger redacts it", line)
	}
	for _, line := range dynamic {
		t.Errorf("%s: classify the file in dynamicEnvReads (why it reads no secret) or read through secrets.GetenvNamed", line)
	}
	for file := range dynamicEnvReads {
		if _, err := os.Stat(filepath.Join(root, file)); err != nil {
			t.Errorf("dynamicEnvReads lists %s, which does not exist: remove the row", file)
		}
	}
}
