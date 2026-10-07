package synccli

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// classifiedPrintSites is the closed list of the calls in this package that
// write text an operator sees: writeLine, writeError, fmt.Fprint*, and the
// slog and logger level calls, counted per file, with the reason what each
// prints cannot hold a database login or password. A new site fails
// TestEverySyncPrintSiteIsClassified until it is classified here.
//
// The verdicts, by site:
//   - target.go: usage and argparse refusals (argv echo, no database text); a
//     Refusal's own message (dho's text); every other error through
//     planBoundary (the ClickHouse sink boundary plus the PostgreSQL one); the
//     process logger wrapped with WithValueRedaction for the whole run.
//   - batch.go: the listing/failure texts built through a Boundary of the sink
//     (and the run's secrets); the counts and the "no repositories" line.
//   - local.go: ErrNoRepository text only (a path); every other error is
//     returned to runTarget.
//   - synccli.go: `dho sync teams`: ResolveDSN errors (redacted by the config
//     package), stored-credential errors (redacted by pgstorage.Boundary), and
//     redact() through the sink boundary for every ClickHouse text; counts only
//     in the logger lines, except the incomplete-project-links warning, whose
//     error text is the gateway's link-read error passed through redact() (the
//     sink boundary and the credential this run authenticated with).
//   - teamscatalog.go: constant messages and redact() for every ClickHouse text.
var classifiedPrintSites = map[string]int{
	"batch.go":        3,
	"local.go":        1,
	"synccli.go":      15,
	"target.go":       5,
	"teamscatalog.go": 13,
}

var printCalls = map[string]bool{"writeLine": true, "writeError": true}

func TestEverySyncPrintSiteIsClassified(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]int{}
	scanned := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				if printCalls[fun.Name] {
					found[name]++
				}
			case *ast.SelectorExpr:
				if isLogLevelMethod(fun.Sel.Name) && isProcessLogger(fun.X) {
					found[name]++
					return true
				}
				owner, _ := fun.X.(*ast.Ident)
				if owner == nil {
					return true
				}
				switch {
				case owner.Name == "fmt" && strings.HasPrefix(fun.Sel.Name, "Print"):
					found[name]++
				case owner.Name == "log" && (strings.HasPrefix(fun.Sel.Name, "Print") || strings.HasPrefix(fun.Sel.Name, "Fatal") || strings.HasPrefix(fun.Sel.Name, "Panic")):
					found[name]++
				case owner.Name == "fmt" && strings.HasPrefix(fun.Sel.Name, "Fprint"):
					// a strings.Builder or bytes.Buffer is not an operator stream
					if len(call.Args) > 0 {
						if unary, ok := call.Args[0].(*ast.UnaryExpr); ok && unary.Op == token.AND {
							return true
						}
					}
					found[name]++
				}
			}
			return true
		})
	}
	if scanned < 5 {
		t.Fatalf("the scan read %d files: it measured nothing", scanned)
	}
	var names []string
	for name := range found {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if want, ok := classifiedPrintSites[name]; !ok || want != found[name] {
			t.Errorf("%s has %d print/log call(s), classified %d: classify each new site in classifiedPrintSites (what it prints, and that it holds no database login or password)", name, found[name], want)
		}
	}
	for name, want := range classifiedPrintSites {
		if found[name] == 0 && want != 0 {
			t.Errorf("%s is classified with %d site(s) but has none: remove the row", name, want)
		}
	}
}

// isLogLevelMethod is a slog level method, with or without a context.
func isLogLevelMethod(name string) bool {
	switch name {
	case "Info", "Warn", "Error", "Debug", "InfoContext", "WarnContext", "ErrorContext", "DebugContext", "Log", "LogAttrs":
		return true
	}
	return false
}

// isProcessLogger is a receiver that writes through a logger: the slog package,
// a variable named logger, or a call that returns one (slog.Default(),
// slog.Default().With(...), logger.With(...)).
func isProcessLogger(expr ast.Expr) bool {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name == "slog" || value.Name == "logger"
	case *ast.CallExpr:
		if selector, ok := value.Fun.(*ast.SelectorExpr); ok {
			if owner, ok := selector.X.(*ast.Ident); ok && owner.Name == "slog" && selector.Sel.Name == "Default" {
				return true
			}
			if selector.Sel.Name == "With" || selector.Sel.Name == "WithGroup" {
				return isProcessLogger(selector.X)
			}
		}
	}
	return false
}
