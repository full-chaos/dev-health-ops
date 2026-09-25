package routing

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Every place this package turns an error into text is found in the code and must be
// either redacted by the run's credential boundary or classified as not carrying a
// database error. A database error text can carry the login and password (pgx puts the
// login in its failure text and a server can echo the password in the error of any
// statement), so an error sink added later fails this test until it is redacted or
// classified (CHAOS-6665).
//
// The sinks are: a call of .Error() (the text of an error), a fmt.Errorf (interpolates
// an error into a new one), a fmt print of a value named like an error, and any call on
// a logger (log, slog, or a level method). The rest
// of the package builds its errors with refuse and internal, which redact.

type sinkTreatment string

const (
	// treatRedacted: the text goes through redactCredentials in the same expression.
	treatRedacted sinkTreatment = "redacted"
	// treatBuilder: the function is one of the redacting error builders.
	treatBuilder sinkTreatment = "builder"
	// treatNotDatabase: the error cannot carry a database error.
	treatNotDatabase sinkTreatment = "not a database error"
)

// sinkTable is keyed "<file>:<function>:<kind>[ [redacted]]" (a sink inside a
// redactCredentials call is marked [redacted]) with how many times the sink appears.
var sinkTable = map[string]struct {
	treatment sinkTreatment
	count     int
	why       string
}{
	"main.go:printError:.Error() [redacted]":  {treatRedacted, 1, "the command's one error print"},
	"main.go:redacted:.Error() [redacted]":    {treatRedacted, 1, "the builders' redaction of an error's text"},
	"main.go:redacted:.Error()":               {treatBuilder, 1, "the comparison in redacted() that decides whether the text changed"},
	"main.go:refuse:fmt.Errorf":               {treatBuilder, 1, "refuse redacts what it builds"},
	"main.go:internal:fmt.Errorf":             {treatBuilder, 1, "internal redacts what it builds"},
	"status.go:runStatus:.Error() [redacted]": {treatRedacted, 3, "status's database errors (connect, census, classification)"},
	"status.go:runStatus:.Error()":            {treatNotDatabase, 3, "the registry URL sanitizer, the catalog loader and the registry HTTP fetch: no database"},
}

type sinkSite struct {
	file, function, kind string
	redacted             bool
}

func routingSinks(t *testing.T) []sinkSite {
	t.Helper()
	matches, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var sites []sinkSite
	for _, path := range matches {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			name := function.Name.Name
			var walk func(node ast.Node, redacted bool)
			walk = func(node ast.Node, redacted bool) {
				ast.Inspect(node, func(child ast.Node) bool {
					call, ok := child.(*ast.CallExpr)
					if !ok {
						return true
					}
					if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "redactCredentials" {
						for _, argument := range call.Args {
							walk(argument, true)
						}
						return false
					}
					if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
						switch {
						case selector.Sel.Name == "Error" && len(call.Args) == 0:
							sites = append(sites, sinkSite{path, name, ".Error()", redacted})
						case selector.Sel.Name == "Errorf":
							if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "fmt" {
								sites = append(sites, sinkSite{path, name, "fmt.Errorf", redacted})
							}
						case isLogCall(selector):
							sites = append(sites, sinkSite{path, name, "log " + selector.Sel.Name, redacted})
						case strings.HasPrefix(selector.Sel.Name, "Fprint") || strings.HasPrefix(selector.Sel.Name, "Sprint") || strings.HasPrefix(selector.Sel.Name, "Print"):
							for _, argument := range call.Args {
								if ident, ok := argument.(*ast.Ident); ok && (ident.Name == "err" || strings.HasSuffix(ident.Name, "Err")) {
									sites = append(sites, sinkSite{path, name, "print of " + ident.Name, redacted})
								}
							}
						}
					}
					return true
				})
			}
			walk(function.Body, false)
		}
	}
	return sites
}

func TestEveryErrorSinkIsRedactedOrClassified(t *testing.T) {
	sites := routingSinks(t)
	if len(sites) < 8 {
		t.Fatalf("only %d error sinks found: the scan measures nothing", len(sites))
	}
	seen := map[string]int{}
	for _, site := range sites {
		key := fmt.Sprintf("%s:%s:%s", site.file, site.function, site.kind)
		if site.redacted {
			key += " [redacted]"
		}
		seen[key]++
	}
	var keys []string
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		entry, listed := sinkTable[key]
		if !listed {
			t.Errorf("%s (%d sites) is an error sink that is neither redacted nor classified: wrap its text in redactCredentials, build the error with refuse or internal, or classify it in sinkTable with the reason it cannot carry a database error", key, seen[key])
			continue
		}
		if seen[key] != entry.count {
			t.Errorf("%s appears %d times, the table says %d (%s): reclassify the sites", key, seen[key], entry.count, entry.why)
		}
	}
	for key := range sinkTable {
		if seen[key] == 0 {
			t.Errorf("%s is classified but no longer appears: remove it from sinkTable", key)
		}
	}
}

// isLogCall reports a call on a logger: a log or slog package function, or a Warn,
// Info, Debug or Error-with-arguments method (slog.Logger's levels).
func isLogCall(selector *ast.SelectorExpr) bool {
	if pkg, ok := selector.X.(*ast.Ident); ok && (pkg.Name == "log" || pkg.Name == "slog") {
		return true
	}
	switch selector.Sel.Name {
	case "Warn", "Info", "Debug", "WarnContext", "InfoContext", "DebugContext", "ErrorContext":
		return true
	}
	return false
}
