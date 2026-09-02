package providersync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// This guard exists because the "unregistered" claim in a doc comment is not
// checked by anything the compiler runs. Three comments in this package
// asserted their deriver or route was unregistered/inactive while
// cmd/dev-health-worker/provider_sync.go constructed them, and four more were
// found only by a hand sweep (CHAOS-4848). A hand-maintained set of comments
// that must agree with a wiring file is a countdown to the next miss, so the
// agreement is asserted here instead.
//
// WHAT THIS KEYS ON, stated so the next reader knows what it cannot see:
// symbol NAMES referenced through the `providersync` package selector in the
// wiring file, plus ONE hop through the struct fields of those types. It does
// not chase arbitrary depth, interface satisfaction, or symbols reached only
// through a helper in another package. A symbol wired two hops deep can still
// carry a stale comment and this guard will not say so.

// staleRegistrationClaims are the phrases that assert a symbol is not wired.
// Keep them literal: a regex broad enough to catch paraphrases also catches
// every legitimate "not registered" runtime error string in the package.
var staleRegistrationClaims = []string{
	"intentionally unregistered",
	"intentionally not registered",
	"not registered or activated",
	"intentionally not registered or activated",
	"REGISTERS AND ACTIVATES NOTHING",
	"only thing that can reach this constructor today is a test",
}

// quotedSpan matches a double-quoted run, including across newlines once the
// comment has been joined. A stale phrase INSIDE a quotation is a citation of
// superseded text, not an assertion -- every corrected comment in this package
// keeps the old sentence quoted so the wrong model stays visible next to its
// refutation. Stripping quotations before matching is what lets a correction
// quote the very phrase it retracts without tripping this guard.
var quotedSpan = regexp.MustCompile(`(?s)"[^"]*"`)

func wiringFilePath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "cmd", "dev-health-worker", "provider_sync.go"))
	if err != nil {
		t.Fatalf("resolve wiring file: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("wiring file %s is not readable -- this guard is anchored to it: %v", path, err)
	}
	return path
}

// symbolsConstructedByTheWorker returns every providersync symbol the worker's
// wiring file names, plus the struct-field types of those symbols (one hop).
func symbolsConstructedByTheWorker(t *testing.T) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, wiringFilePath(t), nil, 0)
	if err != nil {
		t.Fatalf("parse wiring file: %v", err)
	}

	direct := map[string]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || pkg.Name != "providersync" {
			return true
		}
		name := selector.Sel.Name
		direct[name] = true
		// A constructor names the type its comment lives on: the stale claim
		// sits on GitLabWorkItemDeriver, while the wiring file only ever says
		// NewGitLabWorkItemDeriver.
		if strings.HasPrefix(name, "New") && len(name) > 3 {
			direct[name[3:]] = true
		}
		return true
	})
	if len(direct) == 0 {
		t.Fatal("parsed the wiring file and found ZERO providersync symbols -- the parser is broken, " +
			"not the tree; a green result here would be a false clean")
	}

	// One hop: a handler wired as a FIELD of a constructed handler is just as
	// live as one assigned directly. Both subtle sites found in CHAOS-4848
	// (LinearWorkItemsRouteHandler as LinearWorkItemFamilyRouteHandler.Direct,
	// GitHubWorkItemsRESTCollector as GitHubWorkItemsRouteHandler.REST) are
	// only reachable through this hop -- a guard without it would miss exactly
	// the cases that motivated it.
	reachable := map[string]bool{}
	for name := range direct {
		reachable[name] = true
	}
	for _, decl := range packageTypeDecls(t) {
		if !direct[decl.name] || decl.spec == nil {
			continue
		}
		structType, ok := decl.spec.Type.(*ast.StructType)
		if !ok || structType.Fields == nil {
			continue
		}
		for _, field := range structType.Fields.List {
			for _, ident := range fieldTypeIdents(field.Type) {
				reachable[ident] = true
			}
		}
	}
	return reachable
}

func fieldTypeIdents(expr ast.Expr) []string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return []string{typed.Name}
	case *ast.StarExpr:
		return fieldTypeIdents(typed.X)
	case *ast.ArrayType:
		return fieldTypeIdents(typed.Elt)
	default:
		return nil
	}
}

type packageTypeDecl struct {
	name string
	spec *ast.TypeSpec
	doc  string
	file string
	line int
}

// packageTypeDecls returns every non-test type declaration in this package with
// its doc comment. Types are what these comments hang on; a func-level variant
// would need the same treatment if the class ever appears there.
func packageTypeDecls(t *testing.T) []packageTypeDecl {
	t.Helper()
	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob package files: %v", err)
	}
	var decls []packageTypeDecl
	fset := token.NewFileSet()
	for _, entry := range entries {
		if strings.HasSuffix(entry, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, entry, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", entry, err)
		}
		for _, decl := range file.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.TYPE {
				continue
			}
			for _, spec := range genDecl.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				doc := genDecl.Doc
				if typeSpec.Doc != nil {
					doc = typeSpec.Doc
				}
				decls = append(decls, packageTypeDecl{
					name: typeSpec.Name.Name,
					spec: typeSpec,
					doc:  doc.Text(),
					file: entry,
					line: fset.Position(typeSpec.Pos()).Line,
				})
			}
		}
	}
	if len(decls) == 0 {
		t.Fatal("found ZERO type declarations in this package -- the discovery mechanism is broken, " +
			"and its 'clean' result would be meaningless")
	}
	return decls
}

func assertedStaleClaims(doc string) []string {
	assertions := quotedSpan.ReplaceAllString(doc, " ")
	var found []string
	for _, claim := range staleRegistrationClaims {
		if strings.Contains(assertions, claim) {
			found = append(found, claim)
		}
	}
	sort.Strings(found)
	return found
}

// TestNoLiveSymbolIsDocumentedAsUnregistered is the drift guard. It fails when
// a symbol the worker wires carries a comment asserting it is not wired.
func TestNoLiveSymbolIsDocumentedAsUnregistered(t *testing.T) {
	wired := symbolsConstructedByTheWorker(t)
	var violations []string
	for _, decl := range packageTypeDecls(t) {
		if !wired[decl.name] || decl.doc == "" {
			continue
		}
		if claims := assertedStaleClaims(decl.doc); len(claims) > 0 {
			violations = append(violations, decl.file+":"+strconv.Itoa(decl.line)+" "+decl.name+
				" is wired by cmd/dev-health-worker/provider_sync.go but its comment asserts "+
				strings.Join(claims, " / "))
		}
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("comments deny wiring that provider_sync.go performs:\n  %s\n\n"+
			"Fix the COMMENT, not this test: state the live wiring and cite the case predicate "+
			"and constructor in provider_sync.go. If the symbol genuinely is not wired, the wiring "+
			"file should stop naming it.", strings.Join(violations, "\n  "))
	}
}

// TestDriftGuardCatchesAPlantedStaleClaim drives the real detector against a
// planted violation. Without it, a matcher that silently stopped matching --
// a renamed phrase, a broken quote-stripper -- would report the package clean
// forever, which is the failure mode this whole guard exists to prevent. A
// direct assertion that "the phrase list is non-empty" could not fail in the
// interesting direction.
func TestDriftGuardCatchesAPlantedStaleClaim(t *testing.T) {
	planted := "SomeHandler is intentionally unregistered. It owns only the compute boundary."
	if claims := assertedStaleClaims(planted); len(claims) != 1 || claims[0] != "intentionally unregistered" {
		t.Fatalf("detector missed a planted stale claim: got %v", claims)
	}

	// The corrected form: the SAME phrase, but quoted as superseded text. This
	// must NOT fire, or every correction in this package would be unfixable.
	corrected := `WIRING: LIVE. provider_sync.go constructs this handler. This comment ` +
		`previously read "It is intentionally unregistered: the provider route wiring ` +
		`remains a separate slice." That is now false.`
	if claims := assertedStaleClaims(corrected); len(claims) != 0 {
		t.Fatalf("detector fired on a QUOTED retraction, which would make corrections impossible: %v", claims)
	}

	// A multi-line quotation, the shape github_work_items_composition.go uses.
	multiline := "WIRING: LIVE, and this paragraph used to say the opposite. It read:\n\n" +
		"\t\"It REGISTERS AND ACTIVATES NOTHING. provider_sync.go\n\tgains no case for the work-item family here.\"\n\n" +
		"All of those claims are now false."
	if claims := assertedStaleClaims(multiline); len(claims) != 0 {
		t.Fatalf("detector fired on a MULTI-LINE quoted retraction: %v", claims)
	}
}

// TestDriftGuardSeesTheSymbolsItMustCover validates the discovery mechanism
// against a known superset before its "clean" result is worth anything. Each
// name below is wired today; if the parser stops seeing one, the guard would
// silently stop protecting it (the CHAOS-4834 false-green shape).
func TestDriftGuardSeesTheSymbolsItMustCover(t *testing.T) {
	wired := symbolsConstructedByTheWorker(t)
	mustSee := map[string]string{
		"GitHubWorkItemsRouteHandler":  "assigned to routeHandler directly",
		"GitLabWorkItemsRouteHandler":  "assigned to routeHandler directly",
		"JiraAtlassianRouteHandler":    "assigned to routeHandler directly",
		"GitLabWorkItemDeriver":        "reached via the NewX constructor name",
		"JiraWorkItemDeriver":          "reached via the NewX constructor name",
		"LinearWorkItemsRouteHandler":  "reached via the one-hop field LinearWorkItemFamilyRouteHandler.Direct",
		"GitHubWorkItemsRESTCollector": "reached via the one-hop field GitHubWorkItemsRouteHandler.REST",
	}
	for name, how := range mustSee {
		if !wired[name] {
			t.Errorf("discovery missed %s (%s) -- the guard's clean result is not evidence of coverage", name, how)
		}
	}

	// The negative control: a symbol that is NOT wired must not be claimed as
	// wired, or the guard would demand comment changes that are actually wrong.
	// JiraWorkItemsRouteHandler is genuinely unconstructed -- jira's live path
	// uses JiraAtlassianRouteHandler -- and its "intentionally unregistered"
	// comment is TRUE and must stay (CHAOS-4848).
	if wired["JiraWorkItemsRouteHandler"] {
		t.Error("JiraWorkItemsRouteHandler is reported as wired, but jira's live route is " +
			"JiraAtlassianRouteHandler; treating it as wired would force a correct comment to be falsified")
	}
}
