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
// WHAT THIS KEYS ON, and the bound on what it can ever prove. Discovery is
// symbol NAMES referenced through the `providersync` package selector in the
// wiring file, plus the TRANSITIVE closure of struct fields of those types,
// over both type and function doc comments.
//
// Detection, however, is a fixed list of literal phrasings, and that list is
// NOT and CANNOT BE complete: deciding whether arbitrary English asserts
// non-registration is natural-language understanding, and any paraphrase
// ("the worker does not register this handler") walks past a literal matcher.
// Codex round 2 proved exactly that with a passing mutation. Enumerating one
// more phrase is the same losing shape this guard exists to retire, so the
// claim is bounded instead of inflated: this is a TRIPWIRE for the known
// phrasings that have actually appeared in this package, not a proof that no
// comment misstates wiring. An honest gap beats a coverage claim that cannot
// be defended -- a reader who believes this guard is complete stops checking.
//
// Still outside it: symbols reached only through a helper in another package,
// through an interface, or named only in a string; and any paraphrase.

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

	// TRANSITIVE field closure, to fixpoint. A handler wired as a FIELD of a
	// constructed handler is just as live as one assigned directly, and that is
	// true at any depth: the two subtle sites found in CHAOS-4848
	// (LinearWorkItemsRouteHandler as LinearWorkItemFamilyRouteHandler.Direct,
	// GitHubWorkItemsRESTCollector as GitHubWorkItemsRouteHandler.REST) sit one
	// hop down, and codex round 2 demonstrated a stale comment surviving at two
	// by embedding a type under the REST collector. A fixed hop count is the
	// same "enumerate one more case" shape this whole guard exists to retire,
	// so the traversal runs to fixpoint instead of stopping at a chosen depth.
	byName := map[string]*ast.TypeSpec{}
	for _, decl := range packageTypeDecls(t) {
		if decl.spec != nil {
			byName[decl.name] = decl.spec
		}
	}
	reachable := map[string]bool{}
	queue := make([]string, 0, len(direct))
	for name := range direct {
		reachable[name] = true
		queue = append(queue, name)
	}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		spec, ok := byName[name]
		if !ok {
			continue
		}
		structType, ok := spec.Type.(*ast.StructType)
		if !ok || structType.Fields == nil {
			continue
		}
		for _, field := range structType.Fields.List {
			for _, ident := range fieldTypeIdents(field.Type) {
				if !reachable[ident] {
					reachable[ident] = true
					queue = append(queue, ident)
				}
			}
		}
	}
	return reachable
}

// fieldTypeIdents unwraps a field's type to the package-local identifiers it
// can reach. Embedded fields are an *ast.Ident like any other, so they are
// covered; map keys and values are both followed because either can name a
// package type.
func fieldTypeIdents(expr ast.Expr) []string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return []string{typed.Name}
	case *ast.StarExpr:
		return fieldTypeIdents(typed.X)
	case *ast.ArrayType:
		return fieldTypeIdents(typed.Elt)
	case *ast.MapType:
		return append(fieldTypeIdents(typed.Key), fieldTypeIdents(typed.Value)...)
	case *ast.ChanType:
		return fieldTypeIdents(typed.Value)
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
			// Constructor docs count: codex round 2 planted
			// "This constructor is intentionally unregistered." on
			// NewGitLabWorkItemDeriver -- a func the worker calls directly --
			// and the type-only walk accepted it.
			if funcDecl, ok := decl.(*ast.FuncDecl); ok {
				if funcDecl.Doc != nil && funcDecl.Recv == nil {
					decls = append(decls, packageTypeDecl{
						name: funcDecl.Name.Name,
						doc:  funcDecl.Doc.Text(),
						file: entry,
						line: fset.Position(funcDecl.Pos()).Line,
					})
				}
				continue
			}
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

// retractionMarker introduces a quotation of superseded text. Only a quote a
// marker introduces is stripped -- codex round 2 showed that stripping EVERY
// quoted span lets a live assertion hide inside scare quotes
// (`is "intentionally unregistered" by the worker`), turning the
// false-positive fix into a false negative, which is the worse direction for a
// guard.
var retractionMarker = regexp.MustCompile(
	`(?i)(previously read|previously said|used to say|superseded text read|it read:|the dropped clause was|this comment previously)`,
)

// collapseWhitespace joins the doc into one space-separated run. Without it a
// claim split across two `//` lines ("intentionally\nunregistered") evades
// every literal, which codex round 2 demonstrated with a passing mutation.
func collapseWhitespace(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

func assertedStaleClaims(doc string) []string {
	assertions := collapseWhitespace(doc)
	// Strip only quotations that a retraction marker introduces, walking left
	// from each quote to the nearest preceding marker within the same sentence
	// region.
	for {
		location := quotedSpan.FindStringIndex(assertions)
		if location == nil {
			break
		}
		prefix := assertions[:location[0]]
		// Scope the marker search to the SENTENCE containing the quote, not a
		// fixed character window. A window is proximity, and proximity is not
		// grammar: a legitimate retraction elsewhere in the same doc block
		// sheltered a freshly planted scare-quoted assertion a few hundred
		// characters later, which is a false negative in the direction that
		// matters. Verified by mutation before and after this change.
		window := prefix
		if cut := strings.LastIndexAny(window, ".;"); cut >= 0 {
			window = window[cut+1:]
		}
		if retractionMarker.MatchString(window) {
			assertions = assertions[:location[0]] + " " + assertions[location[1]:]
			continue
		}
		// Not a retraction: keep the quoted text as an assertion, but neutralise
		// the delimiters so the scan advances instead of looping forever.
		assertions = assertions[:location[0]] + " " +
			assertions[location[0]+1:location[1]-1] + " " + assertions[location[1]:]
	}
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

	// Codex round 2 mutations, each of which the detector ACCEPTED before this
	// commit. Pinned by name so a future simplification cannot quietly reopen
	// them.
	t.Run("line wrap across // boundaries", func(t *testing.T) {
		// doc.Text() keeps the newline, so the literal was split in half.
		wrapped := "This handler is intentionally\nunregistered by the worker."
		if claims := assertedStaleClaims(wrapped); len(claims) != 1 {
			t.Fatalf("a claim split across two comment lines evaded the detector: %v", claims)
		}
	})

	t.Run("scare quotes are an assertion, not a retraction", func(t *testing.T) {
		// No retraction marker, so the quotes are the author's emphasis and the
		// sentence still asserts the falsehood. Stripping EVERY quoted span --
		// the previous behaviour -- turned a false positive into a false
		// negative, the worse direction for a guard.
		scare := `This handler is "intentionally unregistered" by the worker.`
		if claims := assertedStaleClaims(scare); len(claims) != 1 {
			t.Fatalf("a quoted assertion hid behind the retraction stripper: %v", claims)
		}
	})

	t.Run("a marked retraction still does not fire", func(t *testing.T) {
		// The discriminator is the marker, not the quotes.
		marked := `WIRING: LIVE. The superseded text read "intentionally unregistered".`
		if claims := assertedStaleClaims(marked); len(claims) != 0 {
			t.Fatalf("a marked retraction fired, which would make corrections impossible: %v", claims)
		}
	})
}

// TestDriftGuardCoversConstructorDocsAndDeepFields pins the two discovery holes
// codex round 2 demonstrated with passing mutations: a stale claim on a
// CONSTRUCTOR the worker calls directly, and one on a type embedded two hops
// below a wired handler. Both were accepted by the type-only, one-hop version.
func TestDriftGuardCoversConstructorDocsAndDeepFields(t *testing.T) {
	wired := symbolsConstructedByTheWorker(t)

	// The worker calls NewGitLabWorkItemDeriver directly, so a doc comment on
	// the FUNC is as load-bearing as one on the type.
	if !wired["NewGitLabWorkItemDeriver"] {
		t.Error("discovery does not see the constructor NewGitLabWorkItemDeriver by name")
	}
	var sawConstructorDoc bool
	for _, decl := range packageTypeDecls(t) {
		if decl.name == "NewGitLabWorkItemDeriver" && decl.spec == nil {
			sawConstructorDoc = true
		}
	}
	if !sawConstructorDoc {
		t.Error("the decl walk yields no func entry for NewGitLabWorkItemDeriver -- " +
			"a stale claim on a constructor would never be checked")
	}

	// Depth: GitHubWorkItemsRouteHandler.Social is a sibling of the REST field
	// and must be reachable, proving the closure does not stop at the first
	// hop it happens to need.
	if !wired["GitHubWorkItemPRSocialFetcher"] {
		t.Error("transitive closure missed GitHubWorkItemPRSocialFetcher, a field of a wired handler")
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
