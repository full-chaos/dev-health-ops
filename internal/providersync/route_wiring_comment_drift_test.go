package providersync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
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
// wiring file, plus the TRANSITIVE closure of struct fields of those types.
// Doc comments are read on TYPE, FUNC (including METHODS, attributed to the
// receiver type), VAR and CONST declarations -- the closed set of Go decl
// kinds, so this is a bounded decision rather than another reachability hop.
//
// MEASURED, not assumed: an earlier version of this sentence claimed "type and
// function doc comments" while discovery filtered `Recv == nil`, so methods
// were silently unread. A guard whose own doc overstates its coverage is the
// exact defect this guard exists to catch, committed by the guard.
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
// Still outside it, each verified by probe rather than assumed: DETACHED
// comments (a comment separated from its declaration by a blank line is not
// attached as Doc by go/ast, so it is unreadable here by construction);
// symbols reached only through a helper in another package, through an
// interface, or named only in a string; function RETURN types (an explicit
// ruling, not an oversight); and any paraphrase of a stale claim.

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

// supersededTag is how a comment cites text it is retracting. A stale phrase
// must stay visible next to its refutation, so the guard needs to tell a
// CITATION from an ASSERTION -- and that distinction is now made by an exact
// token, never by reading the prose around it.
//
// WHY A TAG AND NOT A HEURISTIC. Three consecutive codex rounds each found a
// defect in a hand-rolled prose discriminator, and each fix caused the next:
// stripping every quoted span let a live claim hide in scare quotes; a
// 240-character marker window let an unrelated retraction shelter a planted
// assertion; sentence-scoped markers then failed BOTH ways at once -- an
// unrelated "previously said" in the same sentence shielded a real falsehood,
// and a period inside `planner.go:401` or `v1.27` severed a legitimate
// retraction from its marker and rejected a correct comment. Deciding from
// English whether a sentence asserts or cites is natural-language
// understanding; a fourth heuristic tier would buy a fifth defect. So the
// surface is eliminated rather than tiered: a retraction declares itself.
//
// CONTRACT: every line of a citation begins with this tag, including each line
// of a multi-line quotation. Lines carrying it are excluded from the assertion
// text; everything else is an assertion. Mechanical, exact, no grammar.
const supersededTag = "SUPERSEDED:"

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
// declsFromFile is the single decl-reading rule, shared by the package walk and
// by the synthetic fixture, so the fixture exercises the REAL logic rather than
// a lookalike that could drift away from it.
func declsFromFile(fset *token.FileSet, entry string, decl ast.Decl) []packageTypeDecl {
	var out []packageTypeDecl
	if funcDecl, ok := decl.(*ast.FuncDecl); ok {
		if funcDecl.Doc == nil {
			return nil
		}
		name := funcDecl.Name.Name
		if funcDecl.Recv != nil {
			if recv := receiverTypeName(funcDecl.Recv); recv != "" {
				name = recv
			}
		}
		return []packageTypeDecl{{name: name, doc: funcDecl.Doc.Text(), file: entry,
			line: fset.Position(funcDecl.Pos()).Line}}
	}
	genDecl, ok := decl.(*ast.GenDecl)
	if !ok {
		return nil
	}
	switch genDecl.Tok {
	case token.TYPE:
		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			doc := genDecl.Doc
			if typeSpec.Doc != nil {
				doc = typeSpec.Doc
			}
			out = append(out, packageTypeDecl{name: typeSpec.Name.Name, spec: typeSpec,
				doc: doc.Text(), file: entry, line: fset.Position(typeSpec.Pos()).Line})
		}
	case token.VAR, token.CONST:
		if genDecl.Doc == nil {
			return nil
		}
		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok || len(valueSpec.Names) == 0 {
				continue
			}
			out = append(out, packageTypeDecl{name: valueSpec.Names[0].Name,
				doc: genDecl.Doc.Text(), file: entry, line: fset.Position(valueSpec.Pos()).Line})
		}
	}
	return out
}

// receiverTypeName unwraps `(m T)` / `(m *T)` to T.
func receiverTypeName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) == 0 {
		return ""
	}
	idents := fieldTypeIdents(recv.List[0].Type)
	if len(idents) == 0 {
		return ""
	}
	return idents[0]
}

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
			// ONE decl-reading rule, shared with the synthetic fixture, so the
			// fixture exercises this exact code rather than a lookalike that
			// could drift away from it.
			decls = append(decls, declsFromFile(fset, entry, decl)...)
		}
	}
	if len(decls) == 0 {
		t.Fatal("found ZERO type declarations in this package -- the discovery mechanism is broken, " +
			"and its 'clean' result would be meaningless")
	}
	return decls
}

// collapseWhitespace joins the doc into one space-separated run.
func collapseWhitespace(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

func assertedStaleClaims(doc string) []string {
	// Drop tagged citation lines, then collapse what remains. Whitespace is
	// collapsed so a claim split across two comment lines cannot evade a
	// literal (codex round 2 proved that with a passing mutation).
	var assertionLines []string
	for _, line := range strings.Split(doc, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), supersededTag) {
			continue
		}
		assertionLines = append(assertionLines, line)
	}
	assertions := collapseWhitespace(strings.Join(assertionLines, " "))
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

// TestDriftGuardCatchesAPlantedStaleClaim drives the real detector against
// planted violations. Without it, a matcher that silently stopped matching
// would report the package clean forever -- the failure mode this guard exists
// to prevent. A direct assertion that "the phrase list is non-empty" could not
// fail in the interesting direction.
func TestDriftGuardCatchesAPlantedStaleClaim(t *testing.T) {
	planted := "SomeHandler is intentionally unregistered. It owns only the compute boundary."
	if claims := assertedStaleClaims(planted); len(claims) != 1 || claims[0] != "intentionally unregistered" {
		t.Fatalf("detector missed a planted stale claim: got %v", claims)
	}

	tagged := "WIRING: WIRED. provider_sync.go constructs this handler.\n" +
		"SUPERSEDED: \"It is intentionally unregistered: wiring is a separate slice.\"\n" +
		"That is now false."
	if claims := assertedStaleClaims(tagged); len(claims) != 0 {
		t.Fatalf("a TAGGED citation fired, which would make corrections impossible: %v", claims)
	}

	multiline := "WIRING: WIRED, and this paragraph used to say the opposite:\n" +
		"SUPERSEDED: \"It REGISTERS AND ACTIVATES NOTHING. provider_sync.go\n" +
		"SUPERSEDED: gains no case for the work-item family here.\"\n" +
		"All of those claims are now false."
	if claims := assertedStaleClaims(multiline); len(claims) != 0 {
		t.Fatalf("a MULTI-LINE tagged citation fired: %v", claims)
	}

	t.Run("line wrap across // boundaries", func(t *testing.T) {
		// doc.Text() keeps the newline, so the literal was split in half.
		// Codex round 2 mutation.
		wrapped := "This handler is intentionally\nunregistered by the worker."
		if claims := assertedStaleClaims(wrapped); len(claims) != 1 {
			t.Fatalf("a claim split across two comment lines evaded the detector: %v", claims)
		}
	})

	t.Run("scare quotes are an assertion, not a citation", func(t *testing.T) {
		// Codex round 2 mutation. Quoting alone never exempts anything now:
		// only the tag does, so this is red by construction.
		scare := `This handler is "intentionally unregistered" by the worker.`
		if claims := assertedStaleClaims(scare); len(claims) != 1 {
			t.Fatalf("a quoted assertion escaped the detector: %v", claims)
		}
	})

	// The two round-3 defects, which killed the prose-marker heuristic. Both
	// are red on the sentence-scoped implementation and green on the tag.
	t.Run("R3 F1: unrelated marker must not shield an assertion", func(t *testing.T) {
		// Under sentence-scoped markers this returned [] -- an unrelated
		// "previously said" in the same sentence exempted a live falsehood.
		f1 := `Something was previously said about scope, and this handler is ` +
			`"intentionally unregistered" by the worker.`
		if claims := assertedStaleClaims(f1); len(claims) != 1 {
			t.Fatalf("an unrelated retraction marker shielded a real assertion: %v", claims)
		}
	})

	t.Run("R3 F2: a file:line or version period must not reject a citation", func(t *testing.T) {
		// Under sentence-scoped markers a period inside planner.go:401 or
		// v1.27 severed the marker from its quote and REJECTED a correct
		// comment. With the tag there is no sentence to split.
		f2 := "The superseded text, per planner.go:401, no longer holds.\n" +
			"SUPERSEDED: \"intentionally unregistered\""
		if claims := assertedStaleClaims(f2); len(claims) != 0 {
			t.Fatalf("a legitimate tagged citation was rejected because of a file:line period: %v", claims)
		}
		f2b := "This comment previously read, before v1.27, something else.\n" +
			"SUPERSEDED: \"intentionally unregistered\""
		if claims := assertedStaleClaims(f2b); len(claims) != 0 {
			t.Fatalf("a legitimate tagged citation was rejected because of a version period: %v", claims)
		}
	})
}

// TestDriftGuardReadsEveryDeclKindItClaims drives the REAL decl walk over a
// synthetic package and asserts each newly covered kind is actually read. Each
// case is red on the previous implementation, which filtered `Recv == nil` and
// accepted only `token.TYPE`: methods, vars and consts were silently unread,
// so a stale claim on any of them sailed through. Detached comments are
// asserted UNREADABLE on purpose -- that is a documented bound, and pinning it
// stops a later reader mistaking the gap for an oversight.
func TestDriftGuardReadsEveryDeclKindItClaims(t *testing.T) {
	src := `package p

// WiredThing is fine.
type WiredThing struct{}

// Fetch is intentionally unregistered by the worker.
func (w WiredThing) Fetch() int { return 0 }

// PlainFunc is intentionally unregistered.
func PlainFunc() {}

// SomeVar is intentionally unregistered.
var SomeVar = 1

// SomeConst is intentionally unregistered.
const SomeConst = 2

// Detached is intentionally unregistered.

type Detached struct{}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse synthetic package: %v", err)
	}
	found := map[string][]string{}
	for _, decl := range file.Decls {
		for _, entry := range declsFromFile(fset, "synthetic.go", decl) {
			if claims := assertedStaleClaims(entry.doc); len(claims) > 0 {
				found[entry.name] = claims
			}
		}
	}

	// A method must be attributed to its RECEIVER: that is the symbol the
	// wiring file names, so that is the name the guard must match against.
	for _, want := range []string{"WiredThing", "PlainFunc", "SomeVar", "SomeConst"} {
		if len(found[want]) == 0 {
			t.Errorf("decl kind for %s is NOT read -- a stale claim there would never be checked", want)
		}
	}
	if len(found["Detached"]) != 0 {
		t.Errorf("Detached became readable; the documented bound is now wrong and the doc must change")
	}
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
