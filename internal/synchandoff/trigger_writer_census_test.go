package synchandoff

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// mintCallers is every production file that writes a trigger through Mint,
// with its number of Mint calls. Mint is the one writer of
// sync_manual_triggers and holds the scope guard (checkSelectedSources,
// CHAOS-10064), so a new trigger writer is a design change: it is added here on
// purpose, mapping UnselectedSourcesError to its own refusal, or not at all.
var mintCallers = map[string]struct {
	calls   int
	refusal string
}{
	"internal/backfillrun/handoff.go":             {1, "dho backfill run: refused with exit 3"},
	"internal/api/integrationsadmin/handoff.go":   {1, "integration Sync now and Backfill: 400"},
	"internal/api/syncadmin/manual_trigger.go":    {1, "Sync admin Sync now and Backfill wizard: 400"},
	"internal/scheduler/sync/webhook_requests.go": {1, "webhook sync requests: the request is refused"},
	"internal/scheduler/sync/initial_backfill.go": {1, "the initial backfill: names no sources"},
}

// triggerWriter is the one function that may write sync_manual_triggers.
const triggerWriter = "internal/synchandoff/handoff.go:Mint"

const (
	modulePath   = "github.com/full-chaos/dev-health-ops/"
	handoffDir   = "internal/synchandoff"
	printfVerb   = `%[-+# 0-9.*]*[a-zA-Z%]`
	callerLabelP = `^[a-z0-9_.:]+$`
)

var (
	triggerTableWrite = regexp.MustCompile(`(?is)\binsert\s+into\s+(public\.)?sync_manual_triggers\b|\bupdate\s+(public\.)?sync_manual_triggers\s+set\b`)
	printfVerbs       = regexp.MustCompile(printfVerb)
	callerLabel       = regexp.MustCompile(callerLabelP)
)

type censusFile struct {
	path, dir string
	syntax    *ast.File
	imports   map[string]string // local name -> module-relative dir, "fmt" -> "fmt"
}

// census is the parsed production tree: every file, and every package-level
// string const by directory.
type census struct {
	fset   *token.FileSet
	files  []*censusFile
	consts map[string]map[string]ast.Expr
	// resolving guards a const cycle.
	resolving map[string]bool
}

// triggerCensus reads the production Go files under internal/ and cmd/ of fsys
// and returns every way they write sync_manual_triggers outside Mint or reach
// Mint outside mintCallers, and the number of files it read.
//
// It reads Go syntax, not text. A string expression is folded to its value
// (literals, "+" chains, package-level and function-level string consts, and
// fmt.Sprintf over folded arguments); the strings of one function are also
// read in source order as one text, so a statement split across variables is
// seen. A Mint call is found through any import name, and Mint used as a value
// fails.
//
// Known limit: the census sees Go source only. SQL built at run time from
// values it cannot fold (a non-const variable passed between functions, data
// read from a file or the database, a format with a non-literal argument) is
// not seen. Mint's own refusal of an unnamed Caller (ErrUnnamedCaller) holds
// at run time whatever the census misses.
func triggerCensus(fsys fs.FS) (failures []string, files int) {
	c := &census{fset: token.NewFileSet(), consts: map[string]map[string]ast.Expr{}, resolving: map[string]bool{}}
	for _, dir := range []string{"internal", "cmd"} {
		err := fs.WalkDir(fsys, dir, func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				return nil
			}
			source, err := fs.ReadFile(fsys, name)
			if err != nil {
				return err
			}
			syntax, err := parser.ParseFile(c.fset, name, source, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			files++
			c.add(name, syntax)
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			failures = append(failures, err.Error())
		}
	}

	var writers []string
	calls := map[string]int{}
	for _, file := range c.files {
		writers = append(writers, c.writers(file)...)
		count, problems := c.mintCalls(file)
		failures = append(failures, problems...)
		if count > 0 {
			calls[file.path] = count
		}
	}
	sort.Strings(writers)
	if want := []string{triggerWriter}; !reflect.DeepEqual(writers, want) {
		failures = append(failures, fmt.Sprintf("sync_manual_triggers is written in %v; only %s may write it, behind its scope guard", writers, triggerWriter))
	}
	for file, count := range calls {
		if want, ok := mintCallers[file]; !ok || want.calls != count {
			failures = append(failures, fmt.Sprintf("%s calls Mint %d times, the census names %d: a new trigger writer is added to mintCallers with its refusal, on purpose", file, count, want.calls))
		}
	}
	for file, want := range mintCallers {
		if calls[file] == 0 {
			failures = append(failures, fmt.Sprintf("%s is in mintCallers (%d calls) but does not call Mint: remove the stale entry", file, want.calls))
		}
	}
	sort.Strings(failures)
	return failures, files
}

func (c *census) add(name string, syntax *ast.File) {
	file := &censusFile{path: name, dir: path.Dir(name), syntax: syntax, imports: map[string]string{}}
	for _, spec := range syntax.Imports {
		importPath, _ := strconv.Unquote(spec.Path.Value)
		local := path.Base(importPath)
		if spec.Name != nil {
			local = spec.Name.Name
		}
		switch {
		case importPath == "fmt":
			file.imports[local] = "fmt"
		case strings.HasPrefix(importPath, modulePath):
			file.imports[local] = strings.TrimPrefix(importPath, modulePath)
		}
	}
	consts := c.consts[file.dir]
	if consts == nil {
		consts = map[string]ast.Expr{}
		c.consts[file.dir] = consts
	}
	for _, decl := range syntax.Decls {
		if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.CONST {
			addConsts(consts, gen)
		}
	}
	c.files = append(c.files, file)
}

func addConsts(consts map[string]ast.Expr, gen *ast.GenDecl) {
	for _, spec := range gen.Specs {
		value := spec.(*ast.ValueSpec)
		for i, name := range value.Names {
			if i < len(value.Values) {
				consts[name.Name] = value.Values[i]
			}
		}
	}
}

// scope is where a name is looked up: the file, and the consts of the
// function being read.
type scope struct {
	file   *censusFile
	locals map[string]ast.Expr
}

// fold returns the string value of expr when the census can compute it.
func (c *census) fold(s scope, expr ast.Expr) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", false
		}
		value, err := strconv.Unquote(e.Value)
		return value, err == nil
	case *ast.ParenExpr:
		return c.fold(s, e.X)
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", false
		}
		left, ok := c.fold(s, e.X)
		if !ok {
			return "", false
		}
		right, ok := c.fold(s, e.Y)
		return left + right, ok
	case *ast.Ident:
		if value, ok := s.locals[e.Name]; ok {
			return c.fold(scope{file: s.file}, value)
		}
		return c.constant(s.file, s.file.dir, e.Name)
	case *ast.SelectorExpr:
		if pkg, ok := e.X.(*ast.Ident); ok {
			if dir, ok := s.file.imports[pkg.Name]; ok && dir != "fmt" {
				return c.constant(s.file, dir, e.Sel.Name)
			}
		}
	case *ast.CallExpr:
		if format, ok := c.sprintfFormat(s, e); ok {
			args := e.Args[1:]
			return printfVerbs.ReplaceAllStringFunc(format, func(string) string {
				if len(args) == 0 {
					return ""
				}
				value, _ := c.fold(s, args[0])
				args = args[1:]
				return value
			}), true
		}
	}
	return "", false
}

func (c *census) sprintfFormat(s scope, call *ast.CallExpr) (string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Sprintf" || len(call.Args) == 0 {
		return "", false
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok || s.file.imports[pkg.Name] != "fmt" {
		return "", false
	}
	return c.fold(s, call.Args[0])
}

func (c *census) constant(from *censusFile, dir, name string) (string, bool) {
	value, ok := c.consts[dir][name]
	key := dir + "." + name
	if !ok || c.resolving[key] {
		return "", false
	}
	c.resolving[key] = true
	defer delete(c.resolving, key)
	owner := from
	if dir != from.dir {
		owner = &censusFile{dir: dir, imports: map[string]string{}}
		for _, file := range c.files {
			if file.dir == dir {
				owner = file
				break
			}
		}
	}
	return c.fold(scope{file: owner}, value)
}

// writers returns the units of file that write sync_manual_triggers: a
// function as "path:Name" (a method as "path:Recv.Name"), and package-level
// declarations as "path:<package scope>".
func (c *census) writers(file *censusFile) []string {
	var units []string
	for _, decl := range file.syntax.Decls {
		name := "<package scope>"
		if fn, ok := decl.(*ast.FuncDecl); ok {
			name = fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) > 0 {
				name = receiverName(fn.Recv.List[0].Type) + "." + name
			}
		}
		if c.writes(file, decl) {
			units = append(units, file.path+":"+name)
		}
	}
	return units
}

func receiverName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return receiverName(e.X)
	case *ast.IndexExpr:
		return receiverName(e.X)
	case *ast.IndexListExpr:
		return receiverName(e.X)
	case *ast.Ident:
		return e.Name
	}
	return "?"
}

// writes reports whether one declaration holds a write of the table: in one
// folded string expression, or in its strings read in source order as one
// text.
func (c *census) writes(file *censusFile, decl ast.Decl) bool {
	s := scope{file: file, locals: map[string]ast.Expr{}}
	ast.Inspect(decl, func(node ast.Node) bool {
		if gen, ok := node.(*ast.GenDecl); ok && gen.Tok == token.CONST {
			if _, isFunc := decl.(*ast.FuncDecl); isFunc {
				addConsts(s.locals, gen)
			}
		}
		return true
	})
	var text strings.Builder
	found := false
	ast.Inspect(decl, func(node ast.Node) bool {
		if found {
			return false
		}
		expr, ok := node.(ast.Expr)
		if !ok {
			return true
		}
		if value, ok := c.fold(s, expr); ok && triggerTableWrite.MatchString(value) {
			found = true
			return false
		}
		switch e := expr.(type) {
		case *ast.BasicLit:
			if value, ok := c.fold(s, e); ok {
				text.WriteString(value)
			}
		case *ast.Ident:
			if value, ok := c.fold(s, e); ok {
				text.WriteString(value)
			}
		case *ast.SelectorExpr:
			if value, ok := c.fold(s, e); ok {
				text.WriteString(value)
				return false
			}
		}
		return true
	})
	return found || triggerTableWrite.MatchString(text.String())
}

// mintCalls counts the Mint calls of file and returns a problem for Mint used
// as a value and for a call whose MintInput does not name its Caller.
func (c *census) mintCalls(file *censusFile) (int, []string) {
	var qualifiers []string
	bare := file.dir == handoffDir
	for local, dir := range file.imports {
		if dir != handoffDir || local == "_" {
			continue
		}
		if local == "." {
			bare = true
			continue
		}
		qualifiers = append(qualifiers, local)
	}
	if len(qualifiers) == 0 && !bare {
		return 0, nil
	}
	isMint := func(node ast.Node) bool {
		switch e := node.(type) {
		case *ast.SelectorExpr:
			pkg, ok := e.X.(*ast.Ident)
			if !ok || e.Sel.Name != "Mint" {
				return false
			}
			for _, qualifier := range qualifiers {
				if pkg.Name == qualifier {
					return true
				}
			}
		case *ast.Ident:
			return bare && e.Name == "Mint"
		}
		return false
	}
	var problems []string
	count := 0
	for _, decl := range file.syntax.Decls {
		fn, _ := decl.(*ast.FuncDecl)
		var stack []ast.Node
		ast.Inspect(decl, func(node ast.Node) bool {
			if node == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			parent := ast.Node(nil)
			if len(stack) > 0 {
				parent = stack[len(stack)-1]
			}
			if !isMint(node) || isDeclaredName(node, fn, parent) {
				stack = append(stack, node)
				return true
			}
			position := c.fset.Position(node.Pos())
			call, ok := parent.(*ast.CallExpr)
			if !ok || call.Fun != node {
				problems = append(problems, fmt.Sprintf("%s:%d uses Mint as a value; the census pins Mint calls", file.path, position.Line))
				return false
			}
			count++
			if problem := c.namedCaller(file, fn, call); problem != "" {
				problems = append(problems, fmt.Sprintf("%s:%d %s", file.path, position.Line, problem))
			}
			return false
		})
	}
	return count, problems
}

// isDeclaredName reports a Mint identifier that is not a reference: the
// function's own name, or the selected name of another selector.
func isDeclaredName(node ast.Node, fn *ast.FuncDecl, parent ast.Node) bool {
	ident, ok := node.(*ast.Ident)
	if !ok {
		return false
	}
	if fn != nil && ident == fn.Name {
		return true
	}
	selector, ok := parent.(*ast.SelectorExpr)
	return ok && selector.Sel == ident
}

// namedCaller checks the call's MintInput: a composite literal with a Caller
// whose leading string is a synclog label, written at the call or assigned to
// the identifier the call passes, in the same function.
func (c *census) namedCaller(file *censusFile, fn *ast.FuncDecl, call *ast.CallExpr) string {
	if len(call.Args) != 5 {
		return "calls Mint without its five arguments"
	}
	s := scope{file: file}
	literal := func(expr ast.Expr) string {
		composite, ok := expr.(*ast.CompositeLit)
		if !ok {
			return "passes a MintInput the census cannot read: build it as a literal naming Caller"
		}
		for _, element := range composite.Elts {
			pair, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if key, ok := pair.Key.(*ast.Ident); ok && key.Name == "Caller" {
				if lead := c.leadingString(s, pair.Value); callerLabel.MatchString(lead) {
					return ""
				}
				return "names a Caller that does not start with a synclog label"
			}
		}
		return "calls Mint without naming its Caller (the scope guard's log line names the path)"
	}
	ident, ok := call.Args[3].(*ast.Ident)
	if !ok {
		return literal(call.Args[3])
	}
	if fn == nil || fn.Body == nil {
		return "passes a MintInput the census cannot read: build it as a literal naming Caller"
	}
	var definitions []ast.Expr
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.AssignStmt:
			for i, lhs := range n.Lhs {
				if name, ok := lhs.(*ast.Ident); ok && name.Name == ident.Name && i < len(n.Rhs) && len(n.Lhs) == len(n.Rhs) {
					definitions = append(definitions, n.Rhs[i])
				}
			}
		case *ast.ValueSpec:
			for i, name := range n.Names {
				if name.Name == ident.Name && i < len(n.Values) {
					definitions = append(definitions, n.Values[i])
				}
			}
		}
		return true
	})
	if len(definitions) == 0 {
		return "passes a MintInput the census cannot read: build it as a literal naming Caller"
	}
	for _, definition := range definitions {
		if problem := literal(definition); problem != "" {
			return problem
		}
	}
	return ""
}

// leadingString is the folded value of expr, or of the leftmost operand of a
// "+" chain whose rest is computed at run time.
func (c *census) leadingString(s scope, expr ast.Expr) string {
	if value, ok := c.fold(s, expr); ok {
		return value
	}
	if binary, ok := expr.(*ast.BinaryExpr); ok && binary.Op == token.ADD {
		return c.leadingString(s, binary.X)
	}
	return ""
}

// TestEveryTriggerWriteGoesThroughTheScopeGuard fails when production code
// writes sync_manual_triggers anywhere but Mint, or reaches Mint from a call
// mintCallers does not pin: either would bypass or silently inherit the one
// scope guard of trigger writes.
func TestEveryTriggerWriteGoesThroughTheScopeGuard(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	failures, files := triggerCensus(os.DirFS(root))
	if files < 500 {
		t.Fatalf("read %d Go files; the census must see the production tree", files)
	}
	for _, failure := range failures {
		t.Error(failure)
	}
}

const censusModule = "github.com/full-chaos/dev-health-ops/internal/synchandoff"

// censusTree is a production tree the census accepts: Mint writes the table,
// and each mintCallers file calls Mint once, naming its Caller.
func censusTree() fstest.MapFS {
	tree := fstest.MapFS{
		"internal/synchandoff/handoff.go": {Data: []byte(`package synchandoff

func Mint(ctx context.Context, tx pgx.Tx, config *Config, input MintInput, now time.Time) (Trigger, error) {
	_, err := tx.Exec(ctx, ` + "`" + `
INSERT INTO public.sync_manual_triggers (occurrence_id, mode)
VALUES ($1, $2)` + "`" + `, 1, input.Mode)
	return Trigger{}, err
}
`)},
		"internal/api/syncadmin/manual_trigger.go": {Data: []byte(`package syncadmin

import "` + censusModule + `"

func handOff(spec spec) error {
	input := synchandoff.MintInput{Mode: spec.mode, Caller: "syncadmin." + spec.triggeredBy}
	_, err := synchandoff.Mint(ctx, tx, config, input, now)
	return err
}
`)},
	}
	for _, file := range []string{
		"internal/backfillrun/handoff.go",
		"internal/api/integrationsadmin/handoff.go",
		"internal/scheduler/sync/webhook_requests.go",
		"internal/scheduler/sync/initial_backfill.go",
	} {
		tree[file] = &fstest.MapFile{Data: []byte(`package caller

import "` + censusModule + `"

func trigger() error {
	_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture"}, now)
	return err
}
`)}
	}
	return tree
}

// TestTriggerCensusFailsEveryBypass plants, one at a time, each way to write
// sync_manual_triggers outside Mint or to reach Mint without the census seeing
// the call, in a tree the census otherwise accepts.
func TestTriggerCensusFailsEveryBypass(t *testing.T) {
	if failures, _ := triggerCensus(censusTree()); len(failures) > 0 {
		t.Fatalf("the census refuses the accepted tree: %v", failures)
	}
	const sneak = `package synchandoff

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, %s)
	return err
}
`
	for name, plant := range map[string]map[string]string{
		"a second writing function in handoff.go": {
			"internal/synchandoff/handoff.go": string(censusTree()["internal/synchandoff/handoff.go"].Data) +
				strings.TrimPrefix(strings.Replace(sneak, "%s", "`INSERT INTO public.sync_manual_triggers (occurrence_id) VALUES ($1)`, 1", 1), "package synchandoff\n"),
		},
		"a split literal in package synchandoff": {
			"internal/synchandoff/sneak.go": strings.Replace(sneak, "%s", `"INSERT INTO public.sync_manual" + "_triggers (occurrence_id) VALUES ($1)", 1`, 1),
		},
		"a package-level const table name": {
			"internal/scheduler/sync/sneak.go": `package sync

const triggerTable = "sync_manual_triggers"

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "INSERT INTO public." + triggerTable + " (occurrence_id) VALUES ($1)", 1)
	return err
}
`,
		},
		"a statement split across variables": {
			"internal/scheduler/sync/sneak.go": `package sync

func sneak(ctx context.Context, tx pgx.Tx) error {
	verb := "UPDATE public.sync_manual"
	statement := verb + "_triggers SET mode = $1"
	_, err := tx.Exec(ctx, statement, "x")
	return err
}
`,
		},
		"a table name passed to fmt.Sprintf": {
			"internal/scheduler/sync/sneak.go": `package sync

import "fmt"

func sneak(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO public.%s (occurrence_id) VALUES ($1)", "sync_manual_triggers"), 1)
	return err
}
`,
		},
		"a second Mint call with no Caller in a listed file": {
			"internal/scheduler/sync/initial_backfill.go": `package sync

import "` + censusModule + `"

func trigger() error {
	_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture"}, now)
	return err
}

func again() error {
	_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill"}, now)
	return err
}
`,
		},
		"a second named Mint call in a listed file": {
			"internal/backfillrun/handoff.go": `package backfillrun

import "` + censusModule + `"

func trigger() error {
	for _, caller := range []string{"a", "b"} {
		_, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture"}, now)
		_, err = synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "census_fixture_" + caller}, now)
		return err
	}
	return nil
}
`,
		},
		"a Mint call through an import alias": {
			"internal/other/aliased.go": `package other

import handoff "` + censusModule + `"

func trigger() error {
	_, err := handoff.Mint(ctx, tx, config, handoff.MintInput{Mode: "backfill", Caller: "aliased"}, now)
	return err
}
`,
		},
		"a Mint call through a dot import": {
			"internal/other/dotted.go": `package other

import . "` + censusModule + `"

func trigger() error {
	_, err := Mint(ctx, tx, config, MintInput{Mode: "backfill", Caller: "dotted"}, now)
	return err
}
`,
		},
		"Mint taken as a value": {
			"internal/other/value.go": `package other

import "` + censusModule + `"

var mint = synchandoff.Mint

func trigger() error {
	_, err := mint(ctx, tx, config, synchandoff.MintInput{Mode: "backfill", Caller: "value"}, now)
	return err
}
`,
		},
		"a MintInput built without Caller": {
			"internal/api/syncadmin/manual_trigger.go": `package syncadmin

import "` + censusModule + `"

func handOff(spec spec) error {
	input := synchandoff.MintInput{Mode: spec.mode}
	input.Mode = "backfill"
	_, err := synchandoff.Mint(ctx, tx, config, input, now)
	return err
}
`,
		},
	} {
		tree := censusTree()
		for path, source := range plant {
			tree[path] = &fstest.MapFile{Data: []byte(source)}
		}
		failures, _ := triggerCensus(tree)
		if len(failures) == 0 {
			t.Errorf("%s: the census accepts it", name)
		}
		t.Logf("%s: %v", name, failures)
	}
}
