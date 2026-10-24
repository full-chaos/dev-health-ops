package writerinventory

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// While auth-service owns users (AUTH_USERS_OWNED), go-api writes public.users
// nowhere. This census classifies every go-api (internal/api,
// internal/apiservice) writer of public.users in the allowlist, and the entry
// points (admin routes, operator verbs) that reach them. There is no exempt
// class: a writer either REFUSES (its first act is a top-level
// `if <guard> ... { return }`) or SKIPS its write (every public.users write sits
// inside, or behind a returning branch of, an `if <guard>` whose result is
// used). A guard that is not the first act, whose result is dropped, whose test
// is inverted or takes the literal false, or that sits in a branch that may not
// run fails here. The census is the inventory; behaviourTests names, for each
// row, the test that runs the writer with the switch on.
type usersOwnedClass int

const (
	// refuseFirst: the function refuses before any database work, reads included.
	refuseFirst usersOwnedClass = iota + 1
	// skipWrite: the sign-in keeps working; its public.users write is skipped.
	skipWrite
)

var usersOwnedCensus = map[string]usersOwnedClass{
	"internal/api/session/changepassword.go|handlers.changePassword":    refuseFirst,
	"internal/api/session/verify.go|handlers.resetPassword":             refuseFirst,
	"internal/api/session/verify.go|handlers.verify":                    refuseFirst,
	"internal/api/session/register.go|handlers.register":                refuseFirst,
	"internal/api/session/social.go|handlers.createSocialUser":          refuseFirst,
	"internal/api/sso/provision.go|handlers.createAccountAndMembership": refuseFirst,
	"internal/apiservice/admin/userstore.go|pgStore.insertUser":         refuseFirst,
	"internal/apiservice/admin/userstore.go|pgStore.updateUser":         refuseFirst,
	"internal/apiservice/admin/userstore.go|pgStore.setUserPassword":    refuseFirst,
	"internal/apiservice/admin/userdelete.go|pgStore.softDeleteUser":    refuseFirst,
	"internal/apiservice/admin/userdelete.go|pgStore.forceDeleteUser":   refuseFirst,
	"internal/api/session/store.go|touchLastLogin":                      skipWrite,
	"internal/api/sso/provision.go|handlers.provisionOrGetUser":         skipWrite,
	"internal/api/sso/oauth.go|handlers.oauthProvisionUser":             skipWrite,
}

// usersOwnedEntryPoints are the admin routes and operator verbs that reach the
// guarded store writers. Each refuses before any database work, so a database
// failure cannot replace the refusal.
var usersOwnedEntryPoints = []string{
	"internal/apiservice/admin/users.go|handlers.createUser",
	"internal/apiservice/admin/users.go|handlers.updateUser",
	"internal/apiservice/admin/users.go|handlers.setUserPassword",
	"internal/apiservice/admin/users.go|handlers.deleteUser",
	"internal/apiservice/admin/operator.go|Operator.CreateUser",
	"internal/apiservice/admin/operator.go|Operator.DeleteUser",
	"internal/apiservice/admin/operator.go|Operator.UpdateUser",
}

var guardNames = map[string]bool{"refuseWhileUsersOwned": true, "RefuseWhileUsersOwned": true, "refuseWhileUsersOwnedErr": true}

const guardName = "refuseWhileUsersOwned"

// databaseNames are callees that do database work whatever their receiver.
var databaseNames = map[string]bool{
	"Exec": true, "Query": true, "QueryRow": true, "Begin": true, "BeginTx": true,
	"inTx": true, "inTransaction": true, "redeemLinkToken": true, "userByEmail": true,
	"findUser": true, "resolveOrg": true,
}

// databaseReceivers are receiver names whose methods do database work.
var databaseReceivers = map[string]bool{"Pool": true, "pool": true, "tx": true, "store": true, "db": true}

func calleeParts(call *ast.CallExpr) (name string, chain []string) {
	expr := call.Fun
	for {
		switch f := expr.(type) {
		case *ast.SelectorExpr:
			if name == "" {
				name = f.Sel.Name
			} else {
				chain = append(chain, f.Sel.Name)
			}
			expr = f.X
			continue
		case *ast.Ident:
			if name == "" {
				name = f.Name
			} else {
				chain = append(chain, f.Name)
			}
		}
		return name, chain
	}
}

func isGuardCall(call *ast.CallExpr) bool {
	name, _ := calleeParts(call)
	return guardNames[name]
}

func isDatabaseCall(call *ast.CallExpr) bool {
	name, chain := calleeParts(call)
	if guardNames[name] {
		return false
	}
	if databaseNames[name] {
		return true
	}
	for _, part := range chain {
		if databaseReceivers[part] {
			return true
		}
	}
	return false
}

func containsCall(node ast.Node, match func(*ast.CallExpr) bool) bool {
	found := false
	if node == nil {
		return false
	}
	ast.Inspect(node, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && match(call) {
			found = true
		}
		return !found
	})
	return found
}

// guardIf returns the top-level if statement whose init or condition calls
// the guard, with its position among the body's statements.
func guardIf(fn *ast.FuncDecl) (*ast.IfStmt, int) {
	for index, stmt := range fn.Body.List {
		ifStmt, ok := stmt.(*ast.IfStmt)
		if !ok {
			continue
		}
		if containsCall(ifStmt.Init, isGuardCall) || containsCall(ifStmt.Cond, isGuardCall) {
			return ifStmt, index
		}
	}
	return nil, -1
}

type guardShape int

const (
	// shapeRefuse: the if body runs while the switch is on.
	shapeRefuse guardShape = iota + 1
	// shapeProceed: the if body runs while the switch is off.
	shapeProceed
)

func asGuardCall(expr ast.Expr) (*ast.CallExpr, bool) {
	call, ok := expr.(*ast.CallExpr)
	return call, ok && isGuardCall(call)
}

func isNil(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == "nil"
}

// classifyGuardIf accepts only the plain forms of a guard test and says why any
// other form is not one:
//
//	if guard(...) { }                    refuse (the bool guard)
//	if guard(...) != nil { }             refuse
//	if guard(...) == nil { }             proceed
//	if v := guard(...); v != nil { }     refuse
//	if v := guard(...); v == nil { }     proceed
//
// with no else branch, no closure, and no literal false as a guard argument.
func classifyGuardIf(ifStmt *ast.IfStmt) (guardShape, string) {
	if containsFuncLit(ifStmt.Init) || containsFuncLit(ifStmt.Cond) {
		return 0, "its " + guardName + " test sits in a closure (a closure that is not called decides nothing)"
	}
	if ifStmt.Else != nil {
		return 0, "its " + guardName + " test has an else branch"
	}
	literalFalse := false
	for _, node := range []ast.Node{ifStmt.Init, ifStmt.Cond} {
		if node == nil {
			continue
		}
		containsCall(node, func(call *ast.CallExpr) bool {
			if isGuardCall(call) {
				for _, arg := range call.Args {
					if ident, ok := arg.(*ast.Ident); ok && ident.Name == "false" {
						literalFalse = true
					}
				}
			}
			return false
		})
	}
	if literalFalse {
		return 0, "its " + guardName + " flag argument is the literal false"
	}
	compare := func(expr ast.Expr, subject func(ast.Expr) bool) (guardShape, bool) {
		bin, ok := expr.(*ast.BinaryExpr)
		if !ok || !subject(bin.X) || !isNil(bin.Y) {
			return 0, false
		}
		switch bin.Op {
		case token.NEQ:
			return shapeRefuse, true
		case token.EQL:
			return shapeProceed, true
		}
		return 0, false
	}
	if ifStmt.Init == nil {
		if _, ok := asGuardCall(ifStmt.Cond); ok {
			return shapeRefuse, ""
		}
		if shape, ok := compare(ifStmt.Cond, func(e ast.Expr) bool { _, g := asGuardCall(e); return g }); ok {
			return shape, ""
		}
		if unary, ok := ifStmt.Cond.(*ast.UnaryExpr); ok && unary.Op == token.NOT {
			return 0, "its " + guardName + " test is inverted"
		}
		return 0, "its " + guardName + " test is not a plain comparison of the guard result"
	}
	assign, ok := ifStmt.Init.(*ast.AssignStmt)
	if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
		return 0, "its " + guardName + " result is not the tested value"
	}
	if _, ok := asGuardCall(assign.Rhs[0]); !ok {
		return 0, "its " + guardName + " result is not the tested value"
	}
	name, ok := assign.Lhs[0].(*ast.Ident)
	if !ok || name.Name == "_" {
		return 0, "its " + guardName + " result is discarded, not the tested value"
	}
	shape, ok := compare(ifStmt.Cond, func(e ast.Expr) bool { id, isIdent := e.(*ast.Ident); return isIdent && id.Name == name.Name })
	if !ok {
		return 0, "its " + guardName + " result is not the tested value"
	}
	return shape, ""
}

func containsFuncLit(node ast.Node) bool {
	found := false
	if node == nil {
		return false
	}
	ast.Inspect(node, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			found = true
		}
		return !found
	})
	return found
}

// lastIsReturn: the block ends in a return, so the branch always leaves.
func lastIsReturn(block *ast.BlockStmt) bool {
	if len(block.List) == 0 {
		return false
	}
	_, ok := block.List[len(block.List)-1].(*ast.ReturnStmt)
	return ok
}

func usesGoto(fn *ast.FuncDecl) bool {
	found := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.LabeledStmt:
			found = true
		case *ast.BranchStmt:
			if s.Tok == token.GOTO {
				found = true
			}
		}
		return !found
	})
	return found
}

func findFunc(root, key string) (*ast.FuncDecl, string) {
	fn, _, problem := findFuncFile(root, key)
	return fn, problem
}

func findFuncFile(root, key string) (*ast.FuncDecl, *ast.File, string) {
	file, name, _ := strings.Cut(key, "|")
	parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, file), nil, 0)
	if err != nil {
		return nil, nil, "cannot parse " + file + ": " + err.Error()
	}
	for _, decl := range parsed.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil && funcName(fn) == name {
			return fn, parsed, ""
		}
	}
	return nil, nil, "no function " + name + " in " + file
}

// refuseProblem answers why the function does not refuse first, or "".
func refuseProblem(root, key string) string {
	fn, problem := findFunc(root, key)
	if fn == nil {
		return problem
	}
	if !containsCall(fn.Body, isGuardCall) {
		return "does not call " + guardName
	}
	if usesGoto(fn) {
		return "uses goto or a label, so the guard may be jumped over"
	}
	ifStmt, index := guardIf(fn)
	if index < 0 {
		return "calls " + guardName + " outside a top-level if (the result is dropped, or the call sits in a branch that may not run)"
	}
	shape, why := classifyGuardIf(ifStmt)
	if why != "" {
		return why
	}
	if shape != shapeRefuse {
		return "its " + guardName + " test is inverted: the branch runs while the switch is off"
	}
	if !lastIsReturn(ifStmt.Body) {
		return "its " + guardName + " branch does not return"
	}
	for _, stmt := range fn.Body.List[:index] {
		if containsCall(stmt, func(*ast.CallExpr) bool { return true }) {
			return "does database work before " + guardName + " (a call before the guard; only plain computation may come first)"
		}
	}
	return ""
}

var usersWriteSQL = regexp.MustCompile(`(UPDATE|INSERT INTO|DELETE FROM|MERGE INTO|TRUNCATE( TABLE)?)( ONLY)? (PUBLIC\.)?USERS\b`)

// stringConsts collects the string constants of the file and of the function.
func stringConsts(file *ast.File, fn *ast.FuncDecl) map[string]string {
	consts := map[string]string{}
	collect := func(node ast.Node) {
		ast.Inspect(node, func(n ast.Node) bool {
			gen, ok := n.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				return true
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range value.Names {
					if i < len(value.Values) {
						if text, ok := constString(value.Values[i], consts); ok {
							consts[name.Name] = text
						}
					}
				}
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.CONST {
			collect(gen)
		}
	}
	collect(fn)
	return consts
}

func constString(expr ast.Expr, consts map[string]string) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind == token.STRING {
			if text, err := strconv.Unquote(e.Value); err == nil {
				return text, true
			}
			return e.Value, true
		}
	case *ast.Ident:
		text, ok := consts[e.Name]
		return text, ok
	case *ast.BinaryExpr:
		if e.Op == token.ADD {
			left, lok := constString(e.X, consts)
			right, rok := constString(e.Y, consts)
			return left + right, lok && rok
		}
	case *ast.ParenExpr:
		return constString(e.X, consts)
	}
	return "", false
}

func sqlWritesUsers(text string) bool {
	text = strings.ToUpper(strings.Join(strings.Fields(strings.NewReplacer(`"`, "").Replace(text)), " "))
	return usersWriteSQL.MatchString(text)
}

// databaseHandles are the names that hold a database handle in fn: the fixed
// receiver names, every parameter typed as a pool, transaction, connection or
// store, and every base name a statement call (Exec, Query, QueryRow, Begin) is
// made on, so an alias such as p is still a handle.
func databaseHandles(fn *ast.FuncDecl) map[string]bool {
	handles := map[string]bool{}
	for name := range databaseReceivers {
		handles[name] = true
	}
	if fn.Type.Params != nil {
		for _, field := range fn.Type.Params.List {
			typeText := types.ExprString(field.Type)
			if strings.HasSuffix(typeText, "Tx") || strings.HasSuffix(typeText, "Pool") || strings.HasSuffix(typeText, "DB") ||
				strings.HasSuffix(typeText, "Conn") || strings.HasSuffix(typeText, "Querier") || strings.HasSuffix(typeText, "Executor") {
				for _, name := range field.Names {
					handles[name.Name] = true
				}
			}
		}
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if name, chain := calleeParts(call); statementCalls[name] {
			if len(chain) > 0 {
				handles[chain[0]] = true
			}
		}
		return true
	})
	return handles
}

var statementCalls = map[string]bool{"Exec": true, "Query": true, "QueryRow": true, "Begin": true, "BeginTx": true}

// receivesDatabase: an argument is a database handle, so the callee can write
// through it whatever its name.
func receivesDatabase(call *ast.CallExpr, handles map[string]bool) bool {
	for _, arg := range call.Args {
		switch a := arg.(type) {
		case *ast.Ident:
			if handles[a.Name] {
				return true
			}
		case *ast.SelectorExpr:
			if handles[a.Sel.Name] {
				return true
			}
		}
	}
	return false
}

// isContextArg: the argument is a context, so the SQL text is the next one.
func isContextArg(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name == "ctx" || strings.HasPrefix(e.Name, "ctx") || e.Name == "context"
	case *ast.SelectorExpr:
		return strings.Contains(strings.ToLower(e.Sel.Name), "ctx") || strings.Contains(strings.ToLower(e.Sel.Name), "context")
	case *ast.CallExpr:
		name, chain := calleeParts(e)
		return strings.Contains(strings.ToLower(name), "context") || (len(chain) > 0 && strings.Contains(strings.ToLower(chain[len(chain)-1]), "context"))
	}
	return false
}

// mayWriteUsers: the call carries SQL that writes public.users, SQL the census
// cannot read (a variable, a call result), or a database handle to a callee the
// census cannot see into.
func mayWriteUsers(call *ast.CallExpr, consts map[string]string, handles map[string]bool) bool {
	if isGuardCall(call) {
		return false
	}
	name, _ := calleeParts(call)
	if name == "Exec" || name == "Query" || name == "QueryRow" {
		if len(call.Args) == 0 {
			return false
		}
		sqlArg := call.Args[0]
		if isContextArg(sqlArg) && len(call.Args) > 1 {
			sqlArg = call.Args[1]
		}
		text, ok := constString(sqlArg, consts)
		return !ok || sqlWritesUsers(text)
	}
	return receivesDatabase(call, handles) && !databaseNames[name]
}

// skipProblem answers why a public.users write of the function can run while
// the switch is on, or "".
func skipProblem(root, key string) string {
	fn, file, problem := findFuncFile(root, key)
	if fn == nil {
		return problem
	}
	if usesGoto(fn) {
		return "uses goto or a label, so the guard may be jumped over"
	}
	consts := stringConsts(file, fn)
	handles := databaseHandles(fn)
	var writes []*ast.CallExpr
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && mayWriteUsers(call, consts, handles) {
			writes = append(writes, call)
		}
		return true
	})
	if len(writes) == 0 {
		return "has no public.users write: the census row is stale or the write moved"
	}
	type guard struct {
		stmt  *ast.IfStmt
		shape guardShape
	}
	var guards []guard
	for _, stmt := range fn.Body.List {
		ifStmt, ok := stmt.(*ast.IfStmt)
		if !ok || !(containsCall(ifStmt.Init, isGuardCall) || containsCall(ifStmt.Cond, isGuardCall)) {
			continue
		}
		shape, why := classifyGuardIf(ifStmt)
		if why != "" {
			return why
		}
		guards = append(guards, guard{ifStmt, shape})
	}
	if len(guards) == 0 {
		return "has a public.users write and no top-level if on " + guardName
	}
	for _, write := range writes {
		covered := false
		for _, g := range guards {
			inside := g.shape == shapeProceed && write.Pos() >= g.stmt.Body.Pos() && write.End() <= g.stmt.Body.End()
			behind := g.shape == shapeRefuse && g.stmt.End() <= write.Pos() && lastIsReturn(g.stmt.Body)
			if inside || behind {
				covered = true
			}
		}
		if !covered {
			return "a public.users write can run while the switch is on"
		}
	}
	return ""
}

func goAPIUsersWriters() map[string]bool {
	rows := map[string]bool{}
	for _, entry := range Allowlist {
		if entry.Table != "public.users" {
			continue
		}
		if !strings.HasPrefix(entry.File, "internal/api/") && !strings.HasPrefix(entry.File, "internal/apiservice/") {
			continue
		}
		rows[entry.File+"|"+entry.Func] = true
	}
	return rows
}

func TestEveryGoAPIUsersWriterRefusesOrSkipsItsWrite(t *testing.T) {
	root := repoRoot(t)
	rows := goAPIUsersWriters()
	if len(rows) == 0 {
		t.Fatal("the allowlist holds no go-api public.users writer; the census would pass vacuously")
	}
	var problems []string
	for key := range rows {
		class, ok := usersOwnedCensus[key]
		switch {
		case !ok:
			problems = append(problems, "unclassified go-api public.users writer (it must refuse or skip its write): "+key)
		case class == refuseFirst:
			if why := refuseProblem(root, key); why != "" {
				problems = append(problems, key+": "+why)
			}
		case class == skipWrite:
			if why := skipProblem(root, key); why != "" {
				problems = append(problems, key+": "+why)
			}
		default:
			problems = append(problems, key+": unknown census class")
		}
	}
	for key := range usersOwnedCensus {
		if !rows[key] {
			problems = append(problems, "stale census row (no such allowlist writer): "+key)
		}
	}
	for _, key := range usersOwnedEntryPoints {
		if why := refuseProblem(root, key); why != "" {
			problems = append(problems, "entry point "+key+": "+why)
		}
	}
	sort.Strings(problems)
	for _, problem := range problems {
		t.Error(problem)
	}
}

func TestUsersOwnedCensusCatchesAMisplacedGuard(t *testing.T) {
	dir := t.TempDir()
	write := func(name, src string) {
		if err := writeFile(filepath.Join(dir, name), src); err != nil {
			t.Fatal(err)
		}
	}
	write("a.go", `package a
type s struct{}
func (x s) early() error { if err := x.refuseWhileUsersOwned(); err != nil { return err }; _, _ = x.p.Exec(); return nil }
func (x s) late() error { _, _ = x.p.Exec(); if err := x.refuseWhileUsersOwned(); err != nil { return err }; return nil }
func (x s) readFirst() error { u, _ := x.store.fullUserByID(); _ = u; if err := x.refuseWhileUsersOwned(); err != nil { return err }; return nil }
func (x s) none() error { _, _ = x.p.Exec(); return nil }
func (x s) dropped() error { _ = x.refuseWhileUsersOwned(); _, _ = x.p.Exec(); return nil }
func (x s) bare() error { x.refuseWhileUsersOwned(); _, _ = x.p.Exec(); return nil }
func (x s) dead() error { if false { if err := x.refuseWhileUsersOwned(); err != nil { return err } }; _, _ = x.p.Exec(); return nil }
func (x s) noReturn() error { if err := x.refuseWhileUsersOwned(); err != nil { _ = err }; _, _ = x.p.Exec(); return nil }
func (x s) skipOK() error { if x.refuseWhileUsersOwned() == nil { _, err := x.p.Exec("UPDATE users SET a = 1"); return err }; return nil }
func (x s) skipEarlyReturn() error { if x.refuseWhileUsersOwned() != nil { return nil }; _, err := x.p.Exec("UPDATE users SET a = 1"); return err }
func (x s) skipLeaks() error { _, err := x.p.Exec("UPDATE users SET a = 1"); if x.refuseWhileUsersOwned() != nil { return nil }; return err }
func (x s) skipDropped() error { _ = x.refuseWhileUsersOwned(); _, err := x.p.Exec("UPDATE users SET a = 1"); return err }
`)
	for _, name := range []string{"early"} {
		if got := refuseProblem(dir, "a.go|s."+name); got != "" {
			t.Errorf("%s flagged: %s", name, got)
		}
	}
	for name, want := range map[string]string{
		"late":      "database work before",
		"readFirst": "database work before",
		"none":      "does not call",
		"dropped":   "outside a top-level if",
		"bare":      "outside a top-level if",
		"dead":      "outside a top-level if",
		"noReturn":  "does not return",
	} {
		if got := refuseProblem(dir, "a.go|s."+name); !strings.Contains(got, want) {
			t.Errorf("%s: got %q, want it to contain %q", name, got, want)
		}
	}
	for _, name := range []string{"skipOK", "skipEarlyReturn"} {
		if got := skipProblem(dir, "a.go|s."+name); got != "" {
			t.Errorf("%s flagged: %s", name, got)
		}
	}
	for _, name := range []string{"skipLeaks", "skipDropped"} {
		if got := skipProblem(dir, "a.go|s."+name); got == "" {
			t.Errorf("%s accepted: a public.users write can run while the switch is on", name)
		}
	}
}

func writeFile(path, src string) error { return os.WriteFile(path, []byte(src), 0o600) }
