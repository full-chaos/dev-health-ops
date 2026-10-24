package writerinventory

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
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
// used). A guard that is not the first act, whose result is dropped, or that
// sits in a branch that may not run fails here.
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

func hasReturn(block *ast.BlockStmt) bool {
	found := false
	ast.Inspect(block, func(n ast.Node) bool {
		if _, ok := n.(*ast.ReturnStmt); ok {
			found = true
		}
		return !found
	})
	return found
}

func findFunc(root, key string) (*ast.FuncDecl, string) {
	file, name, _ := strings.Cut(key, "|")
	parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, file), nil, 0)
	if err != nil {
		return nil, "cannot parse " + file + ": " + err.Error()
	}
	for _, decl := range parsed.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil && funcName(fn) == name {
			return fn, ""
		}
	}
	return nil, "no function " + name + " in " + file
}

// refuseProblem answers why the function does not refuse first, or "".
func refuseProblem(root, key string) string {
	fn, problem := findFunc(root, key)
	if fn == nil {
		return problem
	}
	if containsCall(fn.Body, isGuardCall) {
		if _, index := guardIf(fn); index < 0 {
			return "calls " + guardName + " outside a top-level if (the result is dropped, or the call sits in a branch that may not run)"
		}
	} else {
		return "does not call " + guardName
	}
	ifStmt, index := guardIf(fn)
	if !hasReturn(ifStmt.Body) {
		return "its " + guardName + " branch does not return"
	}
	for _, stmt := range fn.Body.List[:index] {
		if containsCall(stmt, isDatabaseCall) {
			return "does database work before " + guardName
		}
	}
	if containsCall(ifStmt.Init, isDatabaseCall) {
		return "does database work in the " + guardName + " statement"
	}
	return ""
}

func isUsersWrite(call *ast.CallExpr) bool {
	for _, arg := range call.Args {
		lit, ok := arg.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			continue
		}
		text := strings.ToUpper(strings.Join(strings.Fields(lit.Value), " "))
		for _, prefix := range []string{"UPDATE USERS", "INSERT INTO USERS", "DELETE FROM USERS"} {
			if strings.Contains(text, prefix) {
				return true
			}
		}
	}
	return false
}

// skipProblem answers why a public.users write of the function can run while
// the switch is on, or "".
func skipProblem(root, key string) string {
	fn, problem := findFunc(root, key)
	if fn == nil {
		return problem
	}
	var writes []*ast.CallExpr
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && isUsersWrite(call) {
			writes = append(writes, call)
		}
		return true
	})
	if len(writes) == 0 {
		return "has no public.users write: the census row is stale or the write moved"
	}
	var guards []*ast.IfStmt
	for _, stmt := range fn.Body.List {
		if ifStmt, ok := stmt.(*ast.IfStmt); ok && (containsCall(ifStmt.Init, isGuardCall) || containsCall(ifStmt.Cond, isGuardCall)) {
			guards = append(guards, ifStmt)
		}
	}
	if len(guards) == 0 {
		return "has a public.users write and no top-level if on " + guardName
	}
	for _, write := range writes {
		covered := false
		for _, g := range guards {
			inside := write.Pos() >= g.Body.Pos() && write.End() <= g.Body.End()
			behind := g.End() <= write.Pos() && hasReturn(g.Body)
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
