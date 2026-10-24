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

// While auth-service owns users (AUTH_USERS_OWNED), go-api must write
// public.users nowhere that matters to the users reconciliation. This census
// classifies every go-api (internal/api, internal/apiservice) writer of
// public.users in the allowlist: Guarded sites call refuseWhileUsersOwned
// before their first database call; Exempt sites say why they are not
// identity-authority writes. A new writer, a stale row or a guard that moved
// behind a database call fails here.
type usersOwnedClass struct {
	guarded bool
	reason  string
}

var guardedWriter = usersOwnedClass{guarded: true}

func exempt(reason string) usersOwnedClass { return usersOwnedClass{reason: reason} }

var usersOwnedCensus = map[string]usersOwnedClass{
	"internal/api/session/changepassword.go|handlers.changePassword":    guardedWriter,
	"internal/api/session/verify.go|handlers.resetPassword":             guardedWriter,
	"internal/api/session/verify.go|handlers.verify":                    guardedWriter,
	"internal/api/session/register.go|handlers.register":                guardedWriter,
	"internal/api/session/social.go|handlers.createSocialUser":          guardedWriter,
	"internal/api/sso/provision.go|handlers.createAccountAndMembership": guardedWriter,
	"internal/apiservice/admin/userstore.go|pgStore.insertUser":         guardedWriter,
	"internal/apiservice/admin/userstore.go|pgStore.updateUser":         guardedWriter,
	"internal/apiservice/admin/userstore.go|pgStore.setUserPassword":    guardedWriter,
	"internal/apiservice/admin/userdelete.go|pgStore.softDeleteUser":    guardedWriter,
	"internal/apiservice/admin/userdelete.go|pgStore.forceDeleteUser":   guardedWriter,
	"internal/api/session/store.go|touchLastLogin":                      exempt("last_login_at only; its own ruling (the D1 last-sign-in gap), not an identity column"),
	"internal/api/sso/provision.go|handlers.provisionOrGetUser":         exempt("full_name and last_login_at of an account already bound to its provider; display drift is informational in the reconciliation"),
	"internal/api/sso/oauth.go|handlers.oauthProvisionUser":             exempt("avatar_url, full_name and last_login_at of an account already found by its key"),
}

var guardNames = map[string]bool{"refuseWhileUsersOwned": true, "RefuseWhileUsersOwned": true, "refuseWhileUsersOwnedErr": true}

const guardName = "refuseWhileUsersOwned"

var databaseCalls = map[string]bool{
	"Exec": true, "Query": true, "QueryRow": true, "Begin": true, "BeginTx": true,
	"inTx": true, "inTransaction": true, "redeemLinkToken": true, "userByEmail": true,
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

// guardProblem answers why the function does not call the guard before its
// first database call, or "".
func guardProblem(root, key string) string {
	file, name, _ := strings.Cut(key, "|")
	parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, file), nil, 0)
	if err != nil {
		return "cannot parse " + file + ": " + err.Error()
	}
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || funcName(fn) != name {
			continue
		}
		guard, firstDB := token.NoPos, token.NoPos
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			var callee string
			switch f := call.Fun.(type) {
			case *ast.SelectorExpr:
				callee = f.Sel.Name
			case *ast.Ident:
				callee = f.Name
			}
			switch {
			case guardNames[callee] && (guard == token.NoPos || call.Pos() < guard):
				guard = call.Pos()
			case databaseCalls[callee] && (firstDB == token.NoPos || call.Pos() < firstDB):
				firstDB = call.Pos()
			}
			return true
		})
		switch {
		case guard == token.NoPos:
			return "does not call " + guardName
		case firstDB != token.NoPos && guard > firstDB:
			return "calls " + guardName + " after its first database call"
		}
		return ""
	}
	return "no function " + name + " in " + file
}

func TestEveryGoAPIUsersWriterIsGuardedOrExempt(t *testing.T) {
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
			problems = append(problems, "unclassified go-api public.users writer (guard it, or exempt it with a reason): "+key)
		case class.guarded:
			if why := guardProblem(root, key); why != "" {
				problems = append(problems, key+": "+why)
			}
		case strings.TrimSpace(class.reason) == "":
			problems = append(problems, key+": exempt without a reason")
		}
	}
	for key := range usersOwnedCensus {
		if !rows[key] {
			problems = append(problems, "stale census row (no such allowlist writer): "+key)
		}
	}
	sort.Strings(problems)
	for _, problem := range problems {
		t.Error(problem)
	}
}

func TestUsersOwnedCensusCatchesAGuardMovedBehindADatabaseCall(t *testing.T) {
	dir := t.TempDir()
	write := func(name, src string) {
		if err := writeFile(filepath.Join(dir, name), src); err != nil {
			t.Fatal(err)
		}
	}
	write("a.go", `package a
type s struct{}
func (x s) late() error { _, _ = x.p.Exec(); return x.refuseWhileUsersOwned() }
func (x s) early() error { if err := x.refuseWhileUsersOwned(); err != nil { return err }; _, _ = x.p.Exec(); return nil }
func (x s) none() error { _, _ = x.p.Exec(); return nil }
`)
	if got := guardProblem(dir, "a.go|s.early"); got != "" {
		t.Errorf("early guard flagged: %s", got)
	}
	if got := guardProblem(dir, "a.go|s.late"); !strings.Contains(got, "after its first database call") {
		t.Errorf("late guard not flagged: %q", got)
	}
	if got := guardProblem(dir, "a.go|s.none"); !strings.Contains(got, "does not call") {
		t.Errorf("missing guard not flagged: %q", got)
	}
}

func writeFile(path, src string) error { return os.WriteFile(path, []byte(src), 0o600) }
