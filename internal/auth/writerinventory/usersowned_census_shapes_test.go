package writerinventory

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// censusFixture parses src as package a and answers the census verdict for the
// named method of s: refuseProblem for a refuse-first writer, skipProblem for a
// skip-write writer.
func censusFixture(t *testing.T, src, name string, skip bool) string {
	t.Helper()
	dir := t.TempDir()
	if err := writeFile(filepath.Join(dir, "a.go"), "package a\ntype s struct{}\n"+src); err != nil {
		t.Fatal(err)
	}
	if skip {
		return skipProblem(dir, "a.go|s."+name)
	}
	return refuseProblem(dir, "a.go|s."+name)
}

func requireRefuseProblem(t *testing.T, src, name, want string) {
	t.Helper()
	if got := censusFixture(t, src, name, false); !strings.Contains(got, want) {
		t.Fatalf("%s: verdict %q, want it to contain %q", name, got, want)
	}
}

func requireSkipProblem(t *testing.T, src, name, want string) {
	t.Helper()
	if got := censusFixture(t, src, name, true); !strings.Contains(got, want) {
		t.Fatalf("%s: verdict %q, want it to contain %q", name, got, want)
	}
}

func TestUsersOwnedCensusRejectsInvertedRefuseCheck(t *testing.T) {
	requireRefuseProblem(t, `func (x s) f() error { if err := x.refuseWhileUsersOwned(); err == nil { return err }; _, _ = x.p.Exec("UPDATE users SET a = 1"); return nil }`, "f", "inverted")
}

func TestUsersOwnedCensusRejectsLiteralFalseGuardArg(t *testing.T) {
	requireRefuseProblem(t, `func (x s) f() error { if err := RefuseWhileUsersOwned(false); err != nil { return err }; _, _ = x.p.Exec("UPDATE users SET a = 1"); return nil }`, "f", "literal false")
}

func TestUsersOwnedCensusRejectsDiscardedGuardInInit(t *testing.T) {
	requireRefuseProblem(t, `func (x s) f() error { if _ = x.refuseWhileUsersOwned(); false { return nil }; _, _ = x.p.Exec("UPDATE users SET a = 1"); return nil }`, "f", "not the tested value")
}

func TestUsersOwnedCensusRejectsShadowedErr(t *testing.T) {
	requireRefuseProblem(t, `func (x s) f() error { other := error(nil); if err := x.refuseWhileUsersOwned(); other != nil { return err }; _, _ = x.p.Exec("UPDATE users SET a = 1"); return nil }`, "f", "not the tested value")
}

func TestUsersOwnedCensusRejectsUncalledClosure(t *testing.T) {
	requireRefuseProblem(t, `func (x s) f() error { if check := func() error { return x.refuseWhileUsersOwned() }; check != nil { return nil }; _, _ = x.p.Exec("UPDATE users SET a = 1"); return nil }`, "f", "closure")
}

func TestUsersOwnedCensusRejectsGoto(t *testing.T) {
	requireRefuseProblem(t, `func (x s) f() error { goto done; if err := x.refuseWhileUsersOwned(); err != nil { return err }; done: _, _ = x.p.Exec("UPDATE users SET a = 1"); return nil }`, "f", "goto")
}

func TestUsersOwnedCensusRejectsReadThroughUnlistedHelper(t *testing.T) {
	requireRefuseProblem(t, `func (x s) f() error { u := x.loadUser(x.p); _ = u; if err := x.refuseWhileUsersOwned(); err != nil { return err }; return nil }`, "f", "database work before")
}

func TestUsersOwnedCensusRejectsInvertedSkipCheck(t *testing.T) {
	requireSkipProblem(t, `func (x s) f() error { if x.refuseWhileUsersOwned() != nil { _, err := x.p.Exec("UPDATE users SET a = 1"); return err }; return nil }`, "f", "can run while the switch is on")
}

func TestUsersOwnedCensusRejectsConditionalReturnSkip(t *testing.T) {
	requireSkipProblem(t, `func (x s) f(c bool) error { if x.refuseWhileUsersOwned() != nil { if c { return nil } }; _, err := x.p.Exec("UPDATE users SET a = 1"); return err }`, "f", "can run while the switch is on")
}

func TestUsersOwnedCensusRejectsConstantSQLLeak(t *testing.T) {
	requireSkipProblem(t, `func (x s) f() error { const q = "UPDATE users SET a = 1"; if x.refuseWhileUsersOwned() == nil { _, _ = x.p.Exec("UPDATE users SET b = 2") }; _, err := x.p.Exec(q); return err }`, "f", "can run while the switch is on")
}

func TestUsersOwnedCensusRejectsHelperThatWrites(t *testing.T) {
	requireSkipProblem(t, `func (x s) f(tx T) error { if x.refuseWhileUsersOwned() == nil { _, _ = tx.Exec("UPDATE users SET b = 2") }; return writeUser(tx) }`, "f", "can run while the switch is on")
}

func TestUsersOwnedCensusKeepsAcceptingTheCleanShapes(t *testing.T) {
	for name, src := range map[string]string{
		"boolGuard":    `func (x s) boolGuard(w W) error { if x.refuseWhileUsersOwned(w) { return nil }; _, _ = x.p.Exec("UPDATE users SET a = 1"); return nil }`,
		"errGuard":     `func (x s) errGuard() error { if err := x.refuseWhileUsersOwned(); err != nil { return err }; _, _ = x.p.Exec("UPDATE users SET a = 1"); return nil }`,
		"calledGuard":  `func (x s) calledGuard() error { if x.refuseWhileUsersOwned() != nil { return nil }; _, _ = x.p.Exec("UPDATE users SET a = 1"); return nil }`,
		"computedFlag": `func (x s) computedFlag(in I) error { changed := in.a != nil || in.b != nil; if err := x.refuseWhileUsersOwned(changed); err != nil { return err }; _, _ = x.p.Exec("UPDATE users SET a = 1"); return nil }`,
	} {
		if got := censusFixture(t, src, name, false); got != "" {
			t.Errorf("%s flagged as a refuse writer: %s", name, got)
		}
	}
	for name, src := range map[string]string{
		"envFlag":       `func (x s) envFlag() error { if RefuseWhileUsersOwned(On()) != nil { return nil }; _, _ = x.p.Exec("UPDATE users SET a = 1"); return nil }`,
		"constantGuard": `func (x s) constantGuard() error { const q = "UPDATE users SET a = 1"; if x.refuseWhileUsersOwned() == nil { _, _ = x.p.Exec(q) }; return nil }`,
	} {
		if got := censusFixture(t, src, name, true); got != "" {
			t.Errorf("%s flagged as a skip writer: %s", name, got)
		}
	}
}

// behaviourTests names, for every census row and entry point, the test that
// runs the code with the switch on and proves it writes nothing to
// public.users. The census is the inventory; these tests are the proof.
var behaviourTests = map[string]string{
	"internal/api/session/changepassword.go|handlers.changePassword":    "internal/api/session/usersowned_test.go|TestUsersOwnedRefusesEverySessionWriterBeforeTouchingTheDatabase",
	"internal/api/session/verify.go|handlers.resetPassword":             "internal/api/session/usersowned_test.go|TestUsersOwnedRefusesEverySessionWriterBeforeTouchingTheDatabase",
	"internal/api/session/verify.go|handlers.verify":                    "internal/api/session/usersowned_test.go|TestUsersOwnedRefusesEverySessionWriterBeforeTouchingTheDatabase",
	"internal/api/session/register.go|handlers.register":                "internal/api/session/usersowned_test.go|TestUsersOwnedRefusesEverySessionWriterBeforeTouchingTheDatabase",
	"internal/api/session/social.go|handlers.createSocialUser":          "internal/api/session/usersowned_test.go|TestUsersOwnedRefusesTheSocialAccountCreate",
	"internal/api/sso/provision.go|handlers.createAccountAndMembership": "internal/api/sso/usersowned_test.go|TestUsersOwnedRefusesTheSSOAccountCreate",
	"internal/apiservice/admin/userstore.go|pgStore.insertUser":         "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedRefusesEveryAdminUsersWriter",
	"internal/apiservice/admin/userstore.go|pgStore.updateUser":         "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedRefusesEveryAdminUsersWriter",
	"internal/apiservice/admin/userstore.go|pgStore.setUserPassword":    "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedRefusesEveryAdminUsersWriter",
	"internal/apiservice/admin/userdelete.go|pgStore.softDeleteUser":    "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedRefusesEveryAdminUsersWriter",
	"internal/apiservice/admin/userdelete.go|pgStore.forceDeleteUser":   "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedRefusesEveryAdminUsersWriter",
	"internal/api/session/store.go|touchLastLogin":                      "internal/api/session/usersowned_test.go|TestTouchLastLoginWritesNothingWhileUsersOwned",
	"internal/api/sso/provision.go|handlers.provisionOrGetUser":         "internal/api/sso/usersowned_signin_integration_test.go|TestUsersOwnedSignInOfAnExistingAccountWritesNoUsersRow",
	"internal/api/sso/oauth.go|handlers.oauthProvisionUser":             "internal/api/sso/usersowned_signin_integration_test.go|TestUsersOwnedSignInOfAnExistingAccountWritesNoUsersRow",

	"internal/apiservice/admin/users.go|handlers.createUser":      "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedAdminRoutesRefuseBeforeAnyDatabaseWork",
	"internal/apiservice/admin/users.go|handlers.updateUser":      "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedAdminRoutesRefuseBeforeAnyDatabaseWork",
	"internal/apiservice/admin/users.go|handlers.setUserPassword": "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedAdminRoutesRefuseBeforeAnyDatabaseWork",
	"internal/apiservice/admin/users.go|handlers.deleteUser":      "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedAdminRoutesRefuseBeforeAnyDatabaseWork",
	"internal/apiservice/admin/operator.go|Operator.CreateUser":   "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedOperatorVerbsRefuseBeforeAnyDatabaseWork",
	"internal/apiservice/admin/operator.go|Operator.DeleteUser":   "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedOperatorVerbsRefuseBeforeAnyDatabaseWork",
	"internal/apiservice/admin/operator.go|Operator.UpdateUser":   "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedOperatorVerbsRefuseBeforeAnyDatabaseWork",
}

func testFuncExists(root, ref string) string {
	file, name, _ := strings.Cut(ref, "|")
	parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, file), nil, 0)
	if err != nil {
		return "cannot parse " + file + ": " + err.Error()
	}
	for _, decl := range parsed.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return ""
		}
	}
	return "no test " + name + " in " + file
}

func TestEveryUsersOwnedWriterHasABehaviourTest(t *testing.T) {
	root := repoRoot(t)
	keys := map[string]bool{}
	for key := range usersOwnedCensus {
		keys[key] = true
	}
	for _, key := range usersOwnedEntryPoints {
		keys[key] = true
	}
	for key := range keys {
		ref, ok := behaviourTests[key]
		if !ok {
			t.Errorf("no behaviour test named for %s", key)
			continue
		}
		if why := testFuncExists(root, ref); why != "" {
			t.Errorf("%s: %s", key, why)
		}
	}
	for key := range behaviourTests {
		if !keys[key] {
			t.Errorf("behaviour test row for a writer the census does not list: %s", key)
		}
	}
}
