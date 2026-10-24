package writerinventory

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
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

// testFuncExists answers why ref (file|TestName) is not a test that runs: a
// _test.go file, a func(*testing.T) with a body that calls the test handle.
func testFuncExists(root, ref string) string {
	file, name, _ := strings.Cut(ref, "|")
	if !strings.HasSuffix(file, "_test.go") {
		return file + " is not a _test.go file"
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, file), nil, 0)
	if err != nil {
		return "cannot parse " + file + ": " + err.Error()
	}
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != name {
			continue
		}
		params := fn.Type.Params.List
		if len(params) != 1 || len(params[0].Names) != 1 || types.ExprString(params[0].Type) != "*testing.T" {
			return name + " in " + file + " is not func(*testing.T)"
		}
		handle := params[0].Names[0].Name
		used := false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if id, ok := sel.X.(*ast.Ident); ok && id.Name == handle {
						used = true
					}
				}
			}
			return !used
		})
		if !used {
			return name + " in " + file + " never calls its testing handle"
		}
		return ""
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

func TestUsersOwnedCensusRejectsPoolPassedToHelperUnderAnyName(t *testing.T) {
	requireSkipProblem(t, `func (x s) f() error { if x.refuseWhileUsersOwned() == nil { _, _ = x.p.Exec("UPDATE users SET b = 2") }; return writeUser(x.p) }`, "f", "can run while the switch is on")
	requireSkipProblem(t, `func (x s) g(conn C) error { if x.refuseWhileUsersOwned() == nil { _, _ = conn.Exec("UPDATE users SET b = 2") }; return writeUser(conn) }`, "g", "can run while the switch is on")
}

func TestUsersOwnedCensusKeepsAPackageSQLConstantOutOfReachOfOtherFunctionLocals(t *testing.T) {
	src := `const q = "UPDATE users SET a = 1"
func (x s) other() { const q = "SELECT 1"; _ = q }
func (x s) f() error { if x.refuseWhileUsersOwned() == nil { _, _ = x.p.Exec("UPDATE users SET b = 2") }; _, err := x.p.Exec(q); return err }`
	requireSkipProblem(t, src, "f", "can run while the switch is on")
}

func TestUsersOwnedCensusReadsQuotedAndQualifiedUsersSQL(t *testing.T) {
	for name, sql := range map[string]string{
		"quoted":    `UPDATE \"public\".\"users\" SET a = 1`,
		"qualified": `UPDATE public.users SET a = 1`,
		"only":      `UPDATE ONLY users SET a = 1`,
		"merge":     `MERGE INTO users USING t ON true WHEN MATCHED THEN DO NOTHING`,
		"truncate":  `TRUNCATE TABLE users`,
	} {
		src := `func (x s) f() error { if x.refuseWhileUsersOwned() == nil { _, _ = x.p.Exec("UPDATE users SET b = 2") }; _, err := x.p.Exec("` + sql + `"); return err }`
		if got := censusFixture(t, src, "f", true); !strings.Contains(got, "can run while the switch is on") {
			t.Errorf("%s: verdict %q, want an unguarded users write", name, got)
		}
	}
}

func TestUsersOwnedCensusDoesNotReadABindValueAsTheSQL(t *testing.T) {
	requireSkipProblem(t, `func (x s) f() error { if x.refuseWhileUsersOwned() == nil { _, _ = x.p.Exec("UPDATE users SET b = 2") }; _, err := x.p.Exec(q, "bind-value"); return err }`, "f", "can run while the switch is on")
	requireSkipProblem(t, `func (x s) g(ctx C) error { if x.refuseWhileUsersOwned() == nil { _, _ = x.p.Exec(ctx, "UPDATE users SET b = 2") }; _, err := x.p.Exec(ctx, q, "bind-value"); return err }`, "g", "can run while the switch is on")
}

func TestUsersOwnedBehaviourLinkNeedsARealTest(t *testing.T) {
	dir := t.TempDir()
	write := func(name, src string) {
		if err := writeFile(filepath.Join(dir, name), src); err != nil {
			t.Fatal(err)
		}
	}
	write("empty_test.go", "package a\nimport \"testing\"\nfunc TestEmpty(t *testing.T) {}\n")
	write("nottest.go", "package a\nimport \"testing\"\nfunc TestInPlainFile(t *testing.T) { t.Fatal(1) }\n")
	write("badsig_test.go", "package a\nfunc TestNoArgs() {}\n")
	write("real_test.go", "package a\nimport \"testing\"\nfunc TestReal(t *testing.T) { t.Setenv(\"A\", \"b\"); t.Run(\"x\", func(t *testing.T) {}) }\n")
	for ref, wantProblem := range map[string]bool{
		"empty_test.go|TestEmpty":     true,
		"nottest.go|TestInPlainFile":  true,
		"badsig_test.go|TestNoArgs":   true,
		"real_test.go|TestReal":       false,
		"real_test.go|TestMissing":    true,
		"missing_test.go|TestMissing": true,
	} {
		if got := testFuncExists(dir, ref); (got != "") != wantProblem {
			t.Errorf("%s: verdict %q, want problem=%v", ref, got, wantProblem)
		}
	}
}
