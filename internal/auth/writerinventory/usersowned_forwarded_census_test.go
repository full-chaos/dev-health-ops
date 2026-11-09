package writerinventory

import (
	"go/ast"
	"strings"
	"testing"
)

// usersOwnedForwardedEntryPoints are the admin users routes that, while auth-service owns users, send their write to
// auth-service's user-admin routes instead of refusing it (CHAOS-13379): each maps to its forward. The route calls the
// forward in a top-level `if ...UsersOwned { ...; return }` before it reaches any guarded store writer, and the forward
// calls the users client and no guarded store writer. The store writers keep refusing while owned
// (usersOwnedCensus), so a forward that fell through to them would still write nothing.
var usersOwnedForwardedEntryPoints = map[string]string{
	"internal/apiservice/admin/users.go|handlers.createUser":      "internal/apiservice/admin/users_owned_forward.go|handlers.createUserInAuth",
	"internal/apiservice/admin/users.go|handlers.updateUser":      "internal/apiservice/admin/users_owned_forward.go|handlers.updateUserInAuth",
	"internal/apiservice/admin/users.go|handlers.setUserPassword": "internal/apiservice/admin/users_owned_forward.go|handlers.setUserPasswordInAuth",
}

// guardedUserWrites are the local writes an owned forward must not reach: the guarded store writers and the guarded
// write transaction.
var guardedUserWrites = map[string]bool{"insertUser": true, "updateUser": true, "setUserPassword": true, "softDeleteUser": true,
	"forceDeleteUser": true, "insertUserWithMembership": true, "insertMembershipTx": true, "superuserGuardedWrite": true}

func callsOneOf(names map[string]bool) func(*ast.CallExpr) bool {
	return func(c *ast.CallExpr) bool { return names[calleeName(c)] }
}

func mentionsUsersOwned(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "UsersOwned" {
			found = true
		}
		return !found
	})
	return found
}

func isUsersAdminCall(c *ast.CallExpr) bool {
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	inner, ok := sel.X.(*ast.SelectorExpr)
	return ok && inner.Sel.Name == "usersAdmin"
}

func forwardProblem(root, key, forward string) string {
	fn, problem := findFunc(root, key)
	if fn == nil {
		return problem
	}
	_, method, _ := strings.Cut(forward, "|")
	_, forwardName, _ := strings.Cut(method, ".")
	index := -1
	for i, stmt := range fn.Body.List {
		branch, ok := stmt.(*ast.IfStmt)
		if !ok || branch.Else != nil || !mentionsUsersOwned(branch.Cond) || !lastIsReturn(branch.Body) {
			continue
		}
		if containsCall(branch.Body, func(c *ast.CallExpr) bool { return calleeName(c) == forwardName }) {
			index = i
			break
		}
	}
	if index < 0 {
		return "has no top-level `if ...UsersOwned { " + forwardName + "(...); return }`"
	}
	if usesGoto(fn) {
		return "uses goto or a label, so the forward may be jumped over"
	}
	for _, stmt := range fn.Body.List[:index] {
		if containsCall(stmt, callsOneOf(guardedUserWrites)) {
			return "reaches a guarded store writer before its owned forward"
		}
	}
	target, problem := findFunc(root, forward)
	if target == nil {
		return problem
	}
	if containsCall(target.Body, callsOneOf(guardedUserWrites)) {
		return forward + " reaches a guarded store writer"
	}
	if !containsCall(target.Body, isUsersAdminCall) {
		return forward + " does not call the users client (h.usersAdmin)"
	}
	return ""
}

func TestEveryGoAPIUsersForwarderSendsItsWriteToAuth(t *testing.T) {
	root := repoRoot(t)
	for key, forward := range usersOwnedForwardedEntryPoints {
		if why := forwardProblem(root, key, forward); why != "" {
			t.Errorf("forwarded entry point %s: %s", key, why)
		}
	}
}
