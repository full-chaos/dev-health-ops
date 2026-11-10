package session

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/sessionrule"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// A hand-written liveness predicate on auth.sessions is a second copy of the
// session rule. Outside internal/auth/sessionrule, every statement that names the
// sessions table and tests revoked_at or expires_at must be listed here with
// why it is not a liveness decision (or which ticket removes it). A new one fails.

var (
	sessionsWord  = regexp.MustCompile(`\bsessions\b`)
	livenessWords = regexp.MustCompile(`(\w+\.)?(revoked_at\s+IS\s+(NOT\s+)?NULL|expires_at\s*(>=|<=|>|<))`)
	spaces        = regexp.MustCompile(`\s+`)
)

// key is "<path from the repo root>#<enclosing func or declared name>".
// allowed is one allowlisted statement: predicates is every revoked_at / expires_at test the
// statement holds (sorted, joined by " | "), so a second predicate added inside an allowlisted
// function changes the string and fails; reason says why it is not a liveness decision.
type allowed struct{ predicates, reason string }

var livenessAllowlist = map[string]allowed{
	"internal/api/policy/session.go#sessionStateQuery":          {`revoked_at IS NOT NULL`, "CHAOS-12701: go-api and query-api reader on column grants; moves to sessionrule with its grant ruling"},
	"internal/auth/session/delegation.go#lockBaseAndPriors":     {`s.revoked_at IS NULL`, "the prior-delegation lock set (own revoked_at); the base decision itself uses the rule"},
	"internal/auth/session/delegation.go#insertDelegation":      {`e.expires_at >`, "creation expiry of a delegated row (min of the base's rule Expiry and grants), not a liveness decision"},
	"internal/auth/session/end_all.go#revokeAllLive":            {`s.revoked_at IS NULL`, "selects rows to revoke by own revoked_at; liveness of each row is the rule's Expiry"},
	"internal/auth/session/end_org.go#EndOrgInTx":               {`s.revoked_at IS NULL`, "selects rows to revoke by own revoked_at; liveness of each row is the rule's Expiry"},
	"internal/auth/session/end_org.go#EndOrganizationInTx":      {`s.revoked_at IS NULL`, "selects rows to revoke by own revoked_at; liveness of each row is the rule's Expiry"},
	"internal/edgetokenmint/eddsa.go#sessionLiveSQL":            {`expires_at <= | revoked_at IS NOT NULL`, "CI-only mint pre-check that mirrors go-api policy.PGStore.SessionState plus expiry (CHAOS-12701 site); moves to sessionrule with it"},
	"internal/auth/session/store.go#liveClientSessions":         {`revoked_at IS NULL`, "selects the live client sessions under a revoked web session to name them in session.revoked events (own revoked_at); the server read of each is the rule"},
	"internal/auth/session/store.go#CurrentWithClientClass":     {`g.revoked_at IS NULL`, "the grant's own revoke (oauth_grants), naming the live grant's client class; the session's liveness in the same statement is the rule's"},
	"internal/auth/session/store.go#endSessions":                {`revoked_at IS NULL | revoked_at IS NULL`, "UPDATE writer: revokes the rows it was given"},
	"internal/auth/session/unpinned.go#CountUnpinned":           {`revoked_at IS NULL`, "operator count of unpinned rows by own revoked_at, not a liveness decision"},
	"internal/auth/session/unpinned.go#RevokeUnpinned":          {`revoked_at IS NULL | revoked_at IS NULL | revoked_at IS NULL`, "operator revoke of unpinned rows by own revoked_at, not a liveness decision"},
	"internal/auth/delegationend/delegationend.go#End":          {`revoked_at IS NULL`, "UPDATE writer for delegated children"},
	"internal/auth/delegationend/delegationend.go#lock":         {`revoked_at IS NULL`, "lock set of delegated children by own revoked_at"},
	"internal/auth/keyrotation/store.go#revokeSessions":         {`revoked_at IS NULL | revoked_at IS NULL`, "UPDATE writer after a signing-key rotation"},
	"internal/auth/keyrotation/store.go#sessionsToRevoke":       {`s.revoked_at IS NULL | s.revoked_at IS NULL`, "selects rows to revoke after a key rotation; over-revoking an already ended row is harmless"},
	"internal/auth/restorefence/specs.go#sections":              {`s.revoked_at IS NOT NULL | s.revoked_at IS NOT NULL | s.revoked_at IS NOT NULL | s.revoked_at IS NOT NULL | s.revoked_at IS NOT NULL | s.revoked_at IS NOT NULL | s.revoked_at IS NOT NULL | s.revoked_at IS NOT NULL | s.revoked_at IS NULL | t.expires_at > | t.expires_at > | t.revoked_at IS NULL | t.revoked_at IS NULL | t.revoked_at IS NULL | t.revoked_at IS NULL | t.revoked_at IS NULL | t.revoked_at IS NULL | t.revoked_at IS NULL | t.revoked_at IS NULL | t.revoked_at IS NULL | x.revoked_at IS NOT NULL`, "restore fence revokes rows the target holds live in the source"},
	"internal/testsupport/authrows/queries.go#refreshTokensACP": {`rc.revoked_at IS NOT NULL | rt.revoked_at IS NOT NULL | s.revoked_at IS NOT NULL`, "test-support golden query, not a runtime decision"},
	"internal/testsupport/pinrig/pinrig.go#ClearPin":            {`revoked_at IS NOT NULL`, "test-support fixture write"},
}

// statementLiterals returns, per top-level string expression of a file (a maximal
// + chain, or a lone literal), the text of every string literal inside it, keyed by the
// enclosing function or declared name.
func statementLiterals(fset *token.FileSet, file *ast.File) map[string]string {
	out := map[string]string{}
	var walk func(n ast.Node, scope string)
	collect := func(n ast.Node) string {
		var parts []string
		ast.Inspect(n, func(c ast.Node) bool {
			if lit, ok := c.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if s, err := strconv.Unquote(lit.Value); err == nil {
					parts = append(parts, s)
				}
			}
			return true
		})
		return strings.Join(parts, " ")
	}
	walk = func(n ast.Node, scope string) {
		ast.Inspect(n, func(c ast.Node) bool {
			switch v := c.(type) {
			case *ast.FuncDecl:
				name := v.Name.Name
				if v.Body != nil {
					walk(v.Body, name)
				}
				return false
			case *ast.ValueSpec:
				name := scope
				if name == "" && len(v.Names) > 0 {
					name = v.Names[0].Name
				}
				for _, val := range v.Values {
					out[name] += " " + collect(val)
				}
				return false
			case *ast.BinaryExpr:
				if v.Op == token.ADD {
					out[scope] += " " + collect(v)
					return false
				}
			case *ast.BasicLit:
				if v.Kind == token.STRING {
					out[scope] += " " + collect(v)
				}
			}
			return true
		})
	}
	for _, d := range file.Decls {
		walk(d, "")
	}
	return out
}

func predicatesOf(text string) string {
	var out []string
	for _, m := range livenessWords.FindAllString(text, -1) {
		out = append(out, spaces.ReplaceAllString(m, " "))
	}
	sort.Strings(out)
	return strings.Join(out, " | ")
}

func TestNoHandWrittenSessionLivenessPredicate(t *testing.T) {
	root := repoRoot(t)
	found := map[string]string{}
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			switch {
			case d.IsDir() && (strings.HasPrefix(rel, "internal/auth/sessionrule") || rel == "internal/queryapi/graph" || d.Name() == "testdata"):
				return fs.SkipDir
			case d.IsDir(), !strings.HasSuffix(path, ".go"), strings.HasSuffix(path, "_test.go"):
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if !strings.Contains(string(raw), "sessions") {
				return nil
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, raw, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			for scope, text := range statementLiterals(fset, file) {
				if sessionsWord.MatchString(text) && livenessWords.MatchString(text) {
					found[rel+"#"+scope] = predicatesOf(text)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var keys []string
	for k := range found {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		entry, ok := livenessAllowlist[k]
		switch {
		case !ok:
			t.Errorf("%s names auth.sessions and tests revoked_at or expires_at by hand: use sessionrule, or list it in livenessAllowlist with its reason", k)
		case entry.predicates != found[k]:
			t.Errorf("%s: its predicates are now [%s], the allowlist holds [%s]: a new hand-written liveness test inside an allowlisted statement needs its own entry", k, found[k], entry.predicates)
		}
	}
	for k := range livenessAllowlist {
		if _, ok := found[k]; !ok {
			t.Errorf("livenessAllowlist entry %s matches no statement; remove it", k)
		}
	}
}

// The store's effective expressions are the shared rule's, over its own schema and lifetime.
func TestTheStoreReadsTheSharedRule(t *testing.T) {
	keyText, _ := testKey(t)
	pool, err := pgxpool.New(context.Background(), "postgres://u@127.0.0.1:1/none")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(Config{Pool: pool, Schema: schema, SuccessorKey: keyText, RefreshTTL: 90 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	rule, err := sessionrule.New(authschema.Quote(schema), 90*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if store.effectiveRevokedAt() != rule.Revoked("s") || store.effectiveExpiry() != rule.Expiry("s") || store.effectiveCap() != rule.Cap("s") {
		t.Error("a store expression is not the shared rule's")
	}
	if store.RefreshTTL() != 90*time.Minute {
		t.Errorf("RefreshTTL = %v", store.RefreshTTL())
	}
}

// The allowlist is per statement and per predicate: a second liveness test added inside an
// allowlisted statement is a different predicate set, and fails the census above.
func TestCensusSeesASecondPredicateInsideAnAllowlistedStatement(t *testing.T) {
	entry := livenessAllowlist["internal/auth/session/end_all.go#revokeAllLive"]
	listed := predicatesOf("SELECT 1 FROM sessions s WHERE s.principal_id = $1 AND s.revoked_at IS NULL")
	if listed != entry.predicates {
		t.Fatalf("fixture drifted: %q vs %q", listed, entry.predicates)
	}
	for _, extra := range []string{" AND s.expires_at > now()", " AND s.revoked_at IS NULL", " AND x.revoked_at IS NOT NULL"} {
		if got := predicatesOf("SELECT 1 FROM sessions s WHERE s.principal_id = $1 AND s.revoked_at IS NULL" + extra); got == entry.predicates {
			t.Errorf("a second predicate %q is not seen", extra)
		}
	}
}
