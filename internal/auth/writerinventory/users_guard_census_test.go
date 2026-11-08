package writerinventory

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// UsersGuardRow is one public.users writer that sets a provider id (INSERT, or UPDATE of auth_provider, auth_provider_id
// or sso_provider_id) and how it is guarded. Key() is File|Func|Verb. A .sql writer has Func "(sql)".
//
// Exempt rows name no guard: the Reason says why none is needed, and Literal (when set) must stay in the writer's SQL.
// Other rows need a call to one of Guards inside GuardFunc of GuardFile, positioned before the write:
//   - Then == "":            before the first Exec/Query call whose SQL text is the users writer (GuardFunc is the writer);
//   - Then == "name":        before the first call to name in GuardFunc (the guard sits in a caller of the writer);
//   - Then == thenLastReturn: before the last return of GuardFunc (the guard sits in the constructor of the row to write).
type UsersGuardRow struct {
	File, Func, Verb string
	Exempt, Literal  string
	GuardFile        string
	GuardFunc        string
	Guards           []string
	Then             string
}

const thenLastReturn = "\x00last-return"

func (r UsersGuardRow) Key() string { return r.File + "|" + r.Func + "|" + r.Verb }

var providerGuards = []string{"RefuseInvalidProviderID", "refuseUnusableSubject"}

// UsersGuardRows lists every public.users writer of a provider id in the tree. A new writer needs a row; a row that
// loses its guard call (or has it after the write) fails.
var UsersGuardRows = []UsersGuardRow{
	{File: "internal/api/session/register.go", Func: "handlers.register", Verb: "INSERT",
		Exempt: "password sign-up: auth_provider is the literal 'local' and auth_provider_id NULL", Literal: "'local', NULL"},
	{File: "internal/api/session/social.go", Func: "handlers.createSocialUser", Verb: "INSERT",
		GuardFile: "internal/api/session/social.go", GuardFunc: "handlers.createSocialUser", Guards: providerGuards},
	{File: "internal/api/sso/provision.go", Func: "handlers.createAccountAndMembership", Verb: "INSERT",
		GuardFile: "internal/api/sso/provision.go", GuardFunc: "handlers.createAccountAndMembership", Guards: providerGuards},
	{File: "internal/apiservice/admin/userstore.go", Func: "pgStore.insertUser", Verb: "INSERT",
		GuardFile: "internal/apiservice/admin/users.go", GuardFunc: "handlers.createUser", Guards: providerGuards, Then: "insertUserWithMembership"},
	{File: "internal/auth/identity/publicusers.go", Func: "insertPublicUser", Verb: "INSERT",
		GuardFile: "internal/auth/identity/publicusers.go", GuardFunc: "federatedPublicUser", Guards: providerGuards, Then: thenLastReturn},

	{File: "internal/pgmigrate/archive/sql/0155_add_users_sso_provider_scope.sql", Func: "(sql)", Verb: "UPDATE",
		Exempt: "one-shot backfill of sso_provider_id from stored rows, applied by the migrator once"},
	{File: "ci/pdp_load_k3s/seed.sql", Func: "(sql)", Verb: "INSERT",
		Exempt: "load-test seed of one local user", Literal: "'local'"},

	{File: "internal/testsupport/pgseed/pgseed.go", Func: "User", Verb: "INSERT",
		Exempt: "test-database fixture, writes no provider id"},
	{File: "internal/testsupport/sessionscenario/sessionscenario.go", Func: "Seed", Verb: "INSERT",
		Exempt: "test-database fixture rows, not a product writer"},
}

var providerColumns = map[string]bool{"auth_provider": true, "auth_provider_id": true, "sso_provider_id": true}

// setsProviderColumn reports whether sql holds an UPDATE of public.users whose SET list assigns a provider column.
func setsProviderColumn(sql string) bool {
	ts := sqlTokens(sql)
	for i := range ts {
		if ts[i].q || ts[i].s != "update" || kw(ts, i-1, "do") || kw(ts, i-1, "for") {
			continue
		}
		schema, name, j, ok := qname(ts, i+1)
		if !ok || qualified(schema, name) != "public.users" {
			continue
		}
		if kw(ts, j, "as") {
			j++
		}
		if !kw(ts, j, "set") && j < len(ts) && isIdent(ts[j]) {
			j++
		}
		if !kw(ts, j, "set") {
			continue
		}
		depth := 0
		for k := j + 1; k < len(ts); k++ {
			t := ts[k]
			if t.q && t.s == "'" {
				continue
			}
			if !t.q {
				switch t.s {
				case "(":
					depth++
				case ")":
					depth--
				case ";":
					depth = -1
				}
				if depth < 0 {
					break
				}
				if depth == 0 && (t.s == "where" || t.s == "from" || t.s == "returning") {
					break
				}
			}
			if depth == 0 && kw(ts, k+1, "=") && providerColumns[strings.ToLower(t.s)] {
				return true
			}
		}
	}
	return false
}

func normSpace(s string) string { return strings.Join(strings.Fields(s), " ") }

type foundWriter struct {
	key  string
	sqls []string
}

// usersProviderWriters finds the public.users writers of a provider id: Go writers through Scan's SQL evaluator, and
// non-test .sql files whole.
func usersProviderWriters(root string, watched map[string]bool) (map[string]*foundWriter, error) {
	found := map[string]*foundWriter{}
	add := func(file, fn, verb, sql string) {
		k := file + "|" + fn + "|" + verb
		if found[k] == nil {
			found[k] = &foundWriter{key: k}
		}
		found[k].sqls = append(found[k].sqls, sql)
	}
	_, err := scanVisit(root, watched, func(w Writer, sql string) {
		if w.Table != "public.users" {
			return
		}
		switch w.Verb {
		case "INSERT", "UPSERT":
			add(w.File, w.Func, w.Verb, sql)
		case "UPDATE":
			if setsProviderColumn(sql) {
				add(w.File, w.Func, w.Verb, sql)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != root && skipDir(p, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".sql") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		text := string(b)
		for _, tv := range sqlWriters(text, watched) {
			if tv[0] != "public.users" {
				continue
			}
			switch tv[1] {
			case "INSERT", "UPSERT":
				add(rel, "(sql)", tv[1], text)
			case "UPDATE":
				if setsProviderColumn(text) {
					add(rel, "(sql)", tv[1], text)
				}
			}
		}
		return nil
	})
	return found, err
}

var usersGuardWriteSQL = regexp.MustCompile(`(?is)(insert\s+into|update)\s+(public\.)?users\b`)

var execNames = map[string]bool{"Exec": true, "ExecContext": true, "QueryRow": true, "QueryRowContext": true, "Query": true, "QueryContext": true}

func calleeName(c *ast.CallExpr) string {
	switch f := c.Fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

func findGuardFunc(af *ast.File, name string) *ast.FuncDecl {
	for _, d := range af.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil && funcName(fd) == name {
			return fd
		}
	}
	return nil
}

// guardProblem returns "" when row's guard call is in GuardFunc and before the write, else why not.
func guardProblem(root string, row UsersGuardRow) string {
	path := filepath.Join(root, filepath.FromSlash(row.GuardFile))
	fset := token.NewFileSet()
	af, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return "cannot parse guard file " + row.GuardFile + ": " + err.Error()
	}
	fd := findGuardFunc(af, row.GuardFunc)
	if fd == nil {
		return "guard func " + row.GuardFunc + " not found in " + row.GuardFile
	}
	var guardPos, target token.Pos
	isGuard := map[string]bool{}
	for _, g := range row.Guards {
		isGuard[g] = true
	}
	lastReturn := token.NoPos
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			name := calleeName(x)
			if isGuard[name] && (guardPos == token.NoPos || x.Pos() < guardPos) {
				guardPos = x.Pos()
			}
			switch {
			case row.Then != "" && row.Then != thenLastReturn:
				if name == row.Then && (target == token.NoPos || x.Pos() < target) {
					target = x.Pos()
				}
			case row.Then == "" && execNames[name]:
				hit := false
				for _, a := range x.Args {
					ast.Inspect(a, func(m ast.Node) bool {
						if bl, ok := m.(*ast.BasicLit); ok && bl.Kind == token.STRING && usersGuardWriteSQL.MatchString(bl.Value) {
							hit = true
						}
						return true
					})
				}
				if hit && (target == token.NoPos || x.Pos() < target) {
					target = x.Pos()
				}
			}
		case *ast.ReturnStmt:
			if x.Pos() > lastReturn {
				lastReturn = x.Pos()
			}
		}
		return true
	})
	if row.Then == thenLastReturn {
		target = lastReturn
	}
	if guardPos == token.NoPos {
		return "no call to " + strings.Join(row.Guards, " or ") + " in " + row.GuardFunc
	}
	if target == token.NoPos {
		return "cannot find the write point in " + row.GuardFunc + " (Then=" + row.Then + ")"
	}
	if guardPos >= target {
		return "the guard call is not before the write in " + row.GuardFunc
	}
	return ""
}

// usersGuardCensus returns one line per problem: a found writer with no row, a row with no writer, an exempt row whose
// literal is gone, and a guarded row whose guard is missing or after the write.
func usersGuardCensus(root string, watched map[string]bool, rows []UsersGuardRow) ([]string, error) {
	found, err := usersProviderWriters(root, watched)
	if err != nil {
		return nil, err
	}
	var out []string
	listed := map[string]bool{}
	for _, r := range rows {
		listed[r.Key()] = true
	}
	for k := range found {
		if !listed[k] {
			out = append(out, "unlisted public.users provider-id writer (add a row with its guard, or an ACP ruling for an exemption): "+k)
		}
	}
	for _, r := range rows {
		fw := found[r.Key()]
		if fw == nil {
			out = append(out, "stale row, no such writer: "+r.Key())
			continue
		}
		if r.Exempt != "" {
			if r.Literal != "" {
				ok := false
				for _, s := range fw.sqls {
					ok = ok || strings.Contains(normSpace(s), r.Literal)
				}
				if !ok {
					out = append(out, "exempt writer no longer holds "+r.Literal+": "+r.Key())
				}
			}
			continue
		}
		if p := guardProblem(root, r); p != "" {
			out = append(out, r.Key()+": "+p)
		}
	}
	sort.Strings(out)
	return out, nil
}

func usersWatched() map[string]bool { return map[string]bool{"public.users": true} }

func TestUsersWriterGuardCensus(t *testing.T) {
	problems, err := usersGuardCensus(repoRoot(t), usersWatched(), UsersGuardRows)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Error(p)
	}
}

func TestUsersGuardRowsAreWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, r := range UsersGuardRows {
		if r.File == "" || r.Func == "" || r.Verb == "" {
			t.Errorf("row with an empty key part: %+v", r)
		}
		if seen[r.Key()] {
			t.Errorf("duplicate row: %s", r.Key())
		}
		seen[r.Key()] = true
		if r.Exempt == "" && (r.GuardFile == "" || r.GuardFunc == "" || len(r.Guards) == 0) {
			t.Errorf("a guarded row needs GuardFile, GuardFunc and Guards: %s", r.Key())
		}
		if r.Exempt != "" && (r.GuardFile != "" || r.GuardFunc != "" || len(r.Guards) > 0 || r.Then != "") {
			t.Errorf("an exempt row names no guard: %s", r.Key())
		}
	}
}

func TestSetsProviderColumn(t *testing.T) {
	for sql, want := range map[string]bool{
		"UPDATE users SET auth_provider_id = $2 WHERE id = $1":                                    true,
		"UPDATE public.users u SET sso_provider_id = c.p FROM c WHERE u.id = c.id":                true,
		"UPDATE users SET \"auth_provider\" = 'x'":                                                true,
		"UPDATE users SET full_name = $2, last_login_at = $3 WHERE auth_provider_id = $1":         false,
		"UPDATE users SET full_name = coalesce($2, auth_provider_id) WHERE id = $1":               false,
		"UPDATE users SET saml_nameid_format = $2 WHERE id = $1":                                  false,
		"UPDATE sessions SET auth_provider_id = $2":                                               false,
		"INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET auth_provider_id = $2": false,
	} {
		if got := setsProviderColumn(sql); got != want {
			t.Errorf("setsProviderColumn(%q) = %v, want %v", sql, got, want)
		}
	}
}

func writePlanted(t *testing.T, src string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "planted"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "planted", "w.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

const plantedGuardHead = `package planted

import "x/identity"

type db interface{ Exec(a string, b ...any) }

var _ = identity.RefuseInvalidProviderID
`

func plantedCensus(t *testing.T, body string, rows []UsersGuardRow) []string {
	t.Helper()
	got, err := usersGuardCensus(writePlanted(t, plantedGuardHead+body), usersWatched(), rows)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func plantedRow(then string) UsersGuardRow {
	return UsersGuardRow{File: "planted/w.go", Func: "Write", Verb: "INSERT",
		GuardFile: "planted/w.go", GuardFunc: "Write", Guards: providerGuards, Then: then}
}

func mustContain(t *testing.T, got []string, want string) {
	t.Helper()
	if !strings.Contains(strings.Join(got, "\n"), want) {
		t.Fatalf("problems %q, want one containing %q", got, want)
	}
}

func TestUsersGuardCensusPlantedCells(t *testing.T) {
	const unguarded = `func Write(d db, p string, id *string) {
	d.Exec("INSERT INTO users (id, auth_provider, auth_provider_id) VALUES ($1, $2, $3)", 1, p, id)
}
`
	const guarded = `func Write(d db, p string, id *string) error {
	if err := identity.RefuseInvalidProviderID(p, id); err != nil {
		return err
	}
	d.Exec("INSERT INTO users (id, auth_provider, auth_provider_id) VALUES ($1, $2, $3)", 1, p, id)
	return nil
}
`
	const guardAfter = `func Write(d db, p string, id *string) error {
	d.Exec("INSERT INTO users (id, auth_provider, auth_provider_id) VALUES ($1, $2, $3)", 1, p, id)
	return identity.RefuseInvalidProviderID(p, id)
}
`
	const updater = `func Write(d db, p string) {
	d.Exec("UPDATE users SET auth_provider_id = $2 WHERE id = $1", 1, p)
}
`
	t.Run("a: unguarded INSERT with no row is unlisted", func(t *testing.T) {
		mustContain(t, plantedCensus(t, unguarded, nil), "unlisted public.users provider-id writer")
	})
	t.Run("a: unguarded provider-id UPDATE with no row is unlisted", func(t *testing.T) {
		mustContain(t, plantedCensus(t, updater, nil), "planted/w.go|Write|UPDATE")
	})
	t.Run("a2: a row with no writer is stale", func(t *testing.T) {
		mustContain(t, plantedCensus(t, "func Other() {}\n", []UsersGuardRow{plantedRow("")}), "stale row, no such writer")
	})
	t.Run("b: a listed writer with no guard call fails", func(t *testing.T) {
		mustContain(t, plantedCensus(t, unguarded, []UsersGuardRow{plantedRow("")}), "no call to")
	})
	t.Run("b: a guard call after the write fails", func(t *testing.T) {
		mustContain(t, plantedCensus(t, guardAfter, []UsersGuardRow{plantedRow("")}), "not before the write")
	})
	t.Run("a listed, guarded writer is clean", func(t *testing.T) {
		if got := plantedCensus(t, guarded, []UsersGuardRow{plantedRow("")}); len(got) != 0 {
			t.Fatalf("unexpected problems: %q", got)
		}
	})
}
