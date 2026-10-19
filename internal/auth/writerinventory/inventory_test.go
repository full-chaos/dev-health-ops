package writerinventory

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func compare(found []Writer, allow []Entry) (unlisted []Writer, stale []string) {
	listed := map[string]bool{}
	for _, e := range allow {
		listed[e.Key()] = true
	}
	foundKeys := map[string]bool{}
	for _, w := range found {
		foundKeys[w.Key()] = true
		if !listed[w.Key()] {
			unlisted = append(unlisted, w)
		}
	}
	for _, e := range allow {
		if !foundKeys[e.Key()] {
			stale = append(stale, e.Key())
		}
	}
	sort.Strings(stale)
	return unlisted, stale
}

func entryLines(ws []Writer) string {
	var b strings.Builder
	for _, w := range ws {
		b.WriteString("\n\t{File: \"" + w.File + "\", Func: \"" + w.Func + "\", Table: \"" + w.Table + "\", Verb: \"" + w.Verb + "\"},")
	}
	return b.String()
}

func TestWriterInventoryMatchesAllowlist(t *testing.T) {
	root := repoRoot(t)
	watched, err := Watched(root)
	if err != nil {
		t.Fatal(err)
	}
	if !watched["auth.sessions"] || !watched["public.users"] {
		t.Fatalf("watched set is missing a known table: %v", watched)
	}
	found, err := Scan(root, watched)
	if err != nil {
		t.Fatal(err)
	}
	unlisted, stale := compare(found, Allowlist)
	if len(unlisted) > 0 {
		t.Errorf("%d SQL writer(s) of an auth table are not in the allowlist; a new writer needs an ACP ruling, then an entry:%s", len(unlisted), entryLines(unlisted))
	}
	for _, k := range stale {
		t.Errorf("stale allowlist entry (no such writer in the tree; delete it): %s", k)
	}
}

func TestAllowlistIsKeyedExactly(t *testing.T) {
	seen := map[string]bool{}
	for _, e := range Allowlist {
		if e.File == "" || e.Func == "" || e.Table == "" || e.Verb == "" {
			t.Errorf("entry with an empty key part: %+v", e)
		}
		if !strings.HasPrefix(e.Table, "public.") && !strings.HasPrefix(e.Table, "auth.") {
			t.Errorf("table must be schema-qualified: %+v", e)
		}
		switch e.Verb {
		case "INSERT", "UPSERT", "UPDATE", "DELETE", "TRUNCATE":
		default:
			t.Errorf("unknown verb: %+v", e)
		}
		if seen[e.Key()] {
			t.Errorf("duplicate entry: %s", e.Key())
		}
		seen[e.Key()] = true
	}
}

const plantedSource = `package planted

const insertCols = "INSERT INTO users (id, email) VALUES ($1, $2)"

func Direct(db interface{ Exec(string, ...any) }) {
	db.Exec("UPDATE refresh_tokens SET revoked_at = now() WHERE id = $1")
}

func Concat(db interface{ Exec(string, ...any) }) {
	db.Exec("DELETE FROM " + "auth.sessions" + " WHERE id = $1")
}

func ViaConst(db interface{ Exec(string, ...any) }) {
	db.Exec(insertCols)
}

func Upsert(db interface{ Exec(string, ...any) }) {
	db.Exec(` + "`" + `
		INSERT INTO memberships (user_id, org_id) VALUES ($1, $2)
		ON CONFLICT (user_id, org_id) DO UPDATE SET role = EXCLUDED.role` + "`" + `)
}

func Cte(db interface{ Exec(string, ...any) }) {
	db.Exec("WITH x AS (UPDATE auth.principals SET status = 'x' RETURNING id) SELECT 1 FROM x")
}

func Hint(db interface{ Exec(string, ...any) }) {
	db.Exec("INSERT /* hint */ INTO -- note\n users (id) VALUES ($1)")
}

func FalseUpsert(db interface{ Exec(string, ...any) }) {
	db.Exec("INSERT INTO users (x) VALUES ('ON CONFLICT (x) DO UPDATE SET y = 1')")
}

func QuotedCTE(db interface{ Exec(string, ...any) }) {
	db.Exec("WITH c AS (SELECT '--' AS x) UPDATE users SET a = 1")
}

func QualifiedComment(db interface{ Exec(string, ...any) }) {
	db.Exec("UPDATE public /* c */ . users SET a = 1")
}

const shadowSQL = "INSERT INTO org_invites (id) VALUES ($1)"

func Shadow(db interface{ Exec(string, ...any) }, shadowSQL string) {
	db.Exec(shadowSQL)
}

var queries = map[string]struct{ ins string }{"a": {ins: "INSERT INTO sso_providers (id)"}}

func Suffix(db interface{ Exec(string, ...any) }, k string) {
	db.Exec(queries[k].ins + " VALUES ($1)")
}

func RuntimeSchema(db interface{ Exec(string, ...any) }, s string) {
	db.Exec("INSERT INTO " + authschema.Quote(s) + ".principals (a) VALUES ($1)")
}

func ViaAssign(db interface{ Exec(string, ...any) }, s string) {
	table := authschema.Quote(s) + ".security_audit_events"
	db.Exec("DELETE FROM " + table + " AS e WHERE e.id = $1")
}

type pub struct{ s string }

func (p *pub) table() string { return authschema.Quote(p.s) + ".auth_outbox_events" }

func (p *pub) ViaMethod(db interface{ Exec(string, ...any) }) {
	db.Exec("UPDATE " + p.table() + " SET a = 1")
}

func ViaBuilder(db interface{ Exec(string, ...any) }, s string) {
	table := authschema.Quote(s) + ".sessions"
	db.Exec(builderStmt(table))
}

func builderStmt(table string) string {
	return "WITH r AS (" + builderSel(table) + ") DELETE FROM " + table + " AS e USING r WHERE e.id = r.id"
}

func builderSel(table string) string { return "SELECT e.id FROM " + table + " AS e" }

func Trunc(db interface{ Exec(string, ...any) }) {
	db.Exec("TRUNCATE TABLE users, refresh_tokens CASCADE")
}

const (
	firstSQL = "INSERT INTO login_attempts (id) VALUES ($1)"
	secondSQL
)

func UsesSecond(db interface{ Exec(string, ...any) }) {
	db.Exec(secondSQL)
}

var linkSQL = map[string]struct{ del, ins string }{
	"a": {del: "DELETE FROM password_reset_tokens WHERE user_id = $1", ins: "INSERT INTO password_reset_tokens (id) VALUES ($1)"},
}

func Replace(db interface{ Exec(string, ...any) }, k string) {
	db.Exec(linkSQL[k].del)
}

func Other(db interface{ Exec(string, ...any) }, k string) {
	db.Exec(linkSQL[k].ins)
}

var tableSQL = map[string]string{
	"a": "TRUNCATE TABLE auth.sessions",
}

func ReadOnly(db interface{ Exec(string, ...any) }) {
	db.Exec("SELECT * FROM users")
	db.Exec("INSERT INTO unrelated_table (id) VALUES ($1)")
}
`

func TestPlantedWritersAreRedAndStaleEntriesFail(t *testing.T) {
	root := repoRoot(t)
	watched, err := Watched(root)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "p"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "p", "planted.go"), []byte(plantedSource), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "src"), 0o755); err != nil {
		t.Fatal(err)
	}
	hidden := "package h\n\nfunc Hidden(db interface{ Exec(string, ...any) }) { db.Exec(\"INSERT INTO users (id) VALUES ($1)\") }\n"
	if err := os.WriteFile(filepath.Join(dir, "src", "h.go"), []byte(hidden), 0o644); err != nil {
		t.Fatal(err)
	}
	found, err := Scan(dir, watched)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"p/planted.go|Concat|auth.sessions|DELETE",
		"p/planted.go|Cte|auth.principals|UPDATE",
		"p/planted.go|Direct|public.refresh_tokens|UPDATE",
		"p/planted.go|FalseUpsert|public.users|INSERT",
		"p/planted.go|Hint|public.users|INSERT",
		"p/planted.go|Other|public.password_reset_tokens|INSERT",
		"p/planted.go|QualifiedComment|public.users|UPDATE",
		"p/planted.go|QuotedCTE|public.users|UPDATE",
		"p/planted.go|Replace|public.password_reset_tokens|DELETE",
		"p/planted.go|RuntimeSchema|auth.principals|INSERT",
		"p/planted.go|Suffix|public.sso_providers|INSERT",
		"p/planted.go|Trunc|public.refresh_tokens|TRUNCATE",
		"p/planted.go|Trunc|public.users|TRUNCATE",
		"p/planted.go|Upsert|public.memberships|UPSERT",
		"p/planted.go|UsesSecond|public.login_attempts|INSERT",
		"p/planted.go|ViaAssign|auth.security_audit_events|DELETE",
		"p/planted.go|ViaBuilder|auth.sessions|DELETE",
		"p/planted.go|ViaConst|public.users|INSERT",
		"p/planted.go|pkgdecl:firstSQL|public.login_attempts|INSERT",
		"p/planted.go|pkgdecl:shadowSQL|public.org_invites|INSERT",
		"p/planted.go|pkgdecl:tableSQL|auth.sessions|TRUNCATE",
		"p/planted.go|pub.ViaMethod|auth.auth_outbox_events|UPDATE",
		"src/h.go|Hidden|public.users|INSERT",
	}
	var got []string
	for _, w := range found {
		got = append(got, w.Key())
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("planted scan mismatch\n got: %v\nwant: %v", got, want)
	}

	unlisted, stale := compare(found, nil)
	if len(unlisted) != len(want) || len(stale) != 0 {
		t.Fatalf("empty allowlist must flag every planted writer: unlisted=%d stale=%v", len(unlisted), stale)
	}

	full := make([]Entry, 0, len(found)+1)
	for _, w := range found {
		full = append(full, Entry{File: w.File, Func: w.Func, Table: w.Table, Verb: w.Verb})
	}
	if u, s := compare(found, full); len(u) != 0 || len(s) != 0 {
		t.Fatalf("exact allowlist must be green: unlisted=%v stale=%v", u, s)
	}
	full = append(full, Entry{File: "p/gone.go", Func: "Gone", Table: "public.users", Verb: "INSERT"})
	if _, s := compare(found, full); len(s) != 1 || s[0] != "p/gone.go|Gone|public.users|INSERT" {
		t.Fatalf("stale entry must fail: %v", s)
	}
	wrongVerb := append([]Entry(nil), full[:len(full)-1]...)
	wrongVerb[0].Table = "public.users"
	if u, s := compare(found, wrongVerb); len(u) != 1 || len(s) != 1 {
		t.Fatalf("a key differing in one part must be both unlisted and stale: u=%v s=%v", u, s)
	}
}

func TestWatchedReadsQuotedAndQualifiedMigrationTables(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "storage", "postgres", "authschema", "migrations")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	sql := "-- CREATE TABLE phantom (id int);\n/* CREATE TABLE phantom2 (id int); */\nCREATE TABLE public.wrong (id int);\nCREATE TABLE \"new_table\" (id int);\nCREATE TABLE IF NOT EXISTS auth.other (id int);\nCREATE UNLOGGED TABLE plain (id int);\n"
	if err := os.WriteFile(filepath.Join(dir, "0001.sql"), []byte(sql), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "0002.SQL"), []byte("CREATE TABLE uppercase_ext (id int);"), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := Watched(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, not := range []string{"auth.phantom", "auth.phantom2", "auth.public", "auth.wrong", "public.wrong"} {
		if w[not] {
			t.Errorf("%s must not be watched", not)
		}
	}
	for _, want := range []string{"auth.new_table", "auth.other", "auth.plain", "auth.uppercase_ext"} {
		if !w[want] {
			t.Errorf("%s missing from the watched set: %v", want, w)
		}
	}
}
