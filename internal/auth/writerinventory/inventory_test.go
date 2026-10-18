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
	found, err := Scan(dir, watched)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"p/planted.go|Concat|auth.sessions|DELETE",
		"p/planted.go|Cte|auth.principals|UPDATE",
		"p/planted.go|Direct|public.refresh_tokens|UPDATE",
		"p/planted.go|Upsert|public.memberships|UPSERT",
		"p/planted.go|ViaConst|public.users|INSERT",
		"p/planted.go|pkgdecl:tableSQL|auth.sessions|TRUNCATE",
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
