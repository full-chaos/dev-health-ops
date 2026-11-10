package writerinventory

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestEveryDynamicWriteSiteIsListed(t *testing.T) {
	found, err := ScanDynamic(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range dynamicProblems(found, DynamicAllowlist, DynamicClickHouseSites) {
		t.Error(p)
	}
}

func TestDynamicAllowlistIsKeyedExactly(t *testing.T) {
	watched, err := Watched(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, e := range DynamicAllowlist {
		if e.File == "" || e.Func == "" || e.Table == "" || e.Verb == "" {
			t.Errorf("entry with an empty key part: %+v", e)
		}
		switch e.Verb {
		case "INSERT", "UPSERT", "UPDATE", "DELETE", "TRUNCATE":
		default:
			t.Errorf("unknown verb: %+v", e)
		}
		if !watched[e.Table] {
			t.Errorf("row names a table that is not watched: %+v", e)
		}
		if seen[e.Key()] {
			t.Errorf("duplicate entry: %s", e.Key())
		}
		seen[e.Key()] = true
	}
	sites := map[string]bool{}
	for _, d := range DynamicClickHouseSites {
		if sites[d.Key()] {
			t.Errorf("duplicate ClickHouse site: %s", d.Key())
		}
		sites[d.Key()] = true
	}
}

func TestDynamicRowsMatchTheirEnumeratedTableSets(t *testing.T) {
	root := repoRoot(t)
	watched, err := Watched(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		file, fn string
		read     func(string) (map[string]bool, error)
		min      int
	}{
		{restoreFenceSpecs, restoreFenceApply, restoreFenceSectionWrites, 30},
		{orgDeletionFile, orgDeletionDelete, orgDeletionTargetWrites, 40},
		{userDeleteFile, userDeleteForce, userDeleteReferenceWrites, 2},
	} {
		all, err := c.read(root)
		if err != nil {
			t.Errorf("%s|%s: %v", c.file, c.fn, err)
			continue
		}
		if len(all) < c.min {
			t.Errorf("%s|%s: the enumeration read %d table writes, fewer than %d: the reader is broken", c.file, c.fn, len(all), c.min)
		}
		want := joinSorted(watchedOnly(all, watched))
		if got := siteRows(DynamicAllowlist, c.file, c.fn); !sameList(got, want) {
			t.Errorf("%s|%s: rows do not equal the enumerated watched table set\n got: %v\nwant: %v", c.file, c.fn, got, want)
		}
	}
}

func TestDynamicClickHouseSitesArePrepareBatch(t *testing.T) {
	root := repoRoot(t)
	for _, d := range DynamicClickHouseSites {
		if err := clickHouseBatchSite(root, d); err != nil {
			t.Error(err)
		}
	}
}

// clickHouseBatchSite checks that d's file imports clickhouse-go and d's function calls PrepareBatch.
func clickHouseBatchSite(root string, d DynamicSite) error {
	af, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, d.File), nil, 0)
	if err != nil {
		return err
	}
	imports := false
	for _, im := range af.Imports {
		if p, _ := strconv.Unquote(im.Path.Value); strings.HasPrefix(p, "github.com/ClickHouse/clickhouse-go/") {
			imports = true
		}
	}
	if !imports {
		return fmt.Errorf("%s: ClickHouse site in a file that does not import clickhouse-go", d.Key())
	}
	for _, decl := range af.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil || funcName(fd) != d.Func {
			continue
		}
		batch := false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if s, ok := c.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "PrepareBatch" {
					batch = true
				}
			}
			return true
		})
		if !batch {
			return fmt.Errorf("%s: ClickHouse site whose function calls no PrepareBatch", d.Key())
		}
		return nil
	}
	return fmt.Errorf("%s: no such function in the file", d.Key())
}

func TestUnresolvedVerbsReadStatementsNotProse(t *testing.T) {
	for _, c := range []struct {
		sql  string
		want string
	}{
		{"DELETE FROM %s WHERE org_id = $1", "DELETE"},
		{"WITH m AS (SELECT 1)\nu AS (UPDATE %s t SET a = 1 FROM m x WHERE t.id = x.id RETURNING 1) SELECT 1", "UPDATE"},
		{"UPDATE %s SET a = NULL WHERE a = $1", "UPDATE"},
		{"UPDATE ONLY %[1]s AS t SET a = 1", "UPDATE"},
		{"INSERT INTO %s (a) SELECT a FROM m ON CONFLICT DO NOTHING", "INSERT"},
		{"INSERT INTO %s (a) VALUES ($1) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a", "UPSERT"},
		{"INSERT INTO %s.sso_flow_states (a) VALUES ($1)", "INSERT:sso_flow_states"},
		{"DELETE FROM \x00 AS e USING r WHERE e.id = r.id", "DELETE"},
		{"DELETE FROM \x00", "DELETE"},
		{"TRUNCATE TABLE users, %s CASCADE", "TRUNCATE"},
		{"TRUNCATE %s", "TRUNCATE"},
		// prose and resolved names
		{"delete from %s: %w", ""},
		{"update %s: %w", ""},
		{"insert into %s failed", ""},
		{"truncate %s: %w", ""},
		{"DELETE FROM \x00.sessions WHERE id = $1", ""},
		{"UPDATE \x00.sessions SET a = 1", ""},
		{"DELETE FROM users WHERE id = $1", ""},
		{"INSERT INTO public.users (a) VALUES ($1) ON CONFLICT (a) DO UPDATE SET a = 1", ""},
		{"SELECT count(*) FROM %s WHERE %s = $1", ""},
		{"SELECT 1 FROM t FOR UPDATE %s", ""},
		{"-- DELETE FROM %s WHERE a\nSELECT 1", ""},
		{"SELECT 'DELETE FROM %s WHERE a'", ""},
	} {
		var got []string
		for _, h := range unresolvedVerbs(c.sql) {
			s := h.verb
			if h.table != "" {
				s += ":" + h.table
			}
			got = append(got, s)
		}
		if strings.Join(got, ",") != c.want {
			t.Errorf("unresolvedVerbs(%q) = %v, want %q", c.sql, got, c.want)
		}
	}
}

const plantedDynamicSource = `package planted

import "fmt"

const tmpl = "DELETE FROM %s WHERE id = $1"

type target struct{ table string }

func (t target) del() string { return fmt.Sprintf("DELETE FROM %s WHERE org_id = $1", t.table) }

func ViaConst(db interface{ Exec(string, ...any) }, table string) {
	db.Exec(fmt.Sprintf(tmpl, table))
}

func Concat(db interface{ Exec(string, ...any) }, table string) {
	db.Exec("UPDATE " + table + " SET a = NULL WHERE a = $1")
}

func Bare(db interface{ Exec(string, ...any) }, table string) {
	db.Exec("DELETE FROM " + table)
}

func Schema(db interface{ Exec(string, ...any) }, s string) {
	db.Exec(fmt.Sprintf("INSERT INTO %s.sessions (id) VALUES ($1)", s))
}

func Resolved(db interface{ Exec(string, ...any) }, s string) {
	db.Exec("INSERT INTO " + authschema.Quote(s) + ".principals (a) VALUES ($1)")
	db.Exec("DELETE FROM " + authschema.Quote(s) + ".sessions WHERE id = $1")
}

func Prose(table string) error {
	return fmt.Errorf("delete from %s: %w", table, nil)
}

var queries = map[string]struct{ ins string }{"a": {ins: "INSERT INTO %s (id) VALUES ($1)"}}

func Field(db interface{ Exec(string, ...any) }, k, table string) {
	db.Exec(fmt.Sprintf(queries[k].ins, table))
}

const unusedSQL = "TRUNCATE %s"
`

func TestPlantedDynamicSitesAreFoundAndChecked(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "p"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "p", "planted.go"), []byte(plantedDynamicSource), 0o644); err != nil {
		t.Fatal(err)
	}
	found, err := ScanDynamic(dir)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, d := range found {
		got = append(got, d.Key()+"|"+strings.Join(d.Named, ","))
	}
	want := []string{
		"p/planted.go|Bare|DELETE|",
		"p/planted.go|Concat|UPDATE|",
		"p/planted.go|Field|INSERT|",
		"p/planted.go|Schema|INSERT|sessions",
		"p/planted.go|ViaConst|DELETE|",
		"p/planted.go|pkgdecl:unusedSQL|TRUNCATE|",
		"p/planted.go|target.del|DELETE|",
	}
	if !sameList(got, want) {
		t.Fatalf("planted dynamic scan mismatch\n got: %v\nwant: %v", got, want)
	}

	if p := dynamicProblems(found, nil, nil); len(p) != len(want) {
		t.Fatalf("no rows must flag every planted site: %v", p)
	}
	var rows []Entry
	for _, d := range found {
		table := "public.users"
		if len(d.Named) > 0 {
			table = "auth." + d.Named[0]
		}
		rows = append(rows, Entry{File: d.File, Func: d.Func, Table: table, Verb: d.Verb})
	}
	if p := dynamicProblems(found, rows, nil); len(p) != 0 {
		t.Fatalf("a row per site must be green: %v", p)
	}
	stale := append(append([]Entry(nil), rows...), Entry{File: "p/gone.go", Func: "Gone", Table: "public.users", Verb: "DELETE"})
	if p := dynamicProblems(found, stale, nil); len(p) != 1 || !strings.Contains(p[0], "stale dynamic row") {
		t.Fatalf("a row with no site must fail: %v", p)
	}
	var unnamed []Entry
	for _, e := range rows {
		if e.Func == "Schema" {
			e.Table = "auth.principals"
		}
		unnamed = append(unnamed, e)
	}
	if p := dynamicProblems(found, unnamed, nil); len(p) != 1 || !strings.Contains(p[0], "%s.sessions") {
		t.Fatalf("a %%s.table site whose rows miss that table must fail: %v", p)
	}
	ch := []DynamicSite{{File: "p/planted.go", Func: "Bare", Verb: "DELETE"}}
	if p := dynamicProblems(found, rows, ch); len(p) != 1 || !strings.Contains(p[0], "both") {
		t.Fatalf("a site listed twice must fail: %v", p)
	}
	ch = append(ch, DynamicSite{File: "p/gone.go", Func: "Gone", Verb: "INSERT"})
	if p := dynamicProblems(found, rows[1:], ch); len(p) != 1 || !strings.Contains(p[0], "stale ClickHouse site") {
		t.Fatalf("a ClickHouse entry with no site must fail: %v", p)
	}
}

func TestEnumeratorsReadPlantedData(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, rel)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, rel), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(restoreFenceSpecs, `package restorefence
var sections = []section{
	{name: "auth.sessions", table: "sessions", kind: kindRevoke},
	{name: "auth.saml", table: "saml_assertion_uses", kind: kindInsert},
	{name: "public.x", legacy: true, table: "x", kind: kindPresence},
	{name: "public.y", legacy: true, table: "y", kind: kindReport},
	{name: "public.users.ids", legacy: true, table: "users", kind: kindTombstone},
}
`)
	got, err := restoreFenceSectionWrites(root)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"auth.saml_assertion_uses|INSERT", "auth.sessions|UPDATE", "public.users|UPDATE", "public.x|DELETE"}; !sameList(joinSorted(got), want) {
		t.Errorf("restore-fence sections: got %v, want %v", joinSorted(got), want)
	}
	write(restoreFenceSpecs, "package restorefence\nvar sections = []section{\n\t{table: \"z\", kind: kindNew},\n}\n")
	if _, err := restoreFenceSectionWrites(root); err == nil || !strings.Contains(err.Error(), "kindNew") {
		t.Errorf("an unmapped section kind must fail the read: %v", err)
	}

	write(orgDeletionTargets, "package admin\nvar orgDeletionTargets = []deletionTarget{\n\t{Table: \"a\"},\n\t{Table: \"organizations\"},\n}\n")
	got, err = orgDeletionTargetWrites(root)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"public.a|DELETE", "public.organizations|DELETE"}; !sameList(joinSorted(got), want) {
		t.Errorf("org deletion targets: got %v, want %v", joinSorted(got), want)
	}

	dump := "CREATE TABLE public.a (\n    id uuid NOT NULL,\n    user_id uuid\n);\n" +
		"CREATE TABLE public.b (\n    user_id uuid NOT NULL\n);\n" +
		"CREATE TABLE public.c (\n    user_id uuid\n);\n" +
		"CREATE TABLE public.d (\n    by_id uuid\n);\n" +
		"ALTER TABLE ONLY public.a\n    ADD CONSTRAINT a_fk FOREIGN KEY (user_id) REFERENCES public.users(id);\n" +
		"ALTER TABLE ONLY public.b\n    ADD CONSTRAINT b_fk FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE RESTRICT;\n" +
		"ALTER TABLE ONLY public.c\n    ADD CONSTRAINT c_fk FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;\n" +
		"ALTER TABLE ONLY public.d\n    ADD CONSTRAINT d_fk FOREIGN KEY (by_id) REFERENCES public.users(id) ON DELETE RESTRICT;\n"
	head := fmt.Sprintf(`{"schema": %s}`, strconv.Quote(dump))
	write(pgBaselineHead, head)
	write(pgPostBaselineSQLs+"/0001_x.sql", "CREATE TABLE q (id int);\n")
	got, err = userDeleteReferenceWrites(root)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"public.a|UPDATE", "public.d|UPDATE"}; !sameList(joinSorted(got), want) {
		t.Errorf("user references: got %v, want %v", joinSorted(got), want)
	}
	write(pgPostBaselineSQLs+"/0002_y.sql", "ALTER TABLE r ADD COLUMN u uuid REFERENCES users (id);\n")
	if _, err := userDeleteReferenceWrites(root); err == nil || !strings.Contains(err.Error(), "0002_y.sql") {
		t.Errorf("a users reference after the baseline must fail the read: %v", err)
	}
}
