package providersync

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The rule of CHAOS-9007 over plain values, without a server: a membership
// whose fact is already open takes the EARLIEST open valid_from (byte for
// byte); a new member keeps the valid_from its run gave it; the rule changes
// nothing but valid_from and returns one stamp per fresh row, in order.
func TestFirstSeenMembershipValidFromReusesTheEarliestOpenStamp(t *testing.T) {
	first := time.Date(2026, 10, 1, 8, 0, 0, 123_000_000, time.UTC)
	later := first.Add(time.Hour)
	latest := first.Add(2 * time.Hour)
	run := first.Add(3 * time.Hour)
	open := []MembershipSnapshotRow{
		{TeamID: "t", MemberID: "alice", ValidFrom: later},
		{TeamID: "t", MemberID: "alice", ValidFrom: first},
		{TeamID: "t", MemberID: "alice", ValidFrom: latest},
		{TeamID: "t", MemberID: "gone", ValidFrom: first},
	}
	fresh := []MembershipSnapshotRow{
		{TeamID: "t", MemberID: "alice", ValidFrom: run},
		{TeamID: "t", MemberID: "bob", ValidFrom: run},
		{TeamID: "u", MemberID: "alice", ValidFrom: run},
	}
	got := firstSeenMembershipValidFrom(fresh, open)
	want := []time.Time{first, run, run}
	if len(got) != len(want) {
		t.Fatalf("got %d stamps, want %d", len(got), len(want))
	}
	for index := range want {
		if !got[index].Equal(want[index]) || got[index].UnixNano() != want[index].UnixNano() {
			t.Errorf("stamp %d (%s|%s) = %s, want %s", index, fresh[index].TeamID, fresh[index].MemberID, got[index], want[index])
		}
	}
	if got := firstSeenMembershipValidFrom(nil, open); got != nil {
		t.Errorf("no fresh rows answered %v, want none", got)
	}
}

// membershipWriters is the named set of the code that INSERTs into
// team_memberships. A writer that is not in the table fails the census until it
// is classified; a catalog writer must call its first-seen function, and the
// writer that plans its own rows must go through the snapshot rule.
var membershipWriters = map[string]struct {
	role   string // "catalog": the run stamps valid_from, reuse is required; "planner": PlanSnapshot; "closer"/"operator": see note
	reuse  string // catalog: the function every run passes its rows through
	caller string // catalog: the file that calls it
	note   string
}{
	"internal/providersync/linear_reference_catalog_effects_clickhouse.go": {role: "catalog", reuse: "linearMembershipWriter.Snapshot(", caller: "internal/providersync/linear_team_catalog_collector.go"},
	"internal/providersync/github_team_catalog_effects_clickhouse.go":      {role: "catalog", reuse: "githubMembershipWriter.Snapshot(", caller: "internal/providersync/github_team_catalog_collector.go"},
	"internal/providersync/gitlab_team_catalog_effects_clickhouse.go":      {role: "catalog", reuse: "gitlabMembershipWriter.Snapshot(", caller: "internal/providersync/gitlab_team_catalog_route.go"},
	"internal/providersync/jira_team_catalog_effects_clickhouse.go": {role: "catalog", reuse: "reuseJiraMembershipFirstSeen(", caller: "internal/providersync/jira_team_catalog_route.go",
		note: "the Jira catalog route builds no membership row today (the Jira teams are the Atlassian teams); the path is wired so a producer cannot bring the defect back"},
	"internal/atlassianteams/write.go":                     {role: "planner", note: "the Atlassian Teams writer plans its memberships (planMemberships) through PlanSnapshot: first-seen valid_from and closes of departed members"},
	"internal/providersync/jira_project_as_team_retire.go": {role: "closer", note: "one-time retraction of the Jira project-as-team rows: writes each row again with valid_to set"},
	"internal/providersync/team_id_carry.go":               {role: "carry", note: "the one-shot team id carry: moves the links of an old team id to the new one, keeping the earliest valid_from of the old id (min(valid_from)), so it adds no open row per run"},
	"internal/providersync/team_roster_move.go":            {role: "operator", note: "the one-time move of the roster entries of admin-made teams (CHAOS-9087): stamps valid_from with the run time, and writes a row only for an entry no open row of its team covers, so a second run writes none"},
	"internal/api/teamsidentity/drift_apply.go":            {role: "operator", note: "an operator's reviewed identity-drift change: one write with the valid_from of the reviewed row, not a per-sync writer"},
}

// membershipSQLFold evaluates the string an expression builds, as far as it is
// a constant: literals, "+", the constants of the package, fmt.Sprintf with the
// verbs left as an unknown part. An unknown part is the byte 0, so a statement
// built from a variable table name is still recognised by its fixed text.
func membershipSQLFold(expression ast.Expr, constants map[string]string) string {
	switch typed := expression.(type) {
	case *ast.BasicLit:
		if typed.Kind == token.STRING {
			if text, err := strconv.Unquote(typed.Value); err == nil {
				return text
			}
		}
	case *ast.ParenExpr:
		return membershipSQLFold(typed.X, constants)
	case *ast.BinaryExpr:
		if typed.Op == token.ADD {
			// Flattened, not recursed: a generated file may chain thousands of terms.
			var operands []ast.Expr
			var current ast.Expr = typed
			for {
				sum, ok := current.(*ast.BinaryExpr)
				if !ok || sum.Op != token.ADD {
					operands = append(operands, current)
					break
				}
				operands = append(operands, sum.Y)
				current = sum.X
			}
			var text strings.Builder
			for index := len(operands) - 1; index >= 0; index-- {
				text.WriteString(membershipSQLFold(operands[index], constants))
			}
			return text.String()
		}
	case *ast.Ident:
		if text, ok := constants[typed.Name]; ok {
			return text
		}
	case *ast.CallExpr:
		if selector, ok := typed.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Sprintf" && len(typed.Args) > 0 {
			format := membershipSQLFold(typed.Args[0], constants)
			for _, verb := range []string{"%s", "%v", "%q"} {
				format = strings.ReplaceAll(format, verb, "\x00")
			}
			return format
		}
	}
	return "\x00"
}

func membershipSQLNormalize(text string) string {
	return strings.Join(strings.Fields(strings.ToLower(text)), " ")
}

// membershipWriterFiles finds the files that INSERT into team_memberships by
// what they build, not by one spelling: any string a file builds (a literal, a
// concatenation, a package constant, a Sprintf) that is an INSERT into the
// table, with whitespace and case ignored; or an INSERT into a table named
// by a variable in a file that names team_memberships (the carry verb takes
// its table from a list). Every non-test Go file of the module is read.
func membershipWriterFiles(t *testing.T, root string) (found map[string]string, scanned int) {
	t.Helper()
	found = map[string]string{}
	type source struct {
		relative string
		parsed   *ast.File
	}
	byDirectory := map[string][]source{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "vendor", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		scanned++
		relative, _ := filepath.Rel(root, path)
		directory := filepath.Dir(path)
		byDirectory[directory] = append(byDirectory[directory], source{filepath.ToSlash(relative), parsed})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, sources := range byDirectory {
		constants := map[string]string{}
		for pass := 0; pass < 3; pass++ {
			for _, file := range sources {
				for _, declaration := range file.parsed.Decls {
					general, ok := declaration.(*ast.GenDecl)
					if !ok || general.Tok != token.CONST {
						continue
					}
					for _, spec := range general.Specs {
						value, ok := spec.(*ast.ValueSpec)
						if !ok || len(value.Names) != len(value.Values) {
							continue
						}
						for index, name := range value.Names {
							constants[name.Name] = membershipSQLFold(value.Values[index], constants)
						}
					}
				}
			}
		}
		for _, file := range sources {
			namesTable := false
			ast.Inspect(file.parsed, func(node ast.Node) bool {
				if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
					if text, err := strconv.Unquote(literal.Value); err == nil && strings.Contains(strings.ToLower(text), "team_memberships") {
						namesTable = true
					}
				}
				return true
			})
			ast.Inspect(file.parsed, func(node ast.Node) bool {
				switch node.(type) {
				case *ast.BasicLit, *ast.BinaryExpr, *ast.CallExpr, *ast.Ident:
				default:
					return true
				}
				text := membershipSQLNormalize(membershipSQLFold(node.(ast.Expr), constants))
				switch {
				case strings.Contains(text, "insert into team_memberships"):
					found[file.relative] = "direct"
				case namesTable && (strings.Contains(text, "insert into \x00") || strings.Contains(text, "insert into %")):
					if found[file.relative] == "" {
						found[file.relative] = "table named by a variable"
					}
				}
				// A concatenation was folded whole: its terms are not folded again.
				if sum, isSum := node.(*ast.BinaryExpr); isSum && sum.Op == token.ADD {
					return false
				}
				return true
			})
		}
	}
	return found, scanned
}

// fileCallsFunction is true when the file holds a CALL of the named function
// or method (a comment or a string that names it does not count).
func fileCallsFunction(t *testing.T, path, name string) bool {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			called = called || fun.Name == name
		case *ast.SelectorExpr:
			if receiver, method, qualified := strings.Cut(name, "."); qualified {
				if x, ok := fun.X.(*ast.Ident); ok {
					called = called || (x.Name == receiver && fun.Sel.Name == method)
				}
			} else {
				called = called || fun.Sel.Name == name
			}
		case *ast.IndexExpr:
			if ident, ok := fun.X.(*ast.Ident); ok {
				called = called || ident.Name == name
			}
		case *ast.IndexListExpr:
			if ident, ok := fun.X.(*ast.Ident); ok {
				called = called || ident.Name == name
			}
		}
		return true
	})
	return called
}

func TestMembershipWritersReuseTheFirstSeenValidFrom(t *testing.T) {
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("no caller file")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	found, scanned := membershipWriterFiles(t, root)
	if scanned < 200 || len(found) == 0 {
		t.Fatalf("the scan read %d files and found %d writers: it measured nothing", scanned, len(found))
	}
	var names []string
	for name := range found {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, named := membershipWriters[name]; !named {
			t.Errorf("%s inserts into team_memberships (%s) and is not in membershipWriters: classify it (a writer that stamps valid_from with the run time adds one open row per run)", name, found[name])
		}
	}
	for name, writer := range membershipWriters {
		if _, ok := found[name]; !ok {
			t.Errorf("membershipWriters names %s, which no longer inserts into team_memberships", name)
			continue
		}
		switch writer.role {
		case "catalog":
			function := strings.TrimSuffix(writer.reuse, "(")
			if !fileCallsFunction(t, filepath.Join(root, writer.caller), function) {
				t.Errorf("%s: %s holds no call of %s: its memberships are written with the run time as valid_from", name, writer.caller, function)
			}
		case "planner":
			if !fileCallsFunction(t, filepath.Join(root, name), "PlanSnapshot") {
				t.Errorf("%s does not plan its memberships through PlanSnapshot", name)
			}
		case "carry":
			data, err := os.ReadFile(filepath.Join(root, name))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), "min(valid_from)") {
				t.Errorf("%s moves memberships to a new team id and no longer reads the first valid_from (min(valid_from)) of the old id", name)
			}
		default:
			if strings.TrimSpace(writer.note) == "" {
				t.Errorf("%s (%s) has no note", name, writer.role)
			}
		}
	}
}

// The writer finder reads what code BUILDS: each spelling below is a real
// writer, and the one after them is not.
func TestMembershipWriterFinderRecognisesEverySpellingOfTheInsert(t *testing.T) {
	spellings := map[string]struct {
		source string
		want   bool
	}{
		"literal":        {`package p; var q = "INSERT INTO team_memberships (a)"`, true},
		"concatenation":  {`package p; var q = "INSERT INTO " + "team_memberships" + " (a)"`, true},
		"constant":       {`package p; const table = "team_memberships"; var q = "INSERT INTO " + table + " (a)"`, true},
		"newline":        {"package p; var q = `INSERT INTO\n   team_memberships (a)`", true},
		"sprintf":        {`package p; import "fmt"; var q = fmt.Sprintf("INSERT INTO %s (a)", "team_memberships")`, true},
		"variable table": {`package p; var tables = []string{"team_memberships"}; func w(t string) string { return "INSERT INTO " + t + " (a)" }`, true},
		"another table":  {`package p; var q = "INSERT INTO team_repo_ownership (a)"`, false},
		"a comment":      {"package p\n// INSERT INTO team_memberships (a)\nvar q = 1", false},
	}
	for name, spelling := range spellings {
		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, "w.go"), []byte(spelling.source), 0o600); err != nil {
			t.Fatal(err)
		}
		found, _ := membershipWriterFiles(t, directory)
		if got := len(found) == 1; got != spelling.want {
			t.Errorf("%s: found = %v, want %v (%v)", name, found, spelling.want, spelling.source)
		}
	}
	// A file under tools/ is read like any other.
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "tools", "plant"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "tools", "plant", "main.go"), []byte(`package main; var q = "insert into team_memberships (a)"`), 0o600); err != nil {
		t.Fatal(err)
	}
	if found, _ := membershipWriterFiles(t, root); len(found) != 1 {
		t.Errorf("a writer under tools/ was not found: %v", found)
	}
}

// A fresh row whose own valid_from is EARLIER than every open one keeps its
// own: the rule reuses an earlier stamp, never a later one.
func TestFirstSeenMembershipValidFromNeverMovesAStampLater(t *testing.T) {
	stored := time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC)
	earlier := stored.Add(-48 * time.Hour)
	got := firstSeenMembershipValidFrom(
		[]MembershipSnapshotRow{{TeamID: "t", MemberID: "alice", ValidFrom: earlier}},
		[]MembershipSnapshotRow{{TeamID: "t", MemberID: "alice", ValidFrom: stored}})
	if len(got) != 1 || !got[0].Equal(earlier) {
		t.Errorf("a fresh stamp earlier than the stored one became %v, want %v", got, earlier)
	}
}

type firstSeenTestRow struct {
	provider, source, team, member string
	from                           time.Time
}

var firstSeenTestAccessors = MembershipFirstSeenAccessors[firstSeenTestRow]{
	Provider:  func(row firstSeenTestRow) string { return row.provider },
	Source:    func(row firstSeenTestRow) string { return row.source },
	TeamID:    func(row firstSeenTestRow) string { return row.team },
	MemberID:  func(row firstSeenTestRow) string { return row.member },
	ValidFrom: func(row firstSeenTestRow) time.Time { return row.from },
	SetFrom:   func(row *firstSeenTestRow, from time.Time) { row.from = from },
}

// A writer that cannot read the open rows must not write: no connection and no
// organization are errors, never "no open rows".
func TestReuseFirstSeenMembershipValidFromRefusesWithoutAConnectionOrAnOrganization(t *testing.T) {
	rows := []firstSeenTestRow{{provider: "github", source: "provider_access", team: "t", member: "m", from: time.Now()}}
	if _, err := ReuseFirstSeenMembershipValidFrom(context.Background(), nil, "org", rows, firstSeenTestAccessors); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("a nil connection answered %v, want ErrInvalidConfiguration", err)
	}
	if _, err := ReuseFirstSeenMembershipValidFrom[firstSeenTestRow](context.Background(), nil, "  ", rows, firstSeenTestAccessors); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("a blank organization answered %v, want ErrInvalidConfiguration", err)
	}
	if got, err := ReuseFirstSeenMembershipValidFrom(context.Background(), nil, "org", []firstSeenTestRow(nil), firstSeenTestAccessors); err != nil || got != nil {
		t.Errorf("no rows answered (%v, %v), want nothing and no error", got, err)
	}
}

type failingRows struct {
	driver.Rows
	err error
}

func (rows failingRows) Next() bool   { return false }
func (rows failingRows) Close() error { return nil }
func (rows failingRows) Err() error   { return rows.err }

type fakeMembershipQuerier struct {
	queryErr, rowsErr error
}

func (fake fakeMembershipQuerier) Query(context.Context, string, ...any) (driver.Rows, error) {
	if fake.queryErr != nil {
		return nil, fake.queryErr
	}
	return failingRows{err: fake.rowsErr}, nil
}

// The read fails loud at both places it can fail: the query, and the rows
// after the last block. Neither is "no open rows" (that would stamp the run
// time on every fact again).
func TestReadOpenMembershipsFailsLoudWhenTheQueryOrTheRowsFail(t *testing.T) {
	boom := errors.New("boom")
	if _, err := readOpenMemberships(context.Background(), fakeMembershipQuerier{queryErr: boom}, "org", "github", "provider_access"); !errors.Is(err, boom) {
		t.Errorf("a failed query answered %v, want the query error", err)
	}
	if _, err := readOpenMemberships(context.Background(), fakeMembershipQuerier{rowsErr: boom}, "org", "github", "provider_access"); !errors.Is(err, boom) {
		t.Errorf("a failure after the last block answered %v, want the rows error", err)
	}
	if open, err := readOpenMemberships(context.Background(), fakeMembershipQuerier{}, "org", "github", "provider_access"); err != nil || len(open) != 0 {
		t.Errorf("an empty read answered (%v, %v), want no rows and no error", open, err)
	}
}

// CHAOS-9079 over plain values, without a server: which open memberships a run
// closes. A member is closed only when its TEAM is closable (the team's own
// member read proved its end in a scope no other integration reads), the member
// is absent from what the provider RETURNED (never from what the conflict guard
// keeps), and the kind's proofs hold.
func TestPlanMembershipSnapshotClosesOnlyAMemberAbsentFromACompleteRead(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	run := t0.Add(5 * time.Hour)
	open := []openMembership{
		{TeamID: "a", MemberID: "alice", ValidFrom: t0},
		{TeamID: "a", MemberID: "bob", ValidFrom: t0},
		{TeamID: "a", MemberID: "carol", ValidFrom: t0},
		{TeamID: "a", MemberID: "carol", ValidFrom: t1}, // a surplus open row of the same fact
		{TeamID: "a", MemberID: "erin", ValidFrom: t0},  // observed, kept out by the conflict guard
		{TeamID: "b", MemberID: "dave", ValidFrom: t0},  // another team: its read did not prove its end
	}
	observed := []MembershipSnapshotRow{
		{TeamID: "a", MemberID: "alice", ValidFrom: run},
		{TeamID: "a", MemberID: "erin", ValidFrom: run},
		{TeamID: "a", MemberID: "frank", ValidFrom: run},
	}
	holds := ScopeProof{stated: true}
	snapshot := func(closable []string, scope ScopeProof) KindSnapshot[MembershipSnapshotRow] {
		return GitHubTeamMembershipKind(closable).Snapshot(scope, ProveSnapshot(SnapshotTerm{Holds: true, Reason: "read_returned"}), AbsenceByCloseWriter[MembershipSnapshotRow]())
	}
	closed := func(retractions []membershipRetraction) map[string]int {
		out := map[string]int{}
		for _, retraction := range retractions {
			out[retraction.open.TeamID+"|"+retraction.open.MemberID]++
			if !retraction.closedAt.Equal(run) {
				t.Errorf("%s closed at %s, want the run time %s", retraction.open.MemberID, retraction.closedAt, run)
			}
		}
		return out
	}

	stamp, retractions, _ := planMembershipSnapshot(open, observed, run, snapshot([]string{"a"}, holds))
	got := closed(retractions)
	if got["a|bob"] != 1 || got["a|carol"] != 2 || len(got) != 2 {
		t.Errorf("closed %v, want bob once and both open rows of carol, nobody else (alice and erin are listed, dave's team is not closable)", got)
	}
	if !stamp["a\x00alice"].Equal(t0) || !stamp["a\x00frank"].Equal(run) {
		t.Errorf("stamps alice=%s frank=%s, want alice on her first-seen %s and the new member on the run time %s", stamp["a\x00alice"], stamp["a\x00frank"], t0, run)
	}

	// A scope another integration may share: nothing closes.
	_, retractions, _ = planMembershipSnapshot(open, observed, run,
		snapshot([]string{"a"}, ScopeProof{stated: true, missing: []string{OwnershipCloseSkippedScopeShared}}))
	if len(retractions) != 0 {
		t.Errorf("a shared scope closed %d rows, want none", len(retractions))
	}
	// No team closable (no member read proved its end): nothing closes.
	_, retractions, _ = planMembershipSnapshot(open, observed, run, snapshot(nil, holds))
	if len(retractions) != 0 {
		t.Errorf("no closable team closed %d rows, want none", len(retractions))
	}
	// A closable team whose complete read lists nobody: every open member of it closes.
	_, retractions, _ = planMembershipSnapshot(open, nil, run, snapshot([]string{"a"}, holds))
	if got := closed(retractions); len(got) != 4 || got["a|erin"] != 1 {
		t.Errorf("an empty complete read closed %v, want the 5 open rows of team a (4 members)", got)
	}
}
