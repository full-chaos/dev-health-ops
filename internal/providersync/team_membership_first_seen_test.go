package providersync

import (
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
	"internal/providersync/linear_reference_catalog_effects_clickhouse.go": {role: "catalog", reuse: "reuseLinearMembershipFirstSeen(", caller: "internal/providersync/linear_team_catalog_collector.go"},
	"internal/providersync/github_team_catalog_effects_clickhouse.go":      {role: "catalog", reuse: "reuseGitHubMembershipFirstSeen(", caller: "internal/providersync/github_team_catalog_collector.go"},
	"internal/providersync/gitlab_team_catalog_effects_clickhouse.go":      {role: "catalog", reuse: "reuseGitLabMembershipFirstSeen(", caller: "internal/providersync/gitlab_team_catalog_route.go"},
	"internal/providersync/jira_team_catalog_effects_clickhouse.go": {role: "catalog", reuse: "reuseJiraMembershipFirstSeen(", caller: "internal/providersync/jira_team_catalog_route.go",
		note: "the Jira catalog route builds no membership row today (the Jira teams are the Atlassian teams); the path is wired so a producer cannot bring the defect back"},
	"internal/atlassianteams/write.go":                     {role: "planner", note: "the Atlassian Teams writer plans its memberships (planMemberships) through PlanSnapshot: first-seen valid_from and closes of departed members"},
	"internal/providersync/jira_project_as_team_retire.go": {role: "closer", note: "one-time retraction of the Jira project-as-team rows: writes each row again with valid_to set"},
	"internal/api/teamsidentity/drift_apply.go":            {role: "operator", note: "an operator's reviewed identity-drift change: one write with the valid_from of the reviewed row, not a per-sync writer"},
}

func TestMembershipWritersReuseTheFirstSeenValidFrom(t *testing.T) {
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("no caller file")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	found := map[string]bool{}
	scanned := 0
	for _, directory := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return walkErr
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			scanned++
			ast.Inspect(parsed, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				if text, err := strconv.Unquote(literal.Value); err == nil && strings.Contains(strings.ToLower(text), "insert into team_memberships") {
					relative, _ := filepath.Rel(root, path)
					found[filepath.ToSlash(relative)] = true
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
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
			t.Errorf("%s inserts into team_memberships and is not in membershipWriters: classify it (a writer that stamps valid_from with the run time adds one open row per run)", name)
		}
	}
	for name, writer := range membershipWriters {
		if !found[name] {
			t.Errorf("membershipWriters names %s, which no longer inserts into team_memberships", name)
			continue
		}
		read := func(relative string) string {
			data, err := os.ReadFile(filepath.Join(root, relative))
			if err != nil {
				t.Fatal(err)
			}
			return string(data)
		}
		switch writer.role {
		case "catalog":
			if !strings.Contains(read(writer.caller), writer.reuse) {
				t.Errorf("%s: %s does not call %s: its memberships are written with the run time as valid_from", name, writer.caller, writer.reuse)
			}
		case "planner":
			if !strings.Contains(read(name), "PlanSnapshot(") {
				t.Errorf("%s does not plan its memberships through PlanSnapshot", name)
			}
		default:
			if strings.TrimSpace(writer.note) == "" {
				t.Errorf("%s (%s) has no note", name, writer.role)
			}
		}
	}
}
