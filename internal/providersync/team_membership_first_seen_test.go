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
	"internal/providersync/linear_reference_catalog_effects_clickhouse.go": {role: "catalog", reuse: "linearMembershipWriter.Snapshot(", caller: "internal/providersync/linear_team_catalog_collector.go"},
	"internal/providersync/github_team_catalog_effects_clickhouse.go":      {role: "catalog", reuse: "githubMembershipWriter.Snapshot(", caller: "internal/providersync/github_team_catalog_collector.go"},
	"internal/providersync/gitlab_team_catalog_effects_clickhouse.go":      {role: "catalog", reuse: "gitlabMembershipWriter.Snapshot(", caller: "internal/providersync/gitlab_team_catalog_route.go"},
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
		return GitHubTeamMembershipKind(closable).Snapshot(scope, ProveSnapshot(SnapshotTerm{Holds: true, Reason: "read_returned"}))
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
