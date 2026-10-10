package providersync

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// testEveryRowKind is a kind that holds every ownership row, for the tests of
// the rule's own arithmetic (first-seen valid_from, what a close closes).
func testEveryRowKind(empty EmptyAnswer) SnapshotKind[OwnershipSnapshotRow] {
	return NewSnapshotKind("test_every_row", empty, func(OwnershipSnapshotRow) bool { return true })
}

// testSoleScope is the scope gate's answer for a run that is the only active
// integration of its provider in the organization.
func testSoleScope() ScopeProof {
	return ProveSoleScope(context.Background(), staticScopeCensus{}, "org-1", "test", "integration-a")
}

// testSharedScope is the gate's answer when one other active integration of
// the provider exists.
func testSharedScope() ScopeProof {
	return ProveSoleScope(context.Background(), staticScopeCensus{siblings: 1}, "org-1", "test", "integration-a")
}

// testProof is a one-term proof.
func testProof(holds bool) SnapshotProof {
	return ProveSnapshot(SnapshotTerm{Holds: holds, Reason: "test_walk_not_read_to_the_end"})
}

func TestPlanOwnershipSnapshotKeepsFirstSeenAndRetractsTheRest(t *testing.T) {
	first := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	later := first.Add(24 * time.Hour)
	now := first.Add(48 * time.Hour)
	fact := func(team, project, source string, validFrom time.Time) OwnershipSnapshotRow {
		return OwnershipSnapshotRow{TeamID: team, ProjectID: testPID(project), Source: source, ValidFrom: validFrom}
	}
	proven := testEveryRowKind(EmptyIsAnAnswer).Snapshot(testSoleScope(), testProof(true))
	fresh := []OwnershipSnapshotRow{fact("T", "10001", "native", now), fact("T", "10002", "native", now)}
	open := []OwnershipSnapshotRow{
		fact("T", "10001", "native", later),              // 0: a later duplicate of a held fact
		fact("T", "10001", "native", first),              // 1: the held fact, first seen
		fact("T", "org-1:jira:OPS", "native", first),     // 2: an id form no writer produces any more
		fact("T", "10001", "jira_legacy", first),         // 3: same team and project, another source
		fact("U", "10001", "native", first),              // 4: same project, another team
		fact("T", "10003", "native", now.Add(time.Hour)), // 5: opened after this run's time
	}
	plan := PlanOwnershipSnapshot(fresh, open, now, proven)
	if len(plan.ValidFrom) != 2 || !plan.ValidFrom[0].Equal(first) || !plan.ValidFrom[1].Equal(now) {
		t.Fatalf("valid_from=%v, want the held fact on its first-seen stamp and the new fact on its own", plan.ValidFrom)
	}
	want := []OwnershipSnapshotRetraction{
		{Open: 0, ClosedAt: now}, {Open: 2, ClosedAt: now}, {Open: 3, ClosedAt: now}, {Open: 4, ClosedAt: now},
		{Open: 5, ClosedAt: now.Add(time.Hour)}, // valid_to is never before valid_from
	}
	if !reflect.DeepEqual(plan.Retract, want) {
		t.Fatalf("retract=%+v\n   want=%+v", plan.Retract, want)
	}
	if got := plan.Kinds[0]; got.Fresh != 2 || got.Open != 6 || got.Closed != 5 || len(got.Abandoned) != 0 {
		t.Fatalf("outcome=%+v, want 2 fresh, 6 open, 5 closed, not abandoned", got)
	}

	// The same data again: the held fact is written on the same key, nothing is closed.
	again := PlanOwnershipSnapshot(fresh[:1], open[1:2], now.Add(time.Hour), proven)
	if !again.ValidFrom[0].Equal(first) || len(again.Retract) != 0 {
		t.Fatalf("second run: %+v", again)
	}

	// A fresh row older than every open row of its fact keeps its own stamp;
	// the open row is then a later duplicate.
	older := PlanOwnershipSnapshot([]OwnershipSnapshotRow{fact("T", "10001", "native", first.Add(-time.Hour))}, open[1:2], now, proven)
	if !older.ValidFrom[0].Equal(first.Add(-time.Hour)) {
		t.Fatalf("an older fresh row moved to %v", older.ValidFrom[0])
	}

	// No fresh row, and the kind says an empty answer is an answer: every open
	// row of the kind is closed. The caller's read is the scope.
	empty := PlanOwnershipSnapshot(nil, open[:2], now, proven)
	if len(empty.ValidFrom) != 0 || len(empty.Retract) != 2 {
		t.Fatalf("empty snapshot of a kind whose empty answer is an answer: %+v, want both open rows closed", empty)
	}

	// The same answer for a kind whose empty answer closes nothing.
	kept := PlanOwnershipSnapshot(nil, open[:2], now, testEveryRowKind(EmptyClosesNothing).Snapshot(testSoleScope(), testProof(true)))
	if len(kept.Retract) != 0 || !reflect.DeepEqual(kept.Kinds[0].Abandoned, []string{SnapshotEmptyAnswer}) {
		t.Fatalf("empty snapshot of a kind whose empty answer closes nothing: %+v", kept)
	}
}

// A snapshot that did not read its source to the end closes nothing: a row
// that is missing from a part of the answer is not a fact the provider
// dropped. A proof nobody stated, a proof with a term that does not hold, a
// term with no reason and a run with no kind at all close nothing either. The
// first-seen valid_from still applies, so the write adds no row.
func TestPlanOwnershipSnapshotClosesNothingForASnapshotThatIsNotComplete(t *testing.T) {
	first := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	now := first.Add(48 * time.Hour)
	fresh := []OwnershipSnapshotRow{{TeamID: "T", ProjectID: testPID("10001"), Source: "native", ValidFrom: now}}
	open := []OwnershipSnapshotRow{
		{TeamID: "T", ProjectID: testPID("10001"), Source: "native", ValidFrom: first},
		{TeamID: "T", ProjectID: testPID("10001"), Source: "native", ValidFrom: first.Add(time.Hour)}, // a later duplicate
		{TeamID: "T", ProjectID: testPID("20002"), Source: "native", ValidFrom: first},                // not in this part of the answer
	}
	kind := testEveryRowKind(EmptyIsAnAnswer)
	for name, c := range map[string]struct {
		fresh  []OwnershipSnapshotRow
		kinds  []KindSnapshot[OwnershipSnapshotRow]
		reason string
	}{
		"stated not complete":      {fresh, []KindSnapshot[OwnershipSnapshotRow]{kind.Snapshot(testSoleScope(), testProof(false))}, "test_walk_not_read_to_the_end"},
		"proof not stated":         {fresh, []KindSnapshot[OwnershipSnapshotRow]{kind.Snapshot(testSoleScope(), SnapshotProof{})}, snapshotProofNotStated},
		"a term with no reason":    {fresh, []KindSnapshot[OwnershipSnapshotRow]{kind.Snapshot(testSoleScope(), ProveSnapshot(SnapshotTerm{Holds: true}))}, "snapshot_term_without_reason"},
		"one of two terms":         {fresh, []KindSnapshot[OwnershipSnapshotRow]{kind.Snapshot(testSoleScope(), ProveSnapshot(SnapshotTerm{Holds: true, Reason: "a"}, SnapshotTerm{Holds: false, Reason: "b"}))}, "b"},
		"no fresh row, not proven": {nil, []KindSnapshot[OwnershipSnapshotRow]{kind.Snapshot(testSoleScope(), testProof(false))}, "test_walk_not_read_to_the_end"},
		"a kind nobody made":       {fresh, []KindSnapshot[OwnershipSnapshotRow]{{}}, "snapshot_kind_not_made"},
		"no kind at all":           {fresh, nil, ""},
	} {
		plan := PlanOwnershipSnapshot(c.fresh, open, now, c.kinds...)
		if len(plan.Retract) != 0 {
			t.Errorf("%s: retract=%+v, want nothing closed", name, plan.Retract)
		}
		if len(c.fresh) == 1 && (len(plan.ValidFrom) != 1 || !plan.ValidFrom[0].Equal(first)) {
			t.Errorf("%s: valid_from=%v, want the first-seen stamp", name, plan.ValidFrom)
		}
		if c.reason == "" {
			if plan.OpenOfNoKind != len(open) || len(plan.Abandoned()) != 0 {
				t.Errorf("%s: open of no kind = %d, abandoned = %+v, want every open row of no kind", name, plan.OpenOfNoKind, plan.Abandoned())
			}
			continue
		}
		if got := plan.SnapshotReasons(); !reflect.DeepEqual(got, []string{c.reason}) {
			t.Errorf("%s: reasons=%v, want %q", name, got, c.reason)
		}
	}
	if complete := PlanOwnershipSnapshot(fresh, open, now, kind.Snapshot(testSoleScope(), testProof(true))); len(complete.Retract) != 2 {
		t.Fatalf("the same rows as a proven snapshot: retract=%+v, want the duplicate and the lost fact closed", complete.Retract)
	}
}

// The census below keeps "who writes team_project_ownership" and "who plans a
// write with the shared snapshot rule" two named sets.
//
// What it pins: (1) the set of Go functions of the production tree that hold
// an INSERT INTO team_project_ownership statement, or name a package-level
// constant that holds one; (2) that each of them that writes Jira rows names
// the function of its package that plans its rows, that this function calls
// the snapshot rule and that production code of the package calls it;
// (3) the set of functions that call the snapshot rule; (4) that each of them
// takes the typed per-kind snapshot (a KindSnapshot parameter, or a proof it
// makes from named terms), never a bool, and names where the proof comes
// from. What it does not pin: a statement built from parts, a write outside
// Go, and the data flow between the planner and the insert: that the rows a
// writer inserts are the planned ones is pinned by behaviour, against a real
// ClickHouse (TestProjectIdentityIsOneIDAcrossCatalogOwnershipAndWorkItems,
// TestAnAtlassianTeamsRunClosesTheKeyBuiltProjectLinks).

type ownershipWriter struct {
	provider string
	// planner is the function that plans this writer's rows through the
	// snapshot rule. Every jira writer that writes from a provider snapshot
	// has one.
	planner string
	// complete says where the planner's proof comes from: the end-of-data
	// signal behind each term. Every planner has one.
	complete string
	// note says what the writer does when it has no planner.
	note string
	// retiresClass says which class of rows the writer closes as a whole. It
	// inserts closed rows only (valid_to set) and reads no provider snapshot,
	// so it has no planner and no completeness.
	retiresClass string
}

var ownershipWriters = map[string]ownershipWriter{
	"internal/providersync.JiraTeamCatalogClickHouseEffects.writeOwnership": {
		provider: "jira", planner: "internal/providersync.jiraOwnershipSnapshot",
		complete: "its KindSnapshot argument = JiraLegacyOwnershipKind with the terms JiraTeamCatalogResult.ProjectSearchComplete (the " +
			"search walk in JiraTeamCatalogRouteHandler.CollectTeamCatalog, jira_team_catalog_route.go: true only at a page's endOfData) AND the " +
			"legacy links read finished (jiraLegacyProjectOwnershipLinks, jira_team_catalog_effects_clickhouse.go); an empty answer closes nothing",
	},
	"internal/atlassianteams.writeOwnership": {
		provider: "jira", planner: "internal/atlassianteams.planOwnership",
		complete: "its KindSnapshot argument = AtlassianTeamLinkKind with the term Rows.ProjectLinksComplete (atlassianteams.Collect, " +
			"collect.go: one finished project-link read for every active team; each read follows the cursor to the end or fails Collect)",
	},
	"internal/providersync.RetireJiraProjectAsTeamRows": {
		provider: "jira", retiresClass: "the open project-as-team rows (source 'native', team_id = project_key, team not an " +
			"Atlassian team): each is written again with valid_to set, none is opened; pinned against a real ClickHouse by " +
			"TestRetireJiraProjectAsTeamRows",
	},
	"internal/providersync.LinearReferenceCatalogClickHouseEffects.writeOwnership": {
		provider: "linear", note: "plain insert of the rows LinearTeamCatalogCollector.CollectTeamCatalog plans through " +
			"LinearReferenceCatalogClickHouseEffects.SnapshotOwnership (linearOwnershipSnapshot, pinned in repoOwnershipPlanners)",
	},
	"internal/providersync.GitLabTeamCatalogClickHouseEffects.writeOwnership": {
		provider: "gitlab", note: "plain insert of the rows GitLabTeamCatalogCollector.CollectTeamCatalog plans through " +
			"GitLabTeamCatalogClickHouseEffects.SnapshotOwnership (gitlabOwnershipSnapshot, pinned in repoOwnershipPlanners)",
	},
}

// repoOwnershipPlanners are the planners of the writers of team_repo_ownership
// (a different table from the one the census above scans for writers) and of
// the catalogs whose sink wraps the planner. They are pinned like the planners
// above: each calls the snapshot rule with the typed snapshot it is given and
// names where the proof comes from.
var repoOwnershipPlanners = map[string]ownershipWriter{
	"internal/providersync.GitHubTeamCatalogClickHouseEffects.SnapshotTeamRepoOwnership": {
		provider: "github", planner: "internal/providersync.githubRepoOwnershipSnapshot",
		complete: "SnapshotTeamRepoOwnership passes ownershipCloseDecision.snapshot(GitHubTeamRepoGrantKind): the kind holds only " +
			"decideOwnershipClose's closable set, the teams of githubTeamCatalogRows.RepoListedTeamIDs whose listing proved its end " +
			"(ownershipListingProvesEnd) in a scope no other active GitHub integration of the org could list (ownership_close_gate.go)",
	},
	"internal/providersync.LinearReferenceCatalogClickHouseEffects.SnapshotOwnership": {
		provider: "linear", planner: "internal/providersync.linearOwnershipSnapshot",
		complete: "SnapshotOwnership passes linearOwnershipKindSnapshots: LinearProjectOwnershipKind with the terms " +
			"LinearReferenceCatalogEvidence.ProjectsComplete (every project page and project-team page reached a stated end) AND no " +
			"project-team link without a key; LinearTeamKeyOwnershipKind with the term TeamsComplete (the team walk reached its end). " +
			"An empty answer of a kind closes no row of that kind (linear_team_catalog_collector.go)",
	},
	"internal/providersync.GitLabTeamCatalogClickHouseEffects.SnapshotOwnership": {
		provider: "gitlab", planner: "internal/providersync.gitlabOwnershipSnapshot",
		complete: "SnapshotOwnership passes ownershipCloseDecision.snapshot(GitLabGroupProjectGrantKind): the kind holds only " +
			"decideOwnershipClose's closable set, the teams of GitLabTeamCatalogRows.OwnershipListedTeamIDs whose /projects listing " +
			"proved its end (ownershipListingProvesEnd) in a group path no other active GitLab integration of the org could list " +
			"(ownership_close_gate.go)",
	},
}

// otherSnapshotPlanners call the snapshot rule for a fact kind that is not an
// ownership row. Each names where its proof comes from.
var otherSnapshotPlanners = map[string]string{
	"internal/atlassianteams.planMemberships": "its KindSnapshot argument = AtlassianTeamMembershipKind with the term " +
		"Rows.MembershipsComplete (atlassianteams.Collect: one finished member read for every active team)",
	"internal/providersync.planMembershipSnapshot": "its KindSnapshot arguments = LinearTeamMembershipKind, GitHubTeamMembershipKind and " +
		"GitLabTeamMembershipKind, made in the collectors from decideOwnershipClose (the member reads' end proof and the sole-scope gate); " +
		"a team outside the closable set is of no kind (CHAOS-9079, membership_departure.go)",
	"internal/providersync.firstSeenMembershipValidFrom": "passes the rule no KindSnapshot: it reuses the first-seen valid_from of a " +
		"membership fact the run holds again and closes nothing, so it has no proof to make (CHAOS-9007, membership_first_seen.go)",
	"internal/atlassianteams.teamsInScope": "makes its proof in place: the team catalog kind with the term Rows.TeamSearchComplete " +
		"(atlassianteams.Collect: the team search followed the cursor to its end); a search that answers no team closes nothing",
}

var ownershipInsertStatement = regexp.MustCompile(`(?is)\binsert\s+into\s+team_project_ownership\b`)

// ownershipSnapshotEntryPoints are the two names of the one snapshot rule.
// PlanOwnershipSnapshot is PlanSnapshot keyed for ownership rows.
var ownershipSnapshotEntryPoints = map[string]bool{"PlanOwnershipSnapshot": true, "PlanSnapshot": true}

const ownershipSnapshotWrapper = "internal/providersync.PlanOwnershipSnapshot"

type ownershipCensus struct {
	files    int
	writers  []string
	planners []string
	// calls: function -> the names of every function it calls.
	calls map[string]map[string]bool
	// takesKind: the function has a parameter of type KindSnapshot[...].
	takesKind map[string]bool
	// boolParams: function -> its parameters of type bool.
	boolParams map[string][]string
	// closes: the function turns the retractions of a plan into rows
	// (it ranges over a .Retract).
	closes map[string]bool
	// setsValidTo: the function assigns a value other than nil to a ValidTo
	// or closedAt field.
	setsValidTo map[string]bool
	// kinds: constructor function -> {name literal, empty-answer identifier}
	// of each NewSnapshotKind call it holds.
	kinds map[string][][2]string
	// kindFiles: the files that hold a NewSnapshotKind call.
	kindFiles map[string]bool
	// constantTerms: the functions that hold a SnapshotTerm literal whose
	// Holds is the constant true or false, or whose Reason is not a name.
	constantTerms []string
	// scopeMakers: the functions that hold a ScopeProof literal with a field
	// (the zero literal is the unproven value and makes nothing).
	scopeMakers []string
	// censusCallers: the functions that call CountActiveSiblingIntegrations.
	censusCallers []string
}

func ownershipCensusFunctionName(directory string, function *ast.FuncDecl) string {
	name := function.Name.Name
	if function.Recv != nil && len(function.Recv.List) == 1 {
		receiver := function.Recv.List[0].Type
		if star, ok := receiver.(*ast.StarExpr); ok {
			receiver = star.X
		}
		if index, ok := receiver.(*ast.IndexExpr); ok {
			receiver = index.X
		}
		if identifier, ok := receiver.(*ast.Ident); ok {
			name = identifier.Name + "." + name
		}
	}
	return directory + "." + name
}

// censusMentions reports whether the type expression names identifier.
func censusMentions(expression ast.Expr, identifier string) bool {
	found := false
	ast.Inspect(expression, func(node ast.Node) bool {
		if name, ok := node.(*ast.Ident); ok && name.Name == identifier {
			found = true
		}
		return true
	})
	return found
}

func scanOwnershipCensus(t *testing.T, repoRoot string, roots ...string) ownershipCensus {
	t.Helper()
	type parsedFile struct {
		directory, path string
		file            *ast.File
	}
	var parsed []parsedFile
	// statements: directory -> the package-level names that hold the statement.
	statements := map[string]map[string]bool{}
	for _, root := range roots {
		err := filepath.WalkDir(filepath.Join(repoRoot, root), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(repoRoot, filepath.Dir(path))
			if err != nil {
				return err
			}
			directory := filepath.ToSlash(relative)
			parsed = append(parsed, parsedFile{directory: directory, path: directory + "/" + filepath.Base(path), file: file})
			for _, declaration := range file.Decls {
				general, ok := declaration.(*ast.GenDecl)
				if !ok {
					continue
				}
				for _, spec := range general.Specs {
					value, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					holds := false
					ast.Inspect(value, func(node ast.Node) bool {
						if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
							if text, err := strconv.Unquote(literal.Value); err == nil && ownershipInsertStatement.MatchString(text) {
								holds = true
							}
						}
						return true
					})
					if !holds {
						continue
					}
					if statements[directory] == nil {
						statements[directory] = map[string]bool{}
					}
					for _, name := range value.Names {
						statements[directory][name.Name] = true
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", root, err)
		}
	}
	census := ownershipCensus{
		files: len(parsed), calls: map[string]map[string]bool{}, takesKind: map[string]bool{}, boolParams: map[string][]string{},
		closes: map[string]bool{}, setsValidTo: map[string]bool{}, kinds: map[string][][2]string{}, kindFiles: map[string]bool{},
	}
	calleeName := func(call *ast.CallExpr) string {
		function := call.Fun
		if index, ok := function.(*ast.IndexExpr); ok {
			function = index.X
		}
		switch callee := function.(type) {
		case *ast.Ident:
			return callee.Name
		case *ast.SelectorExpr:
			return callee.Sel.Name
		}
		return ""
	}
	for _, source := range parsed {
		for _, declaration := range source.file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			qualified := ownershipCensusFunctionName(source.directory, function)
			called := map[string]bool{}
			writes := false
			if function.Type.Params != nil {
				for _, field := range function.Type.Params.List {
					if censusMentions(field.Type, "KindSnapshot") {
						census.takesKind[qualified] = true
					}
					if identifier, ok := field.Type.(*ast.Ident); ok && identifier.Name == "bool" {
						for _, name := range field.Names {
							census.boolParams[qualified] = append(census.boolParams[qualified], name.Name)
						}
					}
				}
			}
			ast.Inspect(function, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CompositeLit:
					name := ""
					switch literal := typed.Type.(type) {
					case *ast.Ident:
						name = literal.Name
					case *ast.SelectorExpr:
						name = literal.Sel.Name
					}
					if name == "ScopeProof" && len(typed.Elts) > 0 {
						census.scopeMakers = append(census.scopeMakers, qualified)
					}
					if name != "SnapshotTerm" {
						break
					}
					named := false
					for _, element := range typed.Elts {
						pair, ok := element.(*ast.KeyValueExpr)
						if !ok {
							census.constantTerms = append(census.constantTerms, qualified+": a SnapshotTerm without field names")
							continue
						}
						key, _ := pair.Key.(*ast.Ident)
						switch {
						case key != nil && key.Name == "Holds":
							if value, ok := pair.Value.(*ast.Ident); ok && (value.Name == "true" || value.Name == "false") {
								census.constantTerms = append(census.constantTerms, qualified+": Holds is the constant "+value.Name)
							}
						case key != nil && key.Name == "Reason":
							switch pair.Value.(type) {
							case *ast.Ident, *ast.SelectorExpr:
								named = true
							}
						}
					}
					if !named {
						census.constantTerms = append(census.constantTerms, qualified+": Reason is not a named constant")
					}
				case *ast.BasicLit:
					if typed.Kind == token.STRING {
						if text, err := strconv.Unquote(typed.Value); err == nil && ownershipInsertStatement.MatchString(text) {
							writes = true
						}
					}
				case *ast.Ident:
					if statements[source.directory][typed.Name] {
						writes = true
					}
				case *ast.RangeStmt:
					if selector, ok := typed.X.(*ast.SelectorExpr); ok && selector.Sel.Name == "Retract" {
						census.closes[qualified] = true
					}
				case *ast.AssignStmt:
					for index, left := range typed.Lhs {
						selector, ok := left.(*ast.SelectorExpr)
						if !ok || (selector.Sel.Name != "ValidTo" && selector.Sel.Name != "closedAt") || index >= len(typed.Rhs) {
							continue
						}
						if value, ok := typed.Rhs[index].(*ast.Ident); ok && value.Name == "nil" {
							continue
						}
						census.setsValidTo[qualified] = true
					}
				case *ast.CallExpr:
					name := calleeName(typed)
					called[name] = true
					if name == "CountActiveSiblingIntegrations" {
						census.censusCallers = append(census.censusCallers, qualified)
					}
					if name == "NewSnapshotKind" && len(typed.Args) == 3 {
						kind := [2]string{"<not a literal>", "<not a name>"}
						if literal, ok := typed.Args[0].(*ast.BasicLit); ok && literal.Kind == token.STRING {
							kind[0], _ = strconv.Unquote(literal.Value)
						}
						if identifier, ok := typed.Args[1].(*ast.Ident); ok {
							kind[1] = identifier.Name
						}
						census.kinds[qualified] = append(census.kinds[qualified], kind)
						census.kindFiles[source.path] = true
					}
				}
				return true
			})
			census.calls[qualified] = called
			if writes {
				census.writers = append(census.writers, qualified)
			}
			for entry := range ownershipSnapshotEntryPoints {
				if called[entry] && qualified != ownershipSnapshotWrapper {
					census.planners = append(census.planners, qualified)
					break
				}
			}
		}
	}
	sort.Strings(census.writers)
	sort.Strings(census.planners)
	sort.Strings(census.constantTerms)
	sort.Strings(census.scopeMakers)
	sort.Strings(census.censusCallers)
	return census
}

func ownershipCensusOfTheTree(t *testing.T) ownershipCensus {
	t.Helper()
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("no caller file")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	census := scanOwnershipCensus(t, repoRoot, "internal", "cmd")
	if census.files < 100 || len(census.writers) == 0 || len(census.planners) == 0 {
		t.Fatalf("the scan read %d files and found %d writers and %d planners: it measured nothing",
			census.files, len(census.writers), len(census.planners))
	}
	return census
}

func TestJiraOwnershipWriterCensus(t *testing.T) {
	census := ownershipCensusOfTheTree(t)

	wantWriters := make([]string, 0, len(ownershipWriters))
	wantPlanners := []string{}
	jira := 0
	for name, writer := range ownershipWriters {
		wantWriters = append(wantWriters, name)
		switch {
		case writer.provider == "jira" && strings.TrimSpace(writer.retiresClass) != "":
			if writer.planner != "" || writer.complete != "" {
				t.Errorf("%s retires a class of rows and names a planner or a completeness: a writer is one or the other", name)
			}
		case writer.provider == "jira":
			jira++
			if writer.planner == "" {
				t.Errorf("%s writes Jira rows and names no planner: every Jira writer of team_project_ownership plans its rows "+
					"through the snapshot rule", name)
				continue
			}
			wantPlanners = append(wantPlanners, writer.planner)
			if strings.TrimSpace(writer.complete) == "" {
				t.Errorf("%s: the planner %s does not name where its proof comes from", name, writer.planner)
			}
			planner := writer.planner[strings.LastIndex(writer.planner, ".")+1:]
			directory := name[:strings.Index(name, ".")]
			if !strings.HasPrefix(writer.planner, directory+".") {
				t.Errorf("%s: the planner %s is in another package", name, writer.planner)
			}
			called := false
			for function, calls := range census.calls {
				if strings.HasPrefix(function, directory+".") && function != writer.planner && calls[planner] {
					called = true
				}
			}
			if !called {
				t.Errorf("%s: no production function of %s calls the planner %s", name, directory, writer.planner)
			}
		case writer.planner != "":
			wantPlanners = append(wantPlanners, writer.planner)
		case strings.TrimSpace(writer.note) == "":
			t.Errorf("%s has no planner and no note", name)
		}
	}
	for name, writer := range repoOwnershipPlanners {
		wantPlanners = append(wantPlanners, writer.planner)
		if strings.TrimSpace(writer.complete) == "" {
			t.Errorf("%s: the planner %s does not name where its proof comes from", name, writer.planner)
		}
		if !census.calls[name][writer.planner[strings.LastIndex(writer.planner, ".")+1:]] {
			t.Errorf("%s does not call its planner %s", name, writer.planner)
		}
	}
	for name, proof := range otherSnapshotPlanners {
		wantPlanners = append(wantPlanners, name)
		if strings.TrimSpace(proof) == "" {
			t.Errorf("%s does not name where its proof comes from", name)
		}
	}
	if jira == 0 {
		t.Fatal("the table names no Jira writer: it pins nothing")
	}
	sort.Strings(wantWriters)
	sort.Strings(wantPlanners)
	if !reflect.DeepEqual(census.writers, wantWriters) {
		t.Fatalf("the functions that write team_project_ownership changed.\n got  %v\n want %v\n"+
			"A new writer is named in ownershipWriters with its provider. A writer of Jira rows plans them through the "+
			"snapshot rule: one rule, never a second copy of it.",
			census.writers, wantWriters)
	}
	if !reflect.DeepEqual(census.planners, wantPlanners) {
		t.Fatalf("the functions that call the snapshot rule changed.\n got  %v\n want %v\n"+
			"Each planner is the planner of a writer in ownershipWriters, or is named in otherSnapshotPlanners.",
			census.planners, wantPlanners)
	}
}
