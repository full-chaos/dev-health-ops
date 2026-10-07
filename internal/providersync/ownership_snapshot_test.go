package providersync

import (
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

func TestPlanOwnershipSnapshotKeepsFirstSeenAndRetractsTheRest(t *testing.T) {
	first := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	later := first.Add(24 * time.Hour)
	now := first.Add(48 * time.Hour)
	fact := func(team, project, source string, validFrom time.Time) OwnershipSnapshotRow {
		return OwnershipSnapshotRow{TeamID: team, ProjectID: project, Source: source, ValidFrom: validFrom}
	}
	fresh := []OwnershipSnapshotRow{fact("T", "10001", "native", now), fact("T", "10002", "native", now)}
	open := []OwnershipSnapshotRow{
		fact("T", "10001", "native", later),              // 0: a later duplicate of a held fact
		fact("T", "10001", "native", first),              // 1: the held fact, first seen
		fact("T", "org-1:jira:OPS", "native", first),     // 2: an id form no writer produces any more
		fact("T", "10001", "jira_legacy", first),         // 3: same team and project, another source
		fact("U", "10001", "native", first),              // 4: same project, another team
		fact("T", "10003", "native", now.Add(time.Hour)), // 5: opened after this run's time
	}
	plan := PlanOwnershipSnapshot(OwnershipSnapshot{Fresh: fresh, Complete: true}, open, now)
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

	// The same data again: the held fact is written on the same key, nothing is closed.
	again := PlanOwnershipSnapshot(OwnershipSnapshot{Fresh: fresh[:1], Complete: true}, open[1:2], now.Add(time.Hour))
	if !again.ValidFrom[0].Equal(first) || len(again.Retract) != 0 {
		t.Fatalf("second run: %+v", again)
	}

	// A fresh row older than every open row of its fact keeps its own stamp;
	// the open row is then a later duplicate.
	older := PlanOwnershipSnapshot(OwnershipSnapshot{Fresh: []OwnershipSnapshotRow{fact("T", "10001", "native", first.Add(-time.Hour))}, Complete: true}, open[1:2], now)
	if !older.ValidFrom[0].Equal(first.Add(-time.Hour)) {
		t.Fatalf("an older fresh row moved to %v", older.ValidFrom[0])
	}

	// No fresh row: every open row given is closed. The caller's read is the scope.
	empty := PlanOwnershipSnapshot(OwnershipSnapshot{Complete: true}, open[:2], now)
	if len(empty.ValidFrom) != 0 || len(empty.Retract) != 2 {
		t.Fatalf("empty snapshot: %+v, want both open rows closed", empty)
	}
}

// A snapshot that did not read its source to the end closes nothing: a row
// that is missing from a part of the answer is not a fact the provider
// dropped. The zero value is not complete, so a caller that does not state
// completeness closes nothing either. The first-seen valid_from still
// applies, so the write adds no row.
func TestPlanOwnershipSnapshotClosesNothingForASnapshotThatIsNotComplete(t *testing.T) {
	first := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	now := first.Add(48 * time.Hour)
	fresh := []OwnershipSnapshotRow{{TeamID: "T", ProjectID: "10001", Source: "native", ValidFrom: now}}
	open := []OwnershipSnapshotRow{
		{TeamID: "T", ProjectID: "10001", Source: "native", ValidFrom: first},
		{TeamID: "T", ProjectID: "10001", Source: "native", ValidFrom: first.Add(time.Hour)}, // a later duplicate
		{TeamID: "T", ProjectID: "20002", Source: "native", ValidFrom: first},                // not in this part of the answer
	}
	for name, snapshot := range map[string]OwnershipSnapshot{
		"stated not complete":        {Fresh: fresh, Complete: false},
		"completeness not stated":    {Fresh: fresh},
		"no fresh row, not complete": {},
	} {
		plan := PlanOwnershipSnapshot(snapshot, open, now)
		if len(plan.Retract) != 0 {
			t.Errorf("%s: retract=%+v, want nothing closed", name, plan.Retract)
		}
		if len(snapshot.Fresh) == 1 && (len(plan.ValidFrom) != 1 || !plan.ValidFrom[0].Equal(first)) {
			t.Errorf("%s: valid_from=%v, want the first-seen stamp", name, plan.ValidFrom)
		}
	}
	if complete := PlanOwnershipSnapshot(OwnershipSnapshot{Fresh: fresh, Complete: true}, open, now); len(complete.Retract) != 2 {
		t.Fatalf("the same rows as a complete snapshot: retract=%+v, want the duplicate and the lost fact closed", complete.Retract)
	}
}

// The census below keeps "who writes team_project_ownership" and "who plans a
// write with the shared snapshot rule" two named sets.
//
// What it pins: (1) the set of Go functions of the production tree that hold
// an INSERT INTO team_project_ownership statement, or name a package-level
// constant that holds one; (2) that each of them that writes Jira rows names
// the function of its package that plans its rows, that this function calls
// PlanOwnershipSnapshot and that production code of the package calls it;
// (3) the set of functions that call PlanOwnershipSnapshot; (4) that each of
// them states the snapshot's completeness in the call, from a value and not
// from a constant, and names where that value comes from. What it does not
// pin: a statement built from parts, a write outside Go, and the data flow
// between the planner and the insert: that the rows a writer inserts are the
// planned ones is pinned by behaviour, against a real ClickHouse
// (TestProjectIdentityIsOneIDAcrossCatalogOwnershipAndWorkItems,
// TestAnAtlassianTeamsRunClosesTheKeyBuiltProjectLinks).

type ownershipWriter struct {
	provider string
	// planner is the function that plans this writer's rows through
	// PlanOwnershipSnapshot. Every jira writer has one.
	planner string
	// complete says where the planner's Complete value comes from: the
	// end-of-data signal behind it. Every planner has one.
	complete string
	// note says what the writer does when it has no planner.
	note string
}

var ownershipWriters = map[string]ownershipWriter{
	"internal/providersync.JiraTeamCatalogClickHouseEffects.writeOwnership": {
		provider: "jira", planner: "internal/providersync.jiraOwnershipSnapshot",
		complete: "its `complete` argument = JiraTeamCatalogResult.ProjectSearchComplete (the search walk in " +
			"JiraTeamCatalogRouteHandler.CollectTeamCatalog, jira_team_catalog_route.go: true only at a page's endOfData) AND the " +
			"legacy links read finished (jiraLegacyProjectOwnershipLinks, jira_team_catalog_effects_clickhouse.go)",
	},
	"internal/atlassianteams.writeOwnership": {
		provider: "jira", planner: "internal/atlassianteams.planOwnership",
		complete: "its `complete` argument = Rows.ProjectLinksComplete (atlassianteams.Collect, collect.go: one finished " +
			"project-link read for every active team; each read follows the cursor to the end or fails Collect)",
	},
	"internal/providersync.LinearReferenceCatalogClickHouseEffects.writeOwnership": {
		provider: "linear", note: "insert only, valid_from = the run time; a stale link is removed by the operator verb retire-stale-linear-project-ownership; not on the shared rule yet",
	},
	"internal/providersync.GitLabTeamCatalogClickHouseEffects.writeOwnership": {
		provider: "gitlab", note: "insert only, valid_from = the run time, no retraction; not on the shared rule yet",
	},
}

var ownershipInsertStatement = regexp.MustCompile(`(?is)\binsert\s+into\s+team_project_ownership\b`)

const ownershipSnapshotEntryPoint = "PlanOwnershipSnapshot"

type ownershipCensus struct {
	files    int
	writers  []string
	planners []string
	// completeness: planner -> the source text of the Complete value it
	// passes in its OwnershipSnapshot literal ("" when it states none).
	completeness map[string]string
	// calls: function -> the names of every function it calls.
	calls map[string]map[string]bool
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

func scanOwnershipCensus(t *testing.T, repoRoot string, roots ...string) ownershipCensus {
	t.Helper()
	type parsedFile struct {
		directory string
		file      *ast.File
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
			parsed = append(parsed, parsedFile{directory: directory, file: file})
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
	census := ownershipCensus{files: len(parsed), calls: map[string]map[string]bool{}, completeness: map[string]string{}}
	for _, source := range parsed {
		for _, declaration := range source.file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			qualified := ownershipCensusFunctionName(source.directory, function)
			called := map[string]bool{}
			writes := false
			complete := ""
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
					if name != "OwnershipSnapshot" {
						break
					}
					for _, element := range typed.Elts {
						pair, ok := element.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						if key, ok := pair.Key.(*ast.Ident); ok && key.Name == "Complete" {
							if value, ok := pair.Value.(*ast.Ident); ok {
								complete = value.Name
							} else {
								complete = "<expression>"
							}
						}
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
				case *ast.CallExpr:
					switch callee := typed.Fun.(type) {
					case *ast.Ident:
						called[callee.Name] = true
					case *ast.SelectorExpr:
						called[callee.Sel.Name] = true
					}
				}
				return true
			})
			census.calls[qualified] = called
			if writes {
				census.writers = append(census.writers, qualified)
			}
			if called[ownershipSnapshotEntryPoint] {
				census.planners = append(census.planners, qualified)
				census.completeness[qualified] = complete
			}
		}
	}
	sort.Strings(census.writers)
	sort.Strings(census.planners)
	return census
}

func TestJiraOwnershipWriterCensus(t *testing.T) {
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

	wantWriters := make([]string, 0, len(ownershipWriters))
	wantPlanners := []string{}
	jira := 0
	for name, writer := range ownershipWriters {
		wantWriters = append(wantWriters, name)
		switch {
		case writer.provider == "jira":
			jira++
			if writer.planner == "" {
				t.Errorf("%s writes Jira rows and names no planner: every Jira writer of team_project_ownership plans its rows "+
					"through %s", name, ownershipSnapshotEntryPoint)
				continue
			}
			wantPlanners = append(wantPlanners, writer.planner)
			if strings.TrimSpace(writer.complete) == "" {
				t.Errorf("%s: the planner %s does not name where its completeness comes from", name, writer.planner)
			}
			// The planner passes its own `complete` parameter: not a
			// constant, not a value it makes up. Where the callers get it
			// from is the named source, read by a person.
			if got := census.completeness[writer.planner]; got != "complete" {
				t.Errorf("%s: the planner %s passes Complete = %q to %s, want its `complete` parameter (a snapshot is complete "+
					"only on an end-of-data signal, never by a constant)", name, writer.planner, got, ownershipSnapshotEntryPoint)
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
	if jira == 0 {
		t.Fatal("the table names no Jira writer: it pins nothing")
	}
	sort.Strings(wantWriters)
	sort.Strings(wantPlanners)
	if !reflect.DeepEqual(census.writers, wantWriters) {
		t.Fatalf("the functions that write team_project_ownership changed.\n got  %v\n want %v\n"+
			"A new writer is named in ownershipWriters with its provider. A writer of Jira rows plans them through %s: "+
			"one snapshot rule, never a second copy of it.",
			census.writers, wantWriters, ownershipSnapshotEntryPoint)
	}
	if !reflect.DeepEqual(census.planners, wantPlanners) {
		t.Fatalf("the functions that call %s changed.\n got  %v\n want %v\n"+
			"Each planner is the planner of a writer in ownershipWriters.",
			ownershipSnapshotEntryPoint, census.planners, wantPlanners)
	}
}
