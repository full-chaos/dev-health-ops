package rowview

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The census of the readers of the two tables that hold team rows (CHAOS-14340): a reader of
// work_item_metrics_daily or work_item_state_durations_daily says which form it reads, so a reader that answers for
// one team does not keep reading the org view (a co-owner team would read none of its items), and a reader that sums
// several teams does not take the team form (an item of two teams of the set would count twice).
//
// What it checks: the TEXT of each non-test file, per file. A file is a reader when it contains the name of one of
// the two tables and is a reader in readerRoles or is listed in teamReaderRoles, when it contains the direct spelling
// "rowview.OrgPredicate(" or "rowview.ScopePredicate(" and no table name of the view column, or when it contains
// "rowview.TeamSource(", "rowview.TeamPick(", "rowview.ServedTeamSource(" or "rowview.ServedTeamPick(". The form of a
// file must match whether it contains one of those four.
//
// NOT pinned (the census does not bind Go references; ticket 14348 does): a helper call through a renamed or dot
// import of rowview; a helper stored as a func value and called later; a registry read of a team table in a file that
// also names a table of the view column (a person query), and a direct read added to a file that readerRoles lists as
// writer or names. A helper name in a comment or a string counts as a call. A file that reaches the table only
// through a parameter (explain/repositories.go: a DISTINCT provider listing, view-invariant) is not seen.
//
// Forms:
//
//	team            answers for one team, or one row per team: takes TeamSource or TeamPick (jobs), ServedTeamSource or
//	                ServedTeamPick (served readers);
//	team-pending    ruled to the team form, still on the org form (CHAOS-10664 ops-2a and ops-2b);
//	org             organization-wide, or kept on the org form by ruling (team quadrants share-weighted);
//	scope           reads one work scope: ScopePredicate;
//	team-set        sums several teams into one number: org form until the once-per-set rule (L2d);
//	discovery       lists the teams a job runs for: keeps today's team set (TestTeamDiscoveryKeepsTodaysTeamSet);
//	view-invariant  a listing, a presence test or a key read that every view answers alike.
var teamReaderRoles = map[string]struct{ form, why string }{
	"internal/jobs/metrics/daily/benchmarking/clickhouse.go":               {"team-pending", "the team series takes the team form (ops-2b); the global series stays org"},
	"internal/jobs/metrics/remaining/capacity_native_clickhouse.go":        {"team", "a forecast of one team reads the team view (TeamPick over the capacity collapse); the all-teams discovery stays on today's team set"},
	"internal/jobs/metrics/remaining/recommendations_loader.go":            {"team", "reads one team: TeamSource"},
	"internal/jobs/metrics/remaining/recommendations_native_clickhouse.go": {"discovery", "lists the teams the recommendations job evaluates"},
	"internal/jobs/report/dedup.go":                                        {"team", "the source of a chart of one filter team: TeamPick over the report collapse; several teams and charts grouped by team stay org"},
	"internal/jobs/report/query.go":                                        {"org", "the chart query; a chart of one filter team takes its team source from dedup.go (teamChart); several teams and charts grouped by team stay org"},
	"internal/orgactivity/chread/chread.go":                                {"view-invariant", "count() > 0 and max(computed_at) of an organization"},
	"internal/queryapi/aggflame/clickhouse.go":                             {"team", "one team reads the served team view (ServedTeamSource); no team keeps the org view"},
	"internal/queryapi/aianalytics/flow_opportunities.go":                  {"team", "one named team reads the served team view (ServedTeamSource); the rows of all teams, one per team, keep the org view (ticket 14377)"},
	"internal/queryapi/capacityforecast/clickhouse.go":                     {"team", "one team reads the served team view (ServedTeamSource, ServedTeamPick for the backlog); no team and a team set keep the org view (L2d)"},
	"internal/queryapi/datahealth/coverage.go":                             {"org", "the row count of an organization's table: takes the org view in its own ticket (14339)"},
	"internal/queryapi/explain/metricconfig.go":                            {"team-pending", "the contributor and driver rows per team take the team form (ticket 14377); the value of one team reads the team view in metrics.go"},
	"internal/queryapi/explain/metrics.go":                                 {"team", "the value of one team reads the served team view (ServedTeamSource); no team, a team set (L2d) and the contributor and driver rows per team (ticket 14377) keep the org view"},
	"internal/queryapi/filteroptions/filteroptions.go":                     {"view-invariant", "DISTINCT status: a team row carries the statuses of the org rows"},
	"internal/queryapi/home/metricspec.go":                                 {"team-pending", "the driver rows per team take the team form (ticket 14377); the metrics of one team read the team view in queries_metrics.go"},
	"internal/queryapi/home/queries_metrics.go":                            {"team", "one team reads the served team view (ServedTeamSource); no team and a team set (L2d) keep the org view, and the driver rows per team (ticket 14377)"},
	"internal/queryapi/operatingreview/operatingreview.go":                 {"team", "one team reads the served team view (ServedTeamSource); no team and a team set (L2d) keep the org view"},
	"internal/queryapi/quadrant/quadrant.go":                               {"org", "team quadrants stay share-weighted (ruling on 10664 Q4); the repository axis reads the scope view"},
	"internal/queryapi/sankey/queries.go":                                  {"team", "one team reads the served team view (ServedTeamSource); no team and a team set (L2d) keep the org view"},
	"internal/queryapi/throughputforecast/clickhouse.go":                   {"team", "one team reads the served team view (ServedTeamSource, ServedTeamPick for the newest-day reads); no team and a team set keep the org view (L2d)"},
}

var teamForms = map[string]bool{
	"team": true, "team-pending": true, "org": true, "scope": true, "team-set": true, "discovery": true, "view-invariant": true,
}

var teamRowTables = []string{"work_item_metrics_daily", "work_item_state_durations_daily"}

// teamCensusFile is what the census needs to know of one non-test file.
type teamCensusFile struct {
	namesTeamTable bool // names work_item_metrics_daily or work_item_state_durations_daily
	namesViewTable bool // names any table of the view column
	callsPredicate bool // calls OrgPredicate or ScopePredicate
	callsTeam      bool // calls TeamSource
}

// checkTeamCensus returns one line per file that breaks the census.
func checkTeamCensus(
	files map[string]teamCensusFile,
	roles map[string]struct{ role, why string },
	teamRoles map[string]struct{ form, why string },
) []string {
	readers := map[string]bool{}
	for path, file := range files {
		role, listed := roles[path]
		switch {
		case file.namesTeamTable && listed && (role.role == "reader-sum" || role.role == "reader-nosum"):
			readers[path] = true
		case file.namesTeamTable && teamRoles[path].form != "":
			readers[path] = true
		case file.callsPredicate && !file.namesViewTable:
			readers[path] = true
		case file.callsTeam:
			readers[path] = true
		}
	}
	var problems []string
	for path := range readers {
		entry, ok := teamRoles[path]
		if !ok {
			problems = append(problems, path+" reads a table of team rows and is not in teamReaderRoles: list it with its form")
			continue
		}
		if files[path].callsTeam != (entry.form == "team") {
			problems = append(problems, path+": form "+entry.form+" and a call of TeamSource must go together")
		}
	}
	for path, entry := range teamRoles {
		if !teamForms[entry.form] {
			problems = append(problems, path+": form "+entry.form+" is not a form of the census")
		}
		if entry.why == "" {
			problems = append(problems, path+": no reason")
		}
		if !readers[path] {
			problems = append(problems, path+" is in teamReaderRoles and reads no table of team rows any more: remove it")
		}
	}
	sort.Strings(problems)
	return problems
}

func scanTeamCensusFiles(t *testing.T) map[string]teamCensusFile {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]teamCensusFile{}
	for _, dir := range []string{"internal", "cmd"} {
		walkErr := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() && (entry.Name() == "testdata" || entry.Name() == ".git") {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			body, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			text := string(body)
			var file teamCensusFile
			for _, table := range teamRowTables {
				file.namesTeamTable = file.namesTeamTable || strings.Contains(text, table)
			}
			for _, table := range Tables() {
				file.namesViewTable = file.namesViewTable || strings.Contains(text, table)
			}
			file.callsPredicate = strings.Contains(text, "rowview.OrgPredicate(") || strings.Contains(text, "rowview.ScopePredicate(")
			file.callsTeam = strings.Contains(text, "rowview.TeamSource(") || strings.Contains(text, "rowview.TeamPick(") ||
				strings.Contains(text, "rowview.ServedTeamSource(") || strings.Contains(text, "rowview.ServedTeamPick(")
			if file != (teamCensusFile{}) {
				relative, _ := filepath.Rel(root, path)
				files[filepath.ToSlash(relative)] = file
			}
			return nil
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	return files
}

func TestTeamCensusEveryReaderIsClassified(t *testing.T) {
	files := scanTeamCensusFiles(t)
	if len(files) < 50 {
		t.Fatalf("the scan found %d files: it read too little", len(files))
	}
	for _, problem := range checkTeamCensus(files, readerRoles, teamReaderRoles) {
		t.Error(problem)
	}
}

// The census refuses a new reader that says nothing about its form, in each of the text forms it checks.
func TestTeamCensusRejectsANewUnclassifiedReader(t *testing.T) {
	const fresh = "internal/queryapi/fresh/fresh.go"
	for _, reader := range []struct {
		name  string
		file  teamCensusFile
		roles map[string]struct{ role, why string }
	}{
		{"names a table, a reader in readerRoles", teamCensusFile{namesTeamTable: true, namesViewTable: true},
			map[string]struct{ role, why string }{fresh: {"reader-sum", "sums"}}},
		{"reads through a registry", teamCensusFile{callsPredicate: true}, nil},
		{"takes the team form", teamCensusFile{callsTeam: true}, nil},
	} {
		problems := checkTeamCensus(map[string]teamCensusFile{fresh: reader.file}, reader.roles, nil)
		if len(problems) != 1 || !strings.HasPrefix(problems[0], fresh+" reads a table of team rows and is not in teamReaderRoles") {
			t.Errorf("%s: problems = %q, want the unclassified reader refused", reader.name, problems)
		}
	}
	// A file that names a table as a writer or a guard is not a reader.
	writer := checkTeamCensus(map[string]teamCensusFile{fresh: {namesTeamTable: true, namesViewTable: true}},
		map[string]struct{ role, why string }{fresh: {"writer", "writes"}}, nil)
	if len(writer) != 0 {
		t.Errorf("a writer is refused: %q", writer)
	}
	// A team-pending reader that already calls TeamSource, and a team reader that does not, are refused.
	for _, mismatch := range []struct {
		form string
		file teamCensusFile
	}{
		{"team-pending", teamCensusFile{namesTeamTable: true, callsTeam: true}},
		{"team", teamCensusFile{namesTeamTable: true}},
	} {
		problems := checkTeamCensus(map[string]teamCensusFile{fresh: mismatch.file},
			map[string]struct{ role, why string }{fresh: {"reader-sum", "sums"}},
			map[string]struct{ form, why string }{fresh: {mismatch.form, "why"}})
		if len(problems) != 1 || !strings.Contains(problems[0], "a call of TeamSource must go together") {
			t.Errorf("form %s: problems = %q, want the form refused", mismatch.form, problems)
		}
	}
}
