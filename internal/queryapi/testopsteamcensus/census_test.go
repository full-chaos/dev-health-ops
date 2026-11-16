package testopsteamcensus

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The three TestOps daily tables store the main-owner team_id of the day they were computed. A TEAM read of them answers
// the ownership of the request instead (CHAOS-13804, internal/queryapi/analytics/testopsownership.go), so no query-api
// reader may group or filter them by that stored column: the census fails on a string literal that names a TestOps
// daily table and the column, and on a file that names a table and holds more team_id literals than the pinned census.
var testopsDaily = []string{"testops_pipeline_metrics_daily", "testops_test_metrics_daily", "testops_coverage_metrics_daily"}

var storedTeamColumn = regexp.MustCompile(`\bteam_id\b`)

// teamLiteralsInReaderFiles pins, per file that names a TestOps daily table, the string literals that mention the stored
// column: work_item_completion_daily's statement (timeseries.go), the shared team dimension column of the other tables
// (validate.go), the owners table of the owner join (testopsownership.go), and the investment statements (ai_opportunities.go),
// and Home, which reads testops_pipeline_metrics_daily by repo_id only: its three metricspec.go literals are the work_item
// tables' dedup keys (work_item_state_durations_daily, work_item_metrics_daily; metricspec.go:52-53) and the work_item metrics'
// group column (metricGroup, :85); its one queries_metrics.go literal is the blocked-hours statement over
// work_item_state_durations_daily (:332-341, the same literal holds the column twice).
// None reads a TestOps table by it (literal level, below). A new literal in one of these files, or a new reader file that
// mentions the column, fails the census and needs a review of the table it reads.
var teamLiteralsInReaderFiles = map[string]int{
	"aianalytics/ai_opportunities.go":        4,
	"testopspooled/testopspooled.go":         0,
	"analytics/timeseries.go":                2,
	"analytics/validate.go":                  1,
	"analytics/testopsownership.go":          2,
	"coveragebaselines/coveragebaselines.go": 0,
	"home/metricspec.go":                     3,
	"home/queries_metrics.go":                1,
	"testopswindow/testopswindow.go":         0,
}

type literal struct {
	file string
	line int
	text string
}

func queryapiLiterals(t *testing.T) map[string][]literal {
	t.Helper()
	out := map[string][]literal{}
	root := ".."
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" || d.Name() == "testopsteamcensus" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator)))
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			out[rel] = append(out[rel], literal{file: rel, line: fset.Position(lit.Pos()).Line, text: text})
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func namesTestopsDaily(text string) bool {
	for _, table := range testopsDaily {
		if strings.Contains(text, table) {
			return true
		}
	}
	return false
}

func TestNoQueryAPIReaderGroupsOrFiltersTestopsDailyByTheStoredTeam(t *testing.T) {
	byFile := queryapiLiterals(t)
	var readers, problems []string
	for file, lits := range byFile {
		names, teamLits := false, 0
		for _, l := range lits {
			if namesTestopsDaily(l.text) {
				names = true
				if storedTeamColumn.MatchString(l.text) {
					problems = append(problems, fmt.Sprintf("%s:%d: one statement names a TestOps daily table and the stored team_id column", l.file, l.line))
				}
			}
			if storedTeamColumn.MatchString(l.text) {
				teamLits++
			}
		}
		if !names {
			continue
		}
		readers = append(readers, file)
		want, known := teamLiteralsInReaderFiles[file]
		switch {
		case !known && teamLits > 0:
			problems = append(problems, fmt.Sprintf("%s: a reader of a TestOps daily table mentions team_id in %d literal(s) and is not in the census", file, teamLits))
		case known && teamLits != want:
			problems = append(problems, fmt.Sprintf("%s: %d literal(s) mention team_id, the census pins %d", file, teamLits, want))
		}
	}
	sort.Strings(readers)
	for known := range teamLiteralsInReaderFiles {
		found := false
		for _, r := range readers {
			if r == known {
				found = true
			}
		}
		if !found {
			t.Errorf("census floor: %s no longer names a TestOps daily table (measured nothing; update the census)", known)
		}
	}
	for _, p := range problems {
		t.Error(p)
	}
	t.Logf("readers of a TestOps daily table: %s", strings.Join(readers, ", "))
}
