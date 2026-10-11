package investmentnormalize

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-9227 confirm round 143: a row this command writes shares work_unit_investments with the canonical
// units, and some reader treated "a row exists" as "the work-graph projection ran" (Work Graph's
// MEMBERSHIP_NOT_MATERIALIZED). This census names EVERY non-test source that reads the table in a statement
// and the verdict for the normalized_legacy rows, so the next reader is judged when it is written, and a
// reader that stops reading it is removed from the list.
var workUnitInvestmentsRead = regexp.MustCompile(`(?i)\b(from|join)\s+work_unit_investments\b`)

var workUnitInvestmentsReaders = map[string]string{
	"internal/investmentnormalize/run.go":                             "this command: reads its own rows (plan, rollback)",
	"internal/queryapi/analytics/investment.go":                       "THE shared source (LatestWorkUnitInvestmentsSource): admits normalized rows on purpose (the pages show them)",
	"internal/queryapi/analytics/investmentmembershipscope.go":        "the scope filter admits normalized rows; the projection clock EXCLUDES them (no scope-state move)",
	"internal/queryapi/analytics/investmentargmaxtransitionguard.go":  "a parity diagnostic of the projection's units: EXCLUDES normalized rows",
	"internal/queryapi/workgraph/scope.go":                            "the Work Graph degraded-reason probe (is the projection missing?): EXCLUDES normalized rows",
	"internal/jobs/metrics/remaining/membership_marker_lag.go":        "the write-side twin of the projection clock: EXCLUDES normalized rows",
	"internal/jobs/metrics/remaining/membership_native_clickhouse.go": "reads the units the projection names by id: a normalized id is never in that list",
	"internal/jobs/investment/chquery/entities.go":                    "the materializer's own cache check by canonical unit id: a normalized id is never asked for",
	"internal/jobs/investment/materialize.go":                         "a comment about the quotes of a run",
	"internal/queryapi/sankey/queries.go":                             "a comment: the statements read the shared source",
	"internal/apiservice/admin/llmspend.go":                           "failures of the LLM runs the caller names by run id: a normalized run id is never an LLM run",
}

func TestEveryReaderOfWorkUnitInvestmentsHasAVerdictForNormalizedRows(t *testing.T) {
	root := filepath.Join("..", "..")
	found := map[string]bool{}
	scanned := 0
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			scanned++
			if workUnitInvestmentsRead.Match(raw) {
				rel, _ := filepath.Rel(root, path)
				found[filepath.ToSlash(rel)] = true
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if scanned < 1000 {
		t.Fatalf("the census read %d Go files: it did not measure the tree", scanned)
	}
	var problems []string
	for path := range found {
		if _, ok := workUnitInvestmentsReaders[path]; !ok {
			problems = append(problems, path+": reads work_unit_investments and has no verdict for the normalized_legacy rows (add it to workUnitInvestmentsReaders with the reason)")
		}
	}
	for path := range workUnitInvestmentsReaders {
		if !found[path] {
			problems = append(problems, path+": listed, but it no longer reads work_unit_investments: remove the entry")
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		t.Fatalf("%s", strings.Join(problems, "\n"))
	}
}
