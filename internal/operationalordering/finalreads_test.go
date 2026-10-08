package operationalordering

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The twelve tables migration 067 gives ordering contract 2.
const operationalTables = `operational_(?:alerts|escalation_policies|incident_notes|incident_responders|incident_timeline_events|incidents|on_call_assignments|on_call_schedules|service_repository_mappings|services|teams|users)`

var (
	// A raw FINAL read of one of the twelve tables: the table, an optional
	// alias, then FINAL. Under contract 2 it returns one row per revision.
	namedFinal = regexp.MustCompile(`(?is)\b(` + operationalTables + `)\b(?:\s+(?:AS\s+)?\w+)?\s+FINAL\b`)
	// A FINAL whose table is not in the literal: a placeholder before it, or
	// the literal starts with it (the table is concatenated in front).
	dynamicFinal = regexp.MustCompile(`(?s)(?:%[sv]|\{\w*(?::\w+)?\}|^\s*["` + "`" + `]?\s*)\s*FINAL\b`)
)

// classifiedRawFinalReads is the closed list of raw FINAL reads of the twelve
// operational tables, keyed "<file>|<table>". It is empty on purpose: the
// current row of a key is read through operationalordering (or a helper that
// delegates to it), never by FINAL.
var classifiedRawFinalReads = map[string]string{}

// classifiedDynamicFinalReads is the closed list of files holding a FINAL whose
// table is a runtime value, with the number of such literals and why each is
// safe. A new one fails until a reader classifies it here.
var classifiedDynamicFinalReads = map[string]struct {
	count int
	why   string
}{
	"jobs/metrics/remaining/dora_native_clickhouse.go": {1,
		"the contract-1 branch of currentOperationalRowsSQL, taken only when OPERATIONAL_ORDERING_CONTRACT=1; the worker's boot guard refuses contract 1 against contract-2 tables (workerservice/daily.go DORA refusal), and contract 2 delegates to operationalordering"},
	"operationalbackfill/write.go": {1,
		"the contract-1 branch of currentRowsSQL for `dho operational backfill`, whose GuardTables refuses a contract that disagrees with the tables; contract 2 delegates to operationalordering"},
	"queryapi/quadrant/quadrant.go": {1,
		"the FROM of a metric read whose table comes from the fixed per-metric table list (user, repo and team metric rollups); no operational table is in it"},
	"queryapi/people/metricconfig.go": {1,
		"dedupTable answers the three ReplacingMergeTree metric sources of that package's fixed config; no operational table"},
	"jobs/report/dedup.go": {1,
		"dedupFromSource reads the registered daily metric rollups (rerunDedupedDailyTables, appendOnlyDailyKeys); no operational table"},
	"queryapi/scopelabel/scopelabel.go": {1,
		"the optional FINAL of the repos and teams name lookups (kind is repo or team)"},
	"jobs/dimensionfold/fold.go": {1,
		"OPTIMIZE TABLE ... FINAL, a merge request, not a read"},
	"providersync/team_id_carry.go": {2,
		"planLink reads one of the three fixed team link tables of teamIDCarryLinks (team_memberships, team_project_ownership, team_repo_ownership); no operational table. FINAL is the current-row read there: each table is a ReplacingMergeTree(updated_at) keyed (org_id, provider, natural, team_id, source, valid_from), the read is bound by org_id, and the carry closes a link by re-inserting the same key with valid_to set and a later updated_at, so only FINAL drops the superseded open version before valid_to IS NULL filters it"},
	"storedversion/storedversion.go": {1,
		"Apply reads the tables of the stored-version spec set; the operational writers are explicitly out of that set (storedversion/writers_test.go outOfScopeWriters)"},
}

// TestNoUnclassifiedFinalReadOfAnOperationalTable walks every non-test Go file
// under internal and cmd and fails on a raw FINAL read of an operational table,
// or on a FINAL over a runtime-valued table, that is not on the closed lists.
func TestNoUnclassifiedFinalReadOfAnOperationalTable(t *testing.T) {
	module, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	named := map[string]bool{}
	dynamic := map[string]int{}
	files := 0
	for _, top := range []string{"internal", "cmd"} {
		root := filepath.Join(module, top)
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			files++
			relative, _ := filepath.Rel(filepath.Join(module, "internal"), path)
			if top == "cmd" {
				relative, _ = filepath.Rel(module, path)
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				for _, match := range namedFinal.FindAllStringSubmatch(literal.Value, -1) {
					named[relative+"|"+strings.ToLower(match[1])] = true
				}
				dynamic[relative] += len(dynamicFinal.FindAllString(literal.Value, -1))
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if files < 500 {
		t.Fatalf("the scan read %d files: it measured nothing", files)
	}
	for key := range named {
		if _, ok := classifiedRawFinalReads[key]; !ok {
			t.Errorf("%s reads an operational table with a raw FINAL: under contract 2 FINAL keeps one row per revision; read the current row through operationalordering instead (or classify it, with a reason, in classifiedRawFinalReads)", key)
		}
	}
	for key := range classifiedRawFinalReads {
		if !named[key] {
			t.Errorf("classifiedRawFinalReads lists %s but the scan no longer finds it: remove the entry", key)
		}
	}
	var seen []string
	for file, count := range dynamic {
		if count == 0 {
			continue
		}
		seen = append(seen, file)
		entry, ok := classifiedDynamicFinalReads[file]
		if !ok {
			t.Errorf("%s has %d FINAL over a runtime-valued table and is not classified in classifiedDynamicFinalReads", file, count)
		} else if entry.count != count {
			t.Errorf("%s has %d dynamic FINAL literal(s), classified %d (%s): a new one must be classified", file, count, entry.count, entry.why)
		}
	}
	sort.Strings(seen)
	for file := range classifiedDynamicFinalReads {
		if dynamic[file] == 0 {
			t.Errorf("classifiedDynamicFinalReads lists %s but the scan no longer finds a dynamic FINAL there: remove the entry", file)
		}
	}
}
