//go:build integration

package fixturescli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// synthetic tables are every ClickHouse table the four synthetic targets
// write (found by counting the rows of every table before and after each
// target on the real Python sync, and asserted again on every freeze).
var syntheticTables = []string{
	"repos", "ci_pipeline_runs", "deployments",
	"operational_service_repository_mappings", "operational_services", "operational_incidents",
	"test_suite_results", "ci_job_runs", "test_case_results",
}

// syntheticParameterSets are the (organization, repository, window) the CI
// scripts run: ci/run_metrics_executed_proof.sh and ci/run_live_backend_e2e.sh.
var syntheticParameterSets = []struct {
	Org, Repo string
	Days      int
}{
	{"c0ffee00-dead-4bee-8bad-f00dfeedface", "ci-metrics-executed-proof/repo", 7},
	{"11111111-2222-4333-8444-555555555555", "acme/live-e2e", 14},
}

// clickHouseHTTP runs one statement over the container's HTTP interface and
// returns the body: the tests read and write rows here in ClickHouse's own
// formats, independent of the native client the verb uses.
func clickHouseHTTP(t *testing.T, dsn, query string) string {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	endpoint := "http://" + parsed.Host + "/?database=" + strings.TrimPrefix(parsed.Path, "/") + "&output_format_json_quote_64bit_integers=0"
	request, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(query))
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth(parsed.User.Username(), password)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("clickhouse %d: %s", response.StatusCode, body)
	}
	return string(body)
}

type clickHouse struct {
	instance *containers.Instance
	httpDSN  string
}

func startClickHouse(t *testing.T) clickHouse {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	// Production's schema: ordering contract 2 (the migrator's head).
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "2")
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	return clickHouse{instance: instance, httpDSN: dsn}
}

// tableColumns reads a table's columns in order; a materialized, alias or
// ephemeral column would not round-trip through SELECT * and INSERT, so it is
// refused.
func tableColumns(t *testing.T, dsn, table string) []FrozenColumn {
	t.Helper()
	body := clickHouseHTTP(t, dsn, "SELECT name, type, default_kind FROM system.columns WHERE database = currentDatabase() AND table = '"+table+"' ORDER BY position FORMAT JSONCompactEachRow")
	var columns []FrozenColumn
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		var row []string
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("columns of %s: %v", table, err)
		}
		if row[2] == "MATERIALIZED" || row[2] == "ALIAS" || row[2] == "EPHEMERAL" {
			t.Fatalf("%s.%s is a %s column; SELECT * would not carry it", table, row[0], row[2])
		}
		if strings.Contains(row[1], "DateTime") && !strings.Contains(row[1], "DateTime64") {
			t.Fatalf("%s.%s is %s: only UTC DateTime64 timestamps are shifted", table, row[0], row[1])
		}
		if strings.Contains(row[1], "DateTime64") && !strings.Contains(row[1], "'UTC'") {
			t.Fatalf("%s.%s is %s: only UTC DateTime64 timestamps are shifted", table, row[0], row[1])
		}
		columns = append(columns, FrozenColumn{Name: row[0], Type: row[1]})
	}
	return columns
}

// dumpTable reads every row of a table as ClickHouse's JSONCompactEachRow
// writes it, sorted by its text so the order is stable.
func dumpTable(t *testing.T, dsn, table string) FrozenTable {
	t.Helper()
	frozen := FrozenTable{Name: table, Columns: tableColumns(t, dsn, table)}
	body := strings.TrimSpace(clickHouseHTTP(t, dsn, "SELECT * FROM `"+table+"` FORMAT JSONCompactEachRow"))
	if body == "" {
		return frozen
	}
	lines := strings.Split(body, "\n")
	sort.Strings(lines)
	for _, line := range lines {
		var row []any
		decoder := json.NewDecoder(strings.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&row); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		frozen.Rows = append(frozen.Rows, row)
	}
	return frozen
}

// rowCounts counts the rows of every base table (materialized views excluded).
func rowCounts(t *testing.T, dsn string) map[string]string {
	t.Helper()
	counts := map[string]string{}
	list := clickHouseHTTP(t, dsn, "SELECT name FROM system.tables WHERE database = currentDatabase() AND engine NOT LIKE '%View' ORDER BY name FORMAT TSV")
	for _, name := range strings.Fields(list) {
		counts[name] = strings.TrimSpace(clickHouseHTTP(t, dsn, "SELECT count() FROM `"+name+"` FORMAT TSV"))
	}
	return counts
}

func truncateSynthetic(t *testing.T, dsn string) {
	t.Helper()
	for _, table := range syntheticTables {
		clickHouseHTTP(t, dsn, "TRUNCATE TABLE `"+table+"`")
	}
	// A table a view fills from a synthetic table keeps the rows of the
	// target loaded before: truncate it with its source, or the next
	// target's check would find rows from nowhere.
	for table := range viewFilledAfterTheCapture {
		clickHouseHTTP(t, dsn, "TRUNCATE TABLE `"+table+"`")
	}
}

func loadThroughTheNativeClient(t *testing.T, ch clickHouse, org, repo string, days int, target string, now time.Time) map[string]int {
	t.Helper()
	dsn := ch.instance.URI
	config := chstorage.DefaultConfig(dsn)
	conn, err := chstorage.Open(context.Background(), config)
	if err != nil {
		t.Fatalf("open the native client: %v", err)
	}
	defer conn.Close()
	counts, err := LoadSynthetic(context.Background(), conn, org, repo, days, target, now)
	if err != nil {
		t.Fatalf("load %s: %v", target, err)
	}
	return counts
}

// The verb's loader puts exactly the frozen rows into a real ClickHouse at the
// migration head, every timestamp moved by the requested time and nothing else
// changed, and writes no table outside the synthetic ones.
func TestLoadSyntheticInsertsTheFrozenRows(t *testing.T) {
	ch := startClickHouse(t)
	for _, set := range syntheticParameterSets {
		frozen, err := LoadFrozenSet(set.Org, set.Repo, set.Days)
		if err != nil {
			t.Fatal(err)
		}
		for _, target := range frozen.Targets {
			t.Run(fmt.Sprintf("%s/%s/%s", set.Org[:8], strings.ReplaceAll(set.Repo, "/", "_"), target.Name), func(t *testing.T) {
				truncateSynthetic(t, ch.httpDSN)
				before := rowCounts(t, ch.httpDSN)
				frozenAt, err := time.Parse(time.RFC3339Nano, target.FrozenAt)
				if err != nil {
					t.Fatal(err)
				}
				delta := 26*time.Hour + 17*time.Minute + 1234*time.Millisecond
				counts := loadThroughTheNativeClient(t, ch, set.Org, set.Repo, set.Days, target.Name, frozenAt.Add(delta))

				wroteTables := map[string]bool{}
				for _, table := range target.Tables {
					wroteTables[table.Name] = true
					want, err := table.ShiftTimes(delta)
					if err != nil {
						t.Fatal(err)
					}
					if counts[table.Name] != len(table.Rows) {
						t.Fatalf("%s: the loader reported %d row(s), frozen %d", table.Name, counts[table.Name], len(table.Rows))
					}
					// See columnsAfterTheCapture: a nullable column added after
					// the capture is held to NULL, then left out.
					got := requireColumnsAfterTheCapture(t, dumpTable(t, ch.httpDSN, table.Name))
					if _, operational := operationalFamilies[table.Name]; operational {
						// The stored stamp is the one derived from the stored row; the
						// remaining columns are then compared with the frozen ones.
						assertStoredStampOfTable(t, ch.httpDSN, WorldTable{FrozenTable: table})
					}
					got = withoutOrdering(t, got)
					if !reflect.DeepEqual(got.Columns, table.Columns) {
						t.Fatalf("%s: the schema's columns differ from the frozen ones:\n schema %v\n frozen %v", table.Name, got.Columns, table.Columns)
					}
					if diff := rowsDiff(want, got.Rows); diff != "" {
						t.Fatalf("%s: the loaded rows differ from the frozen rows (shifted by %s):\n%s", table.Name, delta, diff)
					}
				}
				if problems := unwrittenTableProblems(before, rowCounts(t, ch.httpDSN), wroteTables); len(problems) > 0 {
					t.Fatalf("target %s does not write these tables:\n  %s", target.Name, strings.Join(problems, "\n  "))
				}
				requireViewFilledTables(t, ch.httpDSN)
			})
		}
	}
}

// rowsDiff compares rows as sets of JSON text and names the first differences.
func rowsDiff(want, got [][]any) string {
	text := func(rows [][]any) []string {
		out := make([]string, len(rows))
		for index, row := range rows {
			raw, _ := json.Marshal(row)
			out[index] = string(raw)
		}
		sort.Strings(out)
		return out
	}
	wantText, gotText := text(want), text(got)
	if reflect.DeepEqual(wantText, gotText) {
		return ""
	}
	var out strings.Builder
	fmt.Fprintf(&out, "want %d row(s), got %d\n", len(wantText), len(gotText))
	shown := 0
	for index := 0; index < len(wantText) && index < len(gotText) && shown < 3; index++ {
		if wantText[index] != gotText[index] {
			fmt.Fprintf(&out, "  want %s\n  got  %s\n", wantText[index], gotText[index])
			shown++
		}
	}
	return out.String()
}

// maskTimes replaces every UTC DateTime64 value with a marker: two runs of a
// generator that reads the clock differ in exactly those columns.
func maskTimes(table FrozenTable) [][]any {
	masked := make([][]any, len(table.Rows))
	for index, row := range table.Rows {
		out := make([]any, len(row))
		copy(out, row)
		for column, definition := range table.Columns {
			if _, isTime := timePrecision(definition.Type); isTime && out[column] != nil {
				out[column] = "<ts>"
			}
		}
		masked[index] = out
	}
	return masked
}

// CHAOS-7301: at the migration head (ordering contract 2) the operational tables refuse a row whose
// ordering_contract is not 2, and the frozen incident rows carry no ordering columns. The loader must
// stamp them, or `load-synthetic --target incidents` fails (code 469) on the CI head schema.
func TestLoadSyntheticIncidentsIntoAHeadSchema(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	migrate := exec.Command("go", "run", "../../cmd/dho", "migrate", "clickhouse", "upgrade")
	migrate.Env = append(os.Environ(), "CLICKHOUSE_URI="+instance.URI, "OPERATIONAL_ORDERING_CONTRACT=2")
	if output, err := migrate.CombinedOutput(); err != nil {
		t.Fatalf("dho migrate clickhouse upgrade: %v\n%s", err, output)
	}
	set := syntheticParameterSets[0]
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	counts, err := LoadSynthetic(ctx, conn, set.Org, set.Repo, set.Days, "incidents", time.Now().UTC())
	if err != nil {
		t.Fatalf("load incidents into a head schema: %v", err)
	}
	if counts["operational_service_repository_mappings"] == 0 {
		t.Fatalf("no mapping row was loaded: %v", counts)
	}
	var stamped, all uint64
	row := conn.QueryRow(ctx, "SELECT countIf(ordering_contract = 2), count() FROM operational_service_repository_mappings")
	if err := row.Scan(&stamped, &all); err != nil {
		t.Fatal(err)
	}
	if all == 0 || stamped != all {
		t.Fatalf("mapping rows with ordering_contract 2: %d of %d", stamped, all)
	}
}
