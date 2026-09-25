//go:build integration

package fixturescli

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	freezeEnv = "DHO_SYNTHETIC_FREEZE"
	// producerEnv names the commit whose Python generator the freeze ran, so the
	// frozen files record it.
	producerEnv = "DHO_SYNTHETIC_PRODUCER"
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
}

// pythonSync runs the real `dev-hops sync <target> --provider synthetic` as CI
// runs it (with --defer-finalize) against the ClickHouse at dsn.
func pythonSync(t *testing.T, dsn, org, repo string, days int, target string) time.Time {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	started := time.Now().UTC()
	command := exec.Command(python, "-m", "dev_health_ops.cli", "sync", target, "--provider", "synthetic",
		"--repo-name", repo, "--backfill", fmt.Sprint(days), "--defer-finalize")
	command.Env = append(os.Environ(),
		"PYTHONPATH="+filepath.Join(root, "src"),
		"ORG_ID="+org, "CLICKHOUSE_URI="+dsn, "OTEL_ENABLED=false", "DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1",
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("the Python synthetic sync of %s failed: %v", target, pyoracle.RunError(python, err, output))
	}
	return started
}

// pythonSyncTarget runs one target on an empty set of synthetic tables and
// returns what it wrote, and when it started. It fails if the target wrote a
// table outside syntheticTables.
func pythonSyncTarget(t *testing.T, ch clickHouse, org, repo string, days int, target string) (FrozenTarget, time.Time) {
	t.Helper()
	truncateSynthetic(t, ch.httpDSN)
	before := rowCounts(t, ch.httpDSN)
	started := pythonSync(t, ch.httpDSN, org, repo, days, target)
	after := rowCounts(t, ch.httpDSN)
	known := map[string]bool{}
	for _, table := range syntheticTables {
		known[table] = true
	}
	for table, count := range after {
		if before[table] != count && !known[table] {
			t.Fatalf("target %s wrote %s, which is not in syntheticTables", target, table)
		}
	}
	frozen := FrozenTarget{Name: target, FrozenAt: started.Format(time.RFC3339Nano)}
	for _, table := range syntheticTables {
		if dumped := dumpTable(t, ch.httpDSN, table); len(dumped.Rows) > 0 {
			frozen.Tables = append(frozen.Tables, dumped)
		}
	}
	if len(frozen.Tables) == 0 {
		t.Fatalf("target %s wrote no rows", target)
	}
	return frozen, started
}

// TestFreezeSyntheticRows writes the frozen files from the real Python
// producer. It is not a check: it runs only with DHO_SYNTHETIC_FREEZE=1, needs
// the full project Python environment, and rewrites testdata/synthetic.
func TestFreezeSyntheticRows(t *testing.T) {
	if os.Getenv(freezeEnv) != "1" {
		t.Skip("set " + freezeEnv + "=1 (and " + producerEnv + "=<commit>) to re-freeze the synthetic rows from the live Python producer")
	}
	producer := os.Getenv(producerEnv)
	if producer == "" {
		t.Fatalf("%s must name the commit whose Python generator runs", producerEnv)
	}
	ch := startClickHouse(t)
	if err := os.MkdirAll("testdata/synthetic", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, set := range syntheticParameterSets {
		frozen := FrozenSet{Producer: producer, OrgID: set.Org, RepoName: set.Repo, Days: set.Days}
		for _, target := range Targets {
			rows, _ := pythonSyncTarget(t, ch, set.Org, set.Repo, set.Days, target)
			frozen.Targets = append(frozen.Targets, rows)
		}
		var buffer bytes.Buffer
		writer, err := gzip.NewWriterLevel(&buffer, gzip.BestCompression)
		if err != nil {
			t.Fatal(err)
		}
		encoder := json.NewEncoder(writer)
		if err := encoder.Encode(frozen); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(SyntheticSetFile(set.Org, set.Repo, set.Days), buffer.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("froze %s / %s / %d days: %d bytes", set.Org, set.Repo, set.Days, buffer.Len())
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
					got := dumpTable(t, ch.httpDSN, table.Name)
					if !reflect.DeepEqual(got.Columns, table.Columns) {
						t.Fatalf("%s: the schema's columns differ from the frozen ones:\n schema %v\n frozen %v", table.Name, got.Columns, table.Columns)
					}
					if diff := rowsDiff(want, got.Rows); diff != "" {
						t.Fatalf("%s: the loaded rows differ from the frozen rows (shifted by %s):\n%s", table.Name, delta, diff)
					}
				}
				after := rowCounts(t, ch.httpDSN)
				for table, count := range after {
					if !wroteTables[table] && count != before[table] {
						t.Fatalf("the load changed %s (%s -> %s), which target %s does not write", table, before[table], count, target.Name)
					}
				}
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

// The differential oracle, against the REAL producer while it exists: the same
// target run by the Python synthetic sync in one ClickHouse and loaded by the
// Go verb in another gives the same rows in every table, column for column,
// except the timestamps (the producer reads the clock; the loader shifts the
// frozen ones). It needs the full project Python environment, so it runs by
// hand:
//
//	DEV_HEALTH_LIVE_PYTHON_ORACLES=1 DEV_HEALTH_PYTHON=<full venv python> \
//	  go test -tags=integration -run TestLoadSyntheticVenueOracleMatchesThePythonProducer ./internal/fixturescli
//
// CI runs it in the venue-oracles job (ci/check_go.sh venue-oracles discovers it
// through venueoracle.WriteProof, called only after every comparison passed).
func TestLoadSyntheticVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	python := startClickHouse(t)
	loader := startClickHouse(t)
	compared := 0
	for _, set := range syntheticParameterSets {
		for _, target := range Targets {
			live, _ := pythonSyncTarget(t, python, set.Org, set.Repo, set.Days, target)

			truncateSynthetic(t, loader.httpDSN)
			loadThroughTheNativeClient(t, loader, set.Org, set.Repo, set.Days, target, time.Now().UTC())
			for _, liveTable := range live.Tables {
				loaded := dumpTable(t, loader.httpDSN, liveTable.Name)
				if !reflect.DeepEqual(loaded.Columns, liveTable.Columns) {
					t.Fatalf("%s: columns differ", liveTable.Name)
				}
				if diff := rowsDiff(maskTimes(liveTable), maskTimes(loaded)); diff != "" {
					t.Fatalf("%s / %s / %s / %s: the loader's rows differ from the live Python producer's (timestamps masked):\n%s",
						set.Org[:8], set.Repo, target, liveTable.Name, diff)
				}
				if len(liveTable.Rows) == 0 {
					t.Fatalf("%s wrote no rows on the live producer: the comparison would measure nothing", liveTable.Name)
				}
				compared += len(liveTable.Rows)
			}
		}
	}
	t.Logf("compared %d row(s) across %d parameter set(s) and %d target(s)", compared, len(syntheticParameterSets), len(Targets))
	venueoracle.WriteProof(t)
}
