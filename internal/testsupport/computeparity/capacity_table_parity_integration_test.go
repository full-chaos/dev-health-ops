//go:build integration

package computeparity_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/computeparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The CUT-20 R2 port proof: the native Go capacity executor against the Python
// producer, whole table, across two scratch stores built from the real
// migration chain.
//
// Capacity is the first STOCHASTIC family to be compared. Every quantile below
// is taken over hundreds of simulated draws from CPython's Mersenne Twister
// after random.seed(generation_seed), so agreement is only possible if the Go
// side reproduces that stream draw for draw. That is what
// internal/jobs/metrics/numerical/cpyrandom exists for, and this test is where
// it is proven end to end rather than against recorded vectors.

// capacityForecastRow is the production shape of capacity_forecasts.
//
// forecast_id is absent from the compared set by declaration below rather than
// omitted here, so the exclusion is written down with a reason and checked.
type capacityForecastRow struct {
	OrgID               string     `json:"org_id" ch:"org_id"`
	TeamID              *string    `json:"team_id" ch:"team_id"`
	WorkScopeID         *string    `json:"work_scope_id" ch:"work_scope_id"`
	ForecastID          string     `json:"forecast_id" ch:"forecast_id"`
	ComputedAt          time.Time  `json:"computed_at" ch:"computed_at"`
	BacklogSize         uint32     `json:"backlog_size" ch:"backlog_size"`
	TargetItems         *uint32    `json:"target_items" ch:"target_items"`
	TargetDate          *time.Time `json:"target_date" ch:"target_date"`
	HistoryDays         uint16     `json:"history_days" ch:"history_days"`
	SimulationCount     uint32     `json:"simulation_count" ch:"simulation_count"`
	P50Days             *uint16    `json:"p50_days" ch:"p50_days"`
	P85Days             *uint16    `json:"p85_days" ch:"p85_days"`
	P95Days             *uint16    `json:"p95_days" ch:"p95_days"`
	P50Date             *time.Time `json:"p50_date" ch:"p50_date"`
	P85Date             *time.Time `json:"p85_date" ch:"p85_date"`
	P95Date             *time.Time `json:"p95_date" ch:"p95_date"`
	P50Items            *uint32    `json:"p50_items" ch:"p50_items"`
	P85Items            *uint32    `json:"p85_items" ch:"p85_items"`
	P95Items            *uint32    `json:"p95_items" ch:"p95_items"`
	ThroughputMean      float64    `json:"throughput_mean" ch:"throughput_mean"`
	ThroughputStddev    float64    `json:"throughput_stddev" ch:"throughput_stddev"`
	InsufficientHistory uint8      `json:"insufficient_history" ch:"insufficient_history"`
	HighVariance        uint8      `json:"high_variance" ch:"high_variance"`
}

func capacityTable() computeparity.Table {
	return computeparity.Table{
		Name:    "capacity_forecasts",
		OrderBy: "org_id, team_id, work_scope_id, computed_at",
		// forecast_id cannot key anything: it is a fresh uuid4 per row on both
		// sides. One forecast is produced per scope per run, so the scope
		// identifies the row.
		SemanticKey: []string{"org_id", "team_id", "work_scope_id"},
		Exclusions: map[string]string{
			"forecast_id": "str(uuid.uuid4()) per forecast (compute_capacity.py:305); " +
				"fresh on both sides, so neither can reproduce the other's",
			"computed_at": "datetime.now(UTC) per forecast; carries no product " +
				"meaning and differs on every execution",
			"p50_date": "today + p50_days where today is the WALL CLOCK " +
				"(compute_capacity.py:303); the days column beside it is " +
				"compared exactly, and the arithmetic is pinned by direct " +
				"vectors over month, year and leap boundaries in " +
				"TestDateDerivationMatchesPython",
			"p85_date": "wall-clock derived, as p50_date",
			"p95_date": "wall-clock derived, as p50_date",
		},
		// capacity_forecasts is ReplacingMergeTree(computed_at) ORDER BY
		// (forecast_id), and forecast_id is unique per row, so the engine never
		// collapses anything and a replay APPENDS. Same policy as DORA, for the
		// opposite reason: not the absence of a dedup engine, but a key that
		// can never collide.
		Repeat: computeparity.AppendDuplicates,
	}
}

func TestCapacityNativePortMatchesThePythonProducerAcrossTwoScratchStores(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })

	baseDSN, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	leftDSN := scratchDSN(t, baseDSN, "parity_capacity_left")
	rightDSN := scratchDSN(t, baseDSN, "parity_capacity_right")

	fixtures(t, "setup", "provision", "--dsn", leftDSN, "--reset")
	fixtures(t, "setup", "provision", "--dsn", rightDSN, "--reset")
	fixtures(t, "setup", "seed", "--kind", "metrics.capacity", "--dsn", leftDSN)
	fixtures(t, "setup", "clone",
		"--kind", "metrics.capacity", "--from-dsn", leftDSN, "--to-dsn", rightDSN)

	// ---- The fixture must make the RNG observable -----------------------
	//
	// len(throughput_history) is the n that random.choice draws against, and
	// _randbelow only REJECTS when n is not a power of two. A history of 8, 16
	// or 32 days would compare EQUAL against a port whose rejection loop is
	// broken; a history of 1 day makes choice constant and stops the generator
	// mattering at all. Either way the comparison would be reporting on
	// something other than the stream. Asserted against the STORE rather than
	// the seeder's intent.
	historyDays := distinctHistoryDays(ctx, t, leftDSN)
	if historyDays <= 1 {
		t.Fatalf(
			"fixture history is %d day(s): random.choice is constant over one "+
				"value, so this would compare EQUAL against any generator",
			historyDays)
	}
	if historyDays&(historyDays-1) == 0 {
		t.Fatalf(
			"fixture history is %d days, a power of two: _randbelow never "+
				"rejects at that length, so a broken or missing rejection loop "+
				"would compare EQUAL", historyDays)
	}

	// ---- The configuration must be able to OBSERVE the stream -----------
	//
	// This is the anti-vacuity gate that the input-shape checks above do NOT
	// provide, and it is the one that matters most for a stochastic family.
	//
	// The compared columns are QUANTILES over the simulated draws, so they
	// converge: past a few hundred simulations the same distribution yields the
	// same integers no matter which stream produced them. At that point the
	// comparison reports EQUAL whether or not the Go side reproduces CPython's
	// generator -- the two hypotheses predict identical observations, which is
	// precisely the vacuity this programme exists to refuse.
	//
	// Measured rather than assumed: two DIFFERENT seeds must produce different
	// quantiles on this fixture's actual history. If they do not, no stream
	// divergence could ever show up here either, and the run must say so rather
	// than pass.
	assertConfigurationCanObserveTheStream(ctx, t, leftDSN)

	// ---- Midnight guard, opening half ------------------------------------
	//
	// THREE separate wall-clock reads feed a capacity run: the throughput
	// window start (capacity_queries.py:26, which decides WHICH ROWS LOAD),
	// days_available, and the p*_date columns. A run that crosses UTC midnight
	// therefore hands the two producers different INPUT, and the divergence
	// that follows is real but says nothing about the port. Refused explicitly
	// rather than left to appear as an intermittent red.
	startDay := time.Now().UTC().Format("2006-01-02")

	pythonExecution := fixtures(t, "python",
		"produce", "--kind", "metrics.capacity", "--dsn", leftDSN)
	nativeBinary := buildCapacityProducer(t)
	nativeExecution := computeparity.RunProducer(t, "go_native", repoRoot(t), nil,
		nativeBinary, "produce",
		"--dsn", rightDSN,
		"--org-id", parityOrgID,
		"--seed", strconv.Itoa(capacityParitySeed),
		"--team-id", capacityParityTeamID,
		"--work-scope-id", capacityParityWorkScopeID,
		"--simulations", strconv.Itoa(capacityParitySimulations),
	)

	// ---- Midnight guard, closing half -----------------------------------
	if endDay := time.Now().UTC().Format("2006-01-02"); endDay != startDay {
		t.Fatalf(
			"UTC midnight crossed mid-parity-run (%s -> %s), rerun. The two "+
				"producers read different throughput windows, so any difference "+
				"below would be the clock rather than the port",
			startDay, endDay)
	}

	table := capacityTable()
	left := readCapacity(ctx, t, leftDSN, table, "python")
	right := readCapacity(ctx, t, rightDSN, table, "go_native")

	if len(left.Rows) == 0 {
		t.Fatal("the Python producer wrote no forecast rows -- a comparison " +
			"over an empty table proves nothing")
	}
	if len(right.Rows) == 0 {
		t.Fatal("the NATIVE producer wrote no forecast rows. Two empty tables " +
			"compare EQUAL, so this must fail loudly")
	}

	t.Run("two different implementations really ran", func(t *testing.T) {
		computeparity.RequirePortProof(t, pythonExecution, nativeExecution)
	})

	t.Run("the native port matches the Python producer", func(t *testing.T) {
		if messages := computeparity.Compare(t, table, left, right); len(messages) != 0 {
			t.Fatalf("the Go capacity port diverged from Python:\n  %s",
				strings.Join(messages, "\n  "))
		}
	})

	t.Run("the compared rows actually carry simulation output", func(t *testing.T) {
		// Every exclusion above removes a column. If the quantiles were also
		// absent -- both sides writing NULL because no forecast ran -- the
		// comparison would be EQUAL over nothing but scope identifiers, which
		// is the shape a passing-but-vacuous parity run takes.
		var withDays int
		for _, row := range left.Rows {
			if leaf(row["p50_days"]) != "<nil>" && leaf(row["p50_days"]) != "" {
				withDays++
			}
		}
		if withDays == 0 {
			t.Fatal(
				"no compared row carries a p50_days value, so this run compared " +
					"scope identifiers and nothing the simulation produced")
		}
	})
}

const (
	capacityParitySeed        = 20260823
	capacityParitySimulations = 50
	capacityParityTeamID      = "team-parity"
	capacityParityWorkScopeID = "scope-parity"
)

// distinctHistoryDays reads the fixture's actual day count from the STORE, so a
// seeder change that narrows the history fails the anti-vacuity gate rather
// than silently weakening the comparison.
func distinctHistoryDays(ctx context.Context, t *testing.T, dsn string) int {
	t.Helper()
	conn, err := clickhouse.Open(httpOptions(t, dsn))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer func() { _ = conn.Close() }()

	var days uint64
	if err := conn.QueryRow(ctx, `
        SELECT count(DISTINCT day) FROM work_item_metrics_daily FINAL
    `).Scan(&days); err != nil {
		t.Fatalf("count history days: %v", err)
	}
	return int(days)
}

func buildCapacityProducer(t *testing.T) string {
	t.Helper()
	root := repoRoot(t)
	binary := filepath.Join(t.TempDir(), "capacity-native-producer")
	build := exec.Command("go", "build", "-o", binary, "./cmd/capacity-native-producer")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the native capacity producer: %v\n%s", err, output)
	}
	return binary
}

// readCapacity is the DORA reader's counterpart for this row type.
//
// The SELECT list is derived from capacityForecastRow by reflection, so adding
// a column to the struct adds it to both the query and the diff in one edit --
// there is no second list here to fall out of step with the table.
func readCapacity(
	ctx context.Context, t *testing.T, dsn string,
	table computeparity.Table, side string,
) computeparity.Snapshot {
	t.Helper()
	conn, err := clickhouse.Open(httpOptions(t, dsn))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer func() { _ = conn.Close() }()

	query := computeparity.Query[capacityForecastRow](table)
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("query %s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()

	var collected []capacityForecastRow
	for rows.Next() {
		var row capacityForecastRow
		if err := rows.ScanStruct(&row); err != nil {
			t.Fatalf("scan %s: %v", table.Name, err)
		}
		collected = append(collected, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s: %v", table.Name, err)
	}
	return computeparity.Encode(t, table, side, collected)
}

// assertConfigurationCanObserveTheStream fails when the fixture's simulation
// count is high enough that the output no longer depends on the draws.
//
// It runs the REAL kernel over the fixture's REAL history at the fixture's REAL
// simulation count, under two different seeds. Different seeds mean a
// completely different stream, so if the quantiles still match, this
// configuration cannot distinguish any two generators and a passing comparison
// below would be evidence about the IO path only.
//
// Production runs 10,000 simulations, where this check would FAIL by design --
// which is the honest statement of what row parity can and cannot prove for
// this family, and why the CPython stream is proven by vectors in
// internal/jobs/metrics/numerical/cpyrandom instead of being inferred here.
func assertConfigurationCanObserveTheStream(
	ctx context.Context, t *testing.T, dsn string,
) {
	t.Helper()
	history := readSeededHistory(ctx, t, dsn)
	if len(history) == 0 {
		t.Fatal("no seeded throughput history to reason about")
	}

	quantiles := func(seed int64) string {
		items := 40
		result, err := numerical.ForecastCapacity(numerical.ForecastRequest{
			History: numerical.Throughput{
				DailyThroughputs: history, DaysOfHistory: len(history),
			},
			TargetItems: &items,
			Simulations: capacityParitySimulations,
			Seed:        seed,
		}, time.Now().UTC())
		if err != nil {
			t.Fatalf("probe forecast: %v", err)
		}
		return fmt.Sprintf("%d/%d/%d",
			*result.P50Days, *result.P85Days, *result.P95Days)
	}

	// SEVERAL seeds, not two. Sensitivity is erratic seed to seed -- measured
	// on this fixture, one particular pair agrees at 100 and 200 simulations
	// while disagreeing at 10, 20, 30, 50, 75 and 150 -- so a single pair can
	// call a perfectly discriminating configuration blind, or miss a blind one.
	// Requiring more than one distinct result across a spread of seeds is the
	// robust form of the same question.
	seeds := []int64{
		capacityParitySeed,
		capacityParitySeed + 7919,
		capacityParitySeed + 104729,
		capacityParitySeed + 1299709,
	}
	distinct := map[string]bool{}
	observed := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		shape := quantiles(seed)
		distinct[shape] = true
		observed = append(observed, shape)
	}
	if len(distinct) < 2 {
		t.Fatalf(
			"at %d simulations this fixture gives the SAME quantiles %v for every "+
				"seed, so the comparison below cannot observe the random stream "+
				"at all: a Go port using an entirely different generator would "+
				"still report EQUAL. Lower the simulation count until they "+
				"differ, or stop claiming this run proves anything about the RNG.",
			capacityParitySimulations, observed)
	}
}

// readSeededHistory returns the throughput series exactly as the executor's
// own query would, so the probe reasons about the data the comparison uses
// rather than about the seeder's intent.
func readSeededHistory(ctx context.Context, t *testing.T, dsn string) []int {
	t.Helper()
	conn, err := clickhouse.Open(httpOptions(t, dsn))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer func() { _ = conn.Close() }()

	rows, err := conn.Query(ctx, `
        SELECT day, SUM(items_completed) AS items_completed
        FROM work_item_metrics_daily FINAL
        GROUP BY day ORDER BY day
    `)
	if err != nil {
		t.Fatalf("read seeded history: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var history []int
	for rows.Next() {
		var day time.Time
		var completed uint64
		if err := rows.Scan(&day, &completed); err != nil {
			t.Fatalf("scan history: %v", err)
		}
		history = append(history, int(completed))
	}
	return history
}
