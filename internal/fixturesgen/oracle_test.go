package fixturesgen

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

//go:embed testdata/generators_oracle.py
var generatorsOracleProgram string

type leaf struct {
	T string `json:"t"`
	V string `json:"v"`
}

func requireOracleEnv(t *testing.T) string {
	t.Helper()
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR") == "" {
		t.Fatal("live Python oracle opt-in requires a proof directory from ci/check_go.sh")
	}
	_, currentFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(currentFile)))
	python := pyoracle.Resolve(t, repoRoot)
	probe, probeErr := exec.Command(python, pyoracle.VersionProbeArgs...).Output()
	pyoracle.RequireDeployed(t, python, probe, probeErr)
	return python
}

// askPython sends every request line to ONE python process running the oracle program
// and returns its answer lines, one per request.
func askPython(t *testing.T, python string, requests []any) []json.RawMessage {
	t.Helper()
	var input strings.Builder
	for _, request := range requests {
		line, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		input.Write(line)
		input.WriteByte('\n')
	}
	command := exec.Command(python, "-c", generatorsOracleProgram)
	command.Stdin = strings.NewReader(input.String())
	command.Env = append(os.Environ(), "PYTHONHASHSEED=0")
	output, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, stderr))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != len(requests) {
		t.Fatalf("python answered %d for %d requests", len(lines), len(requests))
	}
	answers := make([]json.RawMessage, len(lines))
	for i, line := range lines {
		answers[i] = json.RawMessage(line)
	}
	return answers
}

func writeProofs(t *testing.T, name string) {
	t.Helper()
	if t.Failed() {
		return
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if err := os.WriteFile(filepath.Join(proof, name), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	venueoracle.WriteProof(t)
}

type randomOp struct {
	Name string
	Args []int64
}

func (op randomOp) MarshalJSON() ([]byte, error) {
	items := []any{op.Name}
	for _, arg := range op.Args {
		items = append(items, arg)
	}
	return json.Marshal(items)
}

// randomCorpus is class-generated: every operation kind over bounds that reject
// (non powers of two), never reject (powers of two, 1), sit on the 32/64-bit word
// boundaries, and go negative for randint, on seeds of both signs and beyond 64 bits.
func randomCorpus() ([]map[string]any, []string) {
	generator := rand.New(rand.NewSource(20260926))
	seeds := []string{"0", "1", "2", "42", "-7", "20260219", "4294967295", "4294967296", "-4294967296",
		"9223372036854775807", "-9223372036854775808", "18446744073709551615", "340282366920938463463374607431768211456"}
	bounds := []int64{1, 2, 3, 5, 7, 8, 10, 13, 60, 64, 65, 100, 541, 1024, 4095, 4096, 1<<31 - 1, 1 << 31, 1<<32 - 1, 1 << 32, 1<<32 + 1, 1<<40 - 1, 1 << 40, 1<<62 + 3}
	var corpus []map[string]any
	var seedsOut []string
	for _, seed := range seeds {
		ops := []randomOp{}
		for k := 0; k <= 64; k++ {
			ops = append(ops, randomOp{"getrandbits", []int64{int64(k)}})
		}
		for _, n := range bounds {
			ops = append(ops, randomOp{"randrange", []int64{n}}, randomOp{"choice", []int64{n}}, randomOp{"random", nil})
			if n <= 1<<40 { // randint's width b-a+1 must fit an int64 here
				ops = append(ops, randomOp{"randint", []int64{0, n}}, randomOp{"randint", []int64{-n, n}}, randomOp{"randint", []int64{n, n + 9}})
			}
		}
		for i := 0; i < 400; i++ {
			n := bounds[generator.Intn(len(bounds))]
			switch generator.Intn(5) {
			case 0:
				ops = append(ops, randomOp{"getrandbits", []int64{int64(generator.Intn(65))}})
			case 1:
				ops = append(ops, randomOp{"randrange", []int64{n}})
			case 2:
				ops = append(ops, randomOp{"randint", []int64{int64(generator.Intn(200)) - 100, 100 + int64(generator.Intn(1000))}})
			case 3:
				ops = append(ops, randomOp{"choice", []int64{n}})
			default:
				ops = append(ops, randomOp{"random", nil})
			}
		}
		corpus = append(corpus, map[string]any{"kind": "random", "seed": seed, "ops": ops})
		seedsOut = append(seedsOut, seed)
	}
	return corpus, seedsOut
}

func floatLeaf(value float64) leaf {
	return leaf{"float", fmt.Sprintf("%016x", math.Float64bits(value))}
}

func intLeaf(value int64) leaf { return leaf{"int", strconv.FormatInt(value, 10)} }

// TestRandMatchesLivePython drives Rand and a REAL random.Random over the same operation
// sequences and compares every value: getrandbits, randrange, randint, choice and random().
func TestRandMatchesLivePython(t *testing.T) {
	python := requireOracleEnv(t)
	corpus, seeds := randomCorpus()
	requests := make([]any, len(corpus))
	for i := range corpus {
		requests[i] = corpus[i]
	}
	answers := askPython(t, python, requests)
	compared := 0
	for index, request := range corpus {
		var want struct {
			Values []leaf `json:"values"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(answers[index], &want); err != nil {
			t.Fatal(err)
		}
		if want.Error != "" {
			t.Fatalf("seed %s: python: %s", seeds[index], want.Error)
		}
		seed, _ := new(big.Int).SetString(seeds[index], 10)
		rng := NewRand(seed)
		ops := request["ops"].([]randomOp)
		if len(want.Values) != len(ops) {
			t.Fatalf("seed %s: python returned %d values for %d ops", seeds[index], len(want.Values), len(ops))
		}
		for i, op := range ops {
			var got leaf
			switch op.Name {
			case "getrandbits":
				got = leaf{"int", strconv.FormatUint(rng.GetRandBits(int(op.Args[0])), 10)}
			case "randrange":
				got = intLeaf(int64(rng.RandRange(int(op.Args[0]))))
			case "randint":
				got = intLeaf(int64(rng.RandInt(int(op.Args[0]), int(op.Args[1]))))
			case "choice":
				got = intLeaf(int64(rng.Choice(int(op.Args[0]))))
			case "random":
				got = floatLeaf(rng.Random())
			}
			compared++
			if got != want.Values[i] {
				t.Fatalf("seed %s op #%d %s%v: go=%v python=%v", seeds[index], i, op.Name, op.Args, got, want.Values[i])
			}
		}
		if err := rng.Err(); err != nil {
			t.Fatalf("seed %s: %v", seeds[index], err)
		}
	}
	t.Logf("%d random draws over %d seeds match the live CPython generator", compared, len(seeds))
	writeProofs(t, "fixtures-pyrand")
}

type telemetryCase struct {
	OrgID          string `json:"org_id"`
	Days           int    `json:"days"`
	SessionsPerDay int    `json:"sessions_per_day"`
	Seed           *int64 `json:"seed"`
	EndTime        string `json:"end_time"`
}

func telemetryCorpus() []telemetryCase {
	seed := func(v int64) *int64 { return &v }
	orgs := []string{"6ba7b810-9dad-11d1-80b4-00c04fd430c8", "org-two", "", "unicode-é-org"}
	ends := []string{"2026-09-26T07:31:22.123456+00:00", "2026-01-01T00:00:00+00:00", "2026-03-01T23:59:59.999999+00:00", "2024-02-29T12:00:00+00:00"}
	var corpus []telemetryCase
	for oi, org := range orgs {
		for si, s := range []*int64{nil, seed(0), seed(1), seed(42), seed(-7), seed(9223372036854775807)} {
			corpus = append(corpus, telemetryCase{org, 2 + (oi+si)%3, []int{1, 2, 5, 50}[(oi+si)%4], s, ends[(oi+si)%len(ends)]})
		}
	}
	// The TTL clamp: days above the ceiling generate the ceiling's worth, and the boundary itself.
	corpus = append(corpus,
		telemetryCase{orgs[0], ProductTelemetryCeilingDays, 1, seed(3), ends[0]},
		telemetryCase{orgs[0], ProductTelemetryCeilingDays + 1, 1, seed(3), ends[0]},
		telemetryCase{orgs[1], 400, 1, seed(3), ends[1]},
		// Nothing to generate.
		telemetryCase{orgs[0], 0, 5, seed(1), ends[0]},
		telemetryCase{orgs[0], -3, 5, seed(1), ends[0]},
		telemetryCase{orgs[0], 3, 0, seed(1), ends[0]},
		telemetryCase{orgs[0], 3, -4, seed(1), ends[0]},
		// The default volume of the verb, a month of it.
		telemetryCase{orgs[0], 30, 50, seed(20260219), ends[0]},
	)
	return corpus
}

func columnValue(row ProductTelemetryRow, column string) (leaf, bool) {
	switch column {
	case "org_id_hash":
		return leaf{"str", row.OrgIDHash}, true
	case "event_id":
		return leaf{"str", row.EventID}, true
	case "name":
		return leaf{"str", row.Name}, true
	case "schema_version":
		return leaf{"str", row.SchemaVersion}, true
	case "session_id":
		return leaf{"str", row.SessionID}, true
	case "anonymous_user_id":
		return leaf{"str", row.AnonymousUserID}, true
	case "route_pattern":
		if row.RoutePattern == nil {
			return leaf{"null", ""}, true
		}
		return leaf{"str", *row.RoutePattern}, true
	case "payload_json":
		return leaf{"str", row.PayloadJSON}, true
	case "occurred_at":
		return leaf{"datetime", row.OccurredAt.UTC().Format("2006-01-02T15:04:05")}, true
	case "source":
		return leaf{"str", row.Source}, true
	}
	return leaf{}, false
}

// TestProductTelemetryMatchesLivePython runs the REAL ProductTelemetryGenerator and the REAL
// persist_product_telemetry_events (only the ClickHouse client is a capture) and compares every
// column of every row with GenerateProductTelemetry. The compared columns are the production
// module's PRODUCT_TELEMETRY_COLUMNS: a column added there that Go does not know fails the test
// instead of being skipped. ingested_at is the wall clock of the write, not of the generation, so
// it is the one column not compared, and the test says so by requiring it to be exactly that.
func TestProductTelemetryMatchesLivePython(t *testing.T) {
	python := requireOracleEnv(t)
	corpus := telemetryCorpus()
	requests := make([]any, len(corpus))
	for i, c := range corpus {
		request := map[string]any{"kind": "product_telemetry", "org_id": c.OrgID, "days": c.Days,
			"sessions_per_day": c.SessionsPerDay, "seed": c.Seed, "end_time": c.EndTime}
		requests[i] = request
	}
	answers := askPython(t, python, requests)
	rowsCompared, nonEmpty := 0, 0
	for index, c := range corpus {
		label := fmt.Sprintf("org=%q days=%d sessions=%d seed=%v end=%s", c.OrgID, c.Days, c.SessionsPerDay, c.Seed, c.EndTime)
		var want struct {
			Columns []string `json:"columns"`
			Rows    [][]leaf `json:"rows"`
			Ceiling *int     `json:"ceiling"`
			Error   string   `json:"error"`
		}
		if err := json.Unmarshal(answers[index], &want); err != nil {
			t.Fatal(err)
		}
		if want.Error != "" {
			t.Fatalf("%s: python: %s", label, want.Error)
		}
		if want.Ceiling == nil || *want.Ceiling != ProductTelemetryCeilingDays {
			t.Fatalf("%s: python's generation ceiling is %v, Go's ProductTelemetryCeilingDays is %d", label, want.Ceiling, ProductTelemetryCeilingDays)
		}
		end, err := time.Parse(time.RFC3339Nano, c.EndTime)
		if err != nil {
			t.Fatal(err)
		}
		got, err := GenerateProductTelemetry(ProductTelemetrySpec{OrgID: c.OrgID, Days: c.Days, SessionsPerDay: c.SessionsPerDay, Seed: c.Seed, EndTime: end})
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if len(got) != len(want.Rows) {
			t.Fatalf("%s: go generated %d rows, python %d", label, len(got), len(want.Rows))
		}
		if len(got) > 0 {
			nonEmpty++
		}
		for r, row := range got {
			for column, name := range want.Columns {
				if name == "ingested_at" {
					if want.Rows[r][column].T != "datetime" {
						t.Fatalf("%s: python's ingested_at is %v, not a datetime", label, want.Rows[r][column])
					}
					continue
				}
				value, known := columnValue(row, name)
				if !known {
					t.Fatalf("PRODUCT_TELEMETRY_COLUMNS has %q, which the Go row does not carry: add it", name)
				}
				if value != want.Rows[r][column] {
					t.Fatalf("%s: row %d column %s: go=%v python=%v", label, r, name, value, want.Rows[r][column])
				}
			}
			rowsCompared++
		}
	}
	if nonEmpty < len(corpus)/2 {
		t.Fatalf("only %d of %d cases generated rows", nonEmpty, len(corpus))
	}
	t.Logf("%d cases, %d rows compared column by column with the live Python producer", len(corpus), rowsCompared)
	writeProofs(t, "fixtures-product-telemetry")
}

// TestSyntheticOrgIDsMatchThePythonFallback compares the fallback org ids with the ones the
// Python verb's own expression yields.
func TestSyntheticOrgIDsMatchThePythonFallback(t *testing.T) {
	python := requireOracleEnv(t)
	answers := askPython(t, python, []any{map[string]any{"kind": "synthetic_orgs", "count": 12}})
	var want struct {
		IDs []string `json:"ids"`
	}
	if err := json.Unmarshal(answers[0], &want); err != nil {
		t.Fatal(err)
	}
	got := SyntheticOrgIDs(12)
	if len(want.IDs) != 12 || strings.Join(got, ",") != strings.Join(want.IDs, ",") {
		t.Fatalf("go %v python %v", got, want.IDs)
	}
	writeProofs(t, "fixtures-synthetic-orgs")
}
