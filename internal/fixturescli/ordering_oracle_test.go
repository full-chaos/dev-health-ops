package fixturescli

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The Go stamp (ordering.go) is a second implementation of the Python producer's ordering derivation
// (models/operational.py CanonicalOperationalEntity.__post_init__ -> operational_ordering.build_entity_ordering).
// "The server accepted the row" proves only the CHECK, not the values, so this oracle builds the REAL
// producer entity from the same frozen row and compares (source_revision, source_conflict_key,
// ingest_revision, ordering_contract) leaf for leaf. Replays a frozen golden.

type oracleLeaf struct {
	T string `json:"t"`
	V string `json:"v"`
}

type oracleCase struct {
	Name   string                `json:"name"`
	Family string                `json:"family"`
	Row    map[string]oracleLeaf `json:"row"`
}

type oracleStamp struct {
	SourceRevision   string `json:"source_revision"`
	SourceConflict   string `json:"source_conflict_key"`
	IngestRevision   string `json:"ingest_revision"`
	OrderingContract int    `json:"ordering_contract"`
}

// leafText is a cell as the oracle sends it: the column type it was read with and its text.
func leafText(column FrozenColumn, cell any) oracleLeaf {
	if cell == nil {
		return oracleLeaf{T: "null"}
	}
	return oracleLeaf{T: column.Type, V: fmt.Sprint(cell)}
}

// orderingPythonBuild is the build whose ordering stamper answered the frozen
// cases: a build that still carried the Python fixtures verb.
const orderingPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// TestOrderingStampMatchesThePythonProducer compares stampOrdering with the Python
// producer's ordering stamp for every operational row of the frozen live-e2e world and
// its variants. The producer's stamps were executed once on orderingPythonBuild and are
// frozen in testdata/golden/ordering.json; the cases are part of the golden's key.
func TestOrderingStampMatchesThePythonProducer(t *testing.T) {
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/ordering.json",
		PythonBuild: orderingPythonBuild,
		SHA256:      "b8d31703b19efd7ae7a23770561ba1695bff77f4e6bd5da234fe9ee4d84c95ce",
	})
	set := generateOracleWorld(t)

	var cases []oracleCase
	stamps := map[string]oracleStamp{}
	seen := map[string]int{}
	for _, table := range set {
		family := operationalFamilies[table.Name]
		if family == "" {
			continue
		}
		rows := table.Rows
		for number, row := range rows {
			name := fmt.Sprintf("%s#%d", table.Name, number)
			cases = append(cases, oracleCase{Name: name, Family: family, Row: oracleRow(table, row)})
			stamped, out, err := stampOrdering(table.WorldTable, [][]any{row})
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			tail := out[0][len(out[0])-4:]
			stamps[name] = oracleStamp{SourceRevision: fmt.Sprint(tail[0]), SourceConflict: fmt.Sprint(tail[1]), IngestRevision: fmt.Sprint(tail[2]), OrderingContract: tail[3].(int)}
			_ = stamped
			seen[table.Name]++
		}
	}
	// Every operational table of the frozen live-e2e world, and the variants that exercise what a
	// frozen row alone does not: a tombstone, fractional seconds, a float, a UUID, nullable fields.
	for _, name := range []string{"operational_incidents", "operational_services", "operational_service_repository_mappings"} {
		if seen[name] < 2 {
			t.Fatalf("%s: %d case(s): the oracle measures too little", name, seen[name])
		}
	}

	input, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	request := venueoracle.ProgramRequest("ordering cases", input)
	answers := golden.Produce(t, []venueoracle.Request{request})
	golden.Consumed(t, answers...)
	var want map[string]oracleStamp
	if err := json.Unmarshal([]byte(venueoracle.UnpackBody(t, answers[0].Body)), &want); err != nil {
		t.Fatalf("decode the producer output: %v", err)
	}
	if len(want) != len(cases) {
		t.Fatalf("the producer stamped %d of %d cases", len(want), len(cases))
	}
	mismatches := 0
	for _, item := range cases {
		if got, exp := stamps[item.Name], want[item.Name]; got != exp {
			mismatches++
			if mismatches <= 5 {
				t.Errorf("%s: Go %+v != Python %+v", item.Name, got, exp)
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d stamped rows differ from the Python producer", mismatches, len(cases))
	}
	t.Logf("%d rows agree with the Python producer's frozen stamps (%s)", len(cases), strings.Join(sortedKeys(seen), ", "))
	golden.SkipDiff(t)
	golden.Finish(t)
}

func sortedKeys(counts map[string]int) []string {
	keys := make([]string, 0, len(counts))
	for key, count := range counts {
		keys = append(keys, fmt.Sprintf("%s=%d", key, count))
	}
	// order-insensitive text, stable enough for a log line
	return keys
}

const (
	oracleShiftDays = 3
	oracleOrg       = "99999999-8888-4777-8666-555555555555"
)

type oracleTable struct {
	WorldTable
	Rows [][]any
}

// generateOracleWorld is the operational tables of the frozen world the web live-e2e run loads,
// each with its frozen rows and the variants a frozen row alone does not exercise.
func generateOracleWorld(t *testing.T) []oracleTable {
	t.Helper()
	world, err := LoadFrozenWorld(GenerateParams{
		Provider: "synthetic", RepoName: "acme/live-e2e", RepoCount: 1, Days: 14, CommitsPerDay: 6, PRCount: 24, TeamCount: 10,
		Seed: 20260219, WithMetrics: true, WithWorkGraph: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var out []oracleTable
	for _, table := range world.Tables {
		if _, operational := operationalFamilies[table.Name]; !operational {
			continue
		}
		// The rows the verb stamps are the TRANSFORMED ones (dates shifted, org rewritten), so the oracle
		// compares those, not the frozen text.
		rows, err := table.Transform(oracleShiftDays, world.OrgID, oracleOrg)
		if err != nil {
			t.Fatal(err)
		}
		index := map[string]int{}
		for position, column := range table.Columns {
			index[column.Name] = position
		}
		base := rows[0]
		variant := func(edit func(row []any)) {
			row := append([]any{}, base...)
			edit(row)
			rows = append(rows, row)
		}
		set := func(row []any, name string, value any) {
			position, ok := index[name]
			if !ok {
				t.Fatalf("%s has no column %s", table.Name, name)
			}
			row[position] = value
		}
		flag := "is_deleted"
		if _, ok := index[flag]; !ok {
			flag = "is_active"
		}
		tombstone := json.Number("1")
		if flag == "is_active" {
			tombstone = json.Number("0")
		}
		variant(func(row []any) { set(row, flag, tombstone) })
		variant(func(row []any) {
			set(row, "last_synced", "2026-09-01 10:00:02.123456")
			set(row, "observed_at", "2026-09-01 10:00:01.654321")
			set(row, "source_version_at", "2026-09-01 09:59:59.000001")
		})
		variant(func(row []any) { set(row, "relationship_confidence", json.Number("0.5")) })
		variant(func(row []any) { set(row, "source_id", "AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE") })
		out = append(out, oracleTable{WorldTable: table, Rows: rows})
	}
	return out
}

func oracleRow(table oracleTable, row []any) map[string]oracleLeaf {
	leaves := map[string]oracleLeaf{}
	for position, column := range table.Columns {
		leaves[column.Name] = leafText(column, row[position])
	}
	return leaves
}
