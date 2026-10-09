package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

func TestClickHouseRepositoryDiscovererUsesPythonLatestRowQueryWithTenantFence(t *testing.T) {
	organizationID := "00000000-0000-4000-8000-000000000009"
	first := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	second := uuid.MustParse("00000000-0000-4000-8000-000000000002")
	connection := &recordingRepositoryConnection{rows: &repositoryRowsStub{identifiers: []uuid.UUID{first, second}}}
	discoverer, err := NewClickHouseRepositoryDiscoverer(connection)
	if err != nil {
		t.Fatal(err)
	}
	identifiers, err := discoverer.RepositoryIDs(context.Background(), organizationID)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(repositoryIDStrings(identifiers), ","), first.String()+","+second.String(); got != want {
		t.Fatalf("identifiers=%s want=%s", got, want)
	}
	if len(connection.queries) != 2 {
		t.Fatalf("queries = %d, want the repos read and one nil-repository work item probe", len(connection.queries))
	}
	if len(connection.argumentSets[0]) != 1 || connection.argumentSets[0][0] != organizationID {
		t.Fatalf("repos query arguments=%v, want only tenant id", connection.argumentSets[0])
	}
	if len(connection.argumentSets[1]) != 2 || connection.argumentSets[1][0] != organizationID || connection.argumentSets[1][1] != uuid.Nil {
		t.Fatalf("work item probe arguments=%v, want tenant id and the nil repository id", connection.argumentSets[1])
	}
	if probe := connection.queries[1]; !strings.Contains(probe, "FROM work_items") ||
		!strings.Contains(probe, "org_id = ?") || !strings.Contains(probe, "repo_id = ?") || !strings.Contains(probe, "LIMIT 1") {
		t.Fatalf("work item probe is not the tenant-fenced primary-key lookup:\n%s", probe)
	}
	connection.query = connection.queries[0]
	for _, fragment := range []string{
		"argMax(tuple(repo, settings, provider), last_synced)",
		"WHERE org_id = ?",
		"GROUP BY org_id, id",
		"ORDER BY id",
	} {
		if !strings.Contains(connection.query, fragment) {
			t.Fatalf("repository query omitted %q:\n%s", fragment, connection.query)
		}
	}
}

// TestPythonDiscoverReposOracle executes the production Python selector, not a
// copied fixture, and compares its repository identities with the Go adapter.
// It prevents an apparently equivalent Go query from drifting in grouping,
// tenant binding, or row-selection semantics while both implementations exist.
func TestPythonDiscoverReposOracle(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	// The script was executed once on the last build that carried the Python sources and its stdout is
	// frozen in testdata/golden/daily_discover_repos_oracle.json (recipe in the golden's spec); a frozen
	// run reads it and runs no Python. The script's text is part of the request.
	const scriptPath = "internal/jobs/metrics/daily/testdata/python_daily_discover_oracle.py"
	source, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(scriptPath)))
	if err != nil {
		t.Fatal(err)
	}
	spec := rotguard.Spec("testdata/golden/daily_discover_repos_oracle.json", "856b8850efef4a090ee9ebfd4a60e915bd9e2e18b4466a2ec622c7e6beabc9aa",
		"./internal/jobs/metrics/daily/", "^TestPythonDiscoverReposOracle$")
	answers := programoracle.Run(t, spec, root, []programoracle.Program{
		programoracle.Script("daily discover repos oracle", scriptPath, string(source), nil),
	})
	output := []byte(answers[0].Stdout)
	if answers[0].ExitCode != 0 {
		t.Fatalf("the production Python discover_repos oracle exited %d (stdout %q)", answers[0].ExitCode, output)
	}
	var oracle struct {
		IDs        []string          `json:"ids"`
		Query      string            `json:"query"`
		Parameters map[string]string `json:"parameters"`
	}
	output = bytes.TrimSpace(output)
	if lastLine := bytes.LastIndexByte(output, '\n'); lastLine >= 0 {
		output = output[lastLine+1:]
	}
	if err := json.Unmarshal(output, &oracle); err != nil {
		t.Fatalf("decode production Python oracle output %q: %v", output, err)
	}
	organizationID := "00000000-0000-4000-8000-000000000009"
	connection := &recordingRepositoryConnection{rows: &repositoryRowsStub{identifiers: []uuid.UUID{
		uuid.MustParse(oracle.IDs[0]), uuid.MustParse(oracle.IDs[1]),
	}}}
	discoverer, err := NewClickHouseRepositoryDiscoverer(connection)
	if err != nil {
		t.Fatal(err)
	}
	identifiers, err := discoverer.RepositoryIDs(context.Background(), organizationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := compareDailyOracleIDs(identifiers, oracle.IDs); err != nil {
		t.Fatal(err)
	}
	if oracle.Parameters["org_id"] != organizationID || !strings.Contains(oracle.Query, "GROUP BY org_id, id") ||
		!strings.Contains(oracle.Query, "argMax(tuple(repo, settings, provider), last_synced)") {
		t.Fatalf("Python production selector changed unexpectedly: parameters=%v query=%s", oracle.Parameters, oracle.Query)
	}
}

func TestDailyOracleComparatorRejectsMismatch(t *testing.T) {
	err := compareDailyOracleIDs([]RepositoryID{"a"}, []string{"b"})
	if err == nil {
		t.Fatal("the deliberate Go/Python identity mismatch was accepted")
	}
}

func compareDailyOracleIDs(goIDs []RepositoryID, pythonIDs []string) error {
	goJoined := strings.Join(repositoryIDStrings(goIDs), ",")
	if goJoined != strings.Join(pythonIDs, ",") {
		return fmt.Errorf("Go ids=%s Python ids=%s", goJoined, strings.Join(pythonIDs, ","))
	}
	return nil
}

type recordingRepositoryConnection struct {
	query     string
	arguments []any
	rows      driver.Rows
	// queries and argumentSets keep EVERY call, in order: the discoverer asks
	// for the repos table first and for a stored nil-repository work item
	// second (CHAOS-8821).
	queries      []string
	argumentSets [][]any
}

func (connection *recordingRepositoryConnection) Query(
	_ context.Context,
	query string,
	arguments ...any,
) (driver.Rows, error) {
	connection.queries = append(connection.queries, query)
	connection.argumentSets = append(connection.argumentSets, arguments)
	if isTeamRuleRead(query) {
		// The reads the team rules add are kept in queries only: query and
		// arguments stay the family's own read, and the canned rows are its
		// rows.
		return &emptyGovernanceRows{}, nil
	}
	connection.query = query
	connection.arguments = arguments
	return connection.rows, nil
}

type repositoryRowsStub struct {
	identifiers []uuid.UUID
	position    int
}

func (rows *repositoryRowsStub) Next() bool { return rows.position < len(rows.identifiers) }
func (rows *repositoryRowsStub) Scan(destinations ...any) error {
	if len(destinations) != 1 || rows.position >= len(rows.identifiers) {
		return errors.New("unexpected repository scan")
	}
	destination, ok := destinations[0].(*uuid.UUID)
	if !ok {
		return errors.New("repository destination has unexpected type")
	}
	*destination = rows.identifiers[rows.position]
	rows.position++
	return nil
}
func (*repositoryRowsStub) ScanStruct(any) error             { return errors.New("unused") }
func (*repositoryRowsStub) ColumnTypes() []driver.ColumnType { return nil }
func (*repositoryRowsStub) Totals(...any) error              { return errors.New("unused") }
func (*repositoryRowsStub) Columns() []string                { return []string{"id"} }
func (*repositoryRowsStub) Close() error                     { return nil }
func (*repositoryRowsStub) Err() error                       { return nil }
func (*repositoryRowsStub) HasData() bool                    { return true }
