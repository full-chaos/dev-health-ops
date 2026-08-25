package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
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
	if len(connection.arguments) != 1 || connection.arguments[0] != organizationID {
		t.Fatalf("query arguments=%v, want only tenant id", connection.arguments)
	}
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
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	python := os.Getenv("PYTHON")
	if python == "" {
		t.Fatal("PYTHON is required for the live daily metrics Python oracle")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "testdata/python_daily_discover_oracle.py")
	command.Dir = filepath.Join(root, "internal", "jobs", "metrics", "daily")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err = command.Run()
	output := stdout.Bytes()
	if err != nil {
		t.Fatalf("execute production Python discover_repos oracle: %v\nstdout:\n%s\nstderr:\n%s", err, output, stderr.String())
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
	if err := os.WriteFile(filepath.Join(proofDirectory, "daily-metrics-discover"), []byte("executed\n"), 0o600); err != nil {
		t.Fatalf("write daily metrics live Python oracle proof: %v", err)
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
}

func (connection *recordingRepositoryConnection) Query(
	_ context.Context,
	query string,
	arguments ...any,
) (driver.Rows, error) {
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
