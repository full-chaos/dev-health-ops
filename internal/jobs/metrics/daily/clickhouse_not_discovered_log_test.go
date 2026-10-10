package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// notDiscoveredRowsStub answers the count of repositories not discovered.
type notDiscoveredRowsStub struct {
	sources  []string
	counts   []uint64
	position int
	scanErr  error
	err      error
	closeErr error
}

func (rows *notDiscoveredRowsStub) Next() bool { return rows.position < len(rows.sources) }
func (rows *notDiscoveredRowsStub) Scan(destinations ...any) error {
	if rows.scanErr != nil {
		return rows.scanErr
	}
	source, okSource := destinations[0].(*string)
	count, okCount := destinations[1].(*uint64)
	if len(destinations) != 2 || !okSource || !okCount {
		return errors.New("unexpected scan of the count")
	}
	*source, *count = rows.sources[rows.position], rows.counts[rows.position]
	rows.position++
	return nil
}
func (*notDiscoveredRowsStub) ScanStruct(any) error             { return errors.New("unused") }
func (*notDiscoveredRowsStub) ColumnTypes() []driver.ColumnType { return nil }
func (*notDiscoveredRowsStub) Totals(...any) error              { return errors.New("unused") }
func (*notDiscoveredRowsStub) Columns() []string                { return []string{"source", "repository_ids"} }
func (rows *notDiscoveredRowsStub) Close() error                { return rows.closeErr }
func (rows *notDiscoveredRowsStub) Err() error                  { return rows.err }
func (*notDiscoveredRowsStub) HasData() bool                    { return true }

// warnRecords runs one discovery and then the report of the repositories it
// cannot discover, as the dispatch of a run does, and returns the result of
// the discovery with the lines of the report.
func warnRecords(t *testing.T, connection repositoryRows, organizationID string) ([]RepositoryID, error, []map[string]any) {
	t.Helper()
	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(previous)
	discoverer, err := NewClickHouseRepositoryDiscoverer(connection)
	if err != nil {
		t.Fatal(err)
	}
	identifiers, discoverErr := discoverer.RepositoryIDs(context.Background(), organizationID)
	discoverer.ReportRepositoriesNotDiscovered(context.Background(), organizationID)
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(captured.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		if record["msg"] == RepositoryRowsNotDiscoveredLogMessage {
			records = append(records, record)
		}
	}
	return identifiers, discoverErr, records
}

// The repos table is the only source of a whole-organization run's
// repositories. Stored rows under a repository id with no repos row are
// computed by no such run, and the run must say so with counts: one line, INFO
// with three zeros when there is none (so a reader of the log sees that the
// count ran), WARN when a count is above 0. It must never take a count it
// could not read as 0, and it must never change or fail the discovery.
func TestClickHouseRepositoryDiscovererSaysTheRepositoriesItCannotDiscover(t *testing.T) {
	organizationID := "00000000-0000-4000-8000-000000000009"
	repository := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	for _, test := range []struct {
		name      string
		rows      driver.Rows
		err       error
		warn      bool
		countRead bool
		want      [3]float64 // pull requests, commits, work items
	}{
		{"no stored row is out of reach", &notDiscoveredRowsStub{}, nil, false, true, [3]float64{}},
		{"pull requests only", &notDiscoveredRowsStub{sources: []string{"git_pull_requests"}, counts: []uint64{2}}, nil, true, true, [3]float64{2, 0, 0}},
		{"commits only", &notDiscoveredRowsStub{sources: []string{"git_commits"}, counts: []uint64{3}}, nil, true, true, [3]float64{0, 3, 0}},
		{"work items only", &notDiscoveredRowsStub{sources: []string{"work_items"}, counts: []uint64{4}}, nil, true, true, [3]float64{0, 0, 4}},
		{"every source", &notDiscoveredRowsStub{sources: []string{"work_items", "git_commits", "git_pull_requests"}, counts: []uint64{5, 11, 19}}, nil, true, true, [3]float64{19, 11, 5}},
		{"a source with a count of 0 only", &notDiscoveredRowsStub{sources: []string{"git_commits"}, counts: []uint64{0}}, nil, false, true, [3]float64{}},
		{"the query fails", nil, errors.New("clickhouse down"), true, false, [3]float64{}},
		{"no rows and no error", nil, nil, true, false, [3]float64{}},
		{"a row cannot be scanned", &notDiscoveredRowsStub{sources: []string{"git_commits"}, counts: []uint64{1}, scanErr: errors.New("scan")}, nil, true, false, [3]float64{}},
		{"the rows fail after a read", &notDiscoveredRowsStub{sources: []string{"git_commits"}, counts: []uint64{1}, err: errors.New("stream broke")}, nil, true, false, [3]float64{}},
		{"the rows fail at their close", &notDiscoveredRowsStub{sources: []string{"git_commits"}, counts: []uint64{1}, closeErr: errors.New("close")}, nil, true, false, [3]float64{}},
		{"a source the count does not name", &notDiscoveredRowsStub{sources: []string{"deployments"}, counts: []uint64{1}}, nil, true, false, [3]float64{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := &scriptedConnection{
				repositoryRows:    &repositoryRowsStub{identifiers: []uuid.UUID{repository}},
				probeRows:         &probeRowsStub{},
				notDiscoveredRows: test.rows, notDiscoveredErr: test.err,
			}
			identifiers, err, records := warnRecords(t, connection, organizationID)
			if err != nil || len(identifiers) != 1 || string(identifiers[0]) != repository.String() {
				t.Fatalf("the discovery returned %v, %v: the count must not change or fail it", identifiers, err)
			}
			if len(records) != 1 {
				t.Fatalf("lines of the report = %d (%v), want exactly 1", len(records), records)
			}
			record := records[0]
			level := "INFO"
			if test.warn {
				level = "WARN"
			}
			if record["level"] != level || record["organization_id"] != organizationID {
				t.Fatalf("level and organization = %v, %v; want %s", record["level"], record["organization_id"], level)
			}
			if read, ok := record["count_read"].(bool); !ok || read != test.countRead {
				t.Fatalf("count_read = %v, want %t", record["count_read"], test.countRead)
			}
			fields := []string{"repositories_with_pull_requests", "repositories_with_commits", "repositories_with_work_items"}
			for index, field := range fields {
				value, present := record[field]
				if !test.countRead {
					if present {
						t.Errorf("%s = %v on a count that was not read: a count that was not read is not a number", field, value)
					}
					continue
				}
				if value != test.want[index] {
					t.Errorf("%s = %v, want %v", field, value, test.want[index])
				}
			}
		})
	}
}

// blockingConnection answers the two reads of the discovery at once and holds
// the count until its context ends, as a scan that takes too long does.
type blockingConnection struct {
	scriptedConnection
	waited time.Duration
}

func (connection *blockingConnection) Query(ctx context.Context, query string, arguments ...any) (driver.Rows, error) {
	if query != notDiscoveredRepositoriesSQL {
		return connection.scriptedConnection.Query(ctx, query, arguments...)
	}
	started := time.Now()
	<-ctx.Done()
	connection.waited = time.Since(started)
	return nil, ctx.Err()
}

// The dispatch of a run waits for the report, and the count scans three source
// tables. The wait is bounded: a count that takes longer than its time is
// given up, the line says the count was not read, and the caller goes on.
func TestTheReportOfRepositoriesNotDiscoveredIsBoundedInTime(t *testing.T) {
	organizationID := "00000000-0000-4000-8000-000000000009"
	connection := &blockingConnection{scriptedConnection: scriptedConnection{
		repositoryRows: &repositoryRowsStub{}, probeRows: &probeRowsStub{},
	}}
	discoverer, err := NewClickHouseRepositoryDiscoverer(connection)
	if err != nil {
		t.Fatal(err)
	}
	discoverer.reportTimeout = 50 * time.Millisecond
	var captured bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(previous)

	done := make(chan struct{})
	started := time.Now()
	go func() {
		discoverer.ReportRepositoriesNotDiscovered(context.Background(), organizationID)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the report did not return: the count has no bound in time")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second || connection.waited < 40*time.Millisecond {
		t.Fatalf("the report took %s and the count waited %s; want the count held for its 50ms and then given up", elapsed, connection.waited)
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(captured.Bytes()), &record); err != nil {
		t.Fatalf("decode %q: %v", captured.String(), err)
	}
	if record["msg"] != RepositoryRowsNotDiscoveredLogMessage || record["level"] != "WARN" || record["count_read"] != false {
		t.Fatalf("the line of a count that ran out of time = %v, want WARN with count_read false", record)
	}
	if notDiscoveredReportTimeout <= 0 || notDiscoveredReportTimeout > time.Minute {
		t.Fatalf("the bound of the report is %s: it must be above 0 and far below the time of a dispatch", notDiscoveredReportTimeout)
	}
}
