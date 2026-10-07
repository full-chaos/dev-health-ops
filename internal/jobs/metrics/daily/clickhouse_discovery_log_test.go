package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// discoveryLogRecords runs one discovery with slog.Default() captured and
// returns the decoded records that carry RepositoryDiscoveryLogMessage.
func discoveryLogRecords(
	t *testing.T, connection repositoryRows, organizationID string,
) ([]RepositoryID, error, []map[string]any) {
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

	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(captured.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		if record["msg"] == RepositoryDiscoveryLogMessage {
			records = append(records, record)
		}
	}
	return identifiers, discoverErr, records
}

// The discoverer decides whether the daily job visits the work items that have
// no repository. That decision must be readable from outside: one line per
// discovery with the flag and the counts, for both answers of the probe.
func TestClickHouseRepositoryDiscovererLogsTheNoRepositoryDecisionAndTheCounts(t *testing.T) {
	organizationID := "00000000-0000-4000-8000-000000000009"
	first := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	second := uuid.MustParse("00000000-0000-4000-8000-000000000002")
	cases := []struct {
		name           string
		repositories   []uuid.UUID
		hasNil         bool
		wantPartitions float64
	}{
		{"an item with no repository is stored", []uuid.UUID{first, second}, true, 3},
		{"no item with no repository is stored", []uuid.UUID{first, second}, false, 2},
		{"only items with no repository are stored", nil, true, 1},
		{"nothing is stored", nil, false, 0},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			connection := &scriptedConnection{
				repositoryRows: &repositoryRowsStub{identifiers: testCase.repositories},
				probeRows:      &probeRowsStub{hasRow: testCase.hasNil},
			}
			identifiers, err, records := discoveryLogRecords(t, connection, organizationID)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != 1 {
				t.Fatalf("discovery log lines = %d (%v), want exactly 1", len(records), records)
			}
			record := records[0]
			if got := record["level"]; got != "INFO" {
				t.Fatalf("level = %v, want INFO", got)
			}
			if got := record["organization_id"]; got != organizationID {
				t.Fatalf("organization_id = %v, want %s", got, organizationID)
			}
			if got, ok := record["no_repository_partition_added"].(bool); !ok || got != testCase.hasNil {
				t.Fatalf("no_repository_partition_added = %v, want %t", record["no_repository_partition_added"], testCase.hasNil)
			}
			if got := record["repositories_discovered"]; got != float64(len(testCase.repositories)) {
				t.Fatalf("repositories_discovered = %v, want %d", got, len(testCase.repositories))
			}
			if got := record["partitions_discovered"]; got != testCase.wantPartitions {
				t.Fatalf("partitions_discovered = %v, want %v", got, testCase.wantPartitions)
			}
			// The line must describe the list the caller gets, not a count taken
			// at another point of the function.
			if float64(len(identifiers)) != testCase.wantPartitions {
				t.Fatalf("returned %d identifiers, the line says %v", len(identifiers), testCase.wantPartitions)
			}
			holdsNil := false
			for _, identifier := range identifiers {
				holdsNil = holdsNil || string(identifier) == uuid.Nil.String()
			}
			if holdsNil != testCase.hasNil {
				t.Fatalf("the returned list holds the nil id = %t, the line says %t", holdsNil, testCase.hasNil)
			}
			// Only the id, the flag and the counts: no query text, no row value.
			allowed := map[string]bool{
				"time": true, "level": true, "msg": true, "organization_id": true,
				"no_repository_partition_added": true, "repositories_discovered": true, "partitions_discovered": true,
			}
			for key := range record {
				if !allowed[key] {
					t.Fatalf("the discovery line carries the attribute %q, want only the id, the flag and the counts", key)
				}
			}
		})
	}
}

// A discovery that fails returns no list, so it must not write a line that
// says a list was discovered.
func TestClickHouseRepositoryDiscovererWritesNoDiscoveryLineWhenTheProbeFails(t *testing.T) {
	connection := &scriptedConnection{
		repositoryRows: &repositoryRowsStub{identifiers: []uuid.UUID{uuid.MustParse("00000000-0000-4000-8000-000000000001")}},
		probeErr:       errors.New("clickhouse down"),
	}
	_, err, records := discoveryLogRecords(t, connection, "00000000-0000-4000-8000-000000000009")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("error = %v, want ErrUnavailable", err)
	}
	if len(records) != 0 {
		t.Fatalf("a failed discovery wrote %d discovery lines: %v", len(records), records)
	}
}
