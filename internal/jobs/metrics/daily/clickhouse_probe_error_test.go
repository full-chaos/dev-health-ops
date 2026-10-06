package daily

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// scriptedConnection answers the repos read and the work item probe with
// separate, programmable results (CHAOS-8821). A failure of the probe must
// reach the caller: if it is swallowed, the discoverer returns the list with no
// nil id and the daily families silently skip every work item that has no
// repository.
type scriptedConnection struct {
	repositoryRows driver.Rows
	probeRows      driver.Rows
	probeErr       error
}

func (connection *scriptedConnection) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	if strings.Contains(query, "FROM work_items") {
		return connection.probeRows, connection.probeErr
	}
	return connection.repositoryRows, nil
}

type probeRowsStub struct {
	repositoryRowsStub
	hasRow bool
	err    error
}

func (rows *probeRowsStub) Next() bool   { return rows.hasRow }
func (rows *probeRowsStub) Err() error   { return rows.err }
func (rows *probeRowsStub) Close() error { return nil }

func TestClickHouseRepositoryDiscovererReturnsTheErrorOfTheNilRepositoryProbe(t *testing.T) {
	organizationID := "00000000-0000-4000-8000-000000000009"
	repository := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	cases := []struct {
		name  string
		probe *scriptedConnection
	}{
		{"query fails", &scriptedConnection{probeErr: errors.New("clickhouse down")}},
		{"rows fail after the first read", &scriptedConnection{probeRows: &probeRowsStub{err: errors.New("stream broke")}}},
		{"rows fail although a row was read", &scriptedConnection{probeRows: &probeRowsStub{hasRow: true, err: errors.New("stream broke")}}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.probe.repositoryRows = &repositoryRowsStub{identifiers: []uuid.UUID{repository}}
			discoverer, err := NewClickHouseRepositoryDiscoverer(testCase.probe)
			if err != nil {
				t.Fatal(err)
			}
			identifiers, err := discoverer.RepositoryIDs(context.Background(), organizationID)
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("error = %v, identifiers = %v, want ErrUnavailable and no partial list", err, identifiers)
			}
			if identifiers != nil {
				t.Fatalf("identifiers = %v returned together with the error, want nil", identifiers)
			}
		})
	}
	t.Run("a stored item adds the nil id and the error stays nil", func(t *testing.T) {
		connection := &scriptedConnection{
			repositoryRows: &repositoryRowsStub{identifiers: []uuid.UUID{repository}},
			probeRows:      &probeRowsStub{hasRow: true},
		}
		discoverer, err := NewClickHouseRepositoryDiscoverer(connection)
		if err != nil {
			t.Fatal(err)
		}
		identifiers, err := discoverer.RepositoryIDs(context.Background(), organizationID)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := strings.Join(repositoryIDStrings(identifiers), ","), uuid.Nil.String()+","+repository.String(); got != want {
			t.Fatalf("identifiers = %s, want %s", got, want)
		}
	})
}
