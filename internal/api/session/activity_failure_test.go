package session

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// fakeConn answers the activity statements from a script keyed by
// organization and table; a missing key is a failed read.
type fakeConn struct {
	driver.Conn
	rows map[string]fakeRow
}

type fakeRow struct {
	count uint64
	last  time.Time
	err   error
}

func (r fakeRow) Err() error           { return r.err }
func (r fakeRow) ScanStruct(any) error { return r.err }
func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*uint64) = r.count
	*dest[1].(*time.Time) = r.last
	return nil
}

func (c fakeConn) QueryRow(_ context.Context, query string, args ...any) driver.Row {
	org := args[0].(driver.NamedValue).Value.(string)
	for _, table := range metricTables {
		if strings.Contains(query, "FROM "+table) {
			if row, ok := c.rows[org+"/"+table]; ok {
				return row
			}
			return fakeRow{err: errors.New("code: 47, message: Missing columns: 'retracted' password=hunter2")}
		}
	}
	return fakeRow{err: errors.New("unexpected statement")}
}

func TestLoginChoiceNeverRestsOnAFailedActivityRead(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	orgB, orgA := uuid.New(), uuid.New() // B joined first (member), A later (owner)
	joinedB := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	joinedA := joinedB.Add(48 * time.Hour)
	memberships := []membershipRow{
		{OrgID: orgB, CreatedAt: joinedB},
		{OrgID: orgA, CreatedAt: joinedA},
	}
	newer := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	older := newer.Add(-72 * time.Hour)

	all := func(org uuid.UUID, count uint64, last time.Time) map[string]fakeRow {
		out := map[string]fakeRow{}
		for _, table := range metricTables {
			out[org.String()+"/"+table] = fakeRow{count: count, last: last}
		}
		return out
	}
	merge := func(parts ...map[string]fakeRow) map[string]fakeRow {
		out := map[string]fakeRow{}
		for _, part := range parts {
			for k, v := range part {
				out[k] = v
			}
		}
		return out
	}
	without := func(rows map[string]fakeRow, org uuid.UUID, table string) map[string]fakeRow {
		delete(rows, org.String()+"/"+table)
		return rows
	}

	cases := []struct {
		name       string
		rows       map[string]fakeRow
		want       uuid.UUID
		wantWarn   []string
		wantFailed int
	}{
		{"all tables readable: org with the newest data", merge(all(orgB, 3, older), all(orgA, 5, newer)), orgA, nil, 0},
		{"one table of the earlier org fails: earliest join, not the other org",
			merge(without(all(orgB, 3, older), orgB, "user_metrics_daily"), all(orgA, 5, newer)), orgB,
			[]string{"table=user_metrics_daily", "error_class=", "earliest join"}, 1},
		{"outage: every read fails: earliest join", map[string]fakeRow{}, orgB,
			[]string{"table=repo_metrics_daily", "table=work_item_metrics_daily", "earliest join"}, 8},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			h := handlers{Deps: Deps{ClickHouse: fakeConn{rows: c.rows}, Logger: slog.New(slog.NewTextHandler(&buf, nil))}}
			activities := h.orgActivity(context.Background(), []uuid.UUID{orgB, orgA})
			if got := selectActiveMembership(memberships, nil, activities); got == nil || got.OrgID != c.want {
				t.Fatalf("selected %v, want %v", got, c.want)
			}
			if got := h.chooseMembership(context.Background(), memberships, nil, activities); got == nil || got.OrgID != c.want {
				t.Fatalf("chooseMembership selected %v, want %v", got, c.want)
			}
			logged := buf.String()
			for _, want := range c.wantWarn {
				if !strings.Contains(logged, want) {
					t.Errorf("log lacks %q:\n%s", want, logged)
				}
			}
			if len(c.wantWarn) == 0 && strings.Contains(logged, "level=WARN") {
				t.Errorf("unexpected WARN:\n%s", logged)
			}
			for _, leak := range []string{"hunter2", "Missing columns", orgA.String(), orgB.String()} {
				if strings.Contains(logged, leak) {
					t.Errorf("log leaks %q:\n%s", leak, logged)
				}
			}
		})
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var failed int64
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "session_org_activity_read_failures_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				failed += point.Value
			}
		}
	}
	// Each case runs orgActivity once: 0 + 1 + 8 failed reads.
	if failed != 9 {
		t.Fatalf("failed reads counted = %d, want 9", failed)
	}
}

func TestRequestedOrganizationIsKeptWhenActivityIsUnknown(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	memberships := []membershipRow{{OrgID: a, CreatedAt: time.Unix(1, 0)}, {OrgID: b, CreatedAt: time.Unix(2, 0)}}
	got := selectActiveMembership(memberships, &b, map[uuid.UUID]activity{a: {unknown: true}})
	if got == nil || got.OrgID != b {
		t.Fatalf("selected %v, want %v", got, b)
	}
}
