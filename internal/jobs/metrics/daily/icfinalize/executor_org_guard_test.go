package icfinalize

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// touchedConn fails the test if the executor reaches ClickHouse at all.
type touchedConn struct{ calls int }

func (c *touchedConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	c.calls++
	return nil, errors.New("conn touched")
}

func (c *touchedConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.calls++
	return nil, errors.New("conn touched")
}

// An empty organization id must be refused before any read or write, through
// both entry points (the run-scoped one the worker calls and the explicit form).
func TestIcFinalizeRefusesAnEmptyOrganizationBeforeTouchingClickHouse(t *testing.T) {
	day := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	for name, call := range map[string]func(*Executor) error{
		"ComputeFinalizeFamily": func(e *Executor) error {
			_, err := e.ComputeFinalizeFamily(context.Background(), RunScope{TargetDay: day})
			return err
		},
		"computeForDay": func(e *Executor) error {
			_, err := e.computeForDay(context.Background(), "", day, nil, nil)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			conn := &touchedConn{}
			err := call(NewExecutor(conn))
			if !errors.Is(err, ErrOrganizationRequired) {
				t.Fatalf("err = %v, want ErrOrganizationRequired", err)
			}
			if conn.calls != 0 {
				t.Fatalf("ClickHouse touched %d time(s) for an empty organization", conn.calls)
			}
		})
	}
}

// A real organization is not refused by the guard: the executor goes on to read.
func TestIcFinalizeGuardDoesNotRefuseARealOrganization(t *testing.T) {
	conn := &touchedConn{}
	_, err := NewExecutor(conn).ComputeFinalizeFamily(context.Background(),
		RunScope{OrganizationID: "11111111-1111-1111-1111-111111111111", TargetDay: time.Now()})
	if errors.Is(err, ErrOrganizationRequired) || conn.calls == 0 {
		t.Fatalf("real org: err=%v calls=%d, want the read to be attempted", err, conn.calls)
	}
}

// The run-scoped entry point refuses before it asks the team mapper anything:
// the mapper reads ClickHouse for the (empty) organization.
func TestIcFinalizeRefusesBeforeTheTeamMapperRuns(t *testing.T) {
	mapperCalls := 0
	executor := NewExecutor(&touchedConn{})
	executor.SetTeamMapper(func(context.Context, string, time.Time) (PersonTeams, error) {
		mapperCalls++
		return nil, nil
	})
	_, err := executor.ComputeFinalizeFamily(context.Background(), RunScope{TargetDay: time.Now()})
	if !errors.Is(err, ErrOrganizationRequired) || mapperCalls != 0 {
		t.Fatalf("err=%v mapperCalls=%d, want a refusal before the mapper", err, mapperCalls)
	}
}

// A refusal is observable: one counter increment and one error log line that
// names the family and the day.
func TestIcFinalizeRefusalIsCountedAndLogged(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	_, _ = NewExecutor(&touchedConn{}).ComputeFinalizeFamily(context.Background(),
		RunScope{TargetDay: time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)})

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "dev_health_ic_finalize_refused_empty_org_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				total += point.Value
			}
		}
	}
	if total != 1 {
		t.Fatalf("refusal counter = %d, want 1", total)
	}
	if lines := strings.Count(strings.TrimSpace(logs.String()), "\n") + 1; lines != 1 {
		t.Fatalf("refusal wrote %d log lines, want exactly 1: %q", lines, logs.String())
	}
	if line := logs.String(); !strings.Contains(line, "level=ERROR") || !strings.Contains(line, "family=ic_finalize") || !strings.Contains(line, "target_day=2026-08-27") {
		t.Fatalf("refusal log line = %q", line)
	}
}

// A failed read of the inactive teams fails the run before any other read and
// any write. With an empty set in its place the family would write the
// person's rows and points of the day under a team that was replaced.
func TestIcFinalizeFailsTheRunWhenTheInactiveTeamsCannotBeRead(t *testing.T) {
	conn := &touchedConn{}
	rows, err := NewExecutor(conn).ComputeFinalizeFamily(context.Background(), RunScope{
		OrganizationID: "org-1", TargetDay: time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC),
	})
	if err == nil || !strings.Contains(err.Error(), "load inactive teams") {
		t.Fatalf("err = %v, want the failed read of the inactive teams", err)
	}
	if rows != 0 || conn.calls != 1 {
		t.Fatalf("rows = %d, ClickHouse calls = %d; want no row and the one failed read", rows, conn.calls)
	}
}
