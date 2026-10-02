package remaining

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining/stepcause"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// iterationFailRows is a result set that yields `data` rows and then reports
// err from Err(): the failure of a read that STARTED fine and broke while the
// rows were being iterated, which is a different statement of the executor than
// the one that issued the query or scanned a row.
type iterationFailRows struct {
	data  [][]any
	index int
	err   error
}

func (r *iterationFailRows) Next() bool { r.index++; return r.index <= len(r.data) }
func (r *iterationFailRows) Scan(dest ...any) error {
	row := r.data[r.index-1]
	for i, value := range row {
		target := reflect.ValueOf(dest[i]).Elem()
		source := reflect.ValueOf(value)
		if target.Kind() == reflect.Ptr && source.Type() == target.Type().Elem() {
			allocated := reflect.New(target.Type().Elem())
			allocated.Elem().Set(source)
			target.Set(allocated)
			continue
		}
		target.Set(source)
	}
	return nil
}
func (r *iterationFailRows) ScanStruct(any) error               { return errStubExhausted }
func (r *iterationFailRows) ColumnTypes() []chdriver.ColumnType { return nil }
func (r *iterationFailRows) Totals(...any) error                { return nil }
func (r *iterationFailRows) Columns() []string                  { return nil }
func (r *iterationFailRows) Close() error                       { return nil }
func (r *iterationFailRows) Err() error                         { return r.err }
func (r *iterationFailRows) HasData() bool                      { return len(r.data) > 0 }

type iterationFailConn struct {
	driverConnStub
	data [][]any
	err  error
}

func (c *iterationFailConn) Query(context.Context, string, ...any) (chdriver.Rows, error) {
	return &iterationFailRows{data: c.data, err: c.err}, nil
}

// TestEveryIterationErrorSiteNamesItsOwnStep forces each executor read to fail
// DURING row iteration (rows.Err()) and requires the safe cause to equal the
// literal step text of that very site, plus the SQLSTATE the error carries. The
// expected text is written out here, not read from stepcause, so a site that
// returns the bare error (no step), or one that names a neighbour's step, fails.
func TestEveryIterationErrorSiteNamesItsOwnStep(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	failure := func() error { return &pgconn.PgError{Code: "08006", Message: hostile} }
	oneTime := [][]any{{now}}
	cases := []struct {
		name string
		data [][]any
		call func(conn *iterationFailConn) error
		want string
	}{
		{"scoped watermarks", nil, func(c *iterationFailConn) error {
			_, _, err := (&WorkItemAttributionExecutor{conn: c}).scopedWatermarks(context.Background(), "org-1")
			return err
		}, "step=iterate scoped watermarks sqlstate=08006"},
		{"scope changes", nil, func(c *iterationFailConn) error {
			_, err := (&WorkItemAttributionExecutor{conn: c}).scopeChanges(context.Background(), "org-1", now, "SELECT 1")
			return err
		}, "step=iterate scope changes sqlstate=08006"},
		{"max updated_at (no row)", nil, func(c *iterationFailConn) error {
			_, err := (&WorkItemAttributionExecutor{conn: c}).maxUpdatedAt(context.Background(), "SELECT 1", "org-1")
			return err
		}, "step=iterate max updated_at sqlstate=08006"},
		{"max updated_at (after a row)", oneTime, func(c *iterationFailConn) error {
			_, err := (&WorkItemAttributionExecutor{conn: c}).maxUpdatedAt(context.Background(), "SELECT 1", "org-1")
			return err
		}, "step=iterate max updated_at sqlstate=08006"},
		{"max effective changed_at (no row)", nil, func(c *iterationFailConn) error {
			_, err := (&WorkItemAttributionExecutor{conn: c}).maxEffectiveChangedAt(context.Background(), now, "org-1", "SELECT 1")
			return err
		}, "step=iterate max effective changed_at sqlstate=08006"},
		{"max effective changed_at (after a row)", oneTime, func(c *iterationFailConn) error {
			_, err := (&WorkItemAttributionExecutor{conn: c}).maxEffectiveChangedAt(context.Background(), now, "org-1", "SELECT 1")
			return err
		}, "step=iterate max effective changed_at sqlstate=08006"},
		{"work items", nil, func(c *iterationFailConn) error {
			_, err := querySubjectsInto(context.Background(), c, "SELECT 1")
			return err
		}, "step=iterate work_items rows sqlstate=08006"},
		{"dependency edges", nil, func(c *iterationFailConn) error {
			subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{"a": {WorkItemID: "a"}}
			_, err := LoadWorkItemDependencyEdges(context.Background(), c, "org-1", subjects)
			return err
		}, "step=iterate work_item_dependencies rows sqlstate=08006"},
		{"reverse closure", nil, func(c *iterationFailConn) error {
			_, err := (&WorkItemAttributionExecutor{conn: c}).loadInheritableDependencySourcesTargeting(
				context.Background(), "org-1", map[string]struct{}{"a": {}})
			return err
		}, "step=iterate work_item_dependencies rows (reverse closure) sqlstate=08006"},
		{"already covered today", nil, func(c *iterationFailConn) error {
			_, err := (&WorkItemAttributionExecutor{conn: c}).alreadyCoveredToday(
				context.Background(), "org-1", now, map[string]struct{}{"a": {}})
			return err
		}, "step=iterate already-covered-today rows sqlstate=08006"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := test.call(&iterationFailConn{data: test.data, err: failure()})
			if err == nil {
				t.Fatal("the forced iteration error did not surface")
			}
			cause, ok := jobruntime.SafeCause(err)
			if !ok || cause != test.want {
				t.Fatalf("cause = %q ok=%v, want %q", cause, ok, test.want)
			}
		})
	}
}

// TestHandlerKeepsTheExecutorsStepAndDoesNotReWrapIt: an executor error that
// already carries its exact step must reach the log as that one step; the
// handler adds compute_partition only to an error that has no step yet.
func TestHandlerKeepsTheExecutorsStepAndDoesNotReWrapIt(t *testing.T) {
	run := Run{ID: handlerRunID, OrganizationID: handlerOrgID, Family: "capacity", Status: "running"}
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"tagged by the executor", stepcause.Failure(stepcause.QueryWorkItems, &clickhouse.Exception{Code: 241}), "step=query work_items ch_code=241"},
		{"untagged", fmt.Errorf("ch: %w", &clickhouse.Exception{Code: 241}), "step=compute_partition ch_code=241"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store := &handlerStore{run: run, claim: handlerClaim()}
			handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](store, &handlerExecutor{computeErr: test.err}, "capacity")
			if err != nil {
				t.Fatal(err)
			}
			cause, ok := jobruntime.SafeCause(handler.Work(t.Context(), capacityExecution()))
			if !ok || cause != test.want {
				t.Fatalf("cause = %q ok=%v, want %q", cause, ok, test.want)
			}
		})
	}
}
