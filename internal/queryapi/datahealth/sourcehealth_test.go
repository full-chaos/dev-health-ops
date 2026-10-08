package datahealth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type sourceHealthRows struct {
	pgx.Rows
	rows   [][]any
	cursor int
}

func (f *sourceHealthRows) Next() bool { return f.cursor < len(f.rows) }
func (f *sourceHealthRows) Err() error { return nil }
func (f *sourceHealthRows) Close()     {}
func (f *sourceHealthRows) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case *[]byte:
			if v, ok := row[i].(string); ok {
				*p = []byte(v)
			}
		case *bool:
			*p = row[i].(bool)
		case **string:
			if v, ok := row[i].(string); ok {
				*p = &v
			}
		case **bool:
			if v, ok := row[i].(bool); ok {
				*p = &v
			}
		case **int32:
			if v, ok := row[i].(int32); ok {
				*p = &v
			}
		case **time.Time:
			if v, ok := row[i].(time.Time); ok {
				*p = &v
			}
		}
	}
	return nil
}

type sourceHealthPG struct {
	rows [][]any
	err  error
	args []any
}

func (f *sourceHealthPG) Query(_ context.Context, _ string, args ...any) (pgx.Rows, error) {
	f.args = args
	if f.err != nil {
		return nil, f.err
	}
	return &sourceHealthRows{rows: f.rows}, nil
}

func TestSourceHealthWithoutAReaderFailsInsteadOfAnsweringEmpty(t *testing.T) {
	rows, err := (&Reader{}).SourceHealth(context.Background(), "org-1")
	if !errors.Is(err, ErrSourceHealthUnavailable) || rows != nil {
		t.Fatalf("rows=%v err=%v, want the unavailable error and no rows", rows, err)
	}
}

func TestSourceHealthFailedReadFailsAndCarriesNoRows(t *testing.T) {
	pg := &sourceHealthPG{err: errors.New("connection refused")}
	rows, err := (&Reader{Postgres: pg}).SourceHealth(context.Background(), "org-1")
	if !errors.Is(err, ErrSourceHealthUnavailable) || rows != nil {
		t.Fatalf("rows=%v err=%v, want the unavailable error and no rows", rows, err)
	}
}

func TestSourceHealthReadsOnlyTheGivenOrg(t *testing.T) {
	pg := &sourceHealthPG{}
	if _, err := (&Reader{Postgres: pg}).SourceHealth(context.Background(), "org-1"); err != nil {
		t.Fatal(err)
	}
	if len(pg.args) != 1 || pg.args[0] != "org-1" {
		t.Fatalf("args = %v, want the org only", pg.args)
	}
}

func TestSourceHealthStageIsAClosedSet(t *testing.T) {
	named, free := "provider_rate_limited", "token=SECRET-PROBE"
	for _, tc := range []struct {
		name            string
		stage, category *string
		want            string
	}{
		{"neither", nil, nil, SourceHealthStageOther},
		{"named stage", &named, nil, "provider_rate_limited"},
		{"named category", nil, &named, "provider_rate_limited"},
		{"free-text stage, named category", &free, &named, "provider_rate_limited"},
		{"free-text stage only", &free, nil, SourceHealthStageOther},
		{"free-text category only", nil, &free, SourceHealthStageOther},
		{"empty strings", ptr(""), ptr(""), SourceHealthStageOther},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sourceHealthStage(tc.stage, tc.category); got != tc.want {
				t.Fatalf("stage = %q, want %q", got, tc.want)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// Every failure clause alone must produce a failure, and a clean row none: the
// states "never synced", "stale" and "failed" stay distinct at the reader.
func TestSourceHealthRowStates(t *testing.T) {
	at := time.Date(2026, 3, 1, 0, 5, 0, 0, time.UTC)
	old := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	failedStatus, okStatus := int32(jobRunFailed), int32(2)
	f, tr := false, true
	r := &Reader{Now: func() time.Time { return at }}

	for _, tc := range []struct {
		name         string
		row          connectorRow
		hasSyncError bool
		hasRunError  bool
		wantTime     bool
		wantFailure  bool
	}{
		{"never synced", connectorRow{}, false, false, false, false},
		{"stale", connectorRow{lastSyncAt: &old, lastSyncSuccess: &tr, hasRun: true, runStatus: &okStatus}, false, false, true, false},
		{"failed, flag", connectorRow{lastSyncAt: &at, lastSyncSuccess: &f}, false, false, false, true},
		{"failed, sync error", connectorRow{lastSyncAt: &at}, true, false, true, true},
		{"failed, run status", connectorRow{lastSyncAt: &at, lastSyncSuccess: &tr, hasRun: true, runStatus: &failedStatus}, false, false, true, true},
		{"failed, run error", connectorRow{lastSyncAt: &at, lastSyncSuccess: &tr, hasRun: true, runStatus: &okStatus}, false, true, true, true},
		{"run error of no run is not a failure", connectorRow{lastSyncAt: &old, lastSyncSuccess: &tr}, false, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := r.sourceHealthRow(tc.row, tc.hasSyncError, tc.hasRunError, nil, nil)
			if (got.LastSyncAt != nil) != tc.wantTime || (got.LastFailure != nil) != tc.wantFailure {
				t.Fatalf("time=%v failure=%v, want time=%v failure=%v", got.LastSyncAt, got.LastFailure, tc.wantTime, tc.wantFailure)
			}
		})
	}
}
