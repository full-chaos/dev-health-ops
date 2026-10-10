package home

import (
	"context"
	"errors"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// A failed read never turns a driver into its id (CHAOS-9046): when the names or
// the active teams cannot be read the sentence names no driver at all.
func TestDriverNamesLeaveTheDriversOutWhenAReadFails(t *testing.T) {
	rows := []driverRow{{ID: "linear:ENG"}, {ID: "github:acme/ops"}}
	named := func(_ *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "is_active = 1"):
			return &fixtureRowScanner{rows: [][]any{{"linear:ENG"}, {"github:acme/ops"}}}, nil
		default:
			return &fixtureRowScanner{rows: [][]any{{"linear:ENG", "Engineering"}, {"github:acme/ops", "Ops"}}}, nil
		}
	}
	client := fakeQueryClient{t: t, handler: named}
	if got := driverNames(context.Background(), client, "org", "team_id", rows); strings.Join(got, ",") != "Engineering,Ops" {
		t.Fatalf("control: driver names = %v, want Engineering, Ops", got)
	}

	activeFails := fakeQueryClient{t: t, handler: func(tt *testing.T, query string, b []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "is_active = 1") {
			return nil, errors.New("boom")
		}
		return named(tt, query, b)
	}}
	if got := driverNames(context.Background(), activeFails, "org", "team_id", rows); len(got) != 0 {
		t.Errorf("a failed read of the active teams must leave the drivers out, got %v", got)
	}

	namesFail := fakeQueryClient{t: t, handler: func(tt *testing.T, query string, b []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "display_name") {
			return nil, errors.New("boom")
		}
		return named(tt, query, b)
	}}
	if got := driverNames(context.Background(), namesFail, "org", "team_id", rows); len(got) != 0 {
		t.Errorf("a failed read of the names must leave the drivers out (never print the ids), got %v", got)
	}

	if got := driverNames(context.Background(), client, "org", "team_id", nil); got != nil {
		t.Errorf("no driver row, no names: %v", got)
	}
}
