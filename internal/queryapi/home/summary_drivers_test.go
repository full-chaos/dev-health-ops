package home

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
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

	captureLog := func() (*bytes.Buffer, func()) {
		var buf bytes.Buffer
		log.SetOutput(&buf)
		return &buf, func() { log.SetOutput(os.Stderr) }
	}
	activeFails := fakeQueryClient{t: t, handler: func(tt *testing.T, query string, b []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "is_active = 1") {
			return nil, errors.New("boom")
		}
		return named(tt, query, b)
	}}
	logged, restore := captureLog()
	if got := driverNames(context.Background(), activeFails, "org", "team_id", rows); len(got) != 0 {
		t.Errorf("a failed read of the active teams must leave the drivers out, got %v", got)
	}
	restore()
	if !strings.Contains(logged.String(), "home summary drivers: could not read the active teams: boom") {
		t.Errorf("a failed read of the active teams is not logged: %q", logged.String())
	}

	namesFail := fakeQueryClient{t: t, handler: func(tt *testing.T, query string, b []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "display_name") {
			return nil, errors.New("boom")
		}
		return named(tt, query, b)
	}}
	logged, restore = captureLog()
	if got := driverNames(context.Background(), namesFail, "org", "team_id", rows); len(got) != 0 {
		t.Errorf("a failed read of the names must leave the drivers out (never print the ids), got %v", got)
	}
	restore()
	if !strings.Contains(logged.String(), "home summary drivers: could not resolve team display names") {
		t.Errorf("a failed read of the names is not logged: %q", logged.String())
	}

	if got := driverNames(context.Background(), client, "org", "team_id", nil); got != nil {
		t.Errorf("no driver row, no names: %v", got)
	}
}

// An id with no name, and a name that is the id, are left out of the sentence
// (CHAOS-9046); the names that exist are kept in the order of the rows.
func TestDriverNamesLeaveOutAnIDWithNoNameAndANameThatIsTheID(t *testing.T) {
	rows := []driverRow{{ID: "linear:ENG"}, {ID: "custom:none"}, {ID: "jira:abc"}, {ID: "github:acme/ops"}, {ID: "gitlab:grp/web"}}
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "is_active = 1") {
			return &fixtureRowScanner{rows: [][]any{{"linear:ENG"}, {"custom:none"}, {"jira:abc"}, {"github:acme/ops"}, {"gitlab:grp/web"}}}, nil
		}
		// custom:none has no row; jira:abc is named by its own id (any case); gitlab:grp/web has a blank name
		return &fixtureRowScanner{rows: [][]any{{"linear:ENG", "Engineering"}, {"jira:abc", "JIRA:ABC"}, {"github:acme/ops", "Ops"}, {"gitlab:grp/web", "  "}}}, nil
	}}
	got := driverNames(context.Background(), client, "org", "team_id", rows)
	if strings.Join(got, ",") != "Engineering,Ops" {
		t.Fatalf("driver names = %v, want Engineering, Ops", got)
	}
}
