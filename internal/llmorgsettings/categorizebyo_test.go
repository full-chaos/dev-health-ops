package llmorgsettings

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

type optInRow struct {
	value *bool
	err   error
}

func (row optInRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	*(dest[0].(**bool)) = row.value
	return nil
}

type optInQueryer struct {
	row  optInRow
	sql  *string
	args *[]any
}

func (queryer optInQueryer) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	if queryer.sql != nil {
		*queryer.sql, *queryer.args = sql, args
	}
	return queryer.row
}

// The value rule ("true" only) lives in the definer function; its parity table
// runs against PostgreSQL in internal/jobs/investment's domain-role test.
func TestCategorizeBYOReadsTheDefinerFunctionAndNeverTheTable(t *testing.T) {
	yes, no := true, false
	var sql string
	var args []any
	got, err := CategorizeBYOEnabled(context.Background(), optInQueryer{optInRow{value: &yes}, &sql, &args}, "org-1")
	if err != nil || !got {
		t.Fatalf("CategorizeBYOEnabled = %v, %v; want true", got, err)
	}
	if sql != "SELECT public.categorize_byo_opted_in($1)" || len(args) != 1 || args[0] != "org-1" {
		t.Fatalf("opt-in read = %q %v; want the definer function with the org id only", sql, args)
	}
	if got, err := CategorizeBYOEnabled(context.Background(), optInQueryer{row: optInRow{value: &no}}, "org"); err != nil || got {
		t.Fatalf("CategorizeBYOEnabled = %v, %v; want false", got, err)
	}
	for name, row := range map[string]optInRow{
		"database fault": {err: errors.New("down")},
		"no rows":        {err: pgx.ErrNoRows},
		"null answer":    {},
	} {
		if _, err := CategorizeBYOEnabled(context.Background(), optInQueryer{row: row}, "org"); err == nil {
			t.Fatalf("%s must be an error, not off", name)
		}
	}
}

func TestTheCategorizeOptInRowIsNotASavedConfigAndNotAPurpose(t *testing.T) {
	rest, turnedOff := withoutControlRows(orgSettings{KeyCategorizeBYOEnabled: "true"})
	if len(rest) != 0 || turnedOff {
		t.Fatalf("an org with only the opt-in: rest = %v turnedOff = %v", rest, turnedOff)
	}
	if _, valid := ParsePurpose(string(PurposeCategorize)); valid {
		t.Fatal("categorize must stay out of the purposes the admin API lists and parses without an opt-in")
	}
	for _, purpose := range Purposes {
		if purpose == PurposeCategorize {
			t.Fatal("categorize is in Purposes")
		}
	}
}
