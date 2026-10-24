package llmorgsettings

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

type optInRow struct {
	value *string
	err   error
}

func (row optInRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	*(dest[0].(**string)) = row.value
	return nil
}

type optInQueryer struct{ row optInRow }

func (queryer optInQueryer) QueryRow(context.Context, string, ...any) pgx.Row { return queryer.row }

func TestCategorizeBYOIsOffUnlessTheRowIsExactlyTrue(t *testing.T) {
	text := func(s string) *string { return &s }
	for name, tc := range map[string]struct {
		row  optInRow
		want bool
	}{
		"no row": {optInRow{err: pgx.ErrNoRows}, false},
		"null":   {optInRow{}, false},
		"true":   {optInRow{value: text("true")}, true},
		"false":  {optInRow{value: text("false")}, false},
		"empty":  {optInRow{value: text("")}, false},
		"upper":  {optInRow{value: text("TRUE")}, false},
		"one":    {optInRow{value: text("1")}, false},
		"padded": {optInRow{value: text(" true")}, false},
		"yes":    {optInRow{value: text("yes")}, false},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := CategorizeBYOEnabled(context.Background(), optInQueryer{tc.row}, "org")
			if err != nil || got != tc.want {
				t.Fatalf("CategorizeBYOEnabled = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
	if _, err := CategorizeBYOEnabled(context.Background(), optInQueryer{optInRow{err: errors.New("down")}}, "org"); err == nil {
		t.Fatal("a database fault must be an error, not off")
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
