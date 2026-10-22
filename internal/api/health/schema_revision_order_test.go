package health

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestSchemaRevisionSatisfies(t *testing.T) {
	cases := []struct {
		name     string
		revision string
		want     bool
	}{
		{"known head", "0152", true},
		{"known minimum", minimumSchemaRevision, true},
		{"known interior revision", "0100", true},
		{"unknown newer head", "0153", true},
		{"unknown far newer head", "0999", true},
		{"unknown newer 9999", "9999", true},
		{"one below the minimum", "0074", false},
		{"cutover branch head", "0066", false},
		{"old revision", "0001", false},
		{"empty", "", false},
		{"letters", "abcd", false},
		{"too long", "00751", false},
		{"five digits above", "12345", false},
		{"trailing letter", "0153a", false},
		{"too short", "153", false},
		{"leading space", " 0153", false},
		{"trailing newline", "0153\n", false},
		{"sign", "+153", false},
		{"sign inside", "9+99", false},
		{"space inside", "9 99", false},
		{"arabic-indic digits", "٠١٥٣", false},
		{"fullwidth digits", "１５２３", false},
		{"hash revision", "3f2a9c1b7d4e", false},
	}
	for _, test := range cases {
		if got := schemaRevisionSatisfies(test.revision); got != test.want {
			t.Errorf("%s: schemaRevisionSatisfies(%q) = %v, want %v", test.name, test.revision, got, test.want)
		}
	}
}

func TestEverySatisfyingRevisionIsFourASCIIDigitsAboveOrAtMinimum(t *testing.T) {
	for revision := range satisfyingRevisions {
		number, ok := revisionNumber(revision)
		if !ok || number < mustRevisionNumber(t, minimumSchemaRevision) {
			t.Errorf("satisfying revision %q is not a four-digit revision at or above %s", revision, minimumSchemaRevision)
		}
	}
}

func mustRevisionNumber(t *testing.T, revision string) int {
	t.Helper()
	number, ok := revisionNumber(revision)
	if !ok {
		t.Fatalf("revision %q is not four ASCII digits", revision)
	}
	return number
}

func TestAnySchemaRevisionSatisfies(t *testing.T) {
	cases := []struct {
		name     string
		recorded []string
		want     bool
	}{
		{"known head", []string{"0152"}, true},
		{"unknown newer head", []string{"0999"}, true},
		{"cutover branch head beside a known head", []string{"0066", "0152"}, true},
		{"cutover branch head beside an unknown newer head", []string{"0066", "0153"}, true},
		{"cutover branch head alone", []string{"0066"}, false},
		{"unparseable beside the cutover branch head", []string{"0066", "abcd"}, false},
		{"no row", nil, false},
	}
	for _, test := range cases {
		if got := anySchemaRevisionSatisfies(test.recorded); got != test.want {
			t.Errorf("%s: anySchemaRevisionSatisfies(%q) = %v, want %v", test.name, test.recorded, got, test.want)
		}
	}
}

type fakeRevisionRows struct {
	pgx.Rows
	revisions  []string
	position   int
	failScanAt int
	iterErr    error
}

func (f *fakeRevisionRows) Next() bool {
	if f.position >= len(f.revisions) {
		return false
	}
	f.position++
	return true
}

func (f *fakeRevisionRows) Scan(dest ...any) error {
	if f.failScanAt == f.position {
		return errors.New("scan")
	}
	*(dest[0].(*string)) = f.revisions[f.position-1]
	return nil
}

func (f *fakeRevisionRows) Err() error { return f.iterErr }

func TestSchemaSatisfiedByRows(t *testing.T) {
	cases := []struct {
		name string
		rows *fakeRevisionRows
		want bool
	}{
		{"known head", &fakeRevisionRows{revisions: []string{"0152"}}, true},
		{"unknown newer head", &fakeRevisionRows{revisions: []string{"0999"}}, true},
		{"cutover branch head beside an unknown newer head", &fakeRevisionRows{revisions: []string{"0066", "0153"}}, true},
		{"cutover branch head alone", &fakeRevisionRows{revisions: []string{"0066"}}, false},
		{"unparseable", &fakeRevisionRows{revisions: []string{"abcd"}}, false},
		{"no row", &fakeRevisionRows{}, false},
		{"scan error on the second row after a satisfying first row", &fakeRevisionRows{revisions: []string{"0152", "0153"}, failScanAt: 2}, false},
		{"iteration error after a satisfying row", &fakeRevisionRows{revisions: []string{"0152"}, iterErr: errors.New("iterate")}, false},
	}
	for _, test := range cases {
		got, recorded := schemaSatisfiedByRows(test.rows)
		if got != test.want {
			t.Errorf("%s: schemaSatisfiedByRows = %v (read %q), want %v", test.name, got, recorded, test.want)
		}
	}
	if _, recorded := schemaSatisfiedByRows(&fakeRevisionRows{revisions: []string{"0066", "0153"}}); strings.Join(recorded, ",") != "0066,0153" {
		t.Errorf("recorded revisions for the log: %q", recorded)
	}
}
