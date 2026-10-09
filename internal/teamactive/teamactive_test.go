package teamactive

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// scriptedRows is a driver.Rows that yields ids, or fails where it is told to.
type scriptedRows struct {
	driver.Rows
	ids      []string
	position int
	scanErr  error
	rowsErr  error
	closed   bool
}

func (rows *scriptedRows) Next() bool { return rows.position < len(rows.ids) }
func (rows *scriptedRows) Scan(destinations ...any) error {
	if rows.scanErr != nil {
		return rows.scanErr
	}
	*(destinations[0].(*string)) = rows.ids[rows.position]
	rows.position++
	return nil
}
func (rows *scriptedRows) Err() error   { return rows.rowsErr }
func (rows *scriptedRows) Close() error { rows.closed = true; return nil }

type scriptedQuerier struct {
	rows     *scriptedRows
	queryErr error
	query    string
	args     []any
}

func (querier *scriptedQuerier) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	querier.query, querier.args = query, args
	if querier.queryErr != nil {
		return nil, querier.queryErr
	}
	return querier.rows, nil
}

func TestLoadInactiveReadsTheIDsOfOneOrganizationByTheNewestRow(t *testing.T) {
	querier := &scriptedQuerier{rows: &scriptedRows{ids: []string{"platform", " ops "}}}
	inactive, err := LoadInactive(context.Background(), querier, "org-1")
	if err != nil {
		t.Fatal(err)
	}
	if !inactive.Has("platform") || !inactive.Has("ops") || !inactive.Has(" platform ") || inactive.Has("github:platform") || len(inactive) != 2 {
		t.Errorf("inactive = %v, want platform and ops, compared with the space around an id removed", inactive)
	}
	if !reflect.DeepEqual(querier.args, []any{"org-1"}) {
		t.Errorf("arguments = %v, want the organization id only", querier.args)
	}
	if !strings.Contains(querier.query, "FROM teams WHERE org_id = ? GROUP BY id HAVING "+NewestRowInactive) {
		t.Errorf("the read is not the newest-row test of one organization: %s", querier.query)
	}
	if !querier.rows.closed {
		t.Error("the rows were not closed")
	}
}

// A failed read is returned: a resolver that went on with an empty set would
// give work to a replaced team.
func TestLoadInactiveReturnsAFailedReadAndNeverAnEmptySet(t *testing.T) {
	failure := errors.New("clickhouse: connection reset")
	for name, querier := range map[string]*scriptedQuerier{
		"the query fails":     {queryErr: failure},
		"a scan fails":        {rows: &scriptedRows{ids: []string{"platform"}, scanErr: failure}},
		"the iteration fails": {rows: &scriptedRows{rowsErr: failure}},
	} {
		inactive, err := LoadInactive(context.Background(), querier, "org-1")
		if !errors.Is(err, failure) || inactive != nil {
			t.Errorf("%s: LoadInactive = %v, %v; want no set and the failure", name, inactive, err)
		}
	}
	if _, err := LoadInactive(context.Background(), nil, "org-1"); err == nil {
		t.Error("no connection is accepted")
	}
	if _, err := LoadInactive(context.Background(), &scriptedQuerier{rows: &scriptedRows{}}, "  "); err == nil {
		t.Error("a blank organization id is accepted")
	}
}

func TestKeepDropsTheInactiveTeamsAndKeepsTheOrder(t *testing.T) {
	type team struct{ id string }
	teams := []team{{"github:platform"}, {"platform"}, {"apps"}, {"ops"}}
	identity := func(value team) string { return value.id }
	kept := Keep(teams, Inactive{"platform": {}, "ops": {}}, identity)
	if !reflect.DeepEqual(kept, []team{{"github:platform"}, {"apps"}}) {
		t.Errorf("kept = %v", kept)
	}
	if len(teams) != 4 || teams[1].id != "platform" {
		t.Errorf("the input was changed: %v", teams)
	}
	// With no inactive team the result is the input.
	for _, none := range []Inactive{nil, {}} {
		if same := Keep(teams, none, identity); !reflect.DeepEqual(same, teams) {
			t.Errorf("with no inactive team the result is %v, want the input", same)
		}
	}
	if (Inactive)(nil).Has("platform") {
		t.Error("a nil set holds an id")
	}
}
