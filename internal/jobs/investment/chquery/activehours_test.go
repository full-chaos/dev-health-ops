package chquery

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TestFetchWorkItemActiveHoursRefusesEmptyOrgWithoutQuerying proves an empty
// organization id is refused before any query reaches ClickHouse, rather than
// silently answered by fusing every tenant's rows for a shared work-item id
// into one group.
func TestFetchWorkItemActiveHoursRefusesEmptyOrgWithoutQuerying(t *testing.T) {
	fake := &capturingConn{}
	reader, err := NewReader(fake)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	_, err = reader.FetchWorkItemActiveHours(context.Background(), []string{"linear:issue-1"}, "")
	if !errors.Is(err, ErrOrganizationIDRequired) {
		t.Fatalf("want ErrOrganizationIDRequired, got %v", err)
	}
	if fake.query != "" {
		t.Fatalf("reader issued a query for an unscoped read, want none:\n%s", fake.query)
	}
}

// TestFetchWorkItemActiveHoursQueryShapeGroupsByOrgAndWorkItem pins the
// emitted SQL: the org filter is unconditional, org_id is bound, and the
// GROUP BY key is (org_id, work_item_id) -- so the aggregation cannot
// collapse rows across tenants even if a future edit weakens the filter.
func TestFetchWorkItemActiveHoursQueryShapeGroupsByOrgAndWorkItem(t *testing.T) {
	fake := &capturingConn{}
	reader, err := NewReader(fake)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	_, _ = reader.FetchWorkItemActiveHours(context.Background(), []string{"linear:issue-1"}, "org1")

	sql := fake.query
	for _, want := range []string{
		"WHERE work_item_id IN {work_item_ids:Array(String)}",
		"AND org_id = {org_id:String}",
		"GROUP BY org_id, work_item_id",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("active hours query missing %q:\n%s", want, sql)
		}
	}
	boundOrg := false
	for _, arg := range fake.args {
		named, ok := arg.(driver.NamedValue)
		if !ok {
			continue
		}
		if named.Name == "org_id" {
			boundOrg = true
			if named.Value != "org1" {
				t.Fatalf("org_id bound to %v, want %q", named.Value, "org1")
			}
		}
	}
	if !boundOrg {
		t.Fatalf("query arguments never bound org_id: %#v", fake.args)
	}
}
