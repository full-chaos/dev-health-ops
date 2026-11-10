package postgres

import (
	"os"
	"regexp"
	"slices"
	"sort"
	"testing"
)

// domainMembershipsReadColumns are the public.memberships columns the billing owner lookup names: the join key, the
// org and role filters and the ordering column.
var domainMembershipsReadColumns = []string{"user_id", "org_id", "role", "created_at"}

func TestDomainPostureReadsOnlyTheMembershipsColumnsTheOwnerLookupNames(t *testing.T) {
	t.Parallel()

	posture := domainPosture()
	for _, table := range posture.RequiredTables {
		if table.TableName == "memberships" {
			t.Fatalf("domain posture declares memberships table-wide: %+v", table)
		}
	}
	var got []string
	for _, column := range posture.ColumnScoped {
		if column.TableName != "memberships" {
			continue
		}
		if column.Privilege != "SELECT" {
			t.Fatalf("domain posture grants %s on memberships.%s, want SELECT only", column.Privilege, column.ColumnName)
		}
		got = append(got, column.ColumnName)
	}
	if !slices.Equal(got, domainMembershipsReadColumns) {
		t.Fatalf("domain posture memberships SELECT columns = %v, want %v", got, domainMembershipsReadColumns)
	}
}

func TestDomainPostureCoversTheOwnerLookupMembershipsReads(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile("../../jobs/operational/billingfence.go")
	if err != nil {
		t.Fatal(err)
	}
	found := regexp.MustCompile("(?s)`([^`]*JOIN public\\.memberships m[^`]*)`").FindSubmatch(source)
	if found == nil {
		t.Fatal("billing owner lookup on public.memberships not found")
	}
	var named []string
	for _, match := range regexp.MustCompile(`\bm\.(\w+)`).FindAllSubmatch(found[1], -1) {
		if !slices.Contains(named, string(match[1])) {
			named = append(named, string(match[1]))
		}
	}
	sort.Strings(named)
	want := slices.Clone(domainMembershipsReadColumns)
	sort.Strings(want)
	if !slices.Equal(named, want) {
		t.Fatalf("billing owner lookup names memberships columns %v, want %v; update domainMembershipsReadColumns", named, want)
	}
}
