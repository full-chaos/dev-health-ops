package authschema

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestWorkloadReaderReadsAreTheReviewedOnes(t *testing.T) {
	want := []ColumnGrant{
		{Table: "workloads", Columns: []string{"principal_id", "name", "organization_id", "audiences", "disabled_at"}},
		{Table: "principals", Columns: []string{"id", "disabled_at"}},
	}
	if got := WorkloadReaderReads(); !reflect.DeepEqual(got, want) {
		t.Fatalf("WorkloadReaderReads() = %+v, want %+v", got, want)
	}
}

// A session reader that is not a workload reader holds SessionReaderReads alone: query-api's role must never read the
// registry, in both states of the platform roles switch.
func TestSessionReaderReadsNeverNameTheWorkloadRegistry(t *testing.T) {
	for _, owned := range []bool{false, true} {
		if got := SessionReaderReadsWith(owned, false); !reflect.DeepEqual(got, SessionReaderReads(owned)) {
			t.Errorf("owned %v: a plain session reader reads %+v, want SessionReaderReads", owned, got)
		}
		for _, read := range SessionReaderReads(owned) {
			if read.Table == "workloads" {
				t.Errorf("owned %v: SessionReaderReads names the workloads table", owned)
			}
			if read.Table == "principals" && !reflect.DeepEqual(read.Columns, []string{"id", "kind", "disabled_at"}) {
				t.Errorf("owned %v: principals columns %v", owned, read.Columns)
			}
		}
	}
}

func TestSessionReaderReadsWithTheWorkloadReadMergeATableBothNameOnce(t *testing.T) {
	off := SessionReaderReadsWith(false, true)
	if len(off) != 3 || off[0].Table != "sessions" || off[1].Table != "workloads" || off[2].Table != "principals" ||
		!reflect.DeepEqual(off[2].Columns, []string{"id", "disabled_at"}) {
		t.Fatalf("platform roles off: %+v", off)
	}
	on := SessionReaderReadsWith(true, true)
	var principals []ColumnGrant
	tables := map[string]int{}
	for _, read := range on {
		tables[read.Table]++
		if read.Table == "principals" {
			principals = append(principals, read)
		}
	}
	for table, n := range tables {
		if n != 1 {
			t.Errorf("table %s named %d times", table, n)
		}
	}
	if len(principals) != 1 || !reflect.DeepEqual(principals[0].Columns, []string{"id", "kind", "disabled_at"}) {
		t.Fatalf("platform roles on: principals = %+v, want the platform columns once", principals)
	}
	if tables["workloads"] != 1 || tables["sessions"] != 1 || tables["platform_role_assignments"] != 1 {
		t.Fatalf("platform roles on: %v", tables)
	}
}

func TestWorkloadReaderRolesMustBeSessionReadersAndNamedOnce(t *testing.T) {
	base := Options{Schema: "auth", RuntimeRole: "rt", SessionReaderRoles: []string{"api", "query"}}
	for name, roles := range map[string][]string{
		"not a session reader": {"other"},
		"named twice":          {"api", "api"},
	} {
		o := base
		o.WorkloadReaderRoles = roles
		err := o.Validate("")
		if !errors.Is(err, ErrInvalidOptions) || !strings.Contains(err.Error(), "workload reader role") {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	o := base
	o.WorkloadReaderRoles = []string{"api"}
	if err := o.Validate(""); err != nil {
		t.Errorf("a session reader named once: %v", err)
	}
}

// Only a role named as a workload reader reads the registry: query-api's role stays in the plain group.
func TestSplitWorkloadReadersKeepsTheOthersPlain(t *testing.T) {
	plain, workload := splitWorkloadReaders(Options{SessionReaderRoles: []string{"api", "query"}, WorkloadReaderRoles: []string{"api"}})
	if !reflect.DeepEqual(plain, []string{"query"}) || !reflect.DeepEqual(workload, []string{"api"}) {
		t.Fatalf("plain %v workload %v", plain, workload)
	}
	plain, workload = splitWorkloadReaders(Options{SessionReaderRoles: []string{"api", "query"}})
	if !reflect.DeepEqual(plain, []string{"api", "query"}) || len(workload) != 0 {
		t.Fatalf("unset: plain %v workload %v: nobody may hold the registry read", plain, workload)
	}
}

func columnRecord(table, column string) GrantRecord {
	return GrantRecord{Kind: "column", Object: table, Column: column, Privilege: "SELECT"}
}

// Removing the workload registry read is a narrowing, named by the reader, whatever the platform roles switch says:
// every workload-set column a reader loses is in the refusal; a column no declared read names is not.
func TestReaderNarrowingCountsTheWorkloadRegistryRead(t *testing.T) {
	var before []GrantRecord
	for _, read := range WorkloadReaderReads() {
		for _, column := range read.Columns {
			before = append(before, columnRecord(read.Table, column))
		}
	}
	before = append(before, columnRecord("sessions", "expires_at"))
	lost := readerNarrowing("api", before, nil)
	if len(lost) != 7 {
		t.Fatalf("%d lost workload reads counted, want the 5 workloads and 2 principals columns: %+v", len(lost), lost)
	}
	for _, g := range lost {
		if g.Role != "api" || g.Object == "sessions" {
			t.Errorf("unexpected record %+v", g)
		}
	}
}
