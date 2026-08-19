package postgres

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The coordinator posture must cover every table the scheduler's SQL names.
//
// fixed_engine_statement_privileges_integration_test.go already proves that the
// statements IT LISTS can run as the real restricted role. That is the right
// check, and it caught the FOR UPDATE and ON CONFLICT traps its header
// describes. What it cannot catch is a producer nobody added to the list: the
// statement set is hand-maintained, so it shares an author with the code it
// audits, exactly the way the posture and the grant list used to share one.
//
// The scheduled-report producer (internal/scheduler/fixed/reports.go) was added
// after that suite and never got entries. Its three tables -- saved_reports,
// scheduled_report_occurrences, report_runs -- were therefore absent from
// coordinatorPosture, both the readiness assertion and the derived GRANTs
// agreed they were not needed, and every scheduled_reports_dispatch cycle in
// production failed with:
//
//	permission denied for table saved_reports (SQLSTATE 42501)
//
// This test derives the table set from the source instead of from a list, so a
// producer added tomorrow either lands in the posture or fails here. It
// deliberately proves only COVERAGE, never sufficiency of privileges: whether
// SELECT is enough or a row lock demands UPDATE is what the integration suite
// executes for real. Coverage is the part a static check can own completely.
func TestCoordinatorPostureCoversSchedulerSQLTables(t *testing.T) {
	declared := make(map[string]struct{})
	posture := CoordinatorPosture()
	for _, table := range posture.RequiredTables {
		declared[table.TableName] = struct{}{}
	}
	// A column-scoped grant is a deliberate refusal to grant table-wide, not an
	// omission, so those tables count as covered here.
	for _, column := range posture.ColumnScoped {
		declared[column.TableName] = struct{}{}
	}

	referenced := schedulerReferencedTables(t)
	if len(referenced) == 0 {
		t.Fatal("scanned the scheduler tree and found no public.<table> references: " +
			"the scan is broken, and a broken scan reports success")
	}

	var missing []string
	for table := range referenced {
		if _, ok := declared[table]; !ok {
			missing = append(missing, table)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf(
			"internal/scheduler SQL names %d table(s) absent from coordinatorPosture: %s\n"+
				"The scheduler runs on the coordinator pool, so an undeclared table is a "+
				"runtime 42501, not a lint nit. Add each to coordinatorPosture with the "+
				"privileges its statements actually need -- remember that FOR UPDATE "+
				"requires UPDATE even when the statement writes nothing -- and add the "+
				"statements to fixed_engine_statement_privileges_integration_test.go.",
			len(missing), strings.Join(missing, ", "),
		)
	}
}

// TestCoordinatorPostureCoversReportProducerTables pins the three tables that
// were missing, so a future narrowing of the posture cannot silently drop them
// back out while the scan above still passes for some unrelated reason.
func TestCoordinatorPostureCoversReportProducerTables(t *testing.T) {
	required := map[string]bool{
		// value is whether the table needs UPDATE
		"saved_reports":                true, // dueScheduledReportsSQL: FOR UPDATE OF job, report
		"scheduled_report_occurrences": true, // linkScheduledReportOccurrenceRunSQL + FOR UPDATE
		"report_runs":                  false,
	}
	got := make(map[string]TablePrivilege)
	for _, table := range CoordinatorPosture().RequiredTables {
		got[table.TableName] = table
	}
	for name, needsUpdate := range required {
		table, ok := got[name]
		if !ok {
			t.Errorf("coordinatorPosture is missing %s, required by internal/scheduler/fixed/reports.go", name)
			continue
		}
		if table.AllowUpdate != needsUpdate {
			t.Errorf("%s: AllowUpdate = %v, want %v", name, table.AllowUpdate, needsUpdate)
		}
	}
}

var schedulerTableReference = regexp.MustCompile(`public\.([a-z][a-z0-9_]*)`)

// schedulerReferencedTables returns every public.<table> named in the
// non-test Go sources under internal/scheduler.
func schedulerReferencedTables(t *testing.T) map[string]struct{} {
	t.Helper()
	root := filepath.Join("..", "..", "scheduler")
	tables := make(map[string]struct{})
	walked := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		contents, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		walked++
		for _, match := range schedulerTableReference.FindAllStringSubmatch(string(contents), -1) {
			tables[match[1]] = struct{}{}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if walked == 0 {
		t.Fatalf("walked %s and read no non-test Go files", root)
	}
	return tables
}
