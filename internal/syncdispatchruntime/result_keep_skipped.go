package syncdispatchruntime

import "fmt"

// resultKeepingSkippedDatasets is the SQL value for a terminal write of
// sync_runs.result: the new result (the placeholder, a JSON text) plus the
// "skipped_datasets" entry the plan wrote, when the row holds one. A terminal
// write replaces the whole result, and the entry is the only record of why a
// dataset has no unit, so it must survive the run's last write.
func resultKeepingSkippedDatasets(placeholder string) string {
	return fmt.Sprintf(`(CASE WHEN (result::jsonb)->'skipped_datasets' IS NOT NULL
  THEN (%[1]s::jsonb || jsonb_build_object('skipped_datasets', (result::jsonb)->'skipped_datasets'))::json
  ELSE %[1]s::json END)`, placeholder)
}
