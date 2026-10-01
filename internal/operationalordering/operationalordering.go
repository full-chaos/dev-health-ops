// Package operationalordering is the one implementation of how a current row
// is read out of an operational_* table that carries ordering contract 2.
//
// Under contract 2 (migration 067) the ReplacingMergeTree sorting key of the
// twelve operational tables ends in source_revision and source_conflict_key,
// so FINAL keeps one row per (org_id, id, source_revision,
// source_conflict_key): the live version of a key and a newer tombstone of the
// same key are both retained, and a reader that filters is_deleted = 0 after
// FINAL returns the older live row. The current row of a key is the one with
// the greatest (source_revision, source_conflict_key, ingest_revision),
// selected explicitly. A reader of these tables must use it; a new raw FINAL
// read of them is refused by TestNoUnclassifiedFinalReadOfAnOperationalTable.
package operationalordering

import (
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"strings"
)

// Env is the process variable that names the ordering contract.
const Env = "OPERATIONAL_ORDERING_CONTRACT"

// RevisionOrder is the contract-2 ordering of the versions of one key, newest
// first.
const RevisionOrder = "source_revision DESC, source_conflict_key DESC, ingest_revision DESC"

// RevisionCurrentRows is the parenthesised sub-select of the current row of
// every (org_id, id) of table that matches where, ready to be spliced into a
// FROM clause. postFilters are applied AFTER the selection on purpose: a filter
// inside it would pick the newest row that matches instead of filtering the
// newest row, which resurrects superseded data.
func RevisionCurrentRows(table, where string, postFilters []string) string {
	outer := ""
	if len(postFilters) > 0 {
		outer = "WHERE " + strings.Join(postFilters, " AND ")
	}
	return fmt.Sprintf(`(
        SELECT *
        FROM (
            SELECT *
            FROM %s
            WHERE %s
            ORDER BY org_id, id, %s
            LIMIT 1 BY org_id, id
        )
        %s
    )`, table, where, RevisionOrder, outer)
}

// LatestRevisionRow selects the newest stored version of the row(s) of table
// that match where: a point lookup of one key, whose where must pin
// (org_id, id).
func LatestRevisionRow(columns, table, where string) string {
	return "SELECT " + columns + " FROM " + table + " WHERE " + where +
		" ORDER BY " + RevisionOrder + " LIMIT 1"
}

// RevisionActiveRows selects the newest stored version of every id of table
// that matches where, and keeps those that pass active.
func RevisionActiveRows(columns, table, where, active string) string {
	return "SELECT " + columns + " FROM (SELECT " + columns + " FROM " + table + " WHERE " + where +
		" ORDER BY org_id, id, " + RevisionOrder + " LIMIT 1 BY org_id, id) WHERE " + active
}

// Contract is the ordering contract a reader reads under. Only contract 2
// exists for readers: the migrator builds only it (contract 1 is unsupported).
type Contract int

// Revision is contract 2: the current row of a key is selected by revision.
const Revision Contract = 2

// UnsupportedError is the refusal for an OPERATIONAL_ORDERING_CONTRACT value
// other than 2. Contract 1 is unsupported (the migrator builds only contract
// 2), and any other value is a typo.
type UnsupportedError struct{ Value string }

func (e UnsupportedError) Error() string {
	return fmt.Sprintf("%s=%q is refused: only contract 2 is supported (leave it unset or set it to 2)", Env, e.Value)
}

// ResolveValue is the one decision every reader of the operational tables takes
// from the process variable: unset or exactly "2" is contract 2 (production's,
// and the default), anything else (1, blank, a padded or misspelled value) is
// refused with an UnsupportedError. No value selects the legacy FINAL read.
func ResolveValue(value string, set bool) (Contract, error) {
	if !set || value == "2" {
		return Revision, nil
	}
	return 0, UnsupportedError{Value: value}
}

// Resolve is ResolveValue over a lookup (os.LookupEnv when lookup is nil).
func Resolve(lookup func(string) (string, bool)) (Contract, error) {
	if lookup == nil {
		lookup = secrets.ProcessLookup
	}
	value, set := lookup(Env)
	return ResolveValue(value, set)
}

// CheckForRead returns nil when a process may read the operational tables: see
// ResolveValue.
func CheckForRead(lookup func(string) (string, bool)) error {
	_, err := Resolve(lookup)
	return err
}
