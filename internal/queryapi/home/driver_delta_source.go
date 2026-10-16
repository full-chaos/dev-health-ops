package home

import "fmt"

// driverDeltaSource is the FROM source of one window of the driver-delta
// readers, which put it straight after FROM with no WHERE of their own. A
// table with a natural key takes metricFromClause's deduplicated source; a
// table without one still gets its window, scope and org_id predicate, where
// metricFromClause gives the bare table name and the reader would read every
// organization.
func driverDeltaSource(table, column, scopeFilter, startParam, endParam string) string {
	if _, dedup := dedupByComputedAt[table]; dedup {
		return metricFromClause(table, column, scopeFilter, startParam, endParam)
	}
	return fmt.Sprintf(`(
            SELECT *
            FROM %s
            WHERE day >= {%s:Date} AND day < {%s:Date}
            %s
              AND org_id = {org_id:String}
        )`, table, startParam, endParam, scopeFilter)
}
