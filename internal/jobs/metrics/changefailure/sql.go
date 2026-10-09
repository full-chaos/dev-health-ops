package changefailure

// Table is the ClickHouse table of the daily counts.
const Table = "repo_change_failure_daily"

// CountColumns are the count columns of Table, in Counts field order.
var CountColumns = []string{
	"deployments_count",
	"failed_deployments_native",
	"failed_deployments_heuristic",
	"incidents_direct",
	"incidents_via_deployment",
}

// WindowRateSQL is Rate as a ClickHouse aggregate expression over rows of
// Table that are already deduplicated to the newest computed_at per
// (org_id, repo_id, day). It is NULL when the view is not applicable or
// unknown, so a reader must scan it into a nullable destination.
const WindowRateSQL = `if(sum(deployments_count) = 0 OR sum(incidents_direct) + sum(incidents_via_deployment) = 0, NULL, toFloat64(sum(failed_deployments_native) + sum(failed_deployments_heuristic)) / toFloat64(sum(deployments_count)))`

// LatestRowsSQL selects the newest row per (org_id, repo_id, day) of Table for
// one organization and a half-open day range, with extraWhere (which starts
// with "AND", or is empty) appended. startParam and endParam name Date
// parameters; the organization parameter is {org_id:String}.
func LatestRowsSQL(startParam, endParam, extraWhere string) string {
	return `(
            SELECT org_id, repo_id, day, ` + joinColumns() + `
            FROM ` + Table + `
            WHERE org_id = {org_id:String}
              AND day >= {` + startParam + `:Date} AND day < {` + endParam + `:Date}
              ` + extraWhere + `
            ORDER BY computed_at DESC
            LIMIT 1 BY org_id, repo_id, day
        )`
}

func joinColumns() string {
	out := ""
	for i, column := range CountColumns {
		if i > 0 {
			out += ", "
		}
		out += column
	}
	return out
}

// ViewSumsSQL selects, from rows of Table already deduplicated to the newest
// computed_at per (org_id, repo_id, day), the summed counts in CountColumns
// order and the number of rows: the columns ViewScanDest scans.
const ViewSumsSQL = `toUInt64(sum(deployments_count)), toUInt64(sum(failed_deployments_native)), toUInt64(sum(failed_deployments_heuristic)), toUInt64(sum(incidents_direct)), toUInt64(sum(incidents_via_deployment)), toUInt64(count())`

// ViewScanDest returns the scan destinations for one ViewSumsSQL row.
func ViewScanDest(view *View) []any {
	return []any{
		&view.Deployments, &view.FailedNative, &view.FailedHeuristic,
		&view.IncidentsDirect, &view.IncidentsViaDeployment, &view.StoredRows,
	}
}
