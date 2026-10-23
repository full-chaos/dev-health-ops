package daily

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// legacyRetractionVersions returns, for each produced key whose legacy (row_view
// empty) row is live, the computed_at of the row of zeros that supersedes it:
// the version of the rows being written, raised one unit above the stored legacy
// row. A writer stores that row of zeros in the SAME insert block as the org and
// scope rows of the key, so a reader that keeps the legacy rows and one view never
// sees a live legacy row beside a live view row, also when the write is ambiguous.
// produced holds the keys in the order of table.Keys without the row_view value;
// the result is keyed by staleKey.text() of the produced key. A connection that
// cannot read (no Query) is an error: the write fails closed. The values a table
// carries (team of a person) come from the newest stored row of the key, as in
// writeStaleKeyZeroRows.
func legacyRetractionVersions(
	ctx context.Context, conn any, table StaleKeyTable, organizationID string, day time.Time,
	produced []staleKey, computedAt time.Time,
) (map[string]legacyRetraction, error) {
	reader, ok := conn.(staleKeyConn)
	if !ok {
		return nil, ErrInvalidState
	}
	if len(produced) == 0 {
		return nil, nil
	}
	if computedAt.IsZero() {
		return nil, ErrInvalidState
	}
	if _, err := staleKeyVersionAfter(table, computedAt, time.Time{}); err != nil {
		return nil, err
	}
	live, versions, err := loadLiveStaleKeys(ctx, reader, table, organizationID, day)
	if err != nil {
		return nil, err
	}
	held := make(map[string]string, len(produced))
	for _, key := range produced {
		held[append(append(staleKey{}, key...), teamkeytables.LegacyRowView).text()] = key.text()
	}
	retract := map[string]legacyRetraction{}
	var carried map[string][]string
	for _, key := range live {
		producedText, ok := held[key.text()]
		if !ok {
			continue
		}
		if carried == nil {
			if carried, err = loadCarriedValues(ctx, reader, table, organizationID, day); err != nil {
				return nil, err
			}
		}
		version, _ := staleKeyVersionAfter(table, computedAt, versions[key.text()])
		retract[producedText] = legacyRetraction{version: version, carried: carried[key.text()]}
	}
	return retract, nil
}

// legacyRetraction is the row of zeros over one live legacy key: its version and
// the values the table carries from the newest stored row of the key.
type legacyRetraction struct {
	version time.Time
	carried []string
}

// LegacyWorkItemViewRetraction is the row of zeros over one live legacy key.
type LegacyWorkItemViewRetraction struct {
	Version time.Time
	// Carried holds the values the table carries from the newest stored row of
	// the key, in the order of the table's Carried columns (empty when none).
	Carried []string
}

// LegacyWorkItemViewRetractionVersions is legacyRetractionVersions for a writer
// outside this package that stores the org and scope rows of one of the three
// work-item daily tables itself. keys are in the order of the table's keys
// without row_view; the result is keyed by the keys joined with a NUL byte.
func LegacyWorkItemViewRetractionVersions(
	ctx context.Context, conn driver.Conn, table string, organizationID string, day time.Time,
	keys [][]string, computedAt time.Time,
) (map[string]LegacyWorkItemViewRetraction, error) {
	var declared StaleKeyTable
	switch table {
	case teamkeytables.WorkItemMetricsDaily.Table:
		declared = teamkeytables.WorkItemMetricsDaily
	case teamkeytables.WorkItemStateDurationsDaily.Table:
		declared = teamkeytables.WorkItemStateDurationsDaily
	case teamkeytables.WorkItemUserMetricsDaily.Table:
		declared = teamkeytables.WorkItemUserMetricsDaily
	default:
		return nil, ErrInvalidState
	}
	produced := make([]staleKey, 0, len(keys))
	for _, key := range keys {
		produced = append(produced, staleKey(key))
	}
	found, err := legacyRetractionVersions(ctx, conn, declared, organizationID, day, produced, computedAt)
	if err != nil {
		return nil, err
	}
	out := make(map[string]LegacyWorkItemViewRetraction, len(found))
	for key, retraction := range found {
		out[key] = LegacyWorkItemViewRetraction{Version: retraction.version, Carried: retraction.carried}
	}
	return out, nil
}
