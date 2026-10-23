package daily

import (
	"context"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// retractLegacyViewKeys writes a row of zeros over the live legacy (row_view
// empty) key of every key a partition just wrote under the org and scope views.
// Without it a day would hold a live legacy row and the org row of one key until
// the run-level retraction runs, and a reader that keeps the legacy rows and one
// view would count the key twice. produced holds the keys in the order of
// table.Keys without the row_view value. The zero row is newer than the
// legacy row it supersedes and never older than the rows just written.
func retractLegacyViewKeys(
	ctx context.Context, conn staleKeyConn, table StaleKeyTable, organizationID string, day time.Time,
	produced []staleKey, computedAt time.Time,
) (int, error) {
	if len(produced) == 0 {
		return 0, nil
	}
	if computedAt.IsZero() {
		return 0, ErrInvalidState
	}
	if _, err := staleKeyVersionAfter(table, computedAt, time.Time{}); err != nil {
		return 0, err
	}
	live, versions, err := loadLiveStaleKeys(ctx, conn, table, organizationID, day)
	if err != nil {
		return 0, err
	}
	held := make(map[string]struct{}, len(produced))
	for _, key := range produced {
		held[append(append(staleKey{}, key...), teamkeytables.LegacyRowView).text()] = struct{}{}
	}
	var stale []staleKey
	for _, key := range live {
		if _, ok := held[key.text()]; ok {
			stale = append(stale, key)
		}
	}
	sort.Slice(stale, func(left, right int) bool { return stale[left].text() < stale[right].text() })
	return writeStaleKeyZeroRows(ctx, conn, table, organizationID, day, stale, func(key staleKey) time.Time {
		version, _ := staleKeyVersionAfter(table, computedAt, versions[key.text()])
		return version
	})
}
