package providersync

import "testing"

// The daily job is the one writer of the three aggregate work-item tables that
// carry row_view. The provider-sync effect writers still exist but are never
// dispatched; their inserts name no row_view, so they would store a legacy row
// beside the view rows. Re-enabling one needs row_view in its insert first.
func TestTheWorkItemAggregateTablesStayRetiredSyncDestinations(t *testing.T) {
	for _, table := range []string{"work_item_metrics_daily", "work_item_state_durations_daily", "work_item_user_metrics_daily"} {
		if _, retired := retiredWorkItemSyncDestinations[table]; !retired {
			t.Fatalf("%s is no longer a retired sync destination: its provider-sync writer names no row_view", table)
		}
	}
}
