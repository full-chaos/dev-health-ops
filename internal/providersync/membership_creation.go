package providersync

import (
	"log/slog"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
)

// membershipCreationStats counts, per sync, what the creation-time ADD rule
// (CHAOS-7361) did. Every skip has a named reason; nothing is silent.
type membershipCreationStats struct {
	Added int
	// Skipped is keyed by a projectmembership.Skip* reason.
	Skipped map[string]int
	// HistoryEndMismatch counts items whose replayed history ends in a project
	// other than work_items.project_id at write time. The presence view
	// answers a subject with history from that history alone, so each such
	// item is a place where the view and the column disagree.
	HistoryEndMismatch int
}

func (stats *membershipCreationStats) skip(reason string) {
	if stats.Skipped == nil {
		stats.Skipped = map[string]int{}
	}
	stats.Skipped[reason]++
}

func (stats *membershipCreationStats) record(outcome projectmembership.CreationOutcome) {
	switch outcome {
	case projectmembership.CreationAdded:
		stats.Added++
	case projectmembership.CreationSkippedCreatedAfterHistory:
		stats.skip(projectmembership.SkipCreatedAfterHistory)
	}
}

// observeHistoryEnd compares the history's last project with the item's
// current project (both "" when the item has none).
func (stats *membershipCreationStats) observeHistoryEnd(history []projectmembership.Row, currentProjectID string) {
	if len(history) == 0 {
		return
	}
	if projectmembership.HistoryEndProject(history) != currentProjectID {
		stats.HistoryEndMismatch++
	}
}

// result adds the counters to a route result map and logs one line per
// non-zero reason (provider and counts only, never item values).
func (stats membershipCreationStats) result(provider string, into map[string]any) {
	into["membership_creation_adds"] = stats.Added
	into["membership_creation_skipped"] = stats.skipTotal()
	into["membership_history_end_mismatch"] = stats.HistoryEndMismatch
	if stats.Added > 0 {
		slog.Info("project membership creation ADD rows written", "provider", provider, "count", stats.Added)
	}
	reasons := make([]string, 0, len(stats.Skipped))
	for reason := range stats.Skipped {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	for _, reason := range reasons {
		slog.Warn("project membership creation ADD skipped", "provider", provider, "reason", reason, "count", stats.Skipped[reason])
	}
	if stats.HistoryEndMismatch > 0 {
		slog.Warn("project membership history ends in a different project than work_items.project_id",
			"provider", provider, "count", stats.HistoryEndMismatch)
	}
}

func (stats membershipCreationStats) skipTotal() int {
	total := 0
	for _, count := range stats.Skipped {
		total += count
	}
	return total
}
