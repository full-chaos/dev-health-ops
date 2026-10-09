package workitemmetrics

import (
	"sort"
	"time"
)

// The two measures of work_item_metrics_daily whose stored 0 can mean "the
// provider does not track this". Their names are the column names.
const (
	MeasureBugCompletedRatio    = "bug_completed_ratio"
	MeasureStoryPointsCompleted = "story_points_completed"
)

// CapabilityWindowDays is the observation window: the target day and the 89
// days before it.
const CapabilityWindowDays = 90

// CapabilityRow answers "does this provider track the measure" for one
// organization, observed in the items of the window. There is no unknown
// value: a provider without items in the window gets no row, and a reader
// reads a missing row as unknown, never as 0 or as "not tracked".
type CapabilityRow struct {
	Provider string
	Measure  string
	Tracked  bool
	// EvidenceCount is the number of items of the window that carry the
	// measure: story points set, or the type "bug".
	EvidenceCount int
	// ItemCount is the number of items of the provider in the window.
	ItemCount   int
	WindowStart time.Time
	WindowEnd   time.Time
}

// CapabilityWindow is the window of day as UTC days: the first day and the
// last day, both inclusive.
func CapabilityWindow(day time.Time) (first, last time.Time) {
	last = UTCDay(day)
	return last.AddDate(0, 0, 1-CapabilityWindowDays), last
}

// InCapabilityWindow is the item rule of the window. It is the predicate of
// the daily items read widened to the window: created before the window ends,
// and not done, or completed in the window.
func InCapabilityWindow(item Item, first, last time.Time) bool {
	end := last.AddDate(0, 0, 1)
	if !item.CreatedAt.UTC().Before(end) {
		return false
	}
	if item.Status != "done" {
		return true
	}
	return item.CompletedAt != nil && !item.CompletedAt.UTC().Before(first)
}

type capabilityCounts struct {
	items, withPoints, bugs int
}

// DeriveMeasureCapability observes, for each provider of items, whether its
// items of the window carry story points and bug-typed items. items must be
// every item of the organization, each counted once: a row is the answer for
// the provider, so a subset would answer "not tracked" from missing items.
// The rows are sorted by provider and measure.
func DeriveMeasureCapability(day time.Time, items []Item) []CapabilityRow {
	first, last := CapabilityWindow(day)
	counts := make(map[string]*capabilityCounts)
	for _, item := range items {
		if !InCapabilityWindow(item, first, last) {
			continue
		}
		count := counts[item.Provider]
		if count == nil {
			count = &capabilityCounts{}
			counts[item.Provider] = count
		}
		count.items++
		if item.StoryPoints != nil {
			count.withPoints++
		}
		if item.Type == "bug" {
			count.bugs++
		}
	}
	providers := make([]string, 0, len(counts))
	for provider := range counts {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	rows := make([]CapabilityRow, 0, 2*len(providers))
	for _, provider := range providers {
		count := counts[provider]
		rows = append(rows,
			CapabilityRow{
				Provider: provider, Measure: MeasureBugCompletedRatio,
				Tracked: count.bugs > 0, EvidenceCount: count.bugs, ItemCount: count.items,
				WindowStart: first, WindowEnd: last,
			},
			CapabilityRow{
				Provider: provider, Measure: MeasureStoryPointsCompleted,
				Tracked: count.withPoints > 0, EvidenceCount: count.withPoints, ItemCount: count.items,
				WindowStart: first, WindowEnd: last,
			},
		)
	}
	return rows
}
