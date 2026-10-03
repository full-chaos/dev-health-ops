package workitemmetrics

// "Blocked" derived from open blocking relations.
//
// A work item's status comes from its provider status name or a label
// (internal/providersync/status_mapping.go). No provider row on a real
// organization carries a "Blocked" status name or label, so the status
// "blocked" never appeared and every reader of it showed zero. The
// providers do hold the fact natively, as a RELATION between two items:
// work_item_dependencies rows.
//
// The rule here adds that source. A work item is "blocked" in the interval
// where
//
//   - its own status is not terminal (done, canceled), and
//   - at least one BLOCKER is open: another stored work item that a current
//     blocking relation names, between that item's creation and its
//     completion.
//
// In that interval "blocked" REPLACES the item's own status, so the hours an
// item contributes to a day do not change; they move from the status the
// item had to "blocked". The status-name and label rule is untouched: a
// segment that is already "blocked" stays "blocked".
//
// # One implementation, two writers
//
// work_item_state_durations_daily has two writers -- the sync-time deriver in
// internal/providersync and the work_item_state daily family in
// internal/jobs/metrics/daily -- and `status` is part of the row key. If the
// two disagreed about which hours are "blocked", one of them would leave a
// "blocked" row beside the other's full-length row for the same item, and a
// reader would count those hours twice. Both call the functions in this file,
// for the reason this package exists.
//
// # What the stored relation does NOT say
//
// A work_item_dependencies row has no start time and no end time: it says the
// relation exists, as of its last_synced. Two consequences, both deliberate:
//
//   - START. The interval starts when both items exist (the later of the two
//     created_at). When the link was added later than that, this is too
//     early. It is the earliest instant the relation can have held, and no
//     stored fact gives a later one.
//   - REMOVAL. A link removed at the provider leaves its row behind: the
//     table is insert-only. A re-synced item re-emits the relations it still
//     has, with the new last_synced, so a relation OLDER than the latest
//     sync of the item that emits it is one the provider no longer reports.
//     RelationIsCurrent applies that.
//
// A relation whose blocker is not a stored work item, or whose blocker is
// terminal without a completion time, gives NO interval. A missing fact is
// not derived, never guessed.

import (
	"sort"
	"strings"
	"time"
)

// StatusBlocked is the status category the overlay writes.
const StatusBlocked = "blocked"

// CanonicalBlocksSemantics is the relationship_semantics_version of a
// work_item_dependencies row whose direction is the canonical one: for
// relationship_type "blocks", source is the BLOCKER and target the BLOCKED
// item. A row of any other version (the column defaults to "legacy.v1") has
// no dependable direction and is not read.
const CanonicalBlocksSemantics = "canonical-blocks.v2"

// ExternalKeyPrefix opens a relation target that is an issue KEY found in
// text, not a work item id. It names a work item only when a stored item
// carries that key.
const ExternalKeyPrefix = "extkey:"

// StatusSegment is one span of a work item's status history.
type StatusSegment struct {
	Status string
	Start  time.Time
	End    time.Time
}

// BlockingEnds is what one work_item_dependencies row says about blocking.
type BlockingEnds struct {
	// BlockedID and BlockerID are the row's two ends, as stored. Either can
	// be an ExternalKeyPrefix target the caller must resolve to a work item.
	BlockedID string
	BlockerID string
	// EmitterID is the one item whose sync writes this row, when only one
	// does: a relation read from TEXT (an issue key in a body or a comment)
	// is emitted by the item that holds the text. Empty when the provider
	// reports the relation on BOTH items (a native link), so either item's
	// sync writes the row.
	EmitterID string
}

// BlockingEndsOf reads one work_item_dependencies row. ok is false when the
// row is not a blocking relation with a dependable direction.
//
//   - "blocks": source blocks target.
//   - "blocked_by": source is blocked by target. Providers normalize a
//     native "is blocked by" link to "blocks"; this form is left only on
//     text relations to an external key.
//
// A row whose target is an external key was read from text in the SOURCE
// item, so the source is its only emitter.
func BlockingEndsOf(sourceID, targetID, relationshipType, semanticsVersion string) (BlockingEnds, bool) {
	if semanticsVersion != CanonicalBlocksSemantics || sourceID == "" || targetID == "" || sourceID == targetID {
		return BlockingEnds{}, false
	}
	var ends BlockingEnds
	switch relationshipType {
	case "blocks":
		ends = BlockingEnds{BlockerID: sourceID, BlockedID: targetID}
	case "blocked_by":
		ends = BlockingEnds{BlockedID: sourceID, BlockerID: targetID}
	default:
		return BlockingEnds{}, false
	}
	if strings.HasPrefix(targetID, ExternalKeyPrefix) {
		ends.EmitterID = sourceID
	}
	return ends, true
}

// ExternalKey returns the normalized issue key of an ExternalKeyPrefix
// target, and whether id is one. The normalization (trim, upper case) is the
// one every other reader of these targets applies.
func ExternalKey(id string) (string, bool) {
	if !strings.HasPrefix(id, ExternalKeyPrefix) {
		return "", false
	}
	key := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(id, ExternalKeyPrefix)))
	return key, key != ""
}

// RelationIsCurrent reports whether a stored relation is one the provider
// still reports.
//
// relationLastSynced is the row's last_synced. For a one-sided relation
// (emitterKnown), emitterLastSynced is the latest sync of the item that
// writes it: a relation older than that sync was not re-emitted by it, so
// the text that made it is gone. For a two-sided relation, blockedLastSynced
// and blockerLastSynced are the latest sync of each end: the relation is
// dropped only when it is older than BOTH, that is when each end has been
// synced again and neither reported it. An existing link is never dropped by
// this rule; a removed native link is dropped as soon as both ends have been
// synced again.
func RelationIsCurrent(
	relationLastSynced time.Time,
	emitterKnown bool, emitterLastSynced time.Time,
	blockedLastSynced, blockerLastSynced time.Time,
) bool {
	if emitterKnown {
		return !relationLastSynced.Before(emitterLastSynced)
	}
	older := blockedLastSynced
	if blockerLastSynced.Before(older) {
		older = blockerLastSynced
	}
	return !relationLastSynced.Before(older)
}

// Blocker is one stored work item that a current blocking relation names as
// the blocker of an item.
type Blocker struct {
	// Status is the blocker's normalized status category.
	Status      string
	CreatedAt   time.Time
	CompletedAt *time.Time
}

// BlockedInterval is a span in which an item has an open blocker. A nil End
// means the blocker is still open.
type BlockedInterval struct {
	Start time.Time
	End   *time.Time
}

// terminalStatuses are the status categories of a finished item.
var terminalStatuses = map[string]struct{}{"done": {}, "canceled": {}}

// IsTerminalStatus reports whether status is one a finished item has.
func IsTerminalStatus(status string) bool {
	_, terminal := terminalStatuses[status]
	return terminal
}

// BlockedIntervalFor returns the span in which blocker blocks an item
// created at itemCreatedAt. ok is false when the blocker gives no span: it
// was completed before both items existed, or it is terminal with no
// completion time (its end is not known, so nothing is derived).
func BlockedIntervalFor(itemCreatedAt time.Time, blocker Blocker) (BlockedInterval, bool) {
	start := itemCreatedAt.UTC()
	if created := blocker.CreatedAt.UTC(); created.After(start) {
		start = created
	}
	if blocker.CompletedAt == nil {
		if IsTerminalStatus(blocker.Status) {
			return BlockedInterval{}, false
		}
		return BlockedInterval{Start: start}, true
	}
	end := blocker.CompletedAt.UTC()
	if !end.After(start) {
		return BlockedInterval{}, false
	}
	return BlockedInterval{Start: start, End: &end}, true
}

// OverlayBlocked rewrites segments so that the parts of a NON-terminal
// segment covered by any interval carry StatusBlocked. Segment boundaries
// outside the intervals, the order of the segments and the total covered
// time are unchanged. With no interval the input is returned as it is.
func OverlayBlocked(segments []StatusSegment, intervals []BlockedInterval) []StatusSegment {
	if len(segments) == 0 || len(intervals) == 0 {
		return segments
	}
	merged := mergeBlockedIntervals(intervals)
	result := make([]StatusSegment, 0, len(segments)+2*len(merged))
	for _, segment := range segments {
		if IsTerminalStatus(segment.Status) || segment.Status == StatusBlocked || !segment.End.After(segment.Start) {
			result = append(result, segment)
			continue
		}
		cursor := segment.Start
		for _, interval := range merged {
			start := interval.Start
			if start.Before(cursor) {
				start = cursor
			}
			end := segment.End
			if interval.End != nil && interval.End.Before(end) {
				end = *interval.End
			}
			if !end.After(start) {
				continue
			}
			if start.After(cursor) {
				result = append(result, StatusSegment{Status: segment.Status, Start: cursor, End: start})
			}
			result = append(result, StatusSegment{Status: StatusBlocked, Start: start, End: end})
			cursor = end
		}
		if segment.End.After(cursor) {
			result = append(result, StatusSegment{Status: segment.Status, Start: cursor, End: segment.End})
		}
	}
	return result
}

// mergeBlockedIntervals returns the union of intervals as disjoint spans in
// start order. An open-ended interval absorbs everything after its start.
func mergeBlockedIntervals(intervals []BlockedInterval) []BlockedInterval {
	ordered := make([]BlockedInterval, len(intervals))
	copy(ordered, intervals)
	sort.SliceStable(ordered, func(left, right int) bool {
		return ordered[left].Start.Before(ordered[right].Start)
	})
	merged := make([]BlockedInterval, 0, len(ordered))
	for _, interval := range ordered {
		if len(merged) == 0 {
			merged = append(merged, interval)
			continue
		}
		last := &merged[len(merged)-1]
		if last.End == nil {
			// Open-ended: every later interval starts inside it.
			continue
		}
		if interval.Start.After(*last.End) {
			merged = append(merged, interval)
			continue
		}
		if interval.End == nil {
			last.End = nil
			continue
		}
		if interval.End.After(*last.End) {
			end := *interval.End
			last.End = &end
		}
	}
	return merged
}

// BlockingRelation is one work_item_dependencies row, trimmed to the columns
// the blocked rule reads.
type BlockingRelation struct {
	SourceID         string
	TargetID         string
	RelationshipType string
	SemanticsVersion string
	LastSynced       time.Time
}

// RelationEnd is the stored work item at one end of a relation.
type RelationEnd struct {
	WorkItemID  string
	Provider    string
	Status      string
	CreatedAt   time.Time
	CompletedAt *time.Time
	LastSynced  time.Time
}

// BlockedIntervalsByItem resolves relations against the stored work items
// and returns, per BLOCKED work item id, the spans in which that item has an
// open blocker. It is the whole "which items are blocked, and when" decision;
// both writers call it and then OverlayBlocked.
//
// A relation gives a span only when every fact it needs is stored:
//
//   - the row is a blocking relation with a dependable direction
//     (BlockingEndsOf);
//   - both ends are stored work items -- an external-key end resolves through
//     the bare issue key of a jira or linear item, and a key that no item or
//     more than one item carries resolves to nothing;
//   - the provider still reports the relation (RelationIsCurrent), which
//     for a one-sided relation also needs its emitter to be stored;
//   - the blocker has a known open span (BlockedIntervalFor).
//
// Anything else contributes nothing. When two rows of ends carry the same
// work item id, the one synced last is the stored item.
func BlockedIntervalsByItem(relations []BlockingRelation, ends []RelationEnd) map[string][]BlockedInterval {
	if len(relations) == 0 || len(ends) == 0 {
		return nil
	}
	byID := make(map[string]RelationEnd, len(ends))
	for _, end := range ends {
		if end.WorkItemID == "" {
			continue
		}
		if existing, seen := byID[end.WorkItemID]; seen && !end.LastSynced.After(existing.LastSynced) {
			continue
		}
		byID[end.WorkItemID] = end
	}
	keyIndex := issueKeyIndex(byID)
	resolve := func(id string) (RelationEnd, bool) {
		if key, external := ExternalKey(id); external {
			id = keyIndex[key]
		} else if strings.HasPrefix(id, ExternalKeyPrefix) {
			return RelationEnd{}, false
		}
		end, stored := byID[id]
		return end, stored
	}

	result := map[string][]BlockedInterval{}
	for _, relation := range relations {
		named, blocking := BlockingEndsOf(relation.SourceID, relation.TargetID, relation.RelationshipType, relation.SemanticsVersion)
		if !blocking {
			continue
		}
		blocked, blockedStored := resolve(named.BlockedID)
		blocker, blockerStored := resolve(named.BlockerID)
		if !blockedStored || !blockerStored || blocked.WorkItemID == blocker.WorkItemID {
			continue
		}
		var emitterLastSynced time.Time
		emitterKnown := named.EmitterID != ""
		if emitterKnown {
			emitter, emitterStored := byID[named.EmitterID]
			if !emitterStored {
				continue
			}
			emitterLastSynced = emitter.LastSynced
		}
		if !RelationIsCurrent(relation.LastSynced, emitterKnown, emitterLastSynced, blocked.LastSynced, blocker.LastSynced) {
			continue
		}
		interval, open := BlockedIntervalFor(blocked.CreatedAt, Blocker{
			Status: blocker.Status, CreatedAt: blocker.CreatedAt, CompletedAt: blocker.CompletedAt,
		})
		if !open {
			continue
		}
		result[blocked.WorkItemID] = append(result[blocked.WorkItemID], interval)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// issueKeyIndex indexes jira and linear work items by their bare issue key
// ("PLAT-9" from "linear:PLAT-9"), the form an external-key target carries.
// A key that more than one work item carries is ambiguous and is left out:
// the same rule the team-attribution readers of these targets apply
// (internal/providersync/team_repo_ownership_derivation.go, buildIssueKeyIndex).
func issueKeyIndex(byID map[string]RelationEnd) map[string]string {
	index := map[string]string{}
	ambiguous := map[string]bool{}
	for id, end := range byID {
		if end.Provider != "linear" && end.Provider != "jira" {
			continue
		}
		separator := strings.Index(id, ":")
		if separator < 0 {
			continue
		}
		key := strings.ToUpper(strings.TrimSpace(id[separator+1:]))
		if key == "" || ambiguous[key] {
			continue
		}
		if existing, seen := index[key]; seen && existing != id {
			delete(index, key)
			ambiguous[key] = true
			continue
		}
		index[key] = id
	}
	return index
}
