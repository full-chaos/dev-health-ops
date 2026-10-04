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
// # When a relation starts and ends
//
// A blocked interval never covers time in which the relation is not KNOWN to
// have existed. A work_item_dependencies row only says the relation exists as
// of its last_synced, so the start and the end come from what is stored
// about the relation, never from the two items' creation times:
//
//   - START: the provider's own time of the link when the synced payload
//     carries one (relation_started_at), else the first time a sync wrote the
//     relation (work_item_dependency_first_seen). The second is too late when
//     the link is older than our first sync of it -- it understates, and it
//     never overstates. A relation with neither stored has no known start and
//     gives no interval.
//   - END: none while the provider still reports the relation. A link
//     removed at the provider leaves its row behind (the table is
//     insert-only), but a sync of an item writes again only the relations
//     that item still has. So a relation is gone when an item that WRITES it
//     has a later sync that did not write it (RelationIsCurrent). Which
//     items write a relation comes from the stored row (RelationWriters): a
//     native link is written by both of its items, a relation read from text
//     only by the item that holds the text. An item that does not write the
//     relation never ends it by being synced, and never keeps it open by
//     NOT being synced.
//
//     The interval of a relation that is gone ends at the relation's own
//     last_synced: the last time a sync saw it. The time a sync first saw it
//     GONE is not stored (an item's last_synced is its latest sync), so this
//     end can be too early and is never too late. The hours before it stay
//     blocked: a removed link does not change the blocked hours of earlier
//     days.
//
//     TWO NAMED CASES in which the end is too early (blocked time is
//     understated, never overstated):
//
//     1. GitLab description keyword "blocks". Its stored raw value is the
//     same as the native link type, so the writer is not derivable and
//     both items are taken as writers: the relation ends at its last write
//     as soon as the BLOCKED issue has a later sync. It is open again when
//     the blocker is synced again and writes it again.
//
//     2. GitHub issues on a Projects v2 board. The board pass writes the
//     issue's work_items row again, with a new last_synced, and reads no
//     issue text; the pass that reads the text is incremental. For an issue
//     that is on a board and is not updated, each board pass makes its text
//     relations look not written again, and they end at their last write.
//
//     Both need a fact the row does not store (who wrote the relation, and
//     when the item's relations were last read).
//
// The two items' creation times and the blocker's completion only CLAMP the
// interval. A relation whose blocker is not a stored work item, or whose
// blocker is terminal without a completion time, gives NO interval. A
// missing fact is not derived, never guessed.

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
	// BlockedWrites and BlockerWrites say which of the two items WRITE the
	// row when they are synced (RelationWriters). At least one is true.
	BlockedWrites bool
	BlockerWrites bool
}

// BlockingEndsOf reads one work_item_dependencies row. ok is false when the
// row is not a blocking relation with a dependable direction.
//
//   - "blocks": source blocks target.
//   - "blocked_by": source is blocked by target. Providers normalize a
//     native "is blocked by" link to "blocks"; this form is left only on
//     text relations to an external key.
//
// raw is the row's relationship_type_raw; it says which item writes the row.
func BlockingEndsOf(sourceID, targetID, relationshipType, raw, semanticsVersion string) (BlockingEnds, bool) {
	if semanticsVersion != CanonicalBlocksSemantics || sourceID == "" || targetID == "" || sourceID == targetID {
		return BlockingEnds{}, false
	}
	sourceWrites, targetWrites := RelationWriters(sourceID, targetID, raw)
	switch relationshipType {
	case "blocks":
		return BlockingEnds{
			BlockerID: sourceID, BlockerWrites: sourceWrites,
			BlockedID: targetID, BlockedWrites: targetWrites,
		}, true
	case "blocked_by":
		return BlockingEnds{
			BlockedID: sourceID, BlockedWrites: sourceWrites,
			BlockerID: targetID, BlockerWrites: targetWrites,
		}, true
	}
	return BlockingEnds{}, false
}

// RelationWriters reports which of a relation's two items WRITE its row when
// they are synced, from what the row itself stores: its two ids and its
// relationship_type_raw.
//
// It matters for the END of a relation (RelationIsCurrent). A sync of an item
// writes again every relation that item still has. So a relation is gone when
// an item that writes it has a later sync that did not write it. An item that
// does NOT write the relation says nothing about it by being synced.
//
//   - A NATIVE link (jira issue links, linear relations, a gitlab issue
//     link) is on both issues at the provider, and the sync of either issue
//     writes it: both items write.
//   - A relation read from TEXT is written only by the item that holds the
//     text. A target that is an external issue key was read from the
//     source's text: the source writes. GitHub has text relations only, and
//     its raw value is the matched phrase: "blocked by #12" and
//     "depends on #12" are in the BLOCKED item's text (the row's target,
//     the normalizer turns the row around), "blocks #12" is in the
//     blocker's text (the row's source). GitLab's description keywords
//     store the keyword: "blocked by" is in the target's text, "blocking"
//     in the source's.
//   - NOT DERIVABLE: gitlab's raw value "blocks" is both the native link
//     type and a description keyword, and a row of unknown shape says
//     nothing. Both items are then taken as writers. That ends the relation
//     at the first later sync of EITHER item: it can end a text relation
//     too early, and it never keeps a removed one open.
func RelationWriters(sourceID, targetID, raw string) (source, target bool) {
	if strings.HasPrefix(targetID, ExternalKeyPrefix) {
		return true, false
	}
	provider := textRelationProvider(sourceID)
	if provider != textRelationProvider(targetID) {
		return true, true
	}
	switch provider {
	case "github":
		// The matched phrase, lower-cased by the normalizer; its keyword can
		// hold any white space ("depends  on").
		phrase := strings.Join(strings.Fields(strings.ToLower(raw)), " ")
		switch {
		case strings.HasPrefix(phrase, "blocked by"), strings.HasPrefix(phrase, "depends on"):
			return false, true
		case strings.HasPrefix(phrase, "blocks"):
			return true, false
		}
	case "gitlab":
		switch raw {
		case "blocked by":
			return false, true
		case "blocking":
			return true, false
		}
	}
	return true, true
}

// textRelationProvider names the provider of a work item id when that
// provider's normalizer reads relations between two work items from TEXT:
// github (issues and pull requests) and gitlab. "" for every other id; the
// relations of those providers are native links.
func textRelationProvider(workItemID string) string {
	prefix, _, _ := strings.Cut(workItemID, ":")
	switch prefix {
	case "gh", "ghpr":
		return "github"
	case "gitlab":
		return "gitlab"
	}
	return ""
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

// BareIssueKey returns the bare issue key of a jira or linear work item
// ("PLAT-9" from "linear:PLAT-9"), trimmed and upper-cased: the form an
// external-key target carries (ExternalKey). ok is false for an item of
// another provider, for an id with no provider prefix and for an empty key.
func BareIssueKey(provider, workItemID string) (string, bool) {
	if provider != "linear" && provider != "jira" {
		return "", false
	}
	separator := strings.Index(workItemID, ":")
	if separator < 0 {
		return "", false
	}
	key := strings.ToUpper(strings.TrimSpace(workItemID[separator+1:]))
	return key, key != ""
}

// ExternalKeyTarget returns the relation target that names a jira or linear
// work item by its issue key: the id a relation read from TEXT carries for
// that item ("extkey:PLAT-9" for "linear:PLAT-9"). A reader that wants every
// relation naming an item must look for this form as well as the item's id.
func ExternalKeyTarget(provider, workItemID string) (string, bool) {
	key, ok := BareIssueKey(provider, workItemID)
	if !ok {
		return "", false
	}
	return ExternalKeyPrefix + key, true
}

// RelationEndedBy finds the sync that shows a stored relation is one the
// provider no longer reports.
//
// relationLastSynced is the row's last_synced. writerLastSynced holds the
// latest sync of each item that WRITES the row (RelationWriters). A sync of
// such an item writes the relation again when the item still has it, so a
// relation older than the latest sync of ANY of its writers was not written
// by that sync: the link, or the text that made it, is gone. The result is
// the position of the first such writer, or -1 when every writer's latest
// sync wrote the relation: it is current.
//
// An item that does not write the relation is not passed here, and its syncs
// never end the relation: its silence says nothing.
func RelationEndedBy(relationLastSynced time.Time, writerLastSynced ...time.Time) int {
	for position, writer := range writerLastSynced {
		if relationLastSynced.Before(writer) {
			return position
		}
	}
	return -1
}

// RelationIsCurrent reports whether a stored relation is one the provider
// still reports: no writer has a later sync that did not write it
// (RelationEndedBy).
func RelationIsCurrent(relationLastSynced time.Time, writerLastSynced ...time.Time) bool {
	return RelationEndedBy(relationLastSynced, writerLastSynced...) < 0
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
// created at itemCreatedAt, through a relation known to exist from
// relationStart until relationEnd (nil: the provider still reports it).
//
// The span starts at relationStart, moved LATER to the creation of either
// item when that is later (a relation cannot hold before both items exist; a
// later start never overstates). It ends at the earlier of relationEnd and
// the blocker's completion. ok is false when that leaves no time, or when the
// blocker is terminal with no completion time: its end is not known, so
// nothing is derived.
func BlockedIntervalFor(
	itemCreatedAt, relationStart time.Time, relationEnd *time.Time, blocker Blocker,
) (BlockedInterval, bool) {
	start := relationStart.UTC()
	for _, created := range []time.Time{itemCreatedAt.UTC(), blocker.CreatedAt.UTC()} {
		if created.After(start) {
			start = created
		}
	}
	var end *time.Time
	if blocker.CompletedAt != nil {
		completed := blocker.CompletedAt.UTC()
		end = &completed
	} else if IsTerminalStatus(blocker.Status) {
		return BlockedInterval{}, false
	}
	if relationEnd != nil {
		if ended := relationEnd.UTC(); end == nil || ended.Before(*end) {
			end = &ended
		}
	}
	if end != nil && !end.After(start) {
		return BlockedInterval{}, false
	}
	return BlockedInterval{Start: start, End: end}, true
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
	// Raw is relationship_type_raw: the provider's own name of the relation,
	// or the text it was read from.
	Raw              string
	SemanticsVersion string
	LastSynced       time.Time
	// StartedAt is the provider's own time of the link
	// (work_item_dependencies.relation_started_at); nil when the synced
	// payload carried none.
	StartedAt *time.Time
	// FirstSeenAt is the first time a sync wrote the relation
	// (work_item_dependency_first_seen); nil when none is stored.
	FirstSeenAt *time.Time
}

// Start returns the time from which the relation is known to have existed:
// the provider's own time when there is one, else the first time a sync
// wrote it. known is false when neither is stored; such a relation gives no
// blocked interval.
func (relation BlockingRelation) Start() (start time.Time, known bool) {
	switch {
	case relation.StartedAt != nil:
		return relation.StartedAt.UTC(), true
	case relation.FirstSeenAt != nil:
		return relation.FirstSeenAt.UTC(), true
	}
	return time.Time{}, false
}

// RelationEnd is the stored work item at one end of a relation.
type RelationEnd struct {
	WorkItemID string
	Provider   string
	// ProjectID is work_items.project_id of this row. The rule reads it for
	// one count only: a github row written by the Projects v2 board pass
	// carries a GitHubBoardProjectPrefix id (EndedRelationStats).
	ProjectID   string
	Status      string
	CreatedAt   time.Time
	CompletedAt *time.Time
	LastSynced  time.Time
}

// GitHubBoardProjectPrefix opens the project_id of a github work_items row
// that the Projects v2 board pass wrote
// (internal/providersync/github_work_items_projects_v2.go). The pass that
// reads an issue's text writes the repository's own project id.
const GitHubBoardProjectPrefix = "ghprojv2:"

// EndedRelationStats counts what the END rule did in one run of
// BlockedIntervalsWithStats. It is telemetry: it changes no interval.
type EndedRelationStats struct {
	// Relations is the number of blocking relations between two stored items
	// with a known start: the ones the end rule was applied to.
	Relations int
	// Ended counts the relations ended because an item that writes them has
	// a later sync that did not write them again, by the provider of that
	// item ("github", "gitlab", "jira", "linear", else "other").
	Ended map[string]int
	// GitHubBoardCandidates counts, of Ended["github"], the relations whose
	// writer's latest stored row is a Projects v2 board row. The board pass
	// writes the issue again and reads no issue text, so such a relation may
	// still be in the text: its end is the named case of a too early end.
	// It is a count of CANDIDATES: the same stored shape is left by a sync
	// that read the text and found the relation gone.
	GitHubBoardCandidates int
}

// endedRelationProviders are the providers EndedRelationStats names; any
// other provider is counted under "other".
var endedRelationProviders = []string{"github", "gitlab", "jira", "linear"}

func (stats *EndedRelationStats) countEnded(writer RelationEnd) {
	provider := "other"
	for _, known := range endedRelationProviders {
		if writer.Provider == known {
			provider = known
		}
	}
	if stats.Ended == nil {
		stats.Ended = map[string]int{}
	}
	stats.Ended[provider]++
	if provider == "github" && strings.HasPrefix(writer.ProjectID, GitHubBoardProjectPrefix) {
		stats.GitHubBoardCandidates++
	}
}

// EndedRelationsLogMessage is the message of the one log line a writer of
// work_item_state_durations_daily emits per run, with the fields of
// EndedRelationCounts as its counts.
const EndedRelationsLogMessage = "blocked rule: relations ended by a later sync of an item that writes them"

// EndedRelationCounts are the counts of that log line: the same fields for
// every run and for both writers, counts only, no id of any kind. Each writer
// logs them itself, as named scalars, under the keys writer, relations,
// ended, ended_github, ended_gitlab, ended_jira, ended_linear, ended_other
// and github_board_candidates (this package imports no log package: the
// sync dispatch runtime depends on it, and its closed list of log users only
// shrinks). The line is the way to see, on a real organization, how many
// relations the end rule closes and how many of those are the named case of
// a github issue on a Projects v2 board.
type EndedRelationCounts struct {
	Relations             int
	Ended                 int
	EndedGitHub           int
	EndedGitLab           int
	EndedJira             int
	EndedLinear           int
	EndedOther            int
	GitHubBoardCandidates int
}

// Counts returns the stats as the counts of the log line.
func (stats EndedRelationStats) Counts() EndedRelationCounts {
	total := 0
	for _, count := range stats.Ended {
		total += count
	}
	return EndedRelationCounts{
		Relations:             stats.Relations,
		Ended:                 total,
		EndedGitHub:           stats.Ended["github"],
		EndedGitLab:           stats.Ended["gitlab"],
		EndedJira:             stats.Ended["jira"],
		EndedLinear:           stats.Ended["linear"],
		EndedOther:            stats.Ended["other"],
		GitHubBoardCandidates: stats.GitHubBoardCandidates,
	}
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
//   - the relation has a known start (BlockingRelation.Start);
//   - the blocker has a known open span that overlaps the relation
//     (BlockedIntervalFor).
//
// Anything else contributes nothing. A relation the provider no longer
// reports (RelationIsCurrent) still gives its span, ended at the last time a
// sync saw it: the hours it covered did happen. When two rows of ends carry the same
// work item id, the one synced last is the stored item.
func BlockedIntervalsByItem(relations []BlockingRelation, ends []RelationEnd) map[string][]BlockedInterval {
	intervals, _ := BlockedIntervalsWithStats(relations, ends)
	return intervals
}

// BlockedIntervalsWithStats is BlockedIntervalsByItem with the counts of what
// its end rule did (EndedRelationStats), for the one log line each writer
// emits per run.
func BlockedIntervalsWithStats(relations []BlockingRelation, ends []RelationEnd) (map[string][]BlockedInterval, EndedRelationStats) {
	var stats EndedRelationStats
	if len(relations) == 0 || len(ends) == 0 {
		return nil, stats
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
		named, blocking := BlockingEndsOf(relation.SourceID, relation.TargetID, relation.RelationshipType, relation.Raw, relation.SemanticsVersion)
		if !blocking {
			continue
		}
		blocked, blockedStored := resolve(named.BlockedID)
		blocker, blockerStored := resolve(named.BlockerID)
		if !blockedStored || !blockerStored || blocked.WorkItemID == blocker.WorkItemID {
			continue
		}
		start, known := relation.Start()
		if !known {
			continue
		}
		// The items that write this relation, and the latest sync of each.
		writers := make([]RelationEnd, 0, 2)
		if named.BlockedWrites {
			writers = append(writers, blocked)
		}
		if named.BlockerWrites {
			writers = append(writers, blocker)
		}
		synced := make([]time.Time, 0, len(writers))
		for _, writer := range writers {
			synced = append(synced, writer.LastSynced)
		}
		stats.Relations++
		var relationEnd *time.Time
		if endedBy := RelationEndedBy(relation.LastSynced, synced...); endedBy >= 0 {
			lastSeen := relation.LastSynced.UTC()
			relationEnd = &lastSeen
			stats.countEnded(writers[endedBy])
		}
		interval, open := BlockedIntervalFor(blocked.CreatedAt, start, relationEnd, Blocker{
			Status: blocker.Status, CreatedAt: blocker.CreatedAt, CompletedAt: blocker.CompletedAt,
		})
		if !open {
			continue
		}
		result[blocked.WorkItemID] = append(result[blocked.WorkItemID], interval)
	}
	if len(result) == 0 {
		return nil, stats
	}
	return result, stats
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
		key, keyed := BareIssueKey(end.Provider, id)
		if !keyed || ambiguous[key] {
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
