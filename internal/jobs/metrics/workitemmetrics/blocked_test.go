package workitemmetrics

import (
	"bytes"
	"log/slog"
	"reflect"
	"testing"
	"time"
)

func blockedTime(hour int) time.Time {
	return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(hour) * time.Hour)
}

func blockedTimePtr(hour int) *time.Time {
	value := blockedTime(hour)
	return &value
}

// A relation row names a blocker and a blocked item only when its direction
// is dependable: the canonical semantics version, one of the two blocking
// types, two different non-empty ends. It also says which of the two items
// write the row.
func TestBlockingEndsOfReadsOnlyCanonicalBlockingRows(t *testing.T) {
	for name, tc := range map[string]struct {
		source, target, relationship, raw, version string
		want                                       BlockingEnds
		ok                                         bool
	}{
		"blocks, a native link: the source is the blocker, both items write": {
			"jira:OPS-1", "jira:OPS-2", "blocks", "blocks", CanonicalBlocksSemantics,
			BlockingEnds{BlockerID: "jira:OPS-1", BlockerWrites: true, BlockedID: "jira:OPS-2", BlockedWrites: true}, true,
		},
		"blocked_by to an external key: the source is the blocked item, and the only writer": {
			"gh:acme/api#7", "extkey:OPS-2", "blocked_by", "external_issue_key", CanonicalBlocksSemantics,
			BlockingEnds{BlockedID: "gh:acme/api#7", BlockedWrites: true, BlockerID: "extkey:OPS-2"}, true,
		},
		"blocks to an external key: the source is the blocker, and the only writer": {
			"gitlab:acme/api#7", "extkey:OPS-2", "blocks", "external_issue_key", CanonicalBlocksSemantics,
			BlockingEnds{BlockerID: "gitlab:acme/api#7", BlockerWrites: true, BlockedID: "extkey:OPS-2"}, true,
		},
		"blocks from github text in the BLOCKED item: the target writes": {
			"gh:acme/api#12", "gh:acme/api#7", "blocks", "blocked by #12", CanonicalBlocksSemantics,
			BlockingEnds{BlockerID: "gh:acme/api#12", BlockedID: "gh:acme/api#7", BlockedWrites: true}, true,
		},
		"blocks from github text in the BLOCKER: the source writes": {
			"gh:acme/api#12", "gh:acme/api#7", "blocks", "blocks #7", CanonicalBlocksSemantics,
			BlockingEnds{BlockerID: "gh:acme/api#12", BlockerWrites: true, BlockedID: "gh:acme/api#7"}, true,
		},
		"blocked_by between two work items: both items write": {
			"linear:A-1", "linear:A-2", "blocked_by", "linear_relation:blocked_by", CanonicalBlocksSemantics,
			BlockingEnds{BlockedID: "linear:A-1", BlockedWrites: true, BlockerID: "linear:A-2", BlockerWrites: true}, true,
		},
		"a relation that does not block":          {"jira:OPS-1", "jira:OPS-2", "relates_to", "relates to", CanonicalBlocksSemantics, BlockingEnds{}, false},
		"a duplicate relation":                    {"linear:A-1", "linear:A-2", "duplicates", "linear_relation:duplicate", CanonicalBlocksSemantics, BlockingEnds{}, false},
		"the legacy direction is not dependable":  {"jira:OPS-1", "jira:OPS-2", "blocks", "blocks", "legacy.v1", BlockingEnds{}, false},
		"no semantics version":                    {"jira:OPS-1", "jira:OPS-2", "blocks", "blocks", "", BlockingEnds{}, false},
		"no source":                               {"", "jira:OPS-2", "blocks", "blocks", CanonicalBlocksSemantics, BlockingEnds{}, false},
		"no target":                               {"jira:OPS-1", "", "blocks", "blocks", CanonicalBlocksSemantics, BlockingEnds{}, false},
		"an item cannot block itself":             {"jira:OPS-1", "jira:OPS-1", "blocks", "blocks", CanonicalBlocksSemantics, BlockingEnds{}, false},
		"the type is matched exactly, not folded": {"jira:OPS-1", "jira:OPS-2", "Blocks", "blocks", CanonicalBlocksSemantics, BlockingEnds{}, false},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := BlockingEndsOf(tc.source, tc.target, tc.relationship, tc.raw, tc.version)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("BlockingEndsOf = %+v, %t; want %+v, %t", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// Which item writes a relation comes from the stored row alone: its two ids
// and its raw type. Each raw value here is one a provider's normalizer writes
// (internal/providersync: jira_work_items_rows.go, linear_work_items_route.go,
// gitlab_work_items_rows.go, github_work_items_rows.go).
func TestRelationWritersComeFromTheStoredRow(t *testing.T) {
	for name, tc := range map[string]struct {
		source, target, raw        string
		sourceWrites, targetWrites bool
	}{
		// Native links: on both issues at the provider, written by either sync.
		"jira link, outward name":     {"jira:OPS-1", "jira:OPS-2", "blocks", true, true},
		"jira link, inward name":      {"jira:OPS-1", "jira:OPS-2", "is blocked by", true, true},
		"linear relation":             {"linear:OPS-1", "linear:OPS-2", "linear_relation:blocks", true, true},
		"linear inverse relation":     {"linear:OPS-1", "linear:OPS-2", "linear_relation:blocked_by", true, true},
		"gitlab link, blocked side":   {"gitlab:acme/api#5", "gitlab:acme/api#7", "is_blocked_by", true, true},
		"gitlab link across projects": {"gitlab:acme/web#5", "gitlab:acme/api#7", "is_blocked_by", true, true},
		// Text: only the item that holds the text writes the row.
		"external key, any provider":               {"gh:acme/api#7", "extkey:OPS-2", "external_issue_key", true, false},
		"external key from a linear comment":       {"gh:acme/api#7", "extkey:OPS-2", "github_comment_linear_url", true, false},
		"external key, gitlab":                     {"gitlab:acme/api#7", "extkey:OPS-2", "external_issue_key", true, false},
		"external key with a native raw value":     {"jira:OPS-1", "extkey:OPS-2", "blocks", true, false},
		"github: blocked by":                       {"gh:acme/api#12", "gh:acme/api#7", "blocked by #12", false, true},
		"github: blocked by another repository":    {"gh:acme/web#12", "gh:acme/api#7", "blocked by acme/web#12", false, true},
		"github: depends on":                       {"gh:acme/api#12", "gh:acme/api#7", "depends on: #12", false, true},
		"github: the keyword in upper case":        {"gh:acme/api#12", "gh:acme/api#7", "Blocked By #12", false, true},
		"github: a merge request id is not github": {"gh:acme/api#12", "gitlab:acme/api!7", "blocked by #12", true, true},
		"github: depends on, wide white space":     {"gh:acme/api#12", "gh:acme/api#7", "depends  \ton #12", false, true},
		"github: blocks":                           {"gh:acme/api#12", "gh:acme/api#7", "blocks #7", true, false},
		"github: a pull request holds the text":    {"gh:acme/api#12", "ghpr:acme/api#7", "blocked by #12", false, true},
		"gitlab keyword: blocked by":               {"gitlab:acme/api#5", "gitlab:acme/api#7", "blocked by", false, true},
		"gitlab keyword: blocking":                 {"gitlab:acme/api#5", "gitlab:acme/api#7", "blocking", true, false},
		// Not derivable: both items are taken as writers.
		"gitlab: blocks is a link type and a keyword": {"gitlab:acme/api#5", "gitlab:acme/api#7", "blocks", true, true},
		"gitlab: an unknown raw value":                {"gitlab:acme/api#5", "gitlab:acme/api#7", "description_reference", true, true},
		"github: an unknown raw value":                {"gh:acme/api#12", "gh:acme/api#7", "github_closing_reference", true, true},
		"github: no raw value":                        {"gh:acme/api#12", "gh:acme/api#7", "", true, true},
		"github: the keyword is not at the start":     {"gh:acme/api#12", "gh:acme/api#7", "not blocked by #12", true, true},
		"the two ids are of two providers":            {"gh:acme/api#12", "gitlab:acme/api#7", "blocked by #12", true, true},
		"an id with no provider prefix":               {"OPS-1", "OPS-2", "blocked by", true, true},
		"an id of an unknown provider":                {"asana:1", "asana:2", "blocked by", true, true},
		"a gitlab keyword is not read as github text": {"gh:acme/api#12", "gh:acme/api#7", "blocking", true, true},
		"github text is not read as a gitlab keyword": {"gitlab:acme/api#5", "gitlab:acme/api#7", "blocked by #5", true, true},
	} {
		t.Run(name, func(t *testing.T) {
			source, target := RelationWriters(tc.source, tc.target, tc.raw)
			if source != tc.sourceWrites || target != tc.targetWrites {
				t.Fatalf("RelationWriters(%q, %q, %q) = source %t, target %t; want %t, %t",
					tc.source, tc.target, tc.raw, source, target, tc.sourceWrites, tc.targetWrites)
			}
			if !source && !target {
				t.Fatal("a relation with no writer could never end")
			}
		})
	}
}

func TestExternalKeyNormalizesAndRefusesAnEmptyKey(t *testing.T) {
	for id, want := range map[string]string{
		"extkey:OPS-2":    "OPS-2",
		"extkey: ops-2 ":  "OPS-2",
		"extkey:":         "",
		"extkey:   ":      "",
		"jira:OPS-2":      "",
		"EXTKEY:OPS-2":    "",
		"linear:extkey:1": "",
	} {
		key, ok := ExternalKey(id)
		if key != want || ok != (want != "") {
			t.Fatalf("ExternalKey(%q) = %q, %t; want %q, %t", id, key, ok, want, want != "")
		}
	}
}

// A relation the provider no longer reports is the one that a WRITER's later
// sync did not write again. The sync of an item that does not write it says
// nothing.
func TestRelationIsCurrentDropsARelationAWriterDidNotWriteAgain(t *testing.T) {
	for name, tc := range map[string]struct {
		relation int
		writers  []int
		want     bool
	}{
		"one writer: written by its latest sync":       {relation: 5, writers: []int{5}, want: true},
		"one writer: written after its latest sync":    {relation: 6, writers: []int{5}, want: true},
		"one writer: its latest sync did not write it": {relation: 5, writers: []int{6}, want: false},
		// A native link: each end writes it.
		"two writers: both at the relation's sync":             {relation: 5, writers: []int{5, 5}, want: true},
		"two writers: the first wrote it again":                {relation: 7, writers: []int{7, 5}, want: true},
		"two writers: the second wrote it again":               {relation: 7, writers: []int{5, 7}, want: true},
		"two writers: only the first was synced again":         {relation: 5, writers: []int{7, 5}, want: false},
		"two writers: only the second was synced again":        {relation: 5, writers: []int{5, 7}, want: false},
		"two writers: both synced again, neither wrote it":     {relation: 5, writers: []int{6, 7}, want: false},
		"no writer is passed: nothing says the relation ended": {relation: 5, writers: nil, want: true},
	} {
		t.Run(name, func(t *testing.T) {
			writers := make([]time.Time, 0, len(tc.writers))
			for _, hour := range tc.writers {
				writers = append(writers, blockedTime(hour))
			}
			if got := RelationIsCurrent(blockedTime(tc.relation), writers...); got != tc.want {
				t.Fatalf("RelationIsCurrent = %t, want %t", got, tc.want)
			}
		})
	}
}

// A blocked span starts at the relation's start -- never earlier -- and the
// creation of either item can only move it later. It ends at the earlier of
// the relation's end and the blocker's completion.
func TestBlockedIntervalForStartsAtTheRelationAndNeverBeforeIt(t *testing.T) {
	open := Blocker{Status: "in_progress", CreatedAt: blockedTime(2)}
	done := func(completed int) Blocker {
		return Blocker{Status: "done", CreatedAt: blockedTime(2), CompletedAt: blockedTimePtr(completed)}
	}
	for name, tc := range map[string]struct {
		itemCreated   int
		relationStart int
		relationEnd   *time.Time
		blocker       Blocker
		want          BlockedInterval
		ok            bool
	}{
		"the relation started after both items existed: from the relation": {
			10, 14, nil, open, BlockedInterval{Start: blockedTime(14)}, true,
		},
		"the relation start is before the item's creation: from the item's creation": {
			10, 5, nil, open, BlockedInterval{Start: blockedTime(10)}, true,
		},
		"the relation start is before the blocker's creation: from the blocker's creation": {
			10, 12, nil, Blocker{Status: "todo", CreatedAt: blockedTime(16)}, BlockedInterval{Start: blockedTime(16)}, true,
		},
		"a completed blocker: until its completion": {
			10, 14, nil, done(20), BlockedInterval{Start: blockedTime(14), End: blockedTimePtr(20)}, true,
		},
		"the relation was last seen at 18 and the blocker is open: until 18": {
			10, 14, blockedTimePtr(18), open, BlockedInterval{Start: blockedTime(14), End: blockedTimePtr(18)}, true,
		},
		"the relation ended before the blocker was completed: until the relation's end": {
			10, 14, blockedTimePtr(18), done(20), BlockedInterval{Start: blockedTime(14), End: blockedTimePtr(18)}, true,
		},
		"the blocker was completed before the relation ended: until the completion": {
			10, 14, blockedTimePtr(25), done(20), BlockedInterval{Start: blockedTime(14), End: blockedTimePtr(20)}, true,
		},
		"the blocker was completed before the relation started: no span": {
			10, 14, nil, done(12), BlockedInterval{}, false,
		},
		"the blocker was completed at the instant the relation started: no span": {
			10, 14, nil, done(14), BlockedInterval{}, false,
		},
		"the relation ended at the instant it started: no span": {
			10, 14, blockedTimePtr(14), open, BlockedInterval{}, false,
		},
		"a done blocker with no completion time: its end is not known": {
			10, 14, nil, Blocker{Status: "done", CreatedAt: blockedTime(2)}, BlockedInterval{}, false,
		},
		"a canceled blocker with no completion time: its end is not known": {
			10, 14, blockedTimePtr(18), Blocker{Status: "canceled", CreatedAt: blockedTime(2)}, BlockedInterval{}, false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := BlockedIntervalFor(blockedTime(tc.itemCreated), blockedTime(tc.relationStart), tc.relationEnd, tc.blocker)
			if ok != tc.ok || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("BlockedIntervalFor = %+v, %t; want %+v, %t", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// The start of a relation: the provider's own time when there is one, else
// the first time a sync wrote it, else not known.
func TestBlockingRelationStartPrefersTheProviderTimeAndNeedsOneOfTheTwo(t *testing.T) {
	for name, tc := range map[string]struct {
		relation BlockingRelation
		want     time.Time
		known    bool
	}{
		"the provider's time":            {BlockingRelation{StartedAt: blockedTimePtr(6)}, blockedTime(6), true},
		"first seen":                     {BlockingRelation{FirstSeenAt: blockedTimePtr(9)}, blockedTime(9), true},
		"both: the provider's time wins": {BlockingRelation{StartedAt: blockedTimePtr(6), FirstSeenAt: blockedTimePtr(9)}, blockedTime(6), true},
		"both, the provider's time later than first seen: still the provider's": {
			BlockingRelation{StartedAt: blockedTimePtr(11), FirstSeenAt: blockedTimePtr(9)}, blockedTime(11), true,
		},
		"neither: not known": {BlockingRelation{LastSynced: blockedTime(50)}, time.Time{}, false},
	} {
		t.Run(name, func(t *testing.T) {
			got, known := tc.relation.Start()
			if known != tc.known || !got.Equal(tc.want) {
				t.Fatalf("Start = %v, %t; want %v, %t", got, known, tc.want, tc.known)
			}
		})
	}
}

func seg(status string, start, end int) StatusSegment {
	return StatusSegment{Status: status, Start: blockedTime(start), End: blockedTime(end)}
}

func TestOverlayBlockedReplacesTheStatusOnlyWhereABlockerIsOpen(t *testing.T) {
	history := []StatusSegment{seg("todo", 0, 10), seg("in_progress", 10, 30), seg("done", 30, 40)}
	for name, tc := range map[string]struct {
		segments  []StatusSegment
		intervals []BlockedInterval
		want      []StatusSegment
	}{
		"no interval: the history is unchanged": {history, nil, history},
		"an interval inside one segment splits it in three": {
			history, []BlockedInterval{{Start: blockedTime(12), End: blockedTimePtr(20)}},
			[]StatusSegment{seg("todo", 0, 10), seg("in_progress", 10, 12), seg("blocked", 12, 20), seg("in_progress", 20, 30), seg("done", 30, 40)},
		},
		"an interval over a status change covers both sides of it": {
			history, []BlockedInterval{{Start: blockedTime(5), End: blockedTimePtr(15)}},
			[]StatusSegment{seg("todo", 0, 5), seg("blocked", 5, 10), seg("blocked", 10, 15), seg("in_progress", 15, 30), seg("done", 30, 40)},
		},
		"an open interval stops at the terminal segment": {
			history, []BlockedInterval{{Start: blockedTime(25)}},
			[]StatusSegment{seg("todo", 0, 10), seg("in_progress", 10, 25), seg("blocked", 25, 30), seg("done", 30, 40)},
		},
		"a terminal segment is never blocked": {
			[]StatusSegment{seg("done", 0, 10), seg("canceled", 10, 20)}, []BlockedInterval{{Start: blockedTime(0)}},
			[]StatusSegment{seg("done", 0, 10), seg("canceled", 10, 20)},
		},
		"a segment that is already blocked stays one segment": {
			[]StatusSegment{seg("blocked", 0, 10)}, []BlockedInterval{{Start: blockedTime(2), End: blockedTimePtr(4)}},
			[]StatusSegment{seg("blocked", 0, 10)},
		},
		"overlapping intervals count once": {
			[]StatusSegment{seg("in_progress", 0, 30)},
			[]BlockedInterval{{Start: blockedTime(5), End: blockedTimePtr(12)}, {Start: blockedTime(10), End: blockedTimePtr(20)}},
			[]StatusSegment{seg("in_progress", 0, 5), seg("blocked", 5, 20), seg("in_progress", 20, 30)},
		},
		"two separate intervals leave the gap as it was": {
			[]StatusSegment{seg("in_progress", 0, 30)},
			[]BlockedInterval{{Start: blockedTime(20), End: blockedTimePtr(25)}, {Start: blockedTime(5), End: blockedTimePtr(10)}},
			[]StatusSegment{seg("in_progress", 0, 5), seg("blocked", 5, 10), seg("in_progress", 10, 20), seg("blocked", 20, 25), seg("in_progress", 25, 30)},
		},
		"an open interval absorbs a later closed one": {
			[]StatusSegment{seg("in_progress", 0, 30)},
			[]BlockedInterval{{Start: blockedTime(5)}, {Start: blockedTime(10), End: blockedTimePtr(12)}},
			[]StatusSegment{seg("in_progress", 0, 5), seg("blocked", 5, 30)},
		},
		"a closed interval joined by an open one stays open": {
			[]StatusSegment{seg("in_progress", 0, 30)},
			[]BlockedInterval{{Start: blockedTime(5), End: blockedTimePtr(12)}, {Start: blockedTime(8)}},
			[]StatusSegment{seg("in_progress", 0, 5), seg("blocked", 5, 30)},
		},
		"an interval before the item or after it changes nothing": {
			[]StatusSegment{seg("in_progress", 10, 20)},
			[]BlockedInterval{{Start: blockedTime(0), End: blockedTimePtr(10)}, {Start: blockedTime(20), End: blockedTimePtr(25)}},
			[]StatusSegment{seg("in_progress", 10, 20)},
		},
		"an interval over the whole segment replaces it": {
			[]StatusSegment{seg("in_review", 10, 20)}, []BlockedInterval{{Start: blockedTime(0), End: blockedTimePtr(30)}},
			[]StatusSegment{seg("blocked", 10, 20)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := OverlayBlocked(tc.segments, tc.intervals)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("OverlayBlocked =\n  %+v\nwant\n  %+v", got, tc.want)
			}
			// The hours an item contributes do not change: they only move
			// between statuses.
			var before, after time.Duration
			for _, segment := range tc.segments {
				before += segment.End.Sub(segment.Start)
			}
			for _, segment := range got {
				after += segment.End.Sub(segment.Start)
				if !segment.End.After(segment.Start) {
					t.Fatalf("empty segment in the result: %+v", segment)
				}
			}
			if before != after {
				t.Fatalf("total time changed from %s to %s", before, after)
			}
		})
	}
}

// The overlay must not edit the caller's intervals: both writers build them
// once per item and may read them again.
func TestOverlayBlockedLeavesItsInputsAlone(t *testing.T) {
	segments := []StatusSegment{seg("in_progress", 0, 30)}
	intervals := []BlockedInterval{{Start: blockedTime(20), End: blockedTimePtr(25)}, {Start: blockedTime(5), End: blockedTimePtr(22)}}
	wantSegments := []StatusSegment{seg("in_progress", 0, 30)}
	wantIntervals := []BlockedInterval{{Start: blockedTime(20), End: blockedTimePtr(25)}, {Start: blockedTime(5), End: blockedTimePtr(22)}}
	OverlayBlocked(segments, intervals)
	if !reflect.DeepEqual(segments, wantSegments) || !reflect.DeepEqual(intervals, wantIntervals) {
		t.Fatalf("inputs changed: %+v %+v", segments, intervals)
	}
}

func relationEnd(id, provider, status string, created int, completed *time.Time, synced int) RelationEnd {
	return RelationEnd{WorkItemID: id, Provider: provider, Status: status, CreatedAt: blockedTime(created), CompletedAt: completed, LastSynced: blockedTime(synced)}
}

// blocksRelation is a canonical `blocks` row first seen at firstSeen and last
// written at synced.
func blocksRelation(source, target string, firstSeen, synced int) BlockingRelation {
	return BlockingRelation{
		SourceID: source, TargetID: target, RelationshipType: "blocks", SemanticsVersion: CanonicalBlocksSemantics,
		LastSynced: blockedTime(synced), FirstSeenAt: blockedTimePtr(firstSeen),
	}
}

// The whole decision, one fact at a time: each case removes or changes ONE
// thing the rule needs. The blocked item was created at 10, the blocker at 4,
// the relation was first seen at 14, and every row was synced at 50.
func TestBlockedIntervalsByItemNeedsEveryStoredFact(t *testing.T) {
	blocked := relationEnd("jira:OPS-2", "jira", "in_progress", 10, nil, 50)
	blocker := relationEnd("jira:OPS-1", "jira", "in_progress", 4, nil, 50)
	relation := blocksRelation("jira:OPS-1", "jira:OPS-2", 14, 50)
	want := map[string][]BlockedInterval{"jira:OPS-2": {{Start: blockedTime(14)}}}
	with := func(mutate func(*BlockingRelation)) []BlockingRelation {
		changed := relation
		mutate(&changed)
		return []BlockingRelation{changed}
	}

	for name, tc := range map[string]struct {
		relations []BlockingRelation
		ends      []RelationEnd
		want      map[string][]BlockedInterval
	}{
		"an open blocker: blocked from the time the relation was first seen, still open": {
			[]BlockingRelation{relation}, []RelationEnd{blocked, blocker}, want,
		},
		"the provider gave the link time: blocked from that time, not from first seen": {
			with(func(r *BlockingRelation) { r.StartedAt = blockedTimePtr(12) }), []RelationEnd{blocked, blocker},
			map[string][]BlockedInterval{"jira:OPS-2": {{Start: blockedTime(12)}}},
		},
		"the provider's link time is before the item existed: from the item's creation": {
			with(func(r *BlockingRelation) { r.StartedAt = blockedTimePtr(6) }), []RelationEnd{blocked, blocker},
			map[string][]BlockedInterval{"jira:OPS-2": {{Start: blockedTime(10)}}},
		},
		"no stored start of the relation: nothing is derived": {
			with(func(r *BlockingRelation) { r.FirstSeenAt = nil }), []RelationEnd{blocked, blocker}, nil,
		},
		"a completed blocker: blocked until its completion": {
			[]BlockingRelation{relation},
			[]RelationEnd{blocked, relationEnd("jira:OPS-1", "jira", "done", 4, blockedTimePtr(30), 50)},
			map[string][]BlockedInterval{"jira:OPS-2": {{Start: blockedTime(14), End: blockedTimePtr(30)}}},
		},
		"the relation points the other way: the OTHER item is blocked": {
			[]BlockingRelation{blocksRelation("jira:OPS-2", "jira:OPS-1", 14, 50)}, []RelationEnd{blocked, blocker},
			map[string][]BlockedInterval{"jira:OPS-1": {{Start: blockedTime(14)}}},
		},
		"the blocker is not a stored work item": {
			[]BlockingRelation{relation}, []RelationEnd{blocked}, nil,
		},
		"the blocked item is not a stored work item": {
			[]BlockingRelation{relation}, []RelationEnd{blocker}, nil,
		},
		"the relation does not block": {
			with(func(r *BlockingRelation) { r.RelationshipType = "relates_to" }), []RelationEnd{blocked, blocker}, nil,
		},
		"the relation has the legacy direction": {
			with(func(r *BlockingRelation) { r.SemanticsVersion = "legacy.v1" }), []RelationEnd{blocked, blocker}, nil,
		},
		// The link was removed at the provider: both items were synced at 50
		// and the relation was last written at 40. The hours up to the last
		// time it was seen did happen; nothing after that is counted.
		"the provider no longer reports the relation: blocked until it was last seen": {
			with(func(r *BlockingRelation) { r.LastSynced = blockedTime(40) }), []RelationEnd{blocked, blocker},
			map[string][]BlockedInterval{"jira:OPS-2": {{Start: blockedTime(14), End: blockedTimePtr(40)}}},
		},
		"the blocker is done with no completion time": {
			[]BlockingRelation{relation},
			[]RelationEnd{blocked, relationEnd("jira:OPS-1", "jira", "done", 4, nil, 50)}, nil,
		},
		"the blocker was completed before the relation was first seen": {
			[]BlockingRelation{relation},
			[]RelationEnd{blocked, relationEnd("jira:OPS-1", "jira", "done", 4, blockedTimePtr(12), 50)}, nil,
		},
		"a relation of another item does not block this one": {
			[]BlockingRelation{blocksRelation("jira:OPS-1", "jira:OPS-3", 14, 50)},
			[]RelationEnd{blocked, blocker, relationEnd("jira:OPS-3", "jira", "todo", 20, nil, 50)},
			map[string][]BlockedInterval{"jira:OPS-3": {{Start: blockedTime(20)}}},
		},
		"two blockers give two spans": {
			[]BlockingRelation{relation, blocksRelation("jira:OPS-4", "jira:OPS-2", 16, 50)},
			[]RelationEnd{blocked, blocker, relationEnd("jira:OPS-4", "jira", "done", 12, blockedTimePtr(20), 50)},
			map[string][]BlockedInterval{"jira:OPS-2": {{Start: blockedTime(14)}, {Start: blockedTime(16), End: blockedTimePtr(20)}}},
		},
		"the newest stored row of an item is the item": {
			[]BlockingRelation{relation},
			[]RelationEnd{blocked, relationEnd("jira:OPS-1", "jira", "in_progress", 4, nil, 40), relationEnd("jira:OPS-1", "jira", "done", 4, blockedTimePtr(30), 50)},
			map[string][]BlockedInterval{"jira:OPS-2": {{Start: blockedTime(14), End: blockedTimePtr(30)}}},
		},
		"no relation":    {nil, []RelationEnd{blocked, blocker}, nil},
		"no stored item": {[]BlockingRelation{relation}, nil, nil},
	} {
		t.Run(name, func(t *testing.T) {
			got := BlockedIntervalsByItem(tc.relations, tc.ends)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("BlockedIntervalsByItem =\n  %+v\nwant\n  %+v", got, tc.want)
			}
		})
	}
}

// A relation read from text names its blocker by an issue KEY. It blocks only
// when exactly one stored jira or linear item carries that key. It is written
// only by the item that holds the text, so that item's later sync without it
// ends it -- and the OTHER item's later sync says nothing.
func TestBlockedIntervalsByItemResolvesAnExternalKeyOrDerivesNothing(t *testing.T) {
	item := relationEnd("gh:acme/api#7", "github", "in_progress", 10, nil, 50)
	blockedBy := func(target string, synced int) BlockingRelation {
		return BlockingRelation{
			SourceID: "gh:acme/api#7", TargetID: target, RelationshipType: "blocked_by", SemanticsVersion: CanonicalBlocksSemantics,
			LastSynced: blockedTime(synced), FirstSeenAt: blockedTimePtr(14),
		}
	}
	linear := relationEnd("linear:OPS-9", "linear", "todo", 2, nil, 99)
	want := map[string][]BlockedInterval{"gh:acme/api#7": {{Start: blockedTime(14)}}}

	for name, tc := range map[string]struct {
		relation BlockingRelation
		ends     []RelationEnd
		want     map[string][]BlockedInterval
	}{
		"the key names one stored linear item":                {blockedBy("extkey:OPS-9", 50), []RelationEnd{item, linear}, want},
		"the key is matched without case and outer spaces":    {blockedBy("extkey: ops-9 ", 50), []RelationEnd{item, linear}, want},
		"the key names one stored jira item":                  {blockedBy("extkey:OPS-9", 50), []RelationEnd{item, relationEnd("jira:OPS-9", "jira", "todo", 2, nil, 99)}, want},
		"no stored item carries the key":                      {blockedBy("extkey:OPS-8", 50), []RelationEnd{item, linear}, nil},
		"two stored items carry the key":                      {blockedBy("extkey:OPS-9", 50), []RelationEnd{item, linear, relationEnd("jira:OPS-9", "jira", "todo", 2, nil, 99)}, nil},
		"a github item with that suffix does not carry a key": {blockedBy("extkey:OPS-9", 50), []RelationEnd{item, relationEnd("gh:OPS-9", "github", "todo", 2, nil, 99)}, nil},
		"an empty key": {blockedBy("extkey:", 50), []RelationEnd{item, linear}, nil},
		// The blocker was synced long after the relation (99 > 50). A text
		// relation is written only by the item that holds the text, so the
		// blocker's later sync does not end it.
		"the blocker's later sync does not end a text relation": {blockedBy("extkey:OPS-9", 50), []RelationEnd{item, linear}, want},
		// The item was synced at 50 and the relation was last written at 40:
		// the text is gone. Blocked until the last time it was seen.
		"the item was synced again and the text is gone": {
			blockedBy("extkey:OPS-9", 40), []RelationEnd{item, linear},
			map[string][]BlockedInterval{"gh:acme/api#7": {{Start: blockedTime(14), End: blockedTimePtr(40)}}},
		},
		"the item that holds the text is not stored": {blockedBy("extkey:OPS-9", 50), []RelationEnd{linear}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			got := BlockedIntervalsByItem([]BlockingRelation{tc.relation}, tc.ends)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("BlockedIntervalsByItem =\n  %+v\nwant\n  %+v", got, tc.want)
			}
		})
	}

	// The other text form: THIS item's text says it blocks an external key.
	// The emitter is still this item (the source), here the BLOCKER.
	blocks := BlockingRelation{
		SourceID: "gh:acme/api#7", TargetID: "extkey:OPS-9", RelationshipType: "blocks", SemanticsVersion: CanonicalBlocksSemantics,
		LastSynced: blockedTime(50), FirstSeenAt: blockedTimePtr(14),
	}
	if got, wantLinear := BlockedIntervalsByItem([]BlockingRelation{blocks}, []RelationEnd{item, linear}), (map[string][]BlockedInterval{"linear:OPS-9": {{Start: blockedTime(14)}}}); !reflect.DeepEqual(got, wantLinear) {
		t.Fatalf("blocks an external key: %+v, want %+v", got, wantLinear)
	}
	// ... and when this item is synced again without the text, it ends.
	blocks.LastSynced = blockedTime(40)
	if got, wantEnded := BlockedIntervalsByItem([]BlockingRelation{blocks}, []RelationEnd{item, linear}), (map[string][]BlockedInterval{"linear:OPS-9": {{Start: blockedTime(14), End: blockedTimePtr(40)}}}); !reflect.DeepEqual(got, wantEnded) {
		t.Fatalf("blocks an external key, text gone: %+v, want %+v", got, wantEnded)
	}
}

// THE END RULE, pinned as one statement: a relation that the latest sync of
// its item(s) did not write again ends at its OWN last_synced -- the last
// time a sync saw it -- and the hours before that stay blocked. Nothing after
// it is blocked, and nothing before it is un-blocked.
func TestARelationNotWrittenAgainEndsAtItsOwnLastSyncAndKeepsEarlierHours(t *testing.T) {
	blocked := relationEnd("jira:OPS-2", "jira", "in_progress", 10, nil, 60)
	blocker := relationEnd("jira:OPS-1", "jira", "in_progress", 4, nil, 70)
	history := []StatusSegment{seg("in_progress", 10, 80)}

	// Last written at 40; both items were synced after that (60 and 70).
	removed := blocksRelation("jira:OPS-1", "jira:OPS-2", 14, 40)
	intervals := BlockedIntervalsByItem([]BlockingRelation{removed}, []RelationEnd{blocked, blocker})
	wantIntervals := map[string][]BlockedInterval{"jira:OPS-2": {{Start: blockedTime(14), End: blockedTimePtr(40)}}}
	if !reflect.DeepEqual(intervals, wantIntervals) {
		t.Fatalf("removed relation: intervals = %+v, want %+v (the end is the relation's own last_synced, not a later sync of an item)", intervals, wantIntervals)
	}
	got := OverlayBlocked(history, intervals["jira:OPS-2"])
	want := []StatusSegment{seg("in_progress", 10, 14), seg("blocked", 14, 40), seg("in_progress", 40, 80)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("removed relation: history = %+v, want %+v", got, want)
	}

	// The same relation while it is still reported (written at 70, the
	// latest sync of its items): the hours up to 40 are the SAME, and it
	// goes on.
	current := blocksRelation("jira:OPS-1", "jira:OPS-2", 14, 70)
	still := OverlayBlocked(history, BlockedIntervalsByItem([]BlockingRelation{current}, []RelationEnd{blocked, blocker})["jira:OPS-2"])
	if wantStill := []StatusSegment{seg("in_progress", 10, 14), seg("blocked", 14, 80)}; !reflect.DeepEqual(still, wantStill) {
		t.Fatalf("current relation: history = %+v, want %+v", still, wantStill)
	}
}

// A relation ends when an item that WRITES it has a later sync that did not
// write it again, and only then. Each case is one relation last written at
// 40 and first seen at 14, with one of its two items synced again at 60 and
// the other not synced since 30.
//
// The first case is a github text relation whose target is a real work item
// id ("Blocked by #12" in #7): the text is removed, #7 is synced again, and
// #12 is not updated. It must end, though #12 was never synced again.
func TestARelationEndsOnlyWhenAnItemThatWritesItIsSyncedWithoutIt(t *testing.T) {
	const firstSeen, lastWritten, before, after = 14, 40, 30, 60
	ended := BlockedInterval{Start: blockedTime(firstSeen), End: blockedTimePtr(lastWritten)}
	open := BlockedInterval{Start: blockedTime(firstSeen)}
	for name, tc := range map[string]struct {
		source, target, relationship, raw string
		// provider of the item an external key resolves to, "" when the
		// target is a work item id.
		keyedProvider              string
		sourceSynced, targetSynced int
		want                       BlockedInterval
	}{
		"github text in the blocked item; the blocked item is synced without it":    {"gh:a/r#12", "gh:a/r#7", "blocks", "blocked by #12", "", before, after, ended},
		"github text in the blocked item; only the blocker is synced again":         {"gh:a/r#12", "gh:a/r#7", "blocks", "blocked by #12", "", after, lastWritten, open},
		"github text in the blocker; the blocker is synced without it":              {"gh:a/r#12", "gh:a/r#7", "blocks", "blocks #7", "", after, before, ended},
		"github text in the blocker; only the blocked item is synced again":         {"gh:a/r#12", "gh:a/r#7", "blocks", "blocks #7", "", lastWritten, after, open},
		"gitlab keyword in the blocked item; the blocked item is synced without it": {"gitlab:a/r#5", "gitlab:a/r#7", "blocks", "blocked by", "", before, after, ended},
		"gitlab keyword in the blocked item; only the blocker is synced again":      {"gitlab:a/r#5", "gitlab:a/r#7", "blocks", "blocked by", "", after, lastWritten, open},
		"gitlab keyword in the blocker; the blocker is synced without it":           {"gitlab:a/r#5", "gitlab:a/r#7", "blocks", "blocking", "", after, before, ended},
		"gitlab keyword in the blocker; only the blocked item is synced again":      {"gitlab:a/r#5", "gitlab:a/r#7", "blocks", "blocking", "", lastWritten, after, open},
		"a text relation to a key; the item with the text is synced without it":     {"gh:a/r#7", "extkey:OPS-9", "blocked_by", "external_issue_key", "linear", after, before, ended},
		"a text relation to a key; only the item the key names is synced again":     {"gh:a/r#7", "extkey:OPS-9", "blocked_by", "external_issue_key", "linear", lastWritten, after, open},
		"a native jira link; the blocker is synced without it":                      {"jira:OPS-1", "jira:OPS-2", "blocks", "blocks", "", after, before, ended},
		"a native jira link; the blocked item is synced without it":                 {"jira:OPS-1", "jira:OPS-2", "blocks", "is blocked by", "", before, after, ended},
		"a native linear relation; the blocker is synced without it":                {"linear:OPS-1", "linear:OPS-2", "blocks", "linear_relation:blocks", "", after, before, ended},
		"a native linear relation; the blocked item is synced without it":           {"linear:OPS-1", "linear:OPS-2", "blocks", "linear_relation:blocks", "", before, after, ended},
		"a native gitlab link; the blocker is synced without it":                    {"gitlab:a/r#5", "gitlab:a/r#7", "blocks", "is_blocked_by", "", after, before, ended},
		"a native gitlab link; the blocked item is synced without it":               {"gitlab:a/r#5", "gitlab:a/r#7", "blocks", "is_blocked_by", "", before, after, ended},
		"gitlab `blocks`, writer not derivable; the blocker is synced without it":   {"gitlab:a/r#5", "gitlab:a/r#7", "blocks", "blocks", "", after, before, ended},
		"gitlab `blocks`, writer not derivable; the blocked item is synced again":   {"gitlab:a/r#5", "gitlab:a/r#7", "blocks", "blocks", "", before, after, ended},
		"a native link that both items wrote at the relation's sync":                {"jira:OPS-1", "jira:OPS-2", "blocks", "blocks", "", lastWritten, lastWritten, open},
	} {
		t.Run(name, func(t *testing.T) {
			relation := BlockingRelation{
				SourceID: tc.source, TargetID: tc.target, RelationshipType: tc.relationship, Raw: tc.raw,
				SemanticsVersion: CanonicalBlocksSemantics, LastSynced: blockedTime(lastWritten), FirstSeenAt: blockedTimePtr(firstSeen),
			}
			targetID, targetProvider := tc.target, "github"
			if key, external := ExternalKey(tc.target); external {
				targetID, targetProvider = tc.keyedProvider+":"+key, tc.keyedProvider
			}
			ends := []RelationEnd{
				relationEnd(tc.source, "github", "in_progress", 4, nil, tc.sourceSynced),
				relationEnd(targetID, targetProvider, "in_progress", 4, nil, tc.targetSynced),
			}
			named, blocking := BlockingEndsOf(tc.source, tc.target, tc.relationship, tc.raw, CanonicalBlocksSemantics)
			if !blocking {
				t.Fatal("the case is not a blocking relation")
			}
			blockedID := named.BlockedID
			if _, external := ExternalKey(blockedID); external {
				blockedID = targetID
			}
			got := BlockedIntervalsByItem([]BlockingRelation{relation}, ends)
			if want := (map[string][]BlockedInterval{blockedID: {tc.want}}); !reflect.DeepEqual(got, want) {
				t.Fatalf("intervals = %+v, want %+v", got, want)
			}
		})
	}
}

// The bare issue key is the id of a jira or linear item after its provider
// prefix, trimmed and upper-cased; no other item has one. The external-key
// target is that key behind the prefix a text relation stores.
func TestBareIssueKeyIsTheKeyOfAJiraOrLinearItemOnly(t *testing.T) {
	for _, tc := range []struct {
		provider, id string
		key          string
	}{
		{"linear", "linear:OPS-9", "OPS-9"},
		{"linear", "linear: ops-9 ", "OPS-9"},
		{"jira", "jira:OPS-9", "OPS-9"},
		{"github", "gh:acme/api#7", ""},
		{"gitlab", "gitlab:acme/api#7", ""},
		{"", "linear:OPS-9", ""},
		{"linear", "OPS-9", ""},
		{"jira", "jira:", ""},
		{"jira", "jira:   ", ""},
	} {
		key, ok := BareIssueKey(tc.provider, tc.id)
		if key != tc.key || ok != (tc.key != "") {
			t.Fatalf("BareIssueKey(%q, %q) = %q, %t; want %q, %t", tc.provider, tc.id, key, ok, tc.key, tc.key != "")
		}
		target, ok := ExternalKeyTarget(tc.provider, tc.id)
		want := ""
		if tc.key != "" {
			want = ExternalKeyPrefix + tc.key
		}
		if target != want || ok != (want != "") {
			t.Fatalf("ExternalKeyTarget(%q, %q) = %q, %t; want %q", tc.provider, tc.id, target, ok, want)
		}
		// The target is one ExternalKey reads back to the same key.
		if back, external := ExternalKey(target); want != "" && (!external || back != tc.key) {
			t.Fatalf("ExternalKey(%q) = %q, %t; want %q", target, back, external, tc.key)
		}
	}
}

// The counts of what the end rule did: one per relation it ended, under the
// provider of the item whose later sync did not write the relation again.
// A github relation whose writer's latest stored row is a Projects v2 board
// row is also counted as a board candidate.
func TestEndedRelationStatsCountByTheProviderOfTheWriterThatEndedIt(t *testing.T) {
	end := func(id, provider, projectID string, synced int) RelationEnd {
		return RelationEnd{WorkItemID: id, Provider: provider, ProjectID: projectID, Status: "in_progress", CreatedAt: blockedTime(4), LastSynced: blockedTime(synced)}
	}
	relation := func(source, target, raw string, synced int) BlockingRelation {
		return BlockingRelation{
			SourceID: source, TargetID: target, RelationshipType: "blocks", Raw: raw,
			SemanticsVersion: CanonicalBlocksSemantics, LastSynced: blockedTime(synced), FirstSeenAt: blockedTimePtr(14),
		}
	}
	relations := []BlockingRelation{
		// github text in #7, last written at 40. #7's latest row is the board
		// pass's (60): ended, a board candidate.
		relation("gh:a/r#12", "gh:a/r#7", "blocked by #12", 40),
		// github text in #8, last written at 40; #8 was synced again at 60 by
		// the pass that reads the text: ended, not a board candidate.
		relation("gh:a/r#12", "gh:a/r#8", "blocked by #12", 40),
		// github text in #9, written by #9's latest sync: current.
		relation("gh:a/r#12", "gh:a/r#9", "blocked by #12", 60),
		// a native jira link; the blocker was synced again without it: ended.
		relation("jira:OPS-1", "jira:OPS-2", "blocks", 40),
		// a native linear relation, both items at its sync: current.
		relation("linear:OPS-1", "linear:OPS-2", "linear_relation:blocks", 40),
		// gitlab `blocks`, the blocked issue synced later: ended.
		relation("gitlab:a/r#5", "gitlab:a/r#7", "blocks", 40),
		// an item of a provider the stats do not name: ended, under "other".
		relation("asana:1", "asana:2", "blocks", 40),
		// not a relation the end rule sees: no stored blocker.
		relation("gh:a/r#99", "gh:a/r#9", "blocked by #99", 40),
		// not a relation the end rule sees: the legacy direction.
		{SourceID: "jira:OPS-1", TargetID: "jira:OPS-2", RelationshipType: "blocks", SemanticsVersion: "legacy.v1", LastSynced: blockedTime(40), FirstSeenAt: blockedTimePtr(14)},
	}
	ends := []RelationEnd{
		end("gh:a/r#12", "github", "a/r", 30),
		// #7 has two stored rows: the row of the pass that read its text (40)
		// and the later board row (60). The later row is the stored item.
		end("gh:a/r#7", "github", "a/r", 40),
		end("gh:a/r#7", "github", GitHubBoardProjectPrefix+"acme#3", 60),
		end("gh:a/r#8", "github", "a/r", 60),
		end("gh:a/r#9", "github", "a/r", 60),
		end("jira:OPS-1", "jira", "", 60), end("jira:OPS-2", "jira", "", 30),
		end("linear:OPS-1", "linear", "", 40), end("linear:OPS-2", "linear", "", 40),
		// The gitlab writer's project id has the board prefix: only a GITHUB
		// row is a board candidate.
		end("gitlab:a/r#5", "gitlab", "a/r", 30), end("gitlab:a/r#7", "gitlab", GitHubBoardProjectPrefix+"x", 60),
		end("asana:1", "asana", "", 60), end("asana:2", "asana", "", 60),
	}
	intervals, stats := BlockedIntervalsWithStats(relations, ends)
	want := EndedRelationStats{
		Relations:             7,
		Ended:                 map[string]int{"github": 2, "jira": 1, "gitlab": 1, "other": 1},
		GitHubBoardCandidates: 1,
	}
	if !reflect.DeepEqual(stats, want) {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}
	// The stats change no interval.
	if plain := BlockedIntervalsByItem(relations, ends); !reflect.DeepEqual(plain, intervals) {
		t.Fatalf("BlockedIntervalsByItem = %+v, BlockedIntervalsWithStats = %+v", plain, intervals)
	}
	// The log line: the same keys for every run, counts only.
	logged := func(stats EndedRelationStats, writer string) string {
		var out bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{
			ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
				if attr.Key == slog.TimeKey {
					return slog.Attr{}
				}
				return attr
			},
		})))
		defer slog.SetDefault(previous)
		slog.Info(EndedRelationsLogMessage, stats.LogArgs(writer)...)
		return out.String()
	}
	const message = `level=INFO msg="blocked rule: relations ended by a later sync of an item that writes them" `
	if got, wantLine := logged(stats, "daily_family"), message+
		"writer=daily_family relations=7 ended=5 ended_github=2 ended_gitlab=1 ended_jira=1 ended_linear=0 ended_other=1 github_board_candidates=1\n"; got != wantLine {
		t.Fatalf("log line = %q, want %q", got, wantLine)
	}
	if got, wantLine := logged(EndedRelationStats{}, "sync_time_deriver"), message+
		"writer=sync_time_deriver relations=0 ended=0 ended_github=0 ended_gitlab=0 ended_jira=0 ended_linear=0 ended_other=0 github_board_candidates=0\n"; got != wantLine {
		t.Fatalf("log line of a run with no relation = %q, want %q", got, wantLine)
	}
	// No relation, or no stored item: nothing counted.
	if _, none := BlockedIntervalsWithStats(nil, ends); !reflect.DeepEqual(none, EndedRelationStats{}) {
		t.Fatalf("no relation: stats = %+v", none)
	}
	if _, none := BlockedIntervalsWithStats(relations, nil); !reflect.DeepEqual(none, EndedRelationStats{}) {
		t.Fatalf("no stored item: stats = %+v", none)
	}
}

// RelationEndedBy names the writer whose later sync did not write the
// relation: the first one in the order given.
func TestRelationEndedByNamesTheFirstWriterWithALaterSync(t *testing.T) {
	for name, tc := range map[string]struct {
		relation int
		writers  []int
		want     int
	}{
		"no writer":                       {5, nil, -1},
		"one writer at the relation":      {5, []int{5}, -1},
		"one writer later":                {5, []int{6}, 0},
		"the second writer is later":      {5, []int{5, 7}, 1},
		"both are later: the first":       {5, []int{6, 7}, 0},
		"both wrote it at or before that": {7, []int{5, 7}, -1},
	} {
		t.Run(name, func(t *testing.T) {
			writers := make([]time.Time, 0, len(tc.writers))
			for _, hour := range tc.writers {
				writers = append(writers, blockedTime(hour))
			}
			if got := RelationEndedBy(blockedTime(tc.relation), writers...); got != tc.want {
				t.Fatalf("RelationEndedBy = %d, want %d", got, tc.want)
			}
		})
	}
}
