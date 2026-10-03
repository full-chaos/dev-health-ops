package workitemmetrics

import (
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
// types, two different non-empty ends.
func TestBlockingEndsOfReadsOnlyCanonicalBlockingRows(t *testing.T) {
	for name, tc := range map[string]struct {
		source, target, relationship, version string
		want                                  BlockingEnds
		ok                                    bool
	}{
		"blocks: the source is the blocker": {
			"jira:OPS-1", "jira:OPS-2", "blocks", CanonicalBlocksSemantics,
			BlockingEnds{BlockerID: "jira:OPS-1", BlockedID: "jira:OPS-2"}, true,
		},
		"blocked_by: the source is the blocked item, and its only emitter": {
			"gh:acme/api#7", "extkey:OPS-2", "blocked_by", CanonicalBlocksSemantics,
			BlockingEnds{BlockedID: "gh:acme/api#7", BlockerID: "extkey:OPS-2", EmitterID: "gh:acme/api#7"}, true,
		},
		"blocks to an external key: the source wrote it": {
			"gitlab:acme/api#7", "extkey:OPS-2", "blocks", CanonicalBlocksSemantics,
			BlockingEnds{BlockerID: "gitlab:acme/api#7", BlockedID: "extkey:OPS-2", EmitterID: "gitlab:acme/api#7"}, true,
		},
		"blocked_by between two work items: no single emitter is claimed": {
			"linear:A-1", "linear:A-2", "blocked_by", CanonicalBlocksSemantics,
			BlockingEnds{BlockedID: "linear:A-1", BlockerID: "linear:A-2"}, true,
		},
		"a relation that does not block":          {"jira:OPS-1", "jira:OPS-2", "relates_to", CanonicalBlocksSemantics, BlockingEnds{}, false},
		"a duplicate relation":                    {"linear:A-1", "linear:A-2", "duplicates", CanonicalBlocksSemantics, BlockingEnds{}, false},
		"the legacy direction is not dependable":  {"jira:OPS-1", "jira:OPS-2", "blocks", "legacy.v1", BlockingEnds{}, false},
		"no semantics version":                    {"jira:OPS-1", "jira:OPS-2", "blocks", "", BlockingEnds{}, false},
		"no source":                               {"", "jira:OPS-2", "blocks", CanonicalBlocksSemantics, BlockingEnds{}, false},
		"no target":                               {"jira:OPS-1", "", "blocks", CanonicalBlocksSemantics, BlockingEnds{}, false},
		"an item cannot block itself":             {"jira:OPS-1", "jira:OPS-1", "blocks", CanonicalBlocksSemantics, BlockingEnds{}, false},
		"the type is matched exactly, not folded": {"jira:OPS-1", "jira:OPS-2", "Blocks", CanonicalBlocksSemantics, BlockingEnds{}, false},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := BlockingEndsOf(tc.source, tc.target, tc.relationship, tc.version)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("BlockingEndsOf = %+v, %t; want %+v, %t", got, ok, tc.want, tc.ok)
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

// A relation the provider no longer reports is the one that was NOT written
// again by the latest sync of the item(s) that write it.
func TestRelationIsCurrentDropsARelationTheProviderNoLongerReports(t *testing.T) {
	for name, tc := range map[string]struct {
		relation                  int
		emitterKnown              bool
		emitter, blocked, blocker int
		want                      bool
	}{
		// One emitter (a relation read from text in that item).
		"one emitter: written by its latest sync":       {relation: 5, emitterKnown: true, emitter: 5, blocked: 9, blocker: 9, want: true},
		"one emitter: written after its latest sync":    {relation: 6, emitterKnown: true, emitter: 5, blocked: 9, blocker: 9, want: true},
		"one emitter: its latest sync did not write it": {relation: 5, emitterKnown: true, emitter: 6, blocked: 1, blocker: 1, want: false},
		// Two emitters (a native link: each end reports it).
		"two emitters: both ends at the relation's sync":         {relation: 5, blocked: 5, blocker: 5, want: true},
		"two emitters: the blocked item's sync wrote it again":   {relation: 7, blocked: 7, blocker: 5, want: true},
		"two emitters: the blocker's sync wrote it again":        {relation: 7, blocked: 5, blocker: 7, want: true},
		"two emitters: only the blocked item was synced again":   {relation: 5, blocked: 7, blocker: 5, want: true},
		"two emitters: only the blocker was synced again":        {relation: 5, blocked: 5, blocker: 7, want: true},
		"two emitters: both were synced again, neither wrote it": {relation: 5, blocked: 6, blocker: 7, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			got := RelationIsCurrent(blockedTime(tc.relation), tc.emitterKnown, blockedTime(tc.emitter), blockedTime(tc.blocked), blockedTime(tc.blocker))
			if got != tc.want {
				t.Fatalf("RelationIsCurrent = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestBlockedIntervalForNeedsAKnownSpan(t *testing.T) {
	for name, tc := range map[string]struct {
		itemCreated int
		blocker     Blocker
		want        BlockedInterval
		ok          bool
	}{
		"an open blocker created first: from the item's creation, open": {
			10, Blocker{Status: "in_progress", CreatedAt: blockedTime(2)},
			BlockedInterval{Start: blockedTime(10)}, true,
		},
		"an open blocker created later: from the blocker's creation": {
			10, Blocker{Status: "todo", CreatedAt: blockedTime(14)},
			BlockedInterval{Start: blockedTime(14)}, true,
		},
		"a completed blocker: until its completion": {
			10, Blocker{Status: "done", CreatedAt: blockedTime(2), CompletedAt: blockedTimePtr(20)},
			BlockedInterval{Start: blockedTime(10), End: blockedTimePtr(20)}, true,
		},
		"a blocker completed before the item existed: no span": {
			10, Blocker{Status: "done", CreatedAt: blockedTime(2), CompletedAt: blockedTimePtr(8)},
			BlockedInterval{}, false,
		},
		"a blocker completed at the instant the item was created: no span": {
			10, Blocker{Status: "done", CreatedAt: blockedTime(2), CompletedAt: blockedTimePtr(10)},
			BlockedInterval{}, false,
		},
		"a done blocker with no completion time: its end is not known": {
			10, Blocker{Status: "done", CreatedAt: blockedTime(2)},
			BlockedInterval{}, false,
		},
		"a canceled blocker with no completion time: its end is not known": {
			10, Blocker{Status: "canceled", CreatedAt: blockedTime(2)},
			BlockedInterval{}, false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := BlockedIntervalFor(blockedTime(tc.itemCreated), tc.blocker)
			if ok != tc.ok || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("BlockedIntervalFor = %+v, %t; want %+v, %t", got, ok, tc.want, tc.ok)
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
