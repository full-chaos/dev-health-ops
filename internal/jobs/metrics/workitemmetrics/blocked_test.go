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
