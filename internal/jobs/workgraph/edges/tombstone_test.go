package edges

import (
	"testing"
	"time"
)

func TestVersionAfterSupersedesPriorAtMillisecondPrecision(t *testing.T) {
	prior := time.Date(2026, 9, 2, 0, 0, 0, 500_000_000, time.UTC)
	cases := []struct {
		name  string
		clock time.Time
		want  time.Time
	}{
		{"clock ahead keeps the clock", prior.Add(time.Second), prior.Add(time.Second)},
		{"clock in the same millisecond moves past prior", prior.Add(400 * time.Microsecond), prior.Add(time.Millisecond)},
		{"clock equal to prior moves past prior", prior, prior.Add(time.Millisecond)},
		{"clock behind prior moves past prior", prior.Add(-time.Hour), prior.Add(time.Millisecond)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := versionAfter(tc.clock, prior)
			if !got.Equal(tc.want) {
				t.Fatalf("versionAfter(%s, %s) = %s, want %s", tc.clock, prior, got, tc.want)
			}
			if !got.Truncate(time.Millisecond).After(prior.Truncate(time.Millisecond)) {
				t.Fatalf("versionAfter(%s) = %s does not beat prior %s once stored as DateTime64(3)", tc.clock, got, prior)
			}
		})
	}
}

func issueEdge(source, edgeType, target string, lastSynced time.Time) Row {
	return Row{
		EdgeID:     EdgeID(NodeTypeIssue, source, edgeType, NodeTypeIssue, target),
		SourceType: NodeTypeIssue, SourceID: source, EdgeType: edgeType,
		TargetType: NodeTypeIssue, TargetID: target,
		Provenance: ProvenanceNative, Confidence: 0.9, Evidence: edgeType,
		DiscoveredAt: lastSynced, LastSynced: lastSynced, EventTs: lastSynced,
	}
}

func versionsOf(entries ...EdgeVersion) EdgeVersions {
	versions := make(EdgeVersions, len(entries))
	for _, entry := range entries {
		versions[identityOf(entry.Latest)] = entry
	}
	return versions
}

func TestPlanCleanupTombstonesRetiresOnlyLiveUnrewrittenCandidates(t *testing.T) {
	stored := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	clock := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)

	retired := issueEdge("gh:a#1", EdgeTypeBlocks, "gh:a#2", stored)
	rewritten := issueEdge("gh:a#3", EdgeTypeBlocks, "gh:a#4", stored)
	alreadyDeleted := issueEdge("gh:a#5", EdgeTypeBlocks, "gh:a#6", stored)
	notCandidate := issueEdge("gh:a#7", EdgeTypeRelates, "gh:a#8", stored)
	future := issueEdge("gh:a#9", EdgeTypeIsBlockedBy, "gh:a#10", clock.Add(time.Hour))

	versions := versionsOf(
		EdgeVersion{Latest: retired, MaxLastSynced: stored},
		EdgeVersion{Latest: rewritten, MaxLastSynced: stored},
		EdgeVersion{Latest: alreadyDeleted, IsDeleted: true, MaxLastSynced: stored},
		EdgeVersion{Latest: notCandidate, MaxLastSynced: stored},
		EdgeVersion{Latest: future, MaxLastSynced: clock.Add(time.Hour)},
	)
	plan := CleanupPlan{CandidateIDs: []string{retired.EdgeID, rewritten.EdgeID, alreadyDeleted.EdgeID, future.EdgeID}}

	tombstones := PlanCleanupTombstones(plan, versions, []Row{issueEdge("gh:a#3", EdgeTypeBlocks, "gh:a#4", clock)}, clock)

	got := map[string]Row{}
	for _, row := range tombstones {
		got[row.EdgeID] = row
	}
	if len(got) != 2 {
		t.Fatalf("got %d tombstones %v, want exactly the live unrewritten candidates", len(got), tombstones)
	}
	if row, ok := got[retired.EdgeID]; !ok || !row.LastSynced.Equal(clock) || row.Evidence != retired.Evidence {
		t.Fatalf("retired candidate tombstone = %+v (present %v), want a copy stamped at the clock", row, ok)
	}
	if row, ok := got[future.EdgeID]; !ok || !row.LastSynced.After(clock.Add(time.Hour)) {
		t.Fatalf("candidate stored ahead of the clock got tombstone %+v (present %v), want it stamped past the stored version", row, ok)
	}
}

func TestStampRewritesOnlyMovesIdentitiesADeleteWouldHaveCleared(t *testing.T) {
	clock := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	ahead := clock.Add(24 * time.Hour)

	candidateAhead := issueEdge("gh:b#1", EdgeTypeBlocks, "gh:b#2", clock)
	tombstonedAhead := issueEdge("gh:b#3", EdgeTypeRelates, "gh:b#4", clock)
	liveAheadNotCandidate := issueEdge("gh:b#5", EdgeTypeRelates, "gh:b#6", clock)
	candidateBehind := issueEdge("gh:b#7", EdgeTypeBlocks, "gh:b#8", clock)
	neverStored := issueEdge("gh:b#9", EdgeTypeBlocks, "gh:b#10", clock)

	versions := versionsOf(
		EdgeVersion{Latest: candidateAhead, MaxLastSynced: ahead},
		EdgeVersion{Latest: tombstonedAhead, IsDeleted: true, MaxLastSynced: ahead},
		EdgeVersion{Latest: liveAheadNotCandidate, MaxLastSynced: ahead},
		EdgeVersion{Latest: candidateBehind, MaxLastSynced: clock.Add(-time.Hour)},
	)
	plan := CleanupPlan{CandidateIDs: []string{candidateAhead.EdgeID, candidateBehind.EdgeID, neverStored.EdgeID}}
	rows := []Row{candidateAhead, tombstonedAhead, liveAheadNotCandidate, candidateBehind, neverStored}

	stamped := StampRewrites(rows, plan, versions)

	want := []time.Time{ahead.Add(time.Millisecond), ahead.Add(time.Millisecond), clock, clock, clock}
	for index, row := range stamped {
		if !row.LastSynced.Equal(want[index]) {
			t.Errorf("row %d (%s %s->%s) last_synced = %s, want %s",
				index, row.EdgeType, row.SourceID, row.TargetID, row.LastSynced, want[index])
		}
	}
	for index := range rows {
		if !rows[index].LastSynced.Equal(clock) {
			t.Fatalf("StampRewrites mutated its input row %d", index)
		}
	}
}

func TestPlanStalePRDependencyTombstonesMatchesTheLegacyPredicate(t *testing.T) {
	stored := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	edge := func(sourceType, sourceID, targetID, evidence string) Row {
		return Row{
			EdgeID: sourceType + sourceID + targetID + evidence, SourceType: sourceType, SourceID: sourceID,
			EdgeType: EdgeTypeRelates, TargetType: NodeTypeIssue, TargetID: targetID,
			Evidence: evidence, LastSynced: stored,
		}
	}
	ghpr := edge(NodeTypeIssue, "ghpr:acme/app#1", "linear:ACME-1", "linear_attachment")
	gitlab := edge(NodeTypeIssue, "gitlab:acme/app#2", "linear:ACME-2", "linear_attachment")
	wrongEvidence := edge(NodeTypeIssue, "ghpr:acme/app#3", "linear:ACME-3", "text_reference")
	prSource := edge("pull_request", "ghpr:acme/app#4", "linear:ACME-4", "linear_attachment")
	notLinear := edge(NodeTypeIssue, "ghpr:acme/app#5", "jira:ACME-5", "linear_attachment")
	issueSource := edge(NodeTypeIssue, "gh:acme/app#6", "linear:ACME-6", "linear_attachment")
	deleted := edge(NodeTypeIssue, "ghpr:acme/app#7", "linear:ACME-7", "linear_attachment")

	versions := versionsOf(
		EdgeVersion{Latest: ghpr, MaxLastSynced: stored},
		EdgeVersion{Latest: gitlab, MaxLastSynced: stored},
		EdgeVersion{Latest: wrongEvidence, MaxLastSynced: stored},
		EdgeVersion{Latest: prSource, MaxLastSynced: stored},
		EdgeVersion{Latest: notLinear, MaxLastSynced: stored},
		EdgeVersion{Latest: issueSource, MaxLastSynced: stored},
		EdgeVersion{Latest: deleted, IsDeleted: true, MaxLastSynced: stored},
	)
	tombstones := PlanStalePRDependencyTombstones(versions, clock)
	if len(tombstones) != 2 || tombstones[0].EdgeID != ghpr.EdgeID && tombstones[1].EdgeID != ghpr.EdgeID {
		t.Fatalf("got tombstones %+v, want exactly the ghpr: and gitlab: stale rows", tombstones)
	}
	for _, row := range tombstones {
		if row.EdgeID != ghpr.EdgeID && row.EdgeID != gitlab.EdgeID {
			t.Fatalf("unexpected tombstone %+v", row)
		}
		if !row.LastSynced.Equal(clock) {
			t.Fatalf("tombstone %s last_synced = %s, want the clock %s", row.EdgeID, row.LastSynced, clock)
		}
	}
}
