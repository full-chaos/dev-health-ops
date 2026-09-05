package icfinalize

import "testing"

func metricFor(metrics []ICUserMetric, identity string) *ICUserMetric {
	for i := range metrics {
		if metrics[i].IdentityID == identity {
			return &metrics[i]
		}
	}
	return nil
}

// The multi-provider fold is explicitly "crude" in the reference: counts SUM,
// but the cycle percentiles take the MAX of the two providers. A max of two
// p50s is not a p50 of anything -- pinned because it looks like a bug and is
// not one, so a future reader does not "fix" it.
func TestWorkItemAggregationSumsCountsButMaxesPercentiles(t *testing.T) {
	folded := AggregateWorkItems([]WorkItemUserMetric{
		{UserIdentity: "u", Provider: "jira", WorkScopeID: "s1", ItemsStarted: 1,
			ItemsCompleted: 2, WIPCountEndOfDay: 3, CycleTimeP50Hrs: 10, CycleTimeP90Hrs: 40},
		{UserIdentity: "u", Provider: "linear", WorkScopeID: "s2", ItemsStarted: 4,
			ItemsCompleted: 5, WIPCountEndOfDay: 6, CycleTimeP50Hrs: 7, CycleTimeP90Hrs: 90},
	})
	got := folded["u"]
	if got.ItemsStarted != 5 || got.ItemsCompleted != 7 || got.WIPCountEndOfDay != 9 {
		t.Fatalf("counts = %d/%d/%d, want summed 5/7/9",
			got.ItemsStarted, got.ItemsCompleted, got.WIPCountEndOfDay)
	}
	if got.CycleTimeP50Hrs != 10 {
		t.Fatalf("p50 = %v, want MAX 10 (not the mean 8.5, not the later 7)", got.CycleTimeP50Hrs)
	}
	if got.CycleTimeP90Hrs != 90 {
		t.Fatalf("p90 = %v, want MAX 90", got.CycleTimeP90Hrs)
	}
	if got.Provider != "mixed" || got.WorkScopeID != "mixed" {
		t.Fatalf("provider/scope = %q/%q, want the literal \"mixed\" for both",
			got.Provider, got.WorkScopeID)
	}
}

// A SINGLE provider must NOT be relabelled "mixed" -- the fold only fires on
// the second record. Without this, an implementation that always rewrote the
// labels would pass the test above.
func TestSingleProviderKeepsItsOwnLabels(t *testing.T) {
	folded := AggregateWorkItems([]WorkItemUserMetric{
		{UserIdentity: "u", Provider: "jira", WorkScopeID: "s1", ItemsCompleted: 2},
	})
	if got := folded["u"]; got.Provider != "jira" || got.WorkScopeID != "s1" {
		t.Fatalf("provider/scope = %q/%q, want jira/s1 -- a lone provider is not mixed",
			got.Provider, got.WorkScopeID)
	}
}

func TestMergeDerivesTheReferenceFields(t *testing.T) {
	merged := MergeICUserMetrics(
		[]GitUserMetric{{
			AuthorEmail: "dev@example.com", TeamID: "git-team",
			LOCAdded: 30, LOCDeleted: 12, PRsAuthored: 4, PRsMerged: 3,
			MedianPRCycleHours: 5.5, PRCycleP90Hours: 20.25,
		}},
		[]WorkItemUserMetric{{UserIdentity: "dev@example.com", ItemsCompleted: 7, WIPCountEndOfDay: 2}},
		nil,
	)
	got := metricFor(merged, "dev@example.com")
	if got.LOCTouched != 42 {
		t.Fatalf("LOCTouched = %d, want 42 (added + deleted)", got.LOCTouched)
	}
	if got.PRsOpened != 4 {
		t.Fatalf("PRsOpened = %d, want prs_authored 4", got.PRsOpened)
	}
	if got.DeliveryUnits != 10 {
		t.Fatalf("DeliveryUnits = %d, want prs_merged 3 + work_items_completed 7", got.DeliveryUnits)
	}
	if got.CycleP50Hours != 5.5 || got.CycleP90Hours != 20.25 {
		t.Fatalf("cycle = %v/%v, want median_pr_cycle_hours 5.5 and pr_cycle_p90_hours 20.25",
			got.CycleP50Hours, got.CycleP90Hours)
	}
	if got.SynthesizedRepoID {
		t.Fatal("row with a git record must not be flagged as synthesized")
	}
}

// team_map WINS over the git record's own team_id.
func TestTeamMapOverridesTheGitTeam(t *testing.T) {
	merged := MergeICUserMetrics(
		[]GitUserMetric{{AuthorEmail: "d", TeamID: "from-git"}},
		nil, map[string]string{"d": "from-map"},
	)
	if got := metricFor(merged, "d").TeamID; got != "from-map" {
		t.Fatalf("TeamID = %q, want from-map -- team_map takes precedence", got)
	}
	// Absent from the map, the git team survives.
	merged = MergeICUserMetrics([]GitUserMetric{{AuthorEmail: "d", TeamID: "from-git"}}, nil, nil)
	if got := metricFor(merged, "d").TeamID; got != "from-git" {
		t.Fatalf("TeamID = %q, want from-git when the map has no entry", got)
	}
}

// An identity present ONLY in work items is flagged, because the reference
// synthesizes repo_id=uuid.uuid4() for it -- a random value inside the dedup
// key. The union must still include it.
func TestWorkItemOnlyIdentityIsIncludedAndFlagged(t *testing.T) {
	merged := MergeICUserMetrics(
		[]GitUserMetric{{AuthorEmail: "has-git"}},
		[]WorkItemUserMetric{{UserIdentity: "wi-only", ItemsCompleted: 3}},
		nil,
	)
	if len(merged) != 2 {
		t.Fatalf("got %d rows, want 2 -- the union of git and work-item identities", len(merged))
	}
	wiOnly := metricFor(merged, "wi-only")
	if wiOnly == nil || !wiOnly.SynthesizedRepoID {
		t.Fatal("the work-item-only identity is missing or not flagged as synthesized")
	}
	if wiOnly.DeliveryUnits != 3 {
		t.Fatalf("DeliveryUnits = %d, want 3 (no PRs merged, 3 items completed)", wiOnly.DeliveryUnits)
	}
	if metricFor(merged, "has-git").SynthesizedRepoID {
		t.Fatal("the git-backed identity must not be flagged")
	}
}

// Sorted output: the reference has no stable order, so the port picks one.
func TestMergeReturnsIdentitiesInSortedOrder(t *testing.T) {
	merged := MergeICUserMetrics(
		[]GitUserMetric{{AuthorEmail: "zeta"}, {AuthorEmail: "alpha"}},
		[]WorkItemUserMetric{{UserIdentity: "mid"}},
		nil,
	)
	want := []string{"alpha", "mid", "zeta"}
	for i, identity := range want {
		if merged[i].IdentityID != identity {
			t.Fatalf("order = %v..., want %v", merged[i].IdentityID, want)
		}
	}
}
