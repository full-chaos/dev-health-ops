package providersync

import (
	"errors"
	"testing"
	"time"
)

// The production feed for computed_at is time.Now() (complete_route.go:118),
// which carries NANOSECONDS. None of the three destinations can store them:
// estimate coverage and team attributions are DateTime64(3) and state
// durations is a plain DateTime. A stamp the builder emits at a precision the
// column cannot hold is written, quantized by the server, read back different,
// and the replay verdict is Absent forever.
//
// Every oracle case uses a whole-second ComputedAt, so the oracle pairs cannot
// see this: it needs a fixture that actually carries sub-precision digits.
func TestGitHubWorkItemDerivedSurfacesQuantizeStampsAtBuilderEntry(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	day := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	// 123456789ns is chosen so the millisecond and the second truncation
	// disagree with each other AND with the raw value: a builder that
	// quantized everything to one precision fails this, not just a builder
	// that quantizes nothing.
	computedAt := time.Date(2026, 8, 5, 0, 30, 0, 123456789, time.UTC)
	wantMillis := time.Date(2026, 8, 5, 0, 30, 0, 123000000, time.UTC)
	wantSeconds := time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC)

	item := githubWorkItemRow{
		WorkItemID: "acme/api#1", Provider: "github", Title: "t", Type: "issue",
		Status: "todo", ProjectID: stringPointer("acme/api"),
		CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
		OrgID:     claim.OrgID,
	}
	rows := githubWorkItemRows{
		WorkItems: []githubWorkItemRow{item},
		StatusTransitions: []githubWorkItemTransitionRow{{
			WorkItemID: "acme/api#1", Provider: "github",
			OccurredAt: time.Date(2026, 8, 4, 6, 0, 0, 0, time.UTC),
			FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID,
		}},
	}
	surfaces, err := buildGitHubWorkItemDerivedSurfaces(
		claim, rows, day, computedAt, newGitHubWorkItemDerivationContext(
			githubWorkItemDerivationFacts{},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(surfaces.EstimateCoverage) == 0 || len(surfaces.TeamAttributions) == 0 ||
		len(surfaces.StateDurations) == 0 {
		t.Fatalf("fixture produced no rows on some surface: %d/%d/%d",
			len(surfaces.EstimateCoverage), len(surfaces.TeamAttributions),
			len(surfaces.StateDurations))
	}
	for _, row := range surfaces.EstimateCoverage {
		if !row.ComputedAt.Equal(wantMillis) {
			t.Errorf("estimate coverage computed_at = %v, want %v (DateTime64(3))",
				row.ComputedAt, wantMillis)
		}
	}
	for _, row := range surfaces.TeamAttributions {
		if !row.ComputedAt.Equal(wantMillis) {
			t.Errorf("team attribution computed_at = %v, want %v (DateTime64(3))",
				row.ComputedAt, wantMillis)
		}
	}
	for _, row := range surfaces.StateDurations {
		if !row.ComputedAt.Equal(wantSeconds) {
			t.Errorf("state duration computed_at = %v, want %v (plain DateTime)",
				row.ComputedAt, wantSeconds)
		}
	}
}

// githubWorkItemDerivedCollisionFixture is the Go twin of the
// two_assignees_one_team_collide_on_sorting_key oracle case: two members of ONE
// team assigned to one issue. It is built from the REAL derivation context and
// the REAL builder, so if the resolver ever stops emitting two candidates here
// the collision tests below fail rather than quietly becoming vacuous.
func githubWorkItemDerivedCollisionFixture(
	t *testing.T, computedAt time.Time,
) githubWorkItemDerivedSurfaces {
	t.Helper()
	claim := githubWorkItemOracleClaim()
	facts := githubWorkItemDerivationFacts{
		Members: []githubWorkItemDerivationMemberFact{
			{
				Provider: "github", TeamID: "payments", TeamName: "Payments",
				MemberID: "m1", RawProviderUserID: stringPointer("octocat"),
				IdentityFacets: []string{"octocat"},
				IsPrimary:      1, Specificity: 50, Priority: 20,
				UpdatedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			},
			{
				Provider: "github", TeamID: "payments", TeamName: "Payments Squad",
				MemberID: "m2", RawProviderUserID: stringPointer("hubcat"),
				IdentityFacets: []string{"hubcat"},
				IsPrimary:      0, Specificity: 50, Priority: 20,
				UpdatedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			},
		},
	}
	rows := githubWorkItemRows{WorkItems: []githubWorkItemRow{{
		WorkItemID: "acme/api#40", Provider: "github", Title: "t", Type: "issue",
		Status: "todo", ProjectID: stringPointer("acme/api"),
		Assignees: []string{"octocat", "hubcat"},
		CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
		OrgID:     claim.OrgID,
	}}}
	surfaces, err := buildGitHubWorkItemDerivedSurfaces(
		claim, rows, time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC), computedAt,
		newGitHubWorkItemDerivationContext(facts),
	)
	if err != nil {
		t.Fatal(err)
	}
	return surfaces
}

// The dedup's whole reason to exist is a real builder output that collides.
// The predecessor's B2 work asserted the dedup in the abstract without ever
// showing the resolver produces a collision, which leaves "the dedup collapses
// it" and "there was nothing to collapse" indistinguishable. This asserts the
// COLLISION first, then the collapse.
//
// CHAOS-4244 codex round-3 (HIGH): this test used to assert the dedup kept
// whichever colliding row happened to sort LAST in the resolver's output --
// which, for this exact fixture, is the NON-PRIMARY duplicate (m2,
// is_primary=0), while the resolver's actual is_primary=1 winner (m1) was
// silently discarded. That was the bug, not a documented tie-break: a real
// batch write would have persisted an item with NO primary attribution row
// even though it genuinely resolved a team. The dedup is now primary-aware
// (githubTeamAttributionIsPrimary) and must keep the PRIMARY row regardless
// of its position in the batch -- see
// TestGitHubTeamAttributionDedupeNeverErasesTheOnlyPrimaryRow for the
// position-independent proof; this test additionally confirms it holds for a
// REAL resolver-produced collision, not just a handcrafted fixture.
func TestGitHubTeamAttributionCollisionIsRealAndCollapses(t *testing.T) {
	computedAt := time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC)
	surfaces := githubWorkItemDerivedCollisionFixture(t, computedAt)

	rows := surfaces.TeamAttributions
	byKey := map[string][]githubWorkItemTeamAttributionRow{}
	for _, row := range rows {
		byKey[githubTeamAttributionSortingKey(row)] = append(
			byKey[githubTeamAttributionSortingKey(row)], row,
		)
	}
	var collided []githubWorkItemTeamAttributionRow
	for _, group := range byKey {
		if len(group) > 1 {
			collided = group
		}
	}
	if len(collided) != 2 {
		t.Fatalf("fixture produced no sorting-key collision (%d rows, %d keys); "+
			"the dedup tests below would be vacuous", len(rows), len(byKey))
	}
	// They must differ in fields the sorting key does NOT carry -- otherwise
	// the collision is harmless and proves nothing about which row survives.
	if collided[0].TeamName == nil || collided[1].TeamName == nil ||
		*collided[0].TeamName == *collided[1].TeamName {
		t.Errorf("collided rows must differ in team_name, got %v / %v",
			collided[0].TeamName, collided[1].TeamName)
	}
	if collided[0].Evidence == collided[1].Evidence {
		t.Errorf("collided rows must differ in evidence, got %q twice",
			collided[0].Evidence)
	}
	// The fixture must genuinely produce exactly one primary among the
	// colliding pair -- otherwise this test cannot tell "kept the primary"
	// from "kept an arbitrary row" apart.
	var wantPrimaryEvidence string
	primaryCount := 0
	for _, row := range collided {
		if row.IsPrimary == 1 {
			primaryCount++
			wantPrimaryEvidence = row.Evidence
		}
	}
	if primaryCount != 1 {
		t.Fatalf("fixture must produce exactly one primary among the collided pair, got %d: %+v",
			primaryCount, collided)
	}

	deduped := githubWorkItemDerivedSortingKeyDedupe(
		rows, githubTeamAttributionSortingKey, githubTeamAttributionVersion, githubTeamAttributionIsPrimary,
	)
	if len(deduped) != len(byKey) {
		t.Fatalf("dedup left %d rows for %d distinct sorting keys", len(deduped), len(byKey))
	}
	// Name the survivor. "The count dropped" would also pass if the dedup kept
	// an arbitrary row, and the readback expectation has to match the row the
	// server actually stores.
	var survivor *githubWorkItemTeamAttributionRow
	for index, row := range deduped {
		if githubTeamAttributionSortingKey(row) == githubTeamAttributionSortingKey(collided[0]) {
			survivor = &deduped[index]
		}
	}
	if survivor == nil {
		t.Fatal("dedup dropped the collided sorting key entirely")
	}
	if survivor.IsPrimary != 1 || survivor.Evidence != wantPrimaryEvidence {
		t.Errorf("dedup kept evidence %q (is_primary=%d), want the PRIMARY row's evidence %q",
			survivor.Evidence, survivor.IsPrimary, wantPrimaryEvidence)
	}
}

// Last-wins is only correct while every row in a batch shares one computed_at.
// That is true today and nothing enforces it: the dedup picks the row the
// server would NOT keep the moment a batch carries mixed versions, because
// ReplacingMergeTree resolves by version, not by insertion order. The
// expectation would then name a row the storage discards, and the readback
// wedges at Conflict forever.
//
// Ordering is still the TIE-break, so the equal-version behaviour the
// collision test above pins is unchanged.
func TestGitHubTeamAttributionDedupePrefersHighestVersion(t *testing.T) {
	older := time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 5, 6, 0, 0, 0, time.UTC)
	teamID := "payments"
	row := func(evidence string, computedAt time.Time) githubWorkItemTeamAttributionRow {
		return githubWorkItemTeamAttributionRow{
			WorkItemID: "acme/api#40", Provider: "github",
			Source: "assignee_membership", IsPrimary: 1, Confidence: "high",
			Evidence: evidence, ComputedAt: computedAt, TeamID: &teamID,
			OrgID: "org-acme",
		}
	}
	// The NEWER row is FIRST, so index order and version order disagree.
	rows := []githubWorkItemTeamAttributionRow{
		row("assignee_membership=m1", newer),
		row("assignee_membership=m2", older),
	}
	if githubTeamAttributionSortingKey(rows[0]) != githubTeamAttributionSortingKey(rows[1]) {
		t.Fatal("fixture rows must share a sorting key")
	}
	deduped := githubWorkItemDerivedSortingKeyDedupe(
		rows, githubTeamAttributionSortingKey, githubTeamAttributionVersion, githubTeamAttributionIsPrimary,
	)
	if len(deduped) != 1 {
		t.Fatalf("dedup left %d rows, want 1", len(deduped))
	}
	if !deduped[0].ComputedAt.Equal(newer) {
		t.Errorf("dedup kept computed_at %v, want the HIGHEST version %v",
			deduped[0].ComputedAt, newer)
	}
	if deduped[0].Evidence != "assignee_membership=m1" {
		t.Errorf("dedup kept %q, want the highest-version row", deduped[0].Evidence)
	}
}

// Equal versions must still fall back to ordering, which is what the stored
// outcome depends on when a batch shares one computed_at -- the case every
// real batch takes today.
func TestGitHubTeamAttributionDedupeTieBreaksByOrder(t *testing.T) {
	stamp := time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC)
	teamID := "payments"
	row := func(evidence string) githubWorkItemTeamAttributionRow {
		return githubWorkItemTeamAttributionRow{
			WorkItemID: "acme/api#40", Provider: "github",
			Source: "assignee_membership", IsPrimary: 1, Confidence: "high",
			Evidence: evidence, ComputedAt: stamp, TeamID: &teamID,
			OrgID: "org-acme",
		}
	}
	deduped := githubWorkItemDerivedSortingKeyDedupe(
		[]githubWorkItemTeamAttributionRow{
			row("assignee_membership=m1"), row("assignee_membership=m2"),
		},
		githubTeamAttributionSortingKey, githubTeamAttributionVersion, githubTeamAttributionIsPrimary,
	)
	if len(deduped) != 1 || deduped[0].Evidence != "assignee_membership=m2" {
		t.Fatalf("equal versions must keep the LAST row, got %+v", deduped)
	}
}

// TestGitHubTeamAttributionDedupeNeverErasesTheOnlyPrimaryRow (CHAOS-4244
// codex round-3, HIGH): a reporter or assignee matched via two membership
// facets naming the SAME team produces two rows sharing this exact sorting
// key (repo_id, work_item_id, team_id, source -- NOT is_primary or evidence),
// one is_primary=1 (the resolver's actual winner) and one is_primary=0 (a
// lower-specificity duplicate provenance row). Before this fix, an
// equal-version tie-break was pure last-wins and had no idea which row was
// primary -- if the non-primary duplicate happened to sort LAST, it would
// silently replace the primary row in storage, leaving the item with NO
// is_primary=1 row even though it genuinely resolved a team. The primary row
// must survive regardless of its position in the batch.
func TestGitHubTeamAttributionDedupeNeverErasesTheOnlyPrimaryRow(t *testing.T) {
	stamp := time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC)
	teamID := "team-ops"
	row := func(evidence string, isPrimary int) githubWorkItemTeamAttributionRow {
		return githubWorkItemTeamAttributionRow{
			WorkItemID: "acme/api#40", Provider: "github",
			Source: "author_membership", IsPrimary: isPrimary, Confidence: "medium",
			Evidence: evidence, ComputedAt: stamp, TeamID: &teamID,
			OrgID: "org-acme",
		}
	}
	// The PRIMARY row is FIRST (the resolver emits it first, ranked highest by
	// specificity) and the non-primary duplicate is LAST -- the exact ordering
	// that used to lose under plain last-wins.
	primaryFirst := []githubWorkItemTeamAttributionRow{
		row("reporter=alice (member_id facet)", 1),
		row("reporter=alice (email facet)", 0),
	}
	deduped := githubWorkItemDerivedSortingKeyDedupe(
		primaryFirst,
		githubTeamAttributionSortingKey, githubTeamAttributionVersion, githubTeamAttributionIsPrimary,
	)
	if len(deduped) != 1 || deduped[0].IsPrimary != 1 {
		t.Fatalf("primary-first: dedup must keep the PRIMARY row, got %+v", deduped)
	}
	if deduped[0].Evidence != "reporter=alice (member_id facet)" {
		t.Errorf("primary-first: kept evidence %q, want the primary row's evidence", deduped[0].Evidence)
	}

	// Same rows, non-primary first -- the primary must STILL survive, proving
	// the preference isn't accidentally just "first wins".
	primaryLast := []githubWorkItemTeamAttributionRow{
		row("reporter=alice (email facet)", 0),
		row("reporter=alice (member_id facet)", 1),
	}
	deduped = githubWorkItemDerivedSortingKeyDedupe(
		primaryLast,
		githubTeamAttributionSortingKey, githubTeamAttributionVersion, githubTeamAttributionIsPrimary,
	)
	if len(deduped) != 1 || deduped[0].IsPrimary != 1 {
		t.Fatalf("primary-last: dedup must keep the PRIMARY row, got %+v", deduped)
	}
	if deduped[0].Evidence != "reporter=alice (member_id facet)" {
		t.Errorf("primary-last: kept evidence %q, want the primary row's evidence", deduped[0].Evidence)
	}
}

// inspectGitHubWorkItemDerivedRows had no test at all, so the precedence it
// encodes -- conflict beats absent beats exact -- was unasserted while every
// batch readback in the package depends on it.
//
// The precedence is not cosmetic. The committer treats Absent as "write this",
// so a batch containing one CONFLICTING row that reported Absent would be
// rewritten straight over data that disagrees with it. Both orders are tested
// because a verdict that wins only when it arrives first is not precedence, and
// the natural mutant (swap the two arms) is invisible to a single ordering.
func TestInspectGitHubWorkItemDerivedRowsTakesTheWeakestVerdict(t *testing.T) {
	for _, tt := range []struct {
		name        string
		inspections []EffectInspection
		want        EffectInspection
	}{
		{"empty batch is exact", nil, EffectExact},
		{"all exact", []EffectInspection{EffectExact, EffectExact}, EffectExact},
		{"one absent", []EffectInspection{EffectExact, EffectAbsent}, EffectAbsent},
		{"absent then exact", []EffectInspection{EffectAbsent, EffectExact}, EffectAbsent},
		{"one conflict", []EffectInspection{EffectExact, EffectConflict}, EffectConflict},
		{
			"conflict BEFORE absent",
			[]EffectInspection{EffectConflict, EffectAbsent}, EffectConflict,
		},
		{
			// The ordering that catches an inverted precedence: absent is seen
			// first and must NOT win.
			"absent BEFORE conflict",
			[]EffectInspection{EffectAbsent, EffectConflict}, EffectConflict,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			index := 0
			got, err := inspectGitHubWorkItemDerivedRows(
				tt.inspections,
				func(inspection EffectInspection) (EffectInspection, error) {
					index++
					return inspection, nil
				},
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("verdict = %v, want %v", got, tt.want)
			}
		})
	}

	t.Run("inspection error is a conflict", func(t *testing.T) {
		got, err := inspectGitHubWorkItemDerivedRows(
			[]EffectInspection{EffectExact},
			func(EffectInspection) (EffectInspection, error) {
				return EffectExact, ErrInvalidConfiguration
			},
		)
		if !errors.Is(err, ErrInvalidConfiguration) || got != EffectConflict {
			t.Fatalf("got (%v, %v), want (EffectConflict, ErrInvalidConfiguration)", got, err)
		}
	})

	t.Run("unknown verdict is rejected", func(t *testing.T) {
		got, err := inspectGitHubWorkItemDerivedRows(
			[]EffectInspection{EffectExact},
			func(EffectInspection) (EffectInspection, error) {
				return EffectInspection("not-a-verdict"), nil
			},
		)
		if !errors.Is(err, ErrInvalidConfiguration) || got != EffectConflict {
			t.Fatalf("got (%v, %v), want (EffectConflict, ErrInvalidConfiguration)", got, err)
		}
	})
}

// assertGitHubWorkItemDerivedTenancy has THREE clauses and, before this test,
// deleting its entire body survived every test in the package: no fixture
// carried a foreign org, a foreign provider or a zero created_at, so the fence
// guarded nothing that was ever exercised.
//
// Each clause is mutated independently. A single "wrong row is rejected" case
// would be satisfied by any one clause surviving, which is exactly how a
// compound predicate keeps a dead clause hidden.
func TestAssertGitHubWorkItemDerivedTenancyRejectsEachClause(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	valid := githubWorkItemRow{
		WorkItemID: "acme/api#1", Provider: claim.Provider, Title: "t", Type: "issue",
		Status: "todo", ProjectID: stringPointer("acme/api"),
		CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
		OrgID:     claim.OrgID,
	}
	if err := assertGitHubWorkItemDerivedTenancy(claim, valid); err != nil {
		t.Fatalf("the valid row must pass, else every case below is vacuous: %v", err)
	}

	for _, tt := range []struct {
		name   string
		mutate func(githubWorkItemRow) githubWorkItemRow
	}{
		{"foreign org", func(row githubWorkItemRow) githubWorkItemRow {
			row.OrgID = "org-other"
			return row
		}},
		{"empty org", func(row githubWorkItemRow) githubWorkItemRow {
			row.OrgID = ""
			return row
		}},
		{"foreign provider", func(row githubWorkItemRow) githubWorkItemRow {
			row.Provider = "gitlab"
			return row
		}},
		{
			// Go's zero time.Time IS 0001-01-01, so "absent" and "the zero
			// instant" are the same value. Python accepts datetime(1,1,1);
			// this port refuses it rather than silently sorting the item
			// before every window.
			"zero created_at",
			func(row githubWorkItemRow) githubWorkItemRow {
				row.CreatedAt = time.Time{}
				return row
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := assertGitHubWorkItemDerivedTenancy(claim, tt.mutate(valid)); !errors.Is(
				err, ErrInvalidConfiguration,
			) {
				t.Errorf("got %v, want ErrInvalidConfiguration", err)
			}
		})
	}
}

// The assertion must run BEFORE the window and terminal skips, or a
// foreign-tenant row that falls outside the window is silently skipped instead
// of refused -- making the tenancy guarantee depend on the row's dates.
//
// Every builder is checked, because each has its own skip and each could
// regress independently. The row is placed OUTSIDE the day window on purpose:
// with an in-window row all three would raise even if the assertion sat after
// the skip, and the ordering would stay unmeasured.
func TestGitHubWorkItemDerivedBuildersAssertTenancyBeforeSkipping(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	day := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC)
	foreign := githubWorkItemRow{
		WorkItemID: "acme/api#99", Provider: "github", Title: "t", Type: "issue",
		Status: "todo", ProjectID: stringPointer("acme/api"),
		// Created AFTER the window ends, so every builder's own skip would
		// drop it before ever looking at the tenant.
		CreatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		OrgID:     "org-other",
	}
	rows := githubWorkItemRows{
		WorkItems: []githubWorkItemRow{foreign},
		StatusTransitions: []githubWorkItemTransitionRow{{
			WorkItemID: "acme/api#99", Provider: "github",
			OccurredAt: time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC),
			FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID,
		}},
	}
	derived := newGitHubWorkItemDerivationContext(githubWorkItemDerivationFacts{})
	if _, err := buildGitHubWorkItemDerivedSurfaces(
		claim, rows, day, computedAt, derived,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("composite: got %v, want ErrInvalidConfiguration", err)
	}
	if _, err := buildGitHubEstimateCoverageMetricsDaily(
		claim, rows, day, day.AddDate(0, 0, 1), computedAt, derived,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("estimate coverage: got %v, want ErrInvalidConfiguration", err)
	}
	if _, err := buildGitHubWorkItemTeamAttributions(
		claim, rows, computedAt, derived,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("team attributions: got %v, want ErrInvalidConfiguration", err)
	}
	if _, err := buildGitHubWorkItemStateDurationsDaily(
		claim, rows, day, day.AddDate(0, 0, 1), computedAt, derived,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Errorf("state durations: got %v, want ErrInvalidConfiguration", err)
	}
}

// Quantizing the STAMP must not quantize the value the compute uses. An open
// item's final segment ends at computed_at, so truncating the arithmetic input
// to seconds would shorten duration_hours by up to a second against Python,
// which passes computed_at through raw. This asserts the segment math still
// sees the FULL-precision instant while the stamp is quantized.
func TestGitHubWorkItemStateDurationsKeepFullPrecisionSegmentEnd(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	day := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	// Sits INSIDE the window so the open segment's end is computed_at itself
	// rather than the window end, which is what exposes the truncation.
	computedAt := time.Date(2026, 8, 4, 12, 0, 0, 500000000, time.UTC)
	rows := githubWorkItemRows{
		WorkItems: []githubWorkItemRow{{
			WorkItemID: "acme/api#1", Provider: "github", Title: "t", Type: "issue",
			Status: "todo", ProjectID: stringPointer("acme/api"),
			CreatedAt: time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
			OrgID:     claim.OrgID,
		}},
		StatusTransitions: []githubWorkItemTransitionRow{{
			WorkItemID: "acme/api#1", Provider: "github",
			OccurredAt: time.Date(2026, 8, 4, 6, 0, 0, 0, time.UTC),
			FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID,
		}},
	}
	surfaces, err := buildGitHubWorkItemDerivedSurfaces(
		claim, rows, day, computedAt, newGitHubWorkItemDerivationContext(
			githubWorkItemDerivationFacts{},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, row := range surfaces.StateDurations {
		total += row.DurationHours
	}
	// 00:00 -> 12:00:00.5 == 12.0001388...h. A seconds-truncated segment end
	// would give exactly 12.
	want := 12.0 + 0.5/3600.0
	if diff := total - want; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("total duration_hours = %.12f, want %.12f (full-precision segment end)",
			total, want)
	}
}
