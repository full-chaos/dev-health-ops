package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// TestBuildGitHubWorkItemTeamAttributionsCarriesOwnershipReasonFromTheRealResolver
// is CHAOS-4320's mutation-resistant pin for codex round 3's first P3: every
// other OwnershipReason test in this file (and the writer-side test below)
// constructs a githubWorkItemTeamAttributionRow BY HAND, never calling
// buildGitHubWorkItemTeamAttributions itself -- so deleting either (a) the
// builder's `OwnershipReason: candidate.OwnershipReason` assignment
// (github_work_item_derived_surfaces.go) or (b) the writer's
// RecordTeamAttributionOwnershipChecked call passed the full committed
// providersync suite. Verified: reverting (a) to `OwnershipReason: ""` alone
// still passes `go test ./internal/providersync/...` in full -- proving the
// gap before this test existed.
//
// This test uses the REAL derivation context (teamattribution.
// NewGitHubWorkItemDerivationContext) and the REAL builder end to end,
// mirroring githubWorkItemDerivedCollisionFixture's "must go through
// production code, not a hand-built struct" discipline. The fixture has NO
// team_repo_ownership facts at all, so the gate's own R74 pass-through fires
// (MembershipOwnershipReasonUnknown) and the surviving assignee_membership
// candidate carries a non-empty OwnershipReason -- this only reaches the
// output row if the builder's field assignment is actually present.
func TestBuildGitHubWorkItemTeamAttributionsCarriesOwnershipReasonFromTheRealResolver(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	facts := teamattribution.GithubWorkItemDerivationFacts{
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-member", TeamName: "Member Team",
			MemberID: "dev@example.com", RawProviderUserID: stringPointer("dev"),
			IdentityFacets: []string{"dev"}, IsPrimary: 1, Specificity: 50,
			UpdatedAt: now,
		}},
	}
	rows := githubWorkItemRows{WorkItems: []githubWorkItemRow{{
		WorkItemID: "acme/api#1", Provider: "github", Title: "t", Type: "issue",
		Status: "todo", ProjectID: stringPointer("acme/api"),
		Assignees: []string{"dev"},
		CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: now, OrgID: claim.OrgID,
	}}}
	attributions, _, err := buildGitHubWorkItemTeamAttributions(
		claim, rows, now, teamattribution.NewGitHubWorkItemDerivationContext(facts),
	)
	if err != nil {
		t.Fatal(err)
	}
	var membership *githubWorkItemTeamAttributionRow
	for index := range attributions {
		if attributions[index].Source == "assignee_membership" {
			membership = &attributions[index]
		}
	}
	if membership == nil {
		t.Fatalf("no assignee_membership row in %+v", attributions)
	}
	if membership.OwnershipReason != teamattribution.MembershipOwnershipReasonUnknown {
		t.Fatalf(
			"OwnershipReason = %q, want %q (the real resolver's R74 pass-through reason) -- "+
				"either the builder dropped candidate.OwnershipReason, or the resolver's "+
				"reason changed",
			membership.OwnershipReason, teamattribution.MembershipOwnershipReasonUnknown,
		)
	}
}

// TestBuildGitHubWorkItemTeamAttributionsCarriesOwnershipReasonForAuthorFromTheRealResolver
// is the author-path sibling of the test above (codex round 4, P3, executed
// mutation-survival finding "05-isolated": erasing cascade.go's author-branch
// `candidate.OwnershipReason = GithubWorkItemDerivationOwnershipCheckedLabel(...)`
// assignment passed the full committed teamattribution AND providersync
// suites, because no test went through the REAL resolver on the author/
// reporter path specifically -- the assignee sibling above does not exercise
// it, and every writer-level test constructs an author_membership row by
// hand. Item type is "pr": the author-gate only applies to pull/merge-
// request types (GithubWorkItemDerivationIsPullOrMergeRequestType), so an
// "issue" fixture like the assignee test above would never reach the
// reporter branch at all.
func TestBuildGitHubWorkItemTeamAttributionsCarriesOwnershipReasonForAuthorFromTheRealResolver(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	facts := teamattribution.GithubWorkItemDerivationFacts{
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-reporter", TeamName: "Reporter Team",
			MemberID: "alice@example.com", RawProviderUserID: stringPointer("alice"),
			IdentityFacets: []string{"alice"}, IsPrimary: 1, Specificity: 50,
			UpdatedAt: now,
		}},
	}
	rows := githubWorkItemRows{WorkItems: []githubWorkItemRow{{
		WorkItemID: "acme/api#2", Provider: "github", Title: "t", Type: "pr",
		Status: "todo", ProjectID: stringPointer("acme/api"),
		Reporter:  stringPointer("alice"),
		CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: now, OrgID: claim.OrgID,
	}}}
	attributions, _, err := buildGitHubWorkItemTeamAttributions(
		claim, rows, now, teamattribution.NewGitHubWorkItemDerivationContext(facts),
	)
	if err != nil {
		t.Fatal(err)
	}
	var membership *githubWorkItemTeamAttributionRow
	for index := range attributions {
		if attributions[index].Source == "author_membership" {
			membership = &attributions[index]
		}
	}
	if membership == nil {
		t.Fatalf("no author_membership row in %+v", attributions)
	}
	if membership.OwnershipReason != teamattribution.MembershipOwnershipReasonUnknown {
		t.Fatalf(
			"OwnershipReason = %q, want %q (the real resolver's R74 pass-through reason on "+
				"the AUTHOR path) -- either cascade.go's author branch dropped "+
				"candidate.OwnershipReason, or the resolver's reason changed",
			membership.OwnershipReason, teamattribution.MembershipOwnershipReasonUnknown,
		)
	}
}

// TestGitHubWorkItemTeamAttributionRowSurvivesTheEffectsJSONRoundTrip is
// CHAOS-4320's red-first pin for codex round 2's P1 (NOT CLEAN, executed
// repro): githubWorkItemTeamAttributionRow does not go directly from
// buildGitHubWorkItemTeamAttributions to WriteGitHubWorkItemEffect -- it is
// json.Marshal'd into an EffectBatch's json.RawMessage rows
// (marshalGitHubWorkItemDerivedRows / derivedRows, the effects/outbox
// layer) and json.Unmarshal'd back into this same struct type in
// validateGitHubWorkItemDerivedEffect before the write boundary ever reads
// it. Priority (`json:"-"`, CHAOS-4321) and OwnershipReason (`json:"-"`,
// this ticket's own r1 fix) were BOTH dropped by that round trip -- a field
// tagged `json:"-"` marshals to nothing and unmarshals back as its zero
// value, regardless of what was actually set. This made round 1's F1 fix a
// no-op in production: every unit test that calls Resolve()/
// buildGitHubWorkItemTeamAttributions directly (never going through the
// effects layer) passed, hiding the defect completely.
//
// This test goes through the REAL round trip (json.Marshal, then the same
// validateGitHubWorkItemDerivedEffect the write path calls) rather than
// just asserting on the struct directly, so it fails the way production
// actually failed.
func TestGitHubWorkItemTeamAttributionRowSurvivesTheEffectsJSONRoundTrip(t *testing.T) {
	original := githubWorkItemTeamAttributionRow{
		WorkItemID: "gh:acme/api#1", Provider: "github", Source: "assignee_membership",
		IsPrimary: 1, Confidence: "high", Evidence: "assignee=dev@example.com",
		OrgID: "org-acme", Priority: 10, OwnershipReason: "ownership_unknown",
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch(
		githubTeamAttributionsDestination, EffectReadbackRequired,
		[]json.RawMessage{raw},
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Destination: githubTeamAttributionsDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	rows, err := validateGitHubWorkItemDerivedEffect[githubWorkItemTeamAttributionRow](
		identity, effect, githubTeamAttributionsDestination,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want exactly 1", rows)
	}
	if rows[0].Priority != 10 {
		t.Fatalf("rows[0].Priority = %d after the effects round trip, want 10 (the value actually set) -- a json:\"-\" tag silently drops this field", rows[0].Priority)
	}
	if rows[0].OwnershipReason != "ownership_unknown" {
		t.Fatalf("rows[0].OwnershipReason = %q after the effects round trip, want ownership_unknown", rows[0].OwnershipReason)
	}
}

// TestGitHubWorkItemTeamAttributionRowNoExportedFieldReadsBackZero is a
// RECURRENCE GUARD (codex round 2 follow-up, team-lead): a test scoped to
// Priority/OwnershipReason only proves those two fields, not the STRUCT --
// the next field added to githubWorkItemTeamAttributionRow with an
// accidental `json:"-"` would reproduce the exact same defect class and
// this test would say nothing about it. This test instead populates EVERY
// exported field to a deliberately non-zero value via reflection, round-
// trips the row through the real effects path (identical mechanism to the
// test above), and fails if ANY exported field reads back as its Go zero
// value -- so a future silently-dropped field is caught structurally,
// without needing its own dedicated test.
func TestGitHubWorkItemTeamAttributionRowNoExportedFieldReadsBackZero(t *testing.T) {
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	teamID := "team-repo"
	teamName := "Repository Team"
	original := githubWorkItemTeamAttributionRow{
		WorkItemID: "gh:acme/api#1", Provider: "github", Source: "assignee_membership",
		IsPrimary: 1, Confidence: "high", Evidence: "assignee=dev@example.com",
		ComputedAt: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
		RepoID:     &repoID, TeamID: &teamID, TeamName: &teamName,
		OrgID: "org-acme", Priority: 10, OwnershipReason: "ownership_unknown",
	}

	// Sanity control: EVERY exported field on the original must itself be
	// non-zero, or this test would vacuously pass on a field it forgot to
	// populate -- the same "positive control" discipline every other
	// mutation/round-trip proof in this codebase requires.
	assertNoExportedFieldIsZero(t, "original", original)

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch(
		githubTeamAttributionsDestination, EffectReadbackRequired,
		[]json.RawMessage{raw},
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Destination: githubTeamAttributionsDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	rows, err := validateGitHubWorkItemDerivedEffect[githubWorkItemTeamAttributionRow](
		identity, effect, githubTeamAttributionsDestination,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want exactly 1", rows)
	}
	assertNoExportedFieldIsZero(t, "round-tripped", rows[0])
}

type ownershipReasonWriteConn struct {
	driver.Conn
	batch *ownershipReasonWriteBatch
	// SendErr, when set, is returned by every batch this conn prepares --
	// used to prove counting only happens AFTER a successful Send (codex
	// round 5, P3).
	SendErr error
}

func (conn *ownershipReasonWriteConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if conn.batch == nil {
		conn.batch = &ownershipReasonWriteBatch{SendErr: conn.SendErr}
	}
	return conn.batch, nil
}

type ownershipReasonWriteBatch struct {
	driver.Batch
	SendErr error
	// Appended records every Append call's WorkItemID+Source argument pair
	// (args[2], args[6] in the INSERT's own column order), so a test can
	// assert on exactly which rows reached the INSERT without needing its
	// own SQL-aware fake.
	Appended []string
}

func (batch *ownershipReasonWriteBatch) Append(args ...any) error {
	if len(args) >= 7 {
		if workItemID, ok := args[2].(string); ok {
			if source, ok := args[6].(string); ok {
				batch.Appended = append(batch.Appended, workItemID+"|"+source)
			}
		}
	}
	return nil
}
func (batch *ownershipReasonWriteBatch) Send() error  { return batch.SendErr }
func (batch *ownershipReasonWriteBatch) Abort() error { return nil }

// TestWriteGitHubWorkItemEffectCountsOwnershipCheckedOnEveryMembershipRow is
// CHAOS-4320's red-first pin for codex round 3's two P1s (NOT CLEAN,
// executed repro): (1) RecordTeamAttributionOwnershipChecked used to fire
// only from `primaryRows`, so a genuinely-gated assignee_membership/
// author_membership candidate that lost primary to a higher-precedence
// source (the OVERWHELMINGLY common case -- repo_ownership and every other
// source ahead of membership in `order` outrank it whenever they also
// resolve) was NEVER counted at all; (2) a row reaching the write boundary
// with OwnershipReason == "" (a legacy/pre-migration effect payload that
// never had the field, or had it explicitly empty -- json.Unmarshal cannot
// tell the two apart on a plain string) was defaulted to "owned", asserting
// a confidence the code does not actually have.
//
// This test writes FOUR rows through the real WriteGitHubWorkItemEffect,
// using a fake ClickHouse conn/batch (Append/Send are no-ops) and a real
// providerfoundation.Metrics sink: a primary repo_ownership row (unrelated
// to either bug), a NON-PRIMARY assignee_membership row with a real
// OwnershipReason (must now be counted despite losing primary), a
// NON-PRIMARY author_membership row with a real OwnershipReason (codex
// round 4, P3: the author source specifically, not just assignee, must be
// counted -- an earlier version of this test only exercised the author
// path via the empty-reason/skip case below, so removing "author_membership"
// from the writer's membershipRows predicate, or erasing the resolver's
// author OwnershipReason assignment, both passed the full suite), and a
// NON-PRIMARY author_membership row with an EMPTY OwnershipReason (must be
// skipped, not silently mislabeled "owned").
func TestWriteGitHubWorkItemEffectCountsOwnershipCheckedOnEveryMembershipRow(t *testing.T) {
	rows := []githubWorkItemTeamAttributionRow{
		{
			WorkItemID: "gh:acme/api#1", Provider: "github", Source: "repo_ownership",
			IsPrimary: 1, Confidence: "high", Evidence: "repo_ownership=x", OrgID: "org-acme",
		},
		{
			WorkItemID: "gh:acme/api#1", Provider: "github", Source: "assignee_membership",
			IsPrimary: 0, Confidence: "high", Evidence: "assignee=dev@example.com",
			OrgID: "org-acme", OwnershipReason: "ownership_unknown",
		},
		{
			WorkItemID: "gh:acme/api#2", Provider: "github", Source: "author_membership",
			IsPrimary: 0, Confidence: "high", Evidence: "reporter=bob",
			OrgID: "org-acme", OwnershipReason: "owned",
		},
		{
			WorkItemID: "gh:acme/api#1", Provider: "github", Source: "author_membership",
			IsPrimary: 0, Confidence: "high", Evidence: "reporter=alice",
			OrgID: "org-acme", OwnershipReason: "", // legacy/absent-field simulation
		},
	}
	effect, err := effectBatchFromValues(githubTeamAttributionsDestination, EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Destination: githubTeamAttributionsDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	metrics := providerfoundation.NewMetrics()
	sink := GitHubWorkItemTeamAttributionsClickHouseEffects{
		Conn:    &ownershipReasonWriteConn{},
		Lease:   providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		Metrics: metrics,
	}
	if err := sink.WriteGitHubWorkItemEffect(context.Background(), identity, effect); err != nil {
		t.Fatal(err)
	}

	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	if !strings.Contains(rendered, `dev_health_team_attribution_ownership_checked_total{reason="ownership_unknown"} 1`) {
		t.Fatalf("non-primary assignee_membership row was not counted by ownership_checked:\n%s", rendered)
	}
	if !strings.Contains(rendered, `dev_health_team_attribution_ownership_checked_total{reason="owned"} 1`) {
		t.Fatalf("the granted author_membership row was not counted by ownership_checked (author source excluded from recording?):\n%s", rendered)
	}
	// codex round 4, P3: the earlier version of this assertion only forbade
	// `reason="owned"} 1` -- with the empty-reason row now sharing a
	// fixture with a GENUINELY owned author row, disabling the empty-reason
	// skip bumps "owned" to 2, which that phrasing would have missed
	// entirely. Count every ownership_checked sample line instead: exactly
	// 2 (the ownership_unknown assignee row and the real owned author row)
	// -- an erroneous THIRD sample under ANY label (owned, other, or a
	// vocabulary this test does not name) fails this, not just the one
	// label an earlier version of this test happened to check for.
	sampleCount := strings.Count(rendered, "dev_health_team_attribution_ownership_checked_total{reason=")
	if sampleCount != 2 {
		t.Fatalf(
			"ownership_checked_total has %d reason samples, want exactly 2 -- the empty-OwnershipReason row must be skipped entirely, not recorded under any label:\n%s",
			sampleCount, rendered,
		)
	}
}

// TestRejectedMembershipsAreCountedByOwnershipChecked is CHAOS-4320's
// red-first pin for codex round 4's P1 (NOT CLEAN, executed repro): a
// repo-ownership-gate REJECTION (a resolved assignee_membership/
// author_membership candidate whose team does NOT own the repo) never
// becomes a candidate at all -- Resolve() drops it before returning, exactly
// as intended, so the row-based recording every earlier round built (round
// 3's membershipRows loop included) can only ever see rows that SURVIVED
// the gate. The instrument advertises a "repo_not_owned" label
// (metricTeamAttributionOwnershipCheckedVocabulary in budget.go) that, before
// this fix, could NEVER actually be emitted in production: the outcome was
// completely unobservable, indistinguishable from "nobody was ever gated at
// all."
//
// Round 6 (chris via team-lead, 2026-09-10) replaced the marker-row
// mechanism this test originally pinned: rejections now travel as
// buildGitHubWorkItemTeamAttributions's own second return value, attached
// to EffectBatch.MembershipRejections -- a field separate from Rows -- and
// never mixed into the candidate list at all. This test still goes through
// the REAL end-to-end path -- the real derivation context, the real
// resolver, the real MembershipRejections JSON round trip, and the real
// WriteGitHubWorkItemEffect -- with only the ClickHouse Conn/Batch faked, so
// it fails the way production actually fails. One work item has BOTH a
// non-owning assignee and a non-owning reporter (same identity, "alice"),
// mirroring the review's own repro: the repo is owned by "owner", but
// "alice" belongs only to "nonowner" -- so both the assignee_membership and
// author_membership candidates for alice are gate-rejected, and the ONLY
// surviving candidate is repo_ownership itself.
func TestRejectedMembershipsAreCountedByOwnershipChecked(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	repoIDString := repoID.String()
	facts := teamattribution.GithubWorkItemDerivationFacts{
		Repos: []teamattribution.GithubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "owner", TeamName: "Owner",
			RepoID: &repoIDString, RepoFullName: "acme/api", IsPrimary: 1,
			Specificity: 70, UpdatedAt: now,
		}},
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "nonowner", TeamName: "Nonowner",
			MemberID: "alice", RawProviderUserID: stringPointer("alice"),
			IdentityFacets: []string{"alice"}, IsPrimary: 1, Specificity: 60,
			UpdatedAt: now,
		}},
	}
	rows := githubWorkItemRows{WorkItems: []githubWorkItemRow{{
		WorkItemID: "acme/api#1", Provider: "github", Type: "pr", Title: "t",
		Status: "todo", ProjectID: stringPointer("acme/api"), RepoID: &repoID,
		Assignees: []string{"alice"}, Reporter: stringPointer("alice"),
		CreatedAt: now, UpdatedAt: now, OrgID: claim.OrgID,
	}}}
	attributions, rejections, err := buildGitHubWorkItemTeamAttributions(
		claim, rows, now, teamattribution.NewGitHubWorkItemDerivationContext(facts),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range attributions {
		if row.Source == "assignee_membership" || row.Source == "author_membership" {
			t.Fatalf(
				"got a real %s row in the candidate list for a non-owning identity -- "+
					"rejections must never become a persisted candidate: %+v",
				row.Source, row,
			)
		}
	}
	if len(rejections) != 2 {
		t.Fatalf("rejections = %+v, want exactly 2 (assignee + author)", rejections)
	}

	effect, err := effectBatchFromValues(githubTeamAttributionsDestination, EffectReadbackRequired, attributions)
	if err != nil {
		t.Fatal(err)
	}
	marshaledRejections, err := marshalGitHubWorkItemTeamAttributionRejections(rejections)
	if err != nil {
		t.Fatal(err)
	}
	effect.MembershipRejections = marshaledRejections
	identity := GitHubWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: "github", Destination: githubTeamAttributionsDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	metrics := providerfoundation.NewMetrics()
	sink := GitHubWorkItemTeamAttributionsClickHouseEffects{
		Conn:    &ownershipReasonWriteConn{},
		Lease:   providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		Metrics: metrics,
	}
	if err := sink.WriteGitHubWorkItemEffect(context.Background(), identity, effect); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	if !strings.Contains(rendered, `dev_health_team_attribution_ownership_checked_total{reason="repo_not_owned"} 2`) {
		t.Fatalf(
			"two executed gate rejections (assignee + author) produced no repo_not_owned counter sample:\n%s",
			rendered,
		)
	}
}

// TestRejectedRowsWithIdenticalSortingKeyCountOnceNotTwice is CHAOS-4320's
// red-first pin for codex round 5's second P1 (NOT CLEAN, executed repro):
// granted candidates go through githubWorkItemDerivedSortingKeyDedupe
// before anything counts them, so two candidates that resolve to an
// identical (repo, work item, team, source) key collapse to ONE row before
// the "owned"/membership-layer counters ever see them -- exactly the
// identity this destination's ReplacingMergeTree engine itself uses.
// Rejection events did NOT get the same treatment: two rejections sharing
// that identical key counted as TWO ownership_checked samples, so the same
// kind of duplicate meant a different count depending on which outcome
// (owned vs rejected) it had.
//
// This test attaches two rejection events sharing an IDENTICAL sorting key
// (same repo, work item, team, source) to EffectBatch.MembershipRejections
// (round 6's mechanism) through the real WriteGitHubWorkItemEffect and
// asserts exactly ONE repo_not_owned sample, matching what the equivalent
// granted-row duplicate would do.
func TestRejectedRowsWithIdenticalSortingKeyCountOnceNotTwice(t *testing.T) {
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	teamID := "nonowner"
	teamName := "Nonowner"
	duplicateRejection := githubWorkItemTeamAttributionRejectionRow{
		WorkItemID: "gh:acme/api#5", Provider: "github",
		RepoID: &repoID, Source: "assignee_membership",
		TeamID: &teamID, TeamName: &teamName, Reason: "repo_not_owned",
	}
	// Same sorting key (repo, work item, team, source) as the rejection
	// above -- a genuine duplicate resolution, mirroring the resolver-
	// recomputation case the existing collision tests for GRANTED rows
	// already cover (e.g. two ownership facts naming one team differently).
	sameKeyRejection := duplicateRejection
	rejections := []githubWorkItemTeamAttributionRejectionRow{duplicateRejection, sameKeyRejection}
	marshaledRejections, err := marshalGitHubWorkItemDerivedRows(rejections)
	if err != nil {
		t.Fatal(err)
	}

	effect, err := effectBatchFromValues(githubTeamAttributionsDestination, EffectReadbackRequired,
		[]githubWorkItemTeamAttributionRow{{
			WorkItemID: "gh:acme/api#5", Provider: "github", Source: "repo_ownership",
			IsPrimary: 1, Confidence: "high", Evidence: "repo_ownership=x", OrgID: "org-acme",
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	effect.MembershipRejections = marshaledRejections
	identity := GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Destination: githubTeamAttributionsDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	metrics := providerfoundation.NewMetrics()
	sink := GitHubWorkItemTeamAttributionsClickHouseEffects{
		Conn:    &ownershipReasonWriteConn{},
		Lease:   providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		Metrics: metrics,
	}
	if err := sink.WriteGitHubWorkItemEffect(context.Background(), identity, effect); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	if !strings.Contains(rendered, `dev_health_team_attribution_ownership_checked_total{reason="repo_not_owned"} 1`) {
		t.Fatalf(
			"two rejection events sharing an identical sorting key were counted separately instead of "+
				"collapsing to one, like the equivalent granted-row duplicate does:\n%s",
			rendered,
		)
	}
}

// TestRejectedRowsWithDistinctKeysAreNotCollapsed is CHAOS-4320's
// mutation-resistant pin for codex round 6's second P3: the sibling test
// above only proves that an IDENTICAL sorting key collapses; it says
// nothing about the four fields the key is actually built from
// individually. Independently dropping the repo, work item, or team
// component from githubTeamAttributionRejectionSortingKey (leaving Source
// alone) passed both that test and the full suite -- only removing Source
// was caught. This test varies ONE component at a time against a baseline
// rejection and asserts each variant is counted SEPARATELY, not collapsed
// into the baseline -- so a sorting key missing any one of those
// components (which would falsely treat these as the same key) fails
// here specifically, not just on a Source-only mutation.
func TestRejectedRowsWithDistinctKeysAreNotCollapsed(t *testing.T) {
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	otherRepoID := uuid.MustParse("d8299fbc-1945-3717-05d8-eb78866b4e80")
	teamID := "nonowner"
	otherTeamID := "other-nonowner"
	baseline := githubWorkItemTeamAttributionRejectionRow{
		WorkItemID: "gh:acme/api#5", Provider: "github",
		RepoID: &repoID, Source: "assignee_membership", TeamID: &teamID, Reason: "repo_not_owned",
	}
	differentRepo := baseline
	differentRepo.RepoID = &otherRepoID
	differentWorkItem := baseline
	differentWorkItem.WorkItemID = "gh:acme/api#6"
	differentTeam := baseline
	differentTeam.TeamID = &otherTeamID

	rejections := []githubWorkItemTeamAttributionRejectionRow{
		baseline, differentRepo, differentWorkItem, differentTeam,
	}
	marshaledRejections, err := marshalGitHubWorkItemDerivedRows(rejections)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := effectBatchFromValues(githubTeamAttributionsDestination, EffectReadbackRequired,
		[]githubWorkItemTeamAttributionRow{{
			WorkItemID: "gh:acme/api#5", Provider: "github", Source: "repo_ownership",
			IsPrimary: 1, Confidence: "high", Evidence: "repo_ownership=x", OrgID: "org-acme",
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	effect.MembershipRejections = marshaledRejections
	identity := GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Destination: githubTeamAttributionsDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	metrics := providerfoundation.NewMetrics()
	sink := GitHubWorkItemTeamAttributionsClickHouseEffects{
		Conn:    &ownershipReasonWriteConn{},
		Lease:   providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		Metrics: metrics,
	}
	if err := sink.WriteGitHubWorkItemEffect(context.Background(), identity, effect); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	if !strings.Contains(rendered, `dev_health_team_attribution_ownership_checked_total{reason="repo_not_owned"} 4`) {
		t.Fatalf(
			"four rejections that each differ by a distinct repo/work-item/team component were not "+
				"counted separately -- the sorting key is missing one of those components:\n%s",
			rendered,
		)
	}
}

// assertNoExportedFieldIsZero fails the test naming every exported field of
// value that reflect.Value.IsZero reports as unset.
func assertNoExportedFieldIsZero(t *testing.T, label string, value any) {
	t.Helper()
	reflected := reflect.ValueOf(value)
	reflectedType := reflected.Type()
	var zeroFields []string
	for index := 0; index < reflected.NumField(); index++ {
		field := reflectedType.Field(index)
		if !field.IsExported() {
			continue
		}
		if reflected.Field(index).IsZero() {
			zeroFields = append(zeroFields, field.Name)
		}
	}
	if len(zeroFields) > 0 {
		t.Fatalf("%s githubWorkItemTeamAttributionRow has zero-valued exported field(s): %v -- either the fixture forgot to populate them (fix the fixture) or the effects JSON round trip silently dropped them (check for a json:\"-\" tag)", label, zeroFields)
	}
}

// TestWriteGitHubWorkItemEffectDoesNotCountOnAFailedSend is CHAOS-4320's
// pin for codex round 5's third P3: every other fake-conn test in this file
// uses a Send that always succeeds, so a mutation that moved ownership
// counting to BEFORE Send() (instead of after, where the surrounding
// comments already say it must happen -- CHAOS-4244's own double-count-on-
// retry reasoning, extended to ownership_checked by round 4) survived every
// committed test. A caller that retries a batch whose Send failed would
// double-count both a granted AND a rejected row's ownership_checked
// sample if counting ever ran before Send.
//
// This test forces Send to fail and asserts WriteGitHubWorkItemEffect
// propagates the error AND that NO ownership_checked sample (granted or
// rejected) was recorded -- proving the counting genuinely gates on a
// successful Send rather than merely being placed after the Append loop
// in source order.
func TestWriteGitHubWorkItemEffectDoesNotCountOnAFailedSend(t *testing.T) {
	sendErr := errors.New("send failed")
	rows := []githubWorkItemTeamAttributionRow{
		{
			WorkItemID: "gh:acme/api#1", Provider: "github", Source: "assignee_membership",
			IsPrimary: 0, Confidence: "high", Evidence: "assignee=dev@example.com",
			OrgID: "org-acme", OwnershipReason: "ownership_unknown",
		},
	}
	teamID := "nonowner"
	rejections := []githubWorkItemTeamAttributionRejectionRow{{
		WorkItemID: "gh:acme/api#1", Provider: "github", Source: "author_membership",
		TeamID: &teamID, Reason: "repo_not_owned",
	}}
	marshaledRejections, err := marshalGitHubWorkItemDerivedRows(rejections)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := effectBatchFromValues(githubTeamAttributionsDestination, EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	effect.MembershipRejections = marshaledRejections
	identity := GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Destination: githubTeamAttributionsDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	metrics := providerfoundation.NewMetrics()
	sink := GitHubWorkItemTeamAttributionsClickHouseEffects{
		Conn:    &ownershipReasonWriteConn{SendErr: sendErr},
		Lease:   providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		Metrics: metrics,
	}
	if err := sink.WriteGitHubWorkItemEffect(context.Background(), identity, effect); !errors.Is(err, sendErr) {
		t.Fatalf("err = %v, want the Send failure to propagate", err)
	}
	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	if strings.Contains(rendered, "dev_health_team_attribution_ownership_checked_total{reason=") {
		t.Fatalf(
			"a failed Send still recorded an ownership_checked sample -- a retry of this batch "+
				"would double-count it:\n%s",
			rendered,
		)
	}
}

// TestWriteGitHubWorkItemEffectInsertsEveryRowNoFilteringPath is CHAOS-4320
// round 6's invariant test (chris via team-lead, 2026-09-10): the design
// this round replaces round 4/5's marker-row mechanism with is explicitly
// "no rows that travel through the effects batch only to be filtered before
// INSERT" -- rejections now travel on a SEPARATE EffectBatch field
// (MembershipRejections) that this function never even looks at when
// building the INSERT, so there is no filtering branch left over Rows at
// all. This test proves that structurally: every row in a batch (four
// distinct sorting keys, including sources that used to be eligible for
// gate rejection) reaches batch.Append exactly once, with no member
// unexpectedly absent -- the shape a reintroduced filtering step would
// break.
func TestWriteGitHubWorkItemEffectInsertsEveryRowNoFilteringPath(t *testing.T) {
	rows := []githubWorkItemTeamAttributionRow{
		{
			WorkItemID: "gh:acme/api#1", Provider: "github", Source: "repo_ownership",
			IsPrimary: 1, Confidence: "high", Evidence: "repo_ownership=x", OrgID: "org-acme",
		},
		{
			WorkItemID: "gh:acme/api#1", Provider: "github", Source: "assignee_membership",
			IsPrimary: 0, Confidence: "high", Evidence: "assignee=dev@example.com",
			OrgID: "org-acme", OwnershipReason: "ownership_unknown",
		},
		{
			WorkItemID: "gh:acme/api#2", Provider: "github", Source: "author_membership",
			IsPrimary: 0, Confidence: "high", Evidence: "reporter=bob",
			OrgID: "org-acme", OwnershipReason: "owned",
		},
		{
			WorkItemID: "gh:acme/api#3", Provider: "github", Source: "unassigned",
			IsPrimary: 1, Confidence: "none", Evidence: "no_candidate", OrgID: "org-acme",
		},
		// codex round 6, P3: the earlier version of this fixture named only
		// four of the cascade's nine sources (order: native_team,
		// issue_project, project_ownership, repo_ownership,
		// assignee_membership, linked_issue, author_membership,
		// manual_fallback, unassigned) -- adding `if row.Source ==
		// "native_team" { continue }` to the writer's Append loop passed
		// this test unchanged, since no row here ever exercised that
		// source. These four cover the rest of the vocabulary this
		// destination's real resolver actually emits.
		{
			WorkItemID: "gh:acme/api#4", Provider: "github", Source: "native_team",
			IsPrimary: 1, Confidence: "high", Evidence: "native_team_key=acme", OrgID: "org-acme",
		},
		{
			WorkItemID: "gh:acme/api#5", Provider: "github", Source: "issue_project",
			IsPrimary: 1, Confidence: "high", Evidence: "issue_project=acme/api", OrgID: "org-acme",
		},
		{
			WorkItemID: "gh:acme/api#6", Provider: "github", Source: "project_ownership",
			IsPrimary: 1, Confidence: "high", Evidence: "project_ownership=acme/api", OrgID: "org-acme",
		},
		{
			WorkItemID: "gh:acme/api#7", Provider: "github", Source: "linked_issue",
			IsPrimary: 1, Confidence: "medium", Evidence: "linked_issue=gh:acme/api#7", OrgID: "org-acme",
		},
		{
			WorkItemID: "gh:acme/api#8", Provider: "github", Source: "manual_fallback",
			IsPrimary: 1, Confidence: "high", Evidence: "manual_fallback=acme/api", OrgID: "org-acme",
		},
	}
	effect, err := effectBatchFromValues(githubTeamAttributionsDestination, EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	conn := &ownershipReasonWriteConn{}
	sink := GitHubWorkItemTeamAttributionsClickHouseEffects{
		Conn:    conn,
		Lease:   providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		Metrics: providerfoundation.NewMetrics(),
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Destination: githubTeamAttributionsDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if err := sink.WriteGitHubWorkItemEffect(context.Background(), identity, effect); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"gh:acme/api#1|repo_ownership", "gh:acme/api#1|assignee_membership",
		"gh:acme/api#2|author_membership", "gh:acme/api#3|unassigned",
		"gh:acme/api#4|native_team", "gh:acme/api#5|issue_project",
		"gh:acme/api#6|project_ownership", "gh:acme/api#7|linked_issue",
		"gh:acme/api#8|manual_fallback",
	}
	got := append([]string(nil), conn.batch.Appended...)
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("appended rows = %v, want %v -- every distinct-key row must reach the INSERT, none filtered", got, want)
	}
}

// TestGitHubWorkItemTeamAttributionRejectionRowSurvivesTheEffectsJSONRoundTrip
// is CHAOS-4320 round 6's pin for team-lead's explicit ask (Trap #125, the
// exact class codex round 2 found on Priority/OwnershipReason): the new
// githubWorkItemTeamAttributionRejectionRow type, carried on
// EffectBatch.MembershipRejections, must survive the SAME json.Marshal/
// json.Unmarshal round trip real rows go through -- a field given
// `json:"-"` by mistake would silently read back its zero value here
// exactly as it did for Priority/OwnershipReason before round 2's fix.
// Populates every exported field to a non-zero value (the same positive-
// control discipline TestGitHubWorkItemTeamAttributionRowNoExportedFieldReadsBackZero
// already uses) and fails if any field reads back zero after the round
// trip through decodeGitHubWorkItemTeamAttributionRejections, the actual
// function WriteGitHubWorkItemEffect calls.
func TestGitHubWorkItemTeamAttributionRejectionRowSurvivesTheEffectsJSONRoundTrip(t *testing.T) {
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	teamID := "team-outsider"
	teamName := "Outsider Team"
	original := githubWorkItemTeamAttributionRejectionRow{
		WorkItemID: "gh:acme/api#1", Provider: "github", RepoID: &repoID,
		Source: "assignee_membership", TeamID: &teamID, TeamName: &teamName,
		Reason: "repo_not_owned",
	}
	assertNoExportedFieldIsZero(t, "original", original)

	marshaled, err := marshalGitHubWorkItemDerivedRows([]githubWorkItemTeamAttributionRejectionRow{original})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGitHubWorkItemTeamAttributionRejections(marshaled)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 {
		t.Fatalf("decoded = %+v, want exactly 1", decoded)
	}
	assertNoExportedFieldIsZero(t, "round-tripped", decoded[0])
}
