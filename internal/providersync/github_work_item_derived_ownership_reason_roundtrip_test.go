package providersync

import (
	"context"
	"encoding/json"
	"reflect"
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
	attributions, err := buildGitHubWorkItemTeamAttributions(
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
}

func (conn *ownershipReasonWriteConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if conn.batch == nil {
		conn.batch = &ownershipReasonWriteBatch{}
	}
	return conn.batch, nil
}

type ownershipReasonWriteBatch struct {
	driver.Batch
}

func (batch *ownershipReasonWriteBatch) Append(...any) error { return nil }
func (batch *ownershipReasonWriteBatch) Send() error         { return nil }
func (batch *ownershipReasonWriteBatch) Abort() error        { return nil }

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
// This test writes THREE rows through the real WriteGitHubWorkItemEffect,
// using a fake ClickHouse conn/batch (Append/Send are no-ops) and a real
// providerfoundation.Metrics sink: a primary repo_ownership row (unrelated
// to either bug), a NON-PRIMARY assignee_membership row with a real
// OwnershipReason (must now be counted despite losing primary), and a
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
	if strings.Contains(rendered, `dev_health_team_attribution_ownership_checked_total{reason="owned"} 1`) {
		t.Fatalf("the empty-OwnershipReason row was counted as owned instead of skipped:\n%s", rendered)
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
