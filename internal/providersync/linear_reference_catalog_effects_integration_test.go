//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

func TestLinearReferenceCatalogEffectsAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}

	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = "linear-org-a"
	otherClaim := claim
	otherClaim.OrgID = "linear-org-b"
	now := time.Date(2026, 8, 10, 12, 34, 56, 789000000, time.UTC)
	rows := linearReferenceCatalogIntegrationRows(claim, now)
	otherRows := linearReferenceCatalogIntegrationRows(otherClaim, now)
	effects, err := BuildLinearReferenceCatalogEffects(rows)
	if err != nil {
		t.Fatal(err)
	}
	otherEffects, err := BuildLinearReferenceCatalogEffects(otherRows)
	if err != nil {
		t.Fatal(err)
	}

	for _, effect := range effects.Batches() {
		inspection, inspectErr := sink.InspectEffect(ctx, claim, effect)
		if inspectErr != nil || inspection != EffectAbsent {
			t.Fatalf("before write destination=%s inspection=%s error=%v", effect.Destination, inspection, inspectErr)
		}
	}
	for _, effect := range otherEffects.Batches() {
		if err := sink.WriteEffect(ctx, otherClaim, effect); err != nil {
			t.Fatalf("foreign write destination=%s: %v", effect.Destination, err)
		}
	}
	for _, effect := range effects.Batches() {
		inspection, inspectErr := sink.InspectEffect(ctx, claim, effect)
		if inspectErr != nil || inspection != EffectAbsent {
			t.Fatalf("foreign tenant leaked into destination=%s inspection=%s error=%v", effect.Destination, inspection, inspectErr)
		}
	}
	for _, effect := range effects.Batches() {
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("write destination=%s: %v", effect.Destination, err)
		}
		inspection, inspectErr := sink.InspectEffect(ctx, claim, effect)
		if inspectErr != nil || inspection != EffectExact {
			t.Fatalf("readback destination=%s inspection=%s error=%v", effect.Destination, inspection, inspectErr)
		}
	}

	changed := rows.Projects[0]
	changed.Name = "Changed after readback"
	changedEffect, err := effectBatchFromValues(
		linearReferenceCatalogProjectsDestination, EffectReadbackRequired,
		[]linearReferenceProjectRow{changed},
	)
	if err != nil {
		t.Fatal(err)
	}
	inspection, inspectErr := sink.InspectEffect(ctx, claim, changedEffect)
	if inspectErr != nil || inspection != EffectConflict {
		t.Fatalf("changed winning project inspection=%s error=%v", inspection, inspectErr)
	}

	// The shared EffectCommitter must recover a durable write that returned an
	// error after ClickHouse accepted it. The first attempt fails after the
	// members destination is sent; the second attempt uses the real sink's
	// readback to mark that effect committed and writes the remaining four.
	recoveryClaim := claim
	recoveryClaim.OrgID = "linear-org-recovery"
	recoveryRows := linearReferenceCatalogIntegrationRows(recoveryClaim, now.Add(time.Hour))
	recoveryEffects, err := BuildLinearReferenceCatalogEffects(recoveryRows)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &memoryEffectLedger{}
	crashingSink := &linearReferenceCrashAfterWriteSink{
		inner: sink, destination: linearReferenceCatalogMembersDestination,
	}
	_, err = (EffectCommitter{
		Ledger: ledger, Sink: crashingSink, Now: func() time.Time { return now },
	}).Commit(ctx, recoveryClaim, recoveryEffects.Batches(), now)
	if !errors.Is(err, errLinearReferenceCrashAfterWrite) {
		t.Fatalf("first recovery attempt error=%v", err)
	}
	result, err := (EffectCommitter{
		Ledger: ledger, Sink: sink, Readback: sink,
		Now: func() time.Time { return now.Add(time.Minute) },
	}).Commit(ctx, recoveryClaim, recoveryEffects.Batches(), now)
	if err != nil {
		t.Fatal(err)
	}
	if result.MarkedCommitted != 1 || result.Written != len(recoveryEffects.Batches())-1 {
		t.Fatalf("recovery result=%+v", result)
	}
	for _, effect := range recoveryEffects.Batches() {
		inspection, inspectErr := sink.InspectEffect(ctx, recoveryClaim, effect)
		if inspectErr != nil || inspection != EffectExact {
			t.Fatalf("recovered destination=%s inspection=%s error=%v", effect.Destination, inspection, inspectErr)
		}
	}
}

// TestLinearReferenceCatalogEffectsPreservesManualMembersAcrossWrites is the
// red-first proof for CHAOS-4446 (filed by lane-4432, executed repro: a
// second Linear sync wiped an admin's CHAOS-4321 manual roster override).
// The Linear teams INSERT omitted manual_members entirely, so every write
// reset it to the schema default ([]) -- this pins that a value set between
// two native writes survives the second one, via the shared
// PreserveExistingTeamManualMembers helper (team_manual_members.go).
func TestLinearReferenceCatalogEffectsPreservesManualMembersAcrossWrites(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = "linear-org-manual-members"
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	teamKey := "ENG"
	teamID := "ENG"
	teamUUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+teamID)).String()
	firstWrite, err := effectBatchFromValues(linearReferenceCatalogTeamsDestination, EffectReadbackRequired, []linearReferenceTeamRow{{
		ID: teamID, TeamUUID: teamUUID, Name: "Engineering", Members: []string{"linear:alice@example.com"},
		ProjectKeys: []string{teamKey}, RepoPatterns: []string{}, IsActive: 1, UpdatedAt: now,
		OrgID: claim.OrgID, Provider: "linear", NativeTeamKey: &teamKey,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, firstWrite); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if manualMembers := readLinearTeamManualMembers(t, ctx, conn, claim.OrgID, teamID); len(manualMembers) != 0 {
		t.Fatalf("new team got non-empty manual_members: %v", manualMembers)
	}

	// Simulate the admin-override write path (CHAOS-4321): directly set
	// manual_members for this team, the same way an operator's override
	// lands outside the sync path.
	// Deliberately a literal INSERT naming manual_members, independent of
	// linearReferenceTeamsInsert -- this step must set up the precondition
	// even if the production query under test does not yet carry the
	// column forward (that is exactly the regression this test catches).
	overrideBatch, err := conn.PrepareBatch(ctx, "INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id)")
	if err != nil {
		t.Fatal(err)
	}
	if err := overrideBatch.Append(
		teamID, uuid.MustParse(teamUUID), "Engineering", (*string)(nil), []string{"linear:alice@example.com"},
		[]string{"manually-added@example.com"}, []string{teamKey}, []string{}, uint8(1), now.Add(time.Second),
		claim.OrgID, "linear", &teamKey, (*string)(nil),
	); err != nil {
		t.Fatal(err)
	}
	if err := overrideBatch.Send(); err != nil {
		t.Fatal(err)
	}
	if manualMembers := readLinearTeamManualMembers(t, ctx, conn, claim.OrgID, teamID); len(manualMembers) != 1 || manualMembers[0] != "manually-added@example.com" {
		t.Fatalf("override did not land: %v", manualMembers)
	}

	// A SECOND native sync of the same team, with no knowledge of the
	// override, must not reset it.
	secondWrite, err := effectBatchFromValues(linearReferenceCatalogTeamsDestination, EffectReadbackRequired, []linearReferenceTeamRow{{
		ID: teamID, TeamUUID: teamUUID, Name: "Engineering", Members: []string{"linear:alice@example.com", "linear:bob@example.com"},
		ProjectKeys: []string{teamKey}, RepoPatterns: []string{}, IsActive: 1, UpdatedAt: now.Add(2 * time.Second),
		OrgID: claim.OrgID, Provider: "linear", NativeTeamKey: &teamKey,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, secondWrite); err != nil {
		t.Fatalf("second write: %v", err)
	}
	if manualMembers := readLinearTeamManualMembers(t, ctx, conn, claim.OrgID, teamID); len(manualMembers) != 1 || manualMembers[0] != "manually-added@example.com" {
		t.Fatalf("second native write did not carry manual_members forward: %v", manualMembers)
	}
}

func readLinearTeamManualMembers(t *testing.T, ctx context.Context, conn driver.Conn, orgID, teamID string) []string {
	t.Helper()
	row := conn.QueryRow(ctx, "SELECT manual_members FROM teams FINAL WHERE org_id = ? AND provider = 'linear' AND id = ?", orgID, teamID)
	var manualMembers []string
	if err := row.Scan(&manualMembers); err != nil {
		t.Fatalf("read manual_members: %v", err)
	}
	return manualMembers
}

func TestLinearReferenceCatalogEffectsHonorLeaseBeforeWrite(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("linear", "work-items")
	now := time.Date(2026, 8, 10, 12, 34, 56, 789000000, time.UTC)
	rows := linearReferenceCatalogIntegrationRows(claim, now)
	effects, err := BuildLinearReferenceCatalogEffects(rows)
	if err != nil {
		t.Fatal(err)
	}
	leaseErr := errors.New("lease lost")
	sink := LinearReferenceCatalogClickHouseEffects{
		Conn:  conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return leaseErr }),
	}
	if err := sink.WriteEffect(ctx, claim, effects.Teams); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("write with lost lease error=%v", err)
	}
	inspection, err := sink.InspectEffect(ctx, claim, effects.Teams)
	if !errors.Is(err, providerfoundation.ErrLeaseLost) || inspection != EffectConflict {
		t.Fatalf("readback with lost lease inspection=%s error=%v", inspection, err)
	}
}

func linearReferenceCatalogIntegrationRows(claim Claim, now time.Time) LinearReferenceCatalogRows {
	teamKey := "ENG"
	memberID := "linear:alice@example.com"
	projectKey := "PLAT"
	projectTargetDate := linearReferenceDate("2026-09-30")
	return LinearReferenceCatalogRows{
		Teams: []linearReferenceTeamRow{{
			ID: "ENG", TeamUUID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:ENG")).String(),
			Name: "Engineering", Members: []string{"linear:alice@example.com", "alice@example.com"}, ProjectKeys: []string{teamKey},
			RepoPatterns: []string{}, IsActive: 1, UpdatedAt: now, OrgID: claim.OrgID,
			Provider: "linear", NativeTeamKey: &teamKey,
		}},
		Members: []linearReferenceMemberRow{{
			OrgID: claim.OrgID, MemberID: memberID, Name: "Alice", Email: linearReferenceStringPtr("alice@example.com"),
			ProviderIdentities: `{"linear": ["alice@example.com"]}`, IsActive: 1, UpdatedAt: now,
		}},
		Memberships: []linearReferenceMembershipRow{{
			OrgID: claim.OrgID, Provider: "linear", TeamID: "ENG", MemberID: memberID,
			RawProviderUserID: linearReferenceStringPtr("alice@example.com"), RawEmail: linearReferenceStringPtr("alice@example.com"),
			IdentityFacets: []string{"linear:alice@example.com", "alice@example.com"}, Source: "native", IsPrimary: 1, Specificity: 100,
			Priority: 10, ValidFrom: now, UpdatedAt: now,
		}},
		Projects: []linearReferenceProjectRow{{
			ID: "project-1", OrgID: claim.OrgID, Provider: "linear", ProjectKey: &projectKey,
			Name: "Platform", IsActive: 1, State: "started", TargetDate: &projectTargetDate,
			URL: "https://linear.app/project-1", TeamIDs: []string{"team-1"}, TeamKeys: []string{"ENG"},
			LeadID: linearReferenceStringPtr("user-7"), LeadName: linearReferenceStringPtr("Alice"),
			LeadEmail: linearReferenceStringPtr("alice@example.com"), UpdatedAt: now, LastSynced: now,
		}},
		Ownership: []linearReferenceOwnershipRow{{
			OrgID: claim.OrgID, Provider: "linear", TeamID: "ENG", ProjectID: "project-1",
			ProjectKey: &projectKey, Source: "native", IsPrimary: 1, Specificity: 100,
			Priority: 10, ValidFrom: now, UpdatedAt: now,
		}},
		Sprints: []linearSprintRow{{
			OrgID: claim.OrgID, Provider: "linear", SprintID: "linear:cycle:1",
			Name: linearReferenceStringPtr("Cycle 1"), State: linearReferenceStringPtr("active"),
			NativeTeamKey: &teamKey, LastSynced: now,
		}},
	}
}

var errLinearReferenceCrashAfterWrite = errors.New("linear reference crash after durable write")

type linearReferenceCrashAfterWriteSink struct {
	inner       EffectSink
	destination string
	crashed     bool
}

func (sink *linearReferenceCrashAfterWriteSink) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if err := sink.inner.WriteEffect(ctx, claim, effect); err != nil {
		return err
	}
	if effect.Destination == sink.destination && !sink.crashed {
		sink.crashed = true
		return errLinearReferenceCrashAfterWrite
	}
	return nil
}
