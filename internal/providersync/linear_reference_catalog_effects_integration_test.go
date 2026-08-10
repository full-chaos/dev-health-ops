//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

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
