//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLinearTypedCompletionAtomicallyAuditsFiveAliasesAndWatermarks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	unitID := uuid.NewString()
	linearFlags, err := json.Marshal(allLinearFamilyFlags(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.integration_datasets
  (id, org_id, integration_id, dataset_key, options)
VALUES ($1, 'org-acme', $2, 'work-items', '{}'::jsonb)`, uuid.NewString(), firstIntegrationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
    id, org_id, sync_run_id, integration_id, source_id, provider,
    dataset_key, cost_class, mode, since_at, before_at, status,
    processor_flags, updated_at
) VALUES (
    $1, 'org-acme', $2, $3, $4, 'linear', 'work-items', 'medium',
    'incremental', '2026-08-09T12:00:00Z', '2026-08-10T12:00:00Z',
    'dispatching', $5::jsonb, $6)`,
		unitID, firstRunID, firstIntegrationID, firstSourceID, string(linearFlags), now); err != nil {
		t.Fatal(err)
	}
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Provider != "linear" || claim.Dataset != "work-items" ||
		len(claim.ProcessorFlags) != len(allLinearFamilyFlags(true)) {
		t.Fatalf("linear claim=%+v", claim)
	}

	_, route := linearTypedLifecycleFixture(t)
	route.Watermark = claim.BeforeAt
	route.Result.Evidence = route.Evidence
	audits := make([]LinearWorkItemEffectAudit, 0, len(route.Effects))
	for _, effect := range route.Effects {
		audits = append(audits, LinearWorkItemEffectAudit{
			Destination: effect.Destination, Rows: len(effect.Rows),
			ContentDigest: effect.ContentDigest, Readback: LinearAliasReadbackExact,
		})
	}
	result, err := buildLinearWorkItemsCompletionResult(claim, route, audits, *claim.BeforeAt)
	if err != nil {
		t.Fatal(err)
	}
	completedAt := now.Add(30 * time.Second)
	if err := repository.CompleteLinearWorkItemFamily(ctx, claim, result, now, completedAt); err != nil {
		t.Fatal(err)
	}

	var status string
	var persistedResult []byte
	if err := pool.QueryRow(ctx, `
SELECT status, result::jsonb
FROM public.sync_run_units WHERE id = $1`, unitID).Scan(&status, &persistedResult); err != nil {
		t.Fatal(err)
	}
	var persisted LinearWorkItemsCompletionResult
	if err := json.Unmarshal(persistedResult, &persisted); err != nil {
		t.Fatal(err)
	}
	if status != "success" || len(persisted.Aliases) != len(workitemcontract.FamilyDatasets()) ||
		persisted.Generation != claim.GenerationKey() {
		t.Fatalf("status=%q persisted=%+v", status, persisted)
	}
	var watermarkCount int
	var watermarkKeys string
	if err := pool.QueryRow(ctx, `
SELECT count(*), string_agg(dataset_key, ',' ORDER BY dataset_key)
FROM public.sync_watermarks
WHERE org_id = 'org-acme' AND source_id = 'acme/api'
  AND dataset_key IN (
    'work-items', 'work-item-labels', 'work-item-projects',
    'work-item-history', 'work-item-comments'
  ) AND last_synced_at = $1`, *claim.BeforeAt).Scan(&watermarkCount, &watermarkKeys); err != nil {
		t.Fatal(err)
	}
	wantKeys := "work-item-comments,work-item-history,work-item-labels,work-item-projects,work-items"
	if watermarkCount != 5 || watermarkKeys != wantKeys {
		t.Fatalf("alias watermark count=%d keys=%q", watermarkCount, watermarkKeys)
	}

	// The validator runs before the transaction. An unsupported alias flag must
	// leave both the unit and every watermark untouched, proving the five writes
	// cannot be partially committed from an invalid audit.
	badUnitID := uuid.NewString()
	badFlags := `{"family_dataset_work_items":true,"family_dataset_future_alias":true}`
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
    id, org_id, sync_run_id, integration_id, source_id, provider,
    dataset_key, cost_class, mode, since_at, before_at, status,
    processor_flags, updated_at
) VALUES (
    $1, 'org-acme', $2, $3, $4, 'linear', 'work-items', 'medium',
    'incremental', '2026-08-09T12:00:00Z', '2026-08-10T12:00:00Z',
    'dispatching', $5::jsonb, $6)`,
		badUnitID, firstRunID, firstIntegrationID, firstSourceID, badFlags, now); err != nil {
		t.Fatal(err)
	}
	badClaim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: badUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.CompleteLinearWorkItemFamily(ctx, badClaim, result, now, completedAt); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid alias completion error=%v", err)
	}
	var badStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM public.sync_run_units WHERE id = $1`, badUnitID).Scan(&badStatus); err != nil {
		t.Fatal(err)
	}
	if badStatus != "running" {
		t.Fatalf("invalid alias status=%q", badStatus)
	}
}

func TestLinearTypedLifecycleUsesMigratedClickHouseAndRecoversExactWrite(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	claim.ProcessorFlags = allLinearFamilyFlags(true)
	now := time.Date(2026, 8, 10, 12, 0, 0, 123000000, time.UTC)
	row := LinearWorkItemRow{
		WorkItemID: "linear:ENG-3718", Provider: "linear", Title: "Linear lifecycle proof",
		Type: "task", Status: "in_progress", StatusRaw: stringPtr("In Progress"),
		NativeTeamKey: stringPtr("ENG"), Assignees: []string{"linear@example.com"},
		CreatedAt: now.Add(-time.Hour), UpdatedAt: now, StartedAt: &now,
		Labels: []string{"lifecycle"}, OrgID: claim.OrgID, LastSynced: now,
	}
	rowRaw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	effects, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems: []json.RawMessage{rowRaw},
	})
	if err != nil {
		t.Fatal(err)
	}
	route := LinearWorkItemsRouteBatch{
		Effects: effects, Watermark: claim.BeforeAt,
		Evidence: FetchEvidence{Provider: "linear", Dataset: "work-items", Requests: 3, Pages: 3, Records: 1},
		Result: LinearWorkItemsRouteResult{
			Rows:   LinearWorkItemsRows{WorkItems: []LinearWorkItemRow{row}},
			Counts: LinearWorkItemCounts{WorkItems: 1}, NonEmpty: true,
			Evidence: FetchEvidence{Provider: "linear", Dataset: "work-items", Requests: 3, Pages: 3, Records: 1},
		},
	}
	lease := &linearLifecycleCountingLease{failAt: 12}
	sink := linearMigratedClickHouseSink(conn, lease)
	ledger := &memoryEffectLedger{}
	lifecycle := LinearWorkItemsLifecycle{Committer: EffectCommitter{
		Ledger: ledger, Sink: sink, Readback: sink, Now: func() time.Time { return now },
	}}
	if _, _, err := lifecycle.Commit(ctx, claim, route, now); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("first lease-fenced commit error=%v", err)
	}
	recoveredSink := linearMigratedClickHouseSink(conn, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	result, commit, err := (LinearWorkItemsLifecycle{Committer: EffectCommitter{
		Ledger: ledger, Sink: recoveredSink, Readback: recoveredSink, Now: func() time.Time { return now.Add(time.Minute) },
	}}).Commit(ctx, claim, route, now)
	if err != nil {
		t.Fatal(err)
	}
	// Effects commit in ALPHABETICAL destination order (effectBatchLess), not
	// manifest order. CHAOS-4193's two new destinations --
	// project_membership_transitions, projects -- sort before "sprints",
	// pushing "work_items" (this fixture's only nonempty destination, and
	// previously the alphabetically-last, LAST-committed one out of 6) to
	// position 8 of 8. failAt=12 (2 lease asserts per write) now lands on the
	// EMPTY work_item_reopen_events at position 6 instead: its write happens
	// (a no-op, since it carries no rows) and then the ack assert fails,
	// leaving it ambiguous. Recovery reads it back as EffectAbsent (nothing
	// was ever really written) rather than EffectExact, so it resets and
	// replays instead of being marked committed -- and the two still-untried
	// destinations (work_item_transitions, work_items) get written fresh in
	// the same pass.
	if commit.MarkedCommitted != 0 || commit.ResetForReplay != 1 || commit.Written != 3 || commit.Skipped != 5 {
		t.Fatalf("recovery commit=%+v", commit)
	}
	if err := ValidateLinearWorkItemsCompletion(claim, result); err != nil {
		t.Fatalf("completion validation=%v", err)
	}
}

func linearMigratedClickHouseSink(conn driver.Conn, lease providerfoundation.LeaseGuard) LinearWorkItemClickHouseEffects {
	return LinearWorkItemClickHouseEffects{
		Lease:             lease,
		WorkItems:         LinearWorkItemsClickHouseAdapter{Conn: conn},
		StatusTransitions: LinearWorkItemTransitionsClickHouseAdapter{Conn: conn},
		Dependencies: LinearWorkItemDependenciesClickHouseAdapter{
			Delegate: GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn},
		},
		ReopenEvents: LinearWorkItemReopenEventsClickHouseAdapter{
			Delegate: GitHubWorkItemReopenEventsClickHouseAdapter{Conn: conn},
		},
		Interactions: LinearWorkItemInteractionsClickHouseAdapter{
			Delegate: GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn},
		},
		Sprints: LinearSprintsClickHouseAdapter{
			Delegate: GitHubSprintsClickHouseAdapter{Conn: conn},
		},
		ProjectMemberships: LinearProjectMembershipClickHouseAdapter{
			Delegate: GitHubProjectMembershipClickHouseAdapter{Conn: conn},
		},
		Projects: LinearProjectCatalogClickHouseAdapter{
			Delegate: GitHubProjectCatalogClickHouseAdapter{Conn: conn},
		},
	}
}

type linearLifecycleCountingLease struct {
	calls  int
	failAt int
}

func (lease *linearLifecycleCountingLease) Assert(context.Context) error {
	lease.calls++
	if lease.failAt > 0 && lease.calls >= lease.failAt {
		return providerfoundation.ErrLeaseLost
	}
	return nil
}
