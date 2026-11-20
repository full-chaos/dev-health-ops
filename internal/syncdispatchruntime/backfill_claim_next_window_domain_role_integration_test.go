//go:build integration

package syncdispatchruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
)

const backfillNextOrgID = "org-backfill-next"

type backfillNextVenue struct {
	venue         domainroledb.Venue
	repository    *providersync.PostgresRepository
	integrationID string
	sourceID      string
	otherSourceID string
	costClass     string
}

// startBackfillNextVenue is the real migrated schema with the go-river-migrate
// grants: seeds go through Admin, claims through the domain role (NOSUPERUSER).
func startBackfillNextVenue(t *testing.T, ctx context.Context) backfillNextVenue {
	t.Helper()
	venue := domainroledb.Start(t, ctx, "backfill_next")
	venue.RequireReady(t, ctx)
	capability, ok := providersync.Capability("github", "tests")
	if !ok {
		t.Fatal("no github/tests capability")
	}
	integrationID, sourceID := uuid.NewString(), uuid.NewString()
	otherSourceID := uuid.NewString()
	pgseed.EnsureSyncIntegration(ctx, t, venue.Admin, backfillNextOrgID, integrationID, sourceID)
	pgseed.EnsureSyncIntegration(ctx, t, venue.Admin, backfillNextOrgID, integrationID, otherSourceID)
	repository, err := providersync.NewPostgresRepository(venue.Domain)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repository.Close)
	return backfillNextVenue{venue: venue, repository: repository, integrationID: integrationID, sourceID: sourceID,
		otherSourceID: otherSourceID, costClass: string(capability.CostClass)}
}

func (fixture backfillNextVenue) run(t *testing.T, ctx context.Context) string {
	t.Helper()
	runID := uuid.NewString()
	pgseed.EnsureSyncRun(ctx, t, fixture.venue.Admin, pgseed.SyncRun{ID: runID, OrgID: backfillNextOrgID, IntegrationID: fixture.integrationID})
	if _, err := fixture.venue.Admin.Exec(ctx,
		`UPDATE public.sync_runs SET mode = 'backfill', credential_id = $2::uuid WHERE id = $1::uuid`,
		runID, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	return runID
}

type backfillNextUnit struct {
	runID, sourceID, dataset, mode string
	since, before                  time.Time
}

func (fixture backfillNextVenue) unit(t *testing.T, ctx context.Context, unit backfillNextUnit) string {
	t.Helper()
	id := uuid.NewString()
	costClass := fixture.costClass
	if unit.dataset != "tests" {
		capability, ok := providersync.Capability("github", unit.dataset)
		if !ok {
			t.Fatalf("no github/%s capability", unit.dataset)
		}
		costClass = string(capability.CostClass)
	}
	since, before := unit.since, unit.before
	pgseed.InsertSyncRunUnit(ctx, t, fixture.venue.Admin, pgseed.SyncRunUnit{
		ID: id, RunID: unit.runID, OrgID: backfillNextOrgID, IntegrationID: fixture.integrationID, SourceID: unit.sourceID,
		Provider: "github", DatasetKey: unit.dataset, CostClass: costClass, Mode: unit.mode, Status: "dispatching",
		SinceAt: &since, BeforeAt: &before,
	})
	return id
}

func (fixture backfillNextVenue) claim(t *testing.T, ctx context.Context, unitID string, now time.Time) providersync.Claim {
	t.Helper()
	claim, err := fixture.repository.Claim(ctx, providersync.ClaimRequest{
		UnitID: unitID, OrgID: backfillNextOrgID, Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatalf("claim %s as the domain role: %v (refused: %q)", unitID, err, fixture.venue.Denials.All())
	}
	return claim
}

func backfillNextString(next *providersync.BackfillNextWindow) string {
	switch {
	case next == nil:
		return "nil (tiling not known)"
	case next.BeforeAt == nil:
		return "last chunk"
	default:
		return "next before " + next.BeforeAt.UTC().Format(time.RFC3339)
	}
}

func requireBackfillNext(t *testing.T, name string, got *providersync.BackfillNextWindow, want *providersync.BackfillNextWindow) {
	t.Helper()
	t.Logf("CELL %s: BackfillNext = %s", name, backfillNextString(got))
	if (got == nil) != (want == nil) {
		t.Fatalf("%s: BackfillNext = %s, want %s", name, backfillNextString(got), backfillNextString(want))
	}
	if got == nil {
		return
	}
	if (got.BeforeAt == nil) != (want.BeforeAt == nil) ||
		(got.BeforeAt != nil && !got.BeforeAt.Equal(*want.BeforeAt)) {
		t.Fatalf("%s: BackfillNext = %s, want %s", name, backfillNextString(got), backfillNextString(want))
	}
}

func nextBefore(at time.Time) *providersync.BackfillNextWindow {
	return &providersync.BackfillNextWindow{BeforeAt: &at}
}

// CHAOS-14222 (acp conditions A, 2, 4). The claim SQL, run as the domain role
// on the migrated schema and grants, gives each backfill unit the window of the
// sibling unit that starts at its before_at: same run, source, dataset_key and
// mode 'backfill'. Rows that differ in only one of those, or in since_at, are
// never picked. The last chunk gets "last"; an incremental unit gets nil.
func TestBackfillClaimCarriesTheNextChunkAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	fixture := startBackfillNextVenue(t, ctx)
	d := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	run, otherRun := fixture.run(t, ctx), fixture.run(t, ctx)
	tests := func(since, before time.Time) backfillNextUnit {
		return backfillNextUnit{runID: run, sourceID: fixture.sourceID, dataset: "tests", mode: "backfill", since: since, before: before}
	}
	first := fixture.unit(t, ctx, tests(d, d.AddDate(0, 0, 7)))
	middle := fixture.unit(t, ctx, tests(d.AddDate(0, 0, 7), d.AddDate(0, 0, 14)))
	last := fixture.unit(t, ctx, tests(d.AddDate(0, 0, 14), d.AddDate(0, 0, 15)))
	// Near misses at since_at = the first unit's before_at, each with an
	// earlier before_at than the real next chunk, so picking one shows.
	nearMiss := tests(d.AddDate(0, 0, 7), d.AddDate(0, 0, 8))
	nearMiss.sourceID = fixture.otherSourceID
	fixture.unit(t, ctx, nearMiss)
	nearMiss = tests(d.AddDate(0, 0, 7), d.AddDate(0, 0, 7).Add(2*time.Hour))
	nearMiss.dataset = "commits"
	fixture.unit(t, ctx, nearMiss)
	nearMiss = tests(d.AddDate(0, 0, 7), d.AddDate(0, 0, 7).Add(time.Hour))
	nearMiss.mode = "incremental"
	incremental := fixture.unit(t, ctx, nearMiss)
	nearMiss = tests(d.AddDate(0, 0, 7), d.AddDate(0, 0, 9))
	nearMiss.runID = otherRun
	fixture.unit(t, ctx, nearMiss)

	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	requireBackfillNext(t, "first chunk", fixture.claim(t, ctx, first, now).BackfillNext, nextBefore(d.AddDate(0, 0, 14)))
	requireBackfillNext(t, "middle chunk", fixture.claim(t, ctx, middle, now).BackfillNext, nextBefore(d.AddDate(0, 0, 15)))
	requireBackfillNext(t, "last chunk", fixture.claim(t, ctx, last, now).BackfillNext, &providersync.BackfillNextWindow{})
	requireBackfillNext(t, "incremental unit", fixture.claim(t, ctx, incremental, now).BackfillNext, nil)
	if refused := fixture.venue.Denials.All(); len(refused) > 0 {
		t.Fatalf("the domain role was refused: %s", strings.Join(refused, "; "))
	}
}

// CHAOS-14222 (acp condition 2). Two sibling rows start at the claimed unit's
// before_at (a duplicate row). The claim picks ONE window, the earliest
// before_at (the later listing floor: a run is read, never dropped), and picks
// the same one on a recovered claim.
func TestBackfillClaimPicksOneNextChunkFromDuplicateSiblings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	fixture := startBackfillNextVenue(t, ctx)
	e := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	run := fixture.run(t, ctx)
	tests := func(since, before time.Time) backfillNextUnit {
		return backfillNextUnit{runID: run, sourceID: fixture.sourceID, dataset: "tests", mode: "backfill", since: since, before: before}
	}
	unit := fixture.unit(t, ctx, tests(e, e.AddDate(0, 0, 7)))
	fixture.unit(t, ctx, tests(e.AddDate(0, 0, 7), e.AddDate(0, 0, 14)))
	fixture.unit(t, ctx, tests(e.AddDate(0, 0, 7), e.AddDate(0, 0, 10)))
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	requireBackfillNext(t, "duplicate siblings", fixture.claim(t, ctx, unit, now).BackfillNext, nextBefore(e.AddDate(0, 0, 10)))
	requireBackfillNext(t, "duplicate siblings, recovered claim", fixture.claim(t, ctx, unit, now.Add(time.Hour)).BackfillNext, nextBefore(e.AddDate(0, 0, 10)))
	if refused := fixture.venue.Denials.All(); len(refused) > 0 {
		t.Fatalf("the domain role was refused: %s", strings.Join(refused, "; "))
	}
}
