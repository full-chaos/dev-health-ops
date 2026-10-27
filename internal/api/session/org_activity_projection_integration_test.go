//go:build integration

package session

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/orgactivity"
	"github.com/full-chaos/dev-health-ops/internal/orgactivity/refresh"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// go-api is the oracle. On one seed in ClickHouse (measured rows, an organization
// whose only newest rows are retraction rows, an organization with no rows at
// all) the activity go-api reads live equals the projection the finalize job
// writes, and the organization each plane chooses for a user holding all of them
// is the same, for every ordering of the memberships.
func TestOrgActivityProjectionEqualsWhatGoAPIReadsLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)

	control := uuid.MustParse(retractionseed.ControlOrg)
	retracted := uuid.MustParse(retractionseed.RetractedOrg)
	only := uuid.MustParse(retractionseed.RetractionOnlyOrg)
	empty := uuid.MustParse("c0c0c0c0-0000-4000-8000-0000000000ee")
	day := store.Days[len(store.Days)-1]
	for _, team := range retractionseed.Teams {
		retractionseed.Retract(ctx, t, store.Conn, retractionseed.RetractionOnlyOrg, day, team, store.OldComputedAt, store.NewComputedAt)
	}
	orgs := []uuid.UUID{only, empty, control, retracted}
	for index, id := range orgs {
		if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, id, "projection-"+string(rune('a'+index))); err != nil {
			t.Fatalf("seed organization: %v", err)
		}
	}

	refresher := &refresh.Refresher{CH: store.Conn, DB: pool, Logger: slog.Default()}
	written, skipped, err := refresher.RefreshMissing(ctx, 100)
	if err != nil || written != len(orgs) || skipped != 0 {
		t.Fatalf("RefreshMissing = %d written, %d skipped, %v; want %d, 0", written, skipped, err, len(orgs))
	}

	h := handlers{Deps: Deps{ClickHouse: store.Conn, Logger: slog.Default()}}
	compare := func(stage string) (live map[uuid.UUID]activity, projected map[uuid.UUID]orgactivity.Activity) {
		t.Helper()
		live = h.orgActivity(ctx, orgs)
		projected = map[uuid.UUID]orgactivity.Activity{}
		for _, id := range orgs {
			projected[id] = orgactivity.Activity{Unknown: true}
		}
		rows, err := pool.Query(ctx, `SELECT org_id, has_data, computed_at FROM public.org_activity WHERE org_id = ANY($1)`, orgs)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id uuid.UUID
			var hasData bool
			var computedAt *time.Time
			if err := rows.Scan(&id, &hasData, &computedAt); err != nil {
				t.Fatal(err)
			}
			if computedAt != nil {
				value := computedAt.UTC()
				computedAt = &value
			}
			projected[id] = orgactivity.Activity{HasData: hasData, Last: computedAt}
		}
		rows.Close()
		for _, id := range orgs {
			goAPI, auth := live[id], projected[id]
			if goAPI.unknown || auth.Unknown {
				t.Fatalf("%s: org %s: unknown (go-api %v, projection %v)", stage, id, goAPI.unknown, auth.Unknown)
			}
			if goAPI.hasData != auth.HasData {
				t.Errorf("%s: org %s: has data: go-api %v, projection %v", stage, id, goAPI.hasData, auth.HasData)
			}
			if goAPI.hasData && (goAPI.last == nil || auth.Last == nil || !goAPI.last.Equal(*auth.Last)) {
				t.Errorf("%s: org %s: newest metrics: go-api %v, projection %v", stage, id, goAPI.last, auth.Last)
			}
		}
		return live, projected
	}
	live, projected := compare("first fill")
	if !projected[control].HasData || projected[only].HasData || projected[empty].HasData {
		t.Fatalf("projection facts wrong: control %+v retraction-only %+v empty %+v", projected[control], projected[only], projected[empty])
	}

	// A later retraction wave over the control organization, then the finalize hook runs again: the projection follows
	// what go-api reads live (a retraction row is not data and is not newer than the last measurement).
	retractionseed.Retract(ctx, t, store.Conn, retractionseed.ControlOrg, day, retractionseed.Teams[0], store.NewComputedAt, store.NewComputedAt.Add(time.Hour))
	if err := refresher.Refresh(ctx, control); err != nil {
		t.Fatal(err)
	}
	live, projected = compare("after a retraction and a second refresh")
	_ = live

	joined := func(n int) *time.Time { v := time.Date(2026, 1, 1+n, 0, 0, 0, 0, time.UTC); return &v }
	orderings := [][]uuid.UUID{
		{only, empty, control, retracted}, {empty, only, retracted, control}, {control, retracted, only, empty}, {retracted, only, empty, control},
	}
	for index, order := range orderings {
		rows := make([]membershipRow, len(order))
		candidates := make([]orgactivity.Membership, len(order))
		for position, id := range order {
			rows[position] = membershipRow{OrgID: id, JoinedAt: joined(position), CreatedAt: *joined(position)}
			candidates[position] = orgactivity.Membership{OrgID: id, JoinedAt: rows[position].JoinedAt, CreatedAt: rows[position].CreatedAt}
		}
		want := selectActiveMembership(rows, nil, live)
		got := orgactivity.Select(candidates, nil, projected)
		if want == nil || got < 0 || rows[got].OrgID != want.OrgID {
			t.Errorf("ordering %d: go-api chose %v, the projection chose index %d", index, want, got)
		}
	}
}
