//go:build integration

package acr

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	devclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pinrig"
)

// accessReadsSplit is go-api's read set as production holds it: the public
// schema through the api role (here the admin pool), the auth sessions through
// the session reader role with only the columns auth-migrate grants a reader.
type accessReadsSplit struct {
	policy.PGStore
	sessions policy.PGStore
}

func (s accessReadsSplit) SessionState(ctx context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	return s.sessions.SessionState(ctx, id)
}

type staticInventory []string

func (s staticInventory) Repositories(context.Context, uuid.UUID) ([]string, error) { return s, nil }

func startAccessPostgres(t *testing.T, ctx context.Context) (*pgxpool.Pool, *containers.Instance) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	return pool, instance
}

func seedAccessUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, name string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'not-a-hash', true, true, false, 0, now(), now())`, id, "access-"+name+"@example.com"); err != nil {
		t.Fatalf("seed user %s: %v", name, err)
	}
}

func seedMembership(t *testing.T, ctx context.Context, pool *pgxpool.Pool, user, org uuid.UUID) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), user, org); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
}

func setFeature(t *testing.T, ctx context.Context, pool *pgxpool.Pool, org uuid.UUID, key string, enabled bool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `DELETE FROM org_feature_overrides WHERE org_id = $1 AND feature_id = (SELECT id FROM feature_flags WHERE key = $2)`, org, key); err != nil {
		t.Fatal(err)
	}
	var featureID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM feature_flags WHERE key = $1`, key).Scan(&featureID); err != nil {
		t.Fatalf("feature %s is not registered by the migrations: %v", key, err)
	}
	pgseed.OrgOverride(ctx, t, pool, org.String(), featureID, enabled)
}

// TestPrincipalAccessReadsLiveStateEndToEnd drives the production store over a
// migrated public schema and a real auth schema whose sessions auth-service's
// own session store writes. The session read runs as the session reader role:
// the existing grant is enough for the read (no new auth grant).
func TestPrincipalAccessReadsLiveStateEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool, instance := startAccessPostgres(t, ctx)
	rig := pinrig.NewPinRig(t, ctx, pool, instance)

	org, otherOrg := uuid.New(), uuid.New()
	pgseed.Org(ctx, t, pool, org.String(), "community")
	pgseed.Org(ctx, t, pool, otherOrg.String(), "community")
	alice, bob := uuid.New(), uuid.New()
	seedAccessUser(t, ctx, pool, alice, "alice")
	seedAccessUser(t, ctx, pool, bob, "bob")
	seedMembership(t, ctx, pool, alice, org)
	seedMembership(t, ctx, pool, bob, org)
	setFeature(t, ctx, pool, org, "ask_dev", true)
	setFeature(t, ctx, pool, org, "agent_context_runtime", true)

	store := AccessStore{
		Organizations: postgresOrganizations{pool: pool},
		Reads:         accessReadsSplit{PGStore: policy.PGStore{Pool: pool}, sessions: rig.Reader()},
		Features:      NewPostgresAccessStore(pool, policy.PGStore{Pool: pool}, nil).Features,
		Inventory:     staticInventory{"acme/api"},
	}
	read := func(t *testing.T, orgID, user uuid.UUID, sid *uuid.UUID) PrincipalAccess {
		t.Helper()
		access, err := store.Access(ctx, AccessQuery{OrgID: orgID, UserID: user, SessionID: sid})
		if err != nil {
			t.Fatalf("access: %v", err)
		}
		return access
	}
	aliceSession := rig.SignIn(t, ctx, alice)
	bobSession := rig.SignIn(t, ctx, bob)

	t.Run("live session, member, both flags, inventory", func(t *testing.T) {
		got := read(t, org, alice, &aliceSession)
		want := PrincipalAccess{Member: true, Session: &SessionAccess{Live: true, Reason: sessionReasonLive},
			AskDev: true, AgentContextRuntime: true, Repositories: []string{"acme/api"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("access = %+v session %+v", got, got.Session)
		}
	})
	t.Run("another user's sid", func(t *testing.T) {
		if got := read(t, org, alice, &bobSession); got.Session.Live || got.Session.Reason != sessionReasonOtherUser {
			t.Fatalf("session = %+v", got.Session)
		}
	})
	t.Run("unknown sid", func(t *testing.T) {
		unknown := uuid.New()
		if got := read(t, org, alice, &unknown); got.Session.Live || got.Session.Reason != sessionReasonNotFound {
			t.Fatalf("session = %+v", got.Session)
		}
	})
	t.Run("not a member of the other org", func(t *testing.T) {
		if got := read(t, otherOrg, alice, &aliceSession); got.Member {
			t.Fatalf("member of an org with no membership row: %+v", got)
		}
	})
	t.Run("impersonation on, then off", func(t *testing.T) {
		impersonation := uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO impersonation_sessions (id, admin_user_id, target_user_id, target_org_id, target_role, expires_at, created_at)
VALUES ($1, $2, $3, $4, 'member', now() + interval '1 hour', now())`, impersonation, alice, bob, org); err != nil {
			t.Fatal(err)
		}
		if got := read(t, org, alice, &aliceSession); !got.Impersonating || !got.Session.Impersonating {
			t.Fatalf("impersonating = %v (session %+v) while an impersonation is active", got.Impersonating, got.Session)
		}
		if got := read(t, org, alice, nil); !got.Impersonating {
			t.Fatal("a row-only read (no sid) does not see an active impersonation")
		}
		if got := read(t, org, bob, &bobSession); got.Session.Impersonating {
			t.Fatal("the impersonated user is not the one impersonating")
		}
		if _, err := pool.Exec(ctx, `UPDATE impersonation_sessions SET ended_at = now() WHERE id = $1`, impersonation); err != nil {
			t.Fatal(err)
		}
		if got := read(t, org, alice, &aliceSession); got.Impersonating || got.Session.Impersonating {
			t.Fatal("impersonating = true after the impersonation ended")
		}
	})
	t.Run("flag off is read on the next call", func(t *testing.T) {
		setFeature(t, ctx, pool, org, "ask_dev", false)
		if got := read(t, org, alice, nil); got.AskDev || !got.AgentContextRuntime {
			t.Fatalf("ask_dev=%v agent_context_runtime=%v after ask_dev off", got.AskDev, got.AgentContextRuntime)
		}
		setFeature(t, ctx, pool, org, "ask_dev", true)
	})
	t.Run("stale token_version", func(t *testing.T) {
		session := rig.SignIn(t, ctx, bob)
		if _, err := pool.Exec(ctx, `UPDATE users SET token_version = token_version + 1 WHERE id = $1`, bob); err != nil {
			t.Fatal(err)
		}
		if got := read(t, org, bob, &session); got.Session.Live || got.Session.Reason != sessionReasonVersionStale {
			t.Fatalf("session = %+v after a token_version bump", got.Session)
		}
	})
	t.Run("revoked session", func(t *testing.T) {
		session := rig.SignIn(t, ctx, alice)
		if got := read(t, org, alice, &session); !got.Session.Live {
			t.Fatalf("new session not live: %+v", got.Session)
		}
		rig.Revoke(t, ctx, session)
		if got := read(t, org, alice, &session); got.Session.Live || got.Session.Reason != sessionReasonRevoked {
			t.Fatalf("session = %+v after revoke", got.Session)
		}
		if got := read(t, org, alice, &aliceSession); !got.Session.Live {
			t.Fatal("revoking one session ended another")
		}
	})
	t.Run("membership removed is read on the next call", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `DELETE FROM memberships WHERE user_id = $1 AND org_id = $2`, bob, org); err != nil {
			t.Fatal(err)
		}
		if got := read(t, org, bob, nil); got.Member {
			t.Fatal("member after the membership row was deleted")
		}
	})
	t.Run("deactivated user", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `UPDATE users SET is_active = false WHERE id = $1`, alice); err != nil {
			t.Fatal(err)
		}
		if got := read(t, org, alice, nil); got.Member {
			t.Fatal("a deactivated user reads as a member")
		}
		if _, err := pool.Exec(ctx, `UPDATE users SET is_active = true WHERE id = $1`, alice); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("unknown org", func(t *testing.T) {
		if _, err := store.Access(ctx, AccessQuery{OrgID: uuid.New(), UserID: alice}); !errors.Is(err, ErrOrgUnknown) {
			t.Fatalf("err = %v, want ErrOrgUnknown", err)
		}
	})
	t.Run("store outage is unavailable", func(t *testing.T) {
		closed, err := pgxpool.New(ctx, instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		closed.Close()
		down := store
		down.Organizations = postgresOrganizations{pool: closed}
		if _, err := down.Access(ctx, AccessQuery{OrgID: org, UserID: alice}); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("err = %v, want ErrUnavailable", err)
		}
	})

	// Latency receipt (CHAOS-12084 asks for p50/p95; target p95 < 100 ms). The
	// compose stack is not usable for lanes (2026-10-29), so the numbers come
	// from this run's containers. Recorded, not asserted: CI runners vary.
	const samples = 200
	durations := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		started := time.Now()
		if _, err := store.Access(ctx, AccessQuery{OrgID: org, UserID: alice, SessionID: &aliceSession}); err != nil {
			t.Fatal(err)
		}
		durations = append(durations, time.Since(started))
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	t.Logf("RECEIPT principal_access_postgres samples=%d p50=%s p95=%s max=%s", samples,
		durations[samples/2], durations[samples*95/100], durations[samples-1])
}

// TestClickHouseRepositoryInventoryEqualsTheCatalog is the differential check
// between the two readers of the inventory: go-api's ClickHouseRepositoryInventory
// on a native connection, and query-api's catalog REPO dimension (the list web
// reads as the org's repository scope) on the dev-health-go query client, over
// the same migrated repos table.
func TestClickHouseRepositoryInventoryEqualsTheCatalog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := devclickhouse.NewClickHouseQueryClientWithOptions(devclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	org, other, empty := uuid.New(), uuid.New(), uuid.New()
	insert := func(orgID uuid.UUID, repo string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)`,
			uuid.New(), repo, orgID.String(), "github", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	for _, repo := range []string{"Acme/Web", " acme/api ", "acme/api", "not-a-slug", "a/b/c"} {
		insert(org, repo)
	}
	insert(other, "elsewhere/repo")

	inventory := ClickHouseRepositoryInventory{Conn: conn}
	catalog := func(orgID uuid.UUID) []string {
		t.Helper()
		result, degraded, err := analytics.ResolveCatalog(ctx, client, orgID.String(), dimensionRef(model.DimensionInputRepo), nil)
		if err != nil || degraded {
			t.Fatalf("catalog: %v degraded=%v", err, degraded)
		}
		values := make([]string, 0, len(result.Values))
		for _, value := range result.Values {
			values = append(values, value.Value)
		}
		return values
	}
	for name, orgID := range map[string]uuid.UUID{"seeded": org, "other": other, "empty": empty} {
		got, err := inventory.Repositories(ctx, orgID)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if want := catalog(orgID); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: go-api inventory %v, catalog %v", name, got, want)
		}
	}
	if got, _ := inventory.Repositories(ctx, org); !reflect.DeepEqual(got, []string{"acme/api", "acme/web"}) {
		t.Fatalf("seeded inventory = %v", got)
	}
	if got, _ := inventory.Repositories(ctx, empty); got == nil || len(got) != 0 {
		t.Fatalf("empty inventory = %#v, want a non-nil empty list", got)
	}

	const samples = 200
	durations := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		started := time.Now()
		if _, err := inventory.Repositories(ctx, org); err != nil {
			t.Fatal(err)
		}
		durations = append(durations, time.Since(started))
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	t.Logf("RECEIPT principal_access_clickhouse samples=%d p50=%s p95=%s max=%s", samples,
		durations[samples/2], durations[samples*95/100], durations[samples-1])
}

func dimensionRef(d model.DimensionInput) *model.DimensionInput { return &d }
