//go:build integration

package admin

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// deprovisionProbe is auth-service's deprovision route: at the call it reads
// what the delete has not yet removed, then answers err.
type deprovisionProbe struct {
	*fakeProvisioner
	at  func() string
	saw []string
}

func (p *deprovisionProbe) Deprovision(ctx context.Context, org, actor uuid.UUID) error {
	p.saw = append(p.saw, p.at())
	return p.fakeProvisioner.Deprovision(ctx, org, actor)
}

// With the switch on, a real delete asks auth to deprovision after the
// ClickHouse preflight and before any PagerDuty revoke or purge: at the call
// the organization's Postgres and ClickHouse rows are all still there. A
// refused or failed call answers 503 and deletes nothing; 404 (auth holds no
// such organization) lets the purge run; a dry run makes no call.
func TestOrgDeletionDeprovisionsInAuthBeforeAnyPurge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
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
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(ch.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, `CREATE TABLE deletion_probe (org_id String, n UInt8) ENGINE = MergeTree ORDER BY org_id`); err != nil {
		t.Fatal(err)
	}
	super := uuid.New()
	pgseed.User(ctx, t, pool, super.String())

	count := func(org uuid.UUID) string {
		var orgs, members int
		if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM organizations WHERE id = $1), (SELECT count(*) FROM memberships WHERE org_id = $1)`, org).
			Scan(&orgs, &members); err != nil {
			t.Fatal(err)
		}
		var rows uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM deletion_probe WHERE org_id = ?`, org.String()).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("orgs=%d members=%d ch=%d", orgs, members, rows)
	}
	seed := func() uuid.UUID {
		org := uuid.New()
		pgseed.Org(ctx, t, pool, org.String(), "community")
		pgseed.Membership(ctx, t, pool, uuid.NewString(), super.String(), org.String(), "owner")
		if err := conn.Exec(ctx, `INSERT INTO deletion_probe VALUES (?, 1)`, org.String()); err != nil {
			t.Fatal(err)
		}
		return org
	}
	const full = "orgs=1 members=1 ch=1"

	for _, tc := range []struct {
		name     string
		err      error
		query    string
		status   int
		calls    int
		after    string
		wantBody string
	}{
		{"deprovisioned", nil, "", http.StatusOK, 1, "orgs=0 members=0 ch=0", ""},
		{"auth holds no such organization", refusedWith(404, "organization_not_found"), "", http.StatusOK, 1, "orgs=0 members=0 ch=0", ""},
		{"auth down", authprovision.ErrUnavailable, "", http.StatusServiceUnavailable, 1, full, provUnavailable},
		{"auth refuses", refusedWith(409, "users_not_owned"), "", http.StatusServiceUnavailable, 1, full, provUnavailable},
		{"dry run", nil, "?dry_run=true", http.StatusOK, 0, full, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			org := seed()
			probe := &deprovisionProbe{fakeProvisioner: &fakeProvisioner{db: &provisionDB{}, err: tc.err}, at: func() string { return count(org) }}
			h := &handlers{store: pgStore{Pool: pool, Now: time.Now}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
				clickHouseDSN: ch.URI, orgProvision: probe}
			request := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/orgs/"+org.String()+tc.query, nil)
			request = request.WithContext(policy.WithUser(request.Context(), &policy.User{IsSuperuser: true, UserID: super.String(), ID: super}))
			request.SetPathValue("org_id", org.String())
			recorder := httptest.NewRecorder()
			h.deleteOrganization(recorder, request)
			if recorder.Code != tc.status || (tc.wantBody != "" && strings.TrimSpace(recorder.Body.String()) != tc.wantBody) {
				t.Fatalf("%d %s, want %d", recorder.Code, recorder.Body.String(), tc.status)
			}
			if len(probe.saw) != tc.calls {
				t.Fatalf("%d deprovision calls, want %d", len(probe.saw), tc.calls)
			}
			for _, seen := range probe.saw {
				if seen != full {
					t.Fatalf("at the deprovision call the delete had already removed rows: %s", seen)
				}
			}
			if got := count(org); got != tc.after {
				t.Fatalf("after: %s, want %s", got, tc.after)
			}
		})
	}
}
