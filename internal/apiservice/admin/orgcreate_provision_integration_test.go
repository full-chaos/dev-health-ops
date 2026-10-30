//go:build integration

package admin

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// hookProvisioner runs onDeprovision (a membership landing meanwhile) before
// the deprovision answers.
type hookProvisioner struct {
	orgCreateProvisioner
	onDeprovision func(org uuid.UUID)
}

func (p *hookProvisioner) Deprovision(ctx context.Context, org, actor uuid.UUID) error {
	if p.onDeprovision != nil {
		p.onDeprovision(org)
	}
	return p.orgCreateProvisioner.Deprovision(ctx, org, actor)
}

const internalServerError = `{"detail":"Internal Server Error"}`

// The runbook's read-only query for organizations without their owner.
const ownerlessOrgsQuery = `SELECT o.id, o.created_at FROM public.organizations o WHERE NOT EXISTS (SELECT 1 FROM public.memberships m WHERE m.org_id = o.id AND m.role = 'owner') ORDER BY o.created_at`

// The create with an owner on a real database, switch off and on: the rows
// left, the answer and the log of each failure path.
func TestOrgCreateWithOwnerOnARealDatabase(t *testing.T) {
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
	super := uuid.New()
	pgseed.User(ctx, t, pool, super.String())

	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL, _ := url.Parse(dead.URL)
	dead.Close()
	down, err := authprovision.New(deadURL, "svc_authsess_"+strings.Repeat("A", 43), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	create := func(t *testing.T, auth authprovision.Provisioner, owner uuid.UUID) (int, string, string, uuid.UUID) {
		t.Helper()
		var logs bytes.Buffer
		h := &handlers{store: pgStore{Pool: pool, Now: time.Now}, logger: slog.New(slog.NewTextHandler(&logs, nil))}
		if auth != nil {
			h.orgProvision = auth
		}
		name := "Org " + uuid.NewString()[:8]
		r := answerWith(h.createOrganization, http.MethodPost, func(req *http.Request) *http.Request {
			req = req.WithContext(policy.WithUser(req.Context(), &policy.User{IsSuperuser: true, UserID: super.String(), ID: super}))
			return withBody(`{"name": "` + name + `", "owner_user_id": "` + owner.String() + `"}`)(req)
		})
		var org uuid.UUID
		if err := pool.QueryRow(ctx, `SELECT id FROM organizations WHERE name = $1`, name).Scan(&org); err != nil {
			org = uuid.Nil
		}
		return r.Code, strings.TrimSpace(r.Body.String()), logs.String(), org
	}
	members := func(org uuid.UUID) (all, owners int) {
		if err := pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE role = 'owner') FROM memberships WHERE org_id = $1`, org).Scan(&all, &owners); err != nil {
			t.Fatal(err)
		}
		return
	}
	newOwner := func() uuid.UUID {
		owner := uuid.New()
		pgseed.User(ctx, t, pool, owner.String())
		return owner
	}
	fake := func(addErr, deprovisionErr error) *hookProvisioner {
		return &hookProvisioner{orgCreateProvisioner: orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: &provisionDB{}}, addErr: addErr, deprovisionErr: deprovisionErr}}
	}

	t.Run("I4 switch off: org and owner in go-api, one commit", func(t *testing.T) {
		code, body, _, org := create(t, nil, newOwner())
		if all, owners := members(org); code != http.StatusCreated || org == uuid.Nil || all != 1 || owners != 1 {
			t.Fatalf("%d %s: org %s, members %d owners %d", code, body, org, all, owners)
		}
	})
	t.Run("I4b switch off and on, an owner go-api does not hold: today's 500, nothing written", func(t *testing.T) {
		for _, auth := range []authprovision.Provisioner{nil, fake(nil, nil)} {
			code, body, _, org := create(t, auth, uuid.New())
			if code != http.StatusInternalServerError || body != internalServerError || org != uuid.Nil {
				t.Fatalf("switch on %v: %d %s, org %s", auth != nil, code, body, org)
			}
		}
	})
	t.Run("I1 switch on: org row, the add, no go-api membership", func(t *testing.T) {
		auth := fake(nil, nil)
		owner := newOwner()
		code, body, _, org := create(t, auth, owner)
		if all, _ := members(org); code != http.StatusCreated || org == uuid.Nil || all != 0 {
			t.Fatalf("%d %s: org %s, go-api members %d; want 201, the org, none", code, body, org, all)
		}
		if len(auth.calls) != 1 || auth.args[0][0] != org || auth.args[0][1] != owner || auth.args[0][2] != super || auth.args[0][3] != "owner" {
			t.Fatalf("auth calls %v %v", auth.calls, auth.args)
		}
	})
	t.Run("I2 owner with no auth principal: no org left, 503 (switch off: 201)", func(t *testing.T) {
		auth := fake(refusedWith(404, "principal_not_found"), nil)
		code, body, _, org := create(t, auth, newOwner())
		if code != http.StatusServiceUnavailable || body != provUnavailable || org != uuid.Nil || len(auth.calls) != 2 {
			t.Fatalf("%d %s: org %s, auth calls %v", code, body, org, auth.calls)
		}
	})
	t.Run("I3 auth down for the add: no org left, 503", func(t *testing.T) {
		auth := fake(authprovision.ErrUnavailable, nil)
		code, body, _, org := create(t, auth, newOwner())
		if code != http.StatusServiceUnavailable || body != provUnavailable || org != uuid.Nil {
			t.Fatalf("%d %s: org %s", code, body, org)
		}
	})
	t.Run("I3b auth down for both: 500, the ERROR, the runbook query lists the org", func(t *testing.T) {
		code, body, logs, org := create(t, down, newOwner())
		if code != http.StatusInternalServerError || body != internalServerError || org == uuid.Nil {
			t.Fatalf("%d %s: org %s", code, body, org)
		}
		if !strings.Contains(logs, "level=ERROR") || !strings.Contains(logs, "org_id="+org.String()) || strings.Contains(logs, "@") {
			t.Fatalf("log:\n%s", logs)
		}
		rows, err := pool.Query(ctx, ownerlessOrgsQuery)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		listed := false
		for rows.Next() {
			var id uuid.UUID
			var at time.Time
			if err := rows.Scan(&id, &at); err != nil {
				t.Fatal(err)
			}
			listed = listed || id == org
		}
		if !listed {
			t.Fatalf("the runbook query does not list %s", org)
		}
	})
	t.Run("G1 a member lands before the DELETE: org kept, 500, the ERROR", func(t *testing.T) {
		auth := fake(authprovision.ErrUnavailable, nil)
		auth.onDeprovision = func(org uuid.UUID) {
			pgseed.Membership(ctx, t, pool, uuid.NewString(), super.String(), org.String(), "member")
		}
		code, body, logs, org := create(t, auth, newOwner())
		if code != http.StatusInternalServerError || body != internalServerError || org == uuid.Nil || !strings.Contains(logs, "org_id="+org.String()) {
			t.Fatalf("%d %s: org %s, log:\n%s", code, body, org, logs)
		}
	})
	t.Run("G2 the owner lands before the DELETE: org kept, 201", func(t *testing.T) {
		auth := fake(authprovision.ErrUnavailable, nil)
		owner := newOwner()
		auth.onDeprovision = func(org uuid.UUID) {
			pgseed.Membership(ctx, t, pool, uuid.NewString(), owner.String(), org.String(), "owner")
		}
		code, body, _, org := create(t, auth, owner)
		if _, owners := members(org); code != http.StatusCreated || org == uuid.Nil || owners != 1 {
			t.Fatalf("%d %s: org %s, owners %d", code, body, org, owners)
		}
	})
}
