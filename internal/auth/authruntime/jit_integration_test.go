//go:build integration

package authruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/oidc"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const jitWaitBound = 20 * time.Second

// samlJITEnv is the real stack with auth-service owning users and the SAML
// routes mounted: a sign-in runs the routes configure builds, on the runtime role.
type samlJITEnv struct {
	*routesEnv
	idp      samlConflictIdP
	provider saml.Provider
	org      uuid.UUID
	member   uuid.UUID
	owned    *identity.Store
}

func newSAMLJITEnv(t *testing.T, ctx context.Context, jit bool) *samlJITEnv {
	t.Helper()
	extra := ssoEnvironment(map[string]string{authconfig.EnvUsersOwned: "true"})
	extra[authconfig.EnvAppBaseURL] = samlConflictBase
	if jit {
		extra[authconfig.EnvSSOJIT] = "true"
	}
	e := newRoutesEnvWith(t, ctx, extra)
	member, org := e.seed(t, ctx)
	idp := newSAMLConflictIdP(t)
	row := uuid.New()
	config := fmt.Sprintf(`{"entity_id": %q, "sso_url": "https://idp.example.test/sso", "certificate": %q}`, samlConflictIdPEntity, idp.certB64)
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET tier = 'enterprise' WHERE id = $1`, org); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status, allow_idp_initiated, config, allowed_domains)
VALUES ($1, $2, $3, 'saml', 'active', true, $4::json, '["example.com"]'::json)`, row, org, "idp-"+row.String(), config); err != nil {
		t.Fatal(err)
	}
	store, err := saml.NewPGStore(e.runtime, authschemaIdent(t))
	if err != nil {
		t.Fatal(err)
	}
	provider, err := store.Provider(ctx, row)
	if err != nil {
		t.Fatal(err)
	}
	owned, err := identity.New(identity.Config{Pool: e.runtime, Schema: authschemaIdent(t), UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	return &samlJITEnv{routesEnv: e, idp: idp, provider: provider, org: org, member: member, owned: owned}
}

func jitEmail(nameID string) string { return nameID + "@example.com" }

func (e *samlJITEnv) body(t *testing.T, nameID string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]string{"SAMLResponse": e.idp.response(t, e.provider, nameID)})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func (e *samlJITEnv) post(body string) (answer, error) {
	request, err := http.NewRequest(http.MethodPost, e.front.URL+"/api/v1/auth/saml/"+e.provider.ID.String()+"/acs", strings.NewReader(body))
	if err != nil {
		return answer{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := e.client.Do(request)
	if err != nil {
		return answer{}, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return answer{}, err
	}
	return answer{status: response.StatusCode, body: raw, header: response.Header}, nil
}

func (e *samlJITEnv) acs(t *testing.T, nameID string) answer {
	t.Helper()
	got, err := e.post(e.body(t, nameID))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func (e *samlJITEnv) wantStatus(t *testing.T, got answer, status int) {
	t.Helper()
	if got.status != status {
		t.Fatalf("ACS = %d %s, want %d", got.status, got.body, status)
	}
}

func (e *samlJITEnv) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (e *samlJITEnv) key(t *testing.T, nameID string) identity.ExternalKey {
	t.Helper()
	subject, err := saml.Subject(saml.NameID{Format: "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent", Value: nameID})
	if err != nil {
		t.Fatal(err)
	}
	return identity.ExternalKey{Issuer: samlConflictIdPEntity, Provider: identity.Provider(e.provider.ID.String()), Subject: subject}
}

// account is an existing account with this provider's identity and no membership.
func (e *samlJITEnv) account(t *testing.T, ctx context.Context, nameID string) uuid.UUID {
	t.Helper()
	id, err := e.owned.ProvisionFederated(ctx, identity.ProvisionRequest{Key: e.key(t, nameID), Email: jitEmail(nameID),
		Actor: identity.SelfSignUp(nil), LegacyProvider: "saml", LegacySubject: nameID})
	if err != nil {
		t.Fatalf("provision the existing account: %v", err)
	}
	return id
}

func (e *samlJITEnv) principalOf(t *testing.T, ctx context.Context, nameID string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT principal_id FROM auth.users WHERE email_lower = $1`, strings.ToLower(jitEmail(nameID))).Scan(&id); err != nil {
		t.Fatalf("the account of %s: %v", nameID, err)
	}
	return id
}

func (e *samlJITEnv) tableCounts(t *testing.T, ctx context.Context) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range []string{"auth.principals", "auth.users", "auth.external_identities", "auth.memberships", "auth.sessions",
		"auth.refresh_credentials", "auth.security_audit_events", "auth.auth_outbox_events", "public.users", "public.memberships"} {
		counts[table] = e.count(t, ctx, `SELECT count(*) FROM `+table)
	}
	return counts
}

func wantSameJITCounts(t *testing.T, before, after map[string]int) {
	t.Helper()
	if !maps.Equal(before, after) {
		t.Fatalf("rows changed: before %v after %v", before, after)
	}
}

func (e *samlJITEnv) failInsertsOf(t *testing.T, ctx context.Context, table string) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `CREATE OR REPLACE FUNCTION public.jit_forced_failure() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'forced mid-transaction failure'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `CREATE TRIGGER jit_forced_failure BEFORE INSERT ON `+table+
		` FOR EACH ROW EXECUTE FUNCTION public.jit_forced_failure()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := e.admin.Exec(context.Background(), `DROP TRIGGER jit_forced_failure ON `+table); err != nil {
			t.Errorf("drop the trigger: %v", err)
		}
	})
}

func (e *samlJITEnv) seedAuthMembership(t *testing.T, ctx context.Context, principal, org uuid.UUID, status string) {
	t.Helper()
	if err := e.owned.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	id, subject, orgText := uuid.New(), principal.String(), org.String()
	wireStatus := status
	if status == "invited" {
		wireStatus = "pending"
	}
	err := audit.Commit(ctx, e.runtime, authschemaIdent(t), audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.memberships (id, organization_id, principal_id, status, role) VALUES ($1, $2, $3, $4, 'member')`,
				id, org, principal, status)
			return err
		},
		Audit: audit.AuditEvent{EventType: "auth.membership.added", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject, OrganizationID: &orgText,
			Attributes: map[string]any{"reason_code": "membership_added", "target_kind": "membership", "target_id": id.String()}},
		Events: []audit.OutboxEvent{{AggregateType: "membership", AggregateID: provisioning.WireID("membership", id.String()),
			EventType: "membership." + status, Change: map[string]any{"status": wireStatus}}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (e *samlJITEnv) otherOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	org := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name, tier) VALUES ($1, $2, 'jit-other', 'enterprise')`,
		org, "org-"+org.String()); err != nil {
		t.Fatal(err)
	}
	return org
}

func (e *samlJITEnv) setDefaultRole(t *testing.T, ctx context.Context, role string) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `UPDATE public.sso_providers SET default_role = $2 WHERE id = $1`, e.provider.ID, role); err != nil {
		t.Fatal(err)
	}
}

func (e *samlJITEnv) wantNoMembership(t *testing.T, ctx context.Context, user uuid.UUID) {
	t.Helper()
	if n := e.count(t, ctx, `SELECT (SELECT count(*) FROM public.memberships WHERE user_id = $1 AND org_id = $2)
  + (SELECT count(*) FROM auth.memberships WHERE principal_id = $1 AND organization_id = $2 AND status = 'active')`, user, e.org); n != 0 {
		t.Fatalf("%d membership rows, want none", n)
	}
}

func (e *samlJITEnv) wantOneMembership(t *testing.T, ctx context.Context, user uuid.UUID, role string) {
	t.Helper()
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships p JOIN auth.memberships a ON a.id = p.id
WHERE p.user_id = $1 AND p.org_id = $2 AND p.role = $3 AND a.principal_id = $1 AND a.organization_id = $2 AND a.role = $3 AND a.status = 'active'`,
		user, e.org, role); n != 1 {
		t.Fatalf("%d membership pairs with one id and role %q, want 1", n, role)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE user_id = $1 AND org_id = $2`, user, e.org); n != 1 {
		t.Fatalf("%d public rows, want 1", n)
	}
}

// whileHeld runs a sign-in while tx holds the account's public.users row: the
// sign-in's plan read sees none of tx's rows, then it waits at the lock.
func (e *samlJITEnv) whileHeld(t *testing.T, ctx context.Context, nameID string, statements func(ctx context.Context, tx pgx.Tx) error) answer {
	t.Helper()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := statements(ctx, tx); err != nil {
		t.Fatal(err)
	}
	body := e.body(t, nameID)
	type result struct {
		got answer
		err error
	}
	out := make(chan result, 1)
	go func() {
		got, err := e.post(body)
		out <- result{got, err}
	}()
	deadline := time.Now().Add(jitWaitBound)
	for e.count(t, ctx, `SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
WHERE NOT l.granted AND a.datname = current_database()`) == 0 {
		select {
		case r := <-out:
			t.Fatalf("the sign-in ended before it waited on a lock: %d %s %v", r.got.status, r.got.body, r.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the sign-in never waited on a lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-out:
		if r.err != nil {
			t.Fatal(r.err)
		}
		return r.got
	case <-time.After(2 * jitWaitBound):
		t.Fatal("the sign-in did not end")
		return answer{}
	}
}

func TestJITNewAccountOneOfEach(t *testing.T) {
	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		e := newSAMLJITEnv(t, ctx, true)
		nameID := "jit-new-" + uuid.NewString()
		before := e.tableCounts(t, ctx)
		got := e.acs(t, nameID)
		e.wantStatus(t, got, http.StatusOK)
		if !strings.Contains(string(got.body), `"access_token"`) {
			t.Fatalf("ACS body %s", got.body)
		}
		after := e.tableCounts(t, ctx)
		for table, grew := range map[string]int{"auth.principals": 1, "auth.users": 1, "auth.external_identities": 1, "auth.memberships": 1,
			"auth.sessions": 1, "auth.refresh_credentials": 1, "public.users": 1, "public.memberships": 1} {
			if after[table]-before[table] != grew {
				t.Errorf("%s grew by %d, want %d", table, after[table]-before[table], grew)
			}
		}
		user := e.principalOf(t, ctx, nameID)
		e.wantOneMembership(t, ctx, user, "member")
		if n := e.count(t, ctx, `SELECT count(*) FROM public.users WHERE id = $1 AND auth_provider = 'saml' AND auth_provider_id = $2 AND sso_provider_id = $3`,
			user, nameID, e.provider.ID); n != 1 {
			t.Fatalf("%d public.users rows with the SAML identity columns, want 1", n)
		}
	})
}

func TestJITFailureAtEachStepWritesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSAMLJITEnv(t, ctx, true)
	for name, table := range map[string]string{"saml after identity": "public.memberships", "saml after membership": "auth.memberships"} {
		t.Run(name, func(t *testing.T) {
			e.failInsertsOf(t, ctx, table)
			before := e.tableCounts(t, ctx)
			if got := e.acs(t, "jit-fail-"+uuid.NewString()); got.status < 500 {
				t.Fatalf("ACS = %d %s, want a 5xx", got.status, got.body)
			}
			wantSameJITCounts(t, before, e.tableCounts(t, ctx))
		})
	}
}

func TestJITJoinFailureWritesNothing(t *testing.T) {
	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		e := newSAMLJITEnv(t, ctx, true)
		nameID := "jit-join-fail-" + uuid.NewString()
		e.account(t, ctx, nameID)
		e.failInsertsOf(t, ctx, "auth.memberships")
		before := e.tableCounts(t, ctx)
		if got := e.acs(t, nameID); got.status < 500 {
			t.Fatalf("ACS = %d %s, want a 5xx", got.status, got.body)
		}
		wantSameJITCounts(t, before, e.tableCounts(t, ctx))
	})
}

func TestJITSwitchOffRefusesBeforeAnyWrite(t *testing.T) {
	t.Run("oidc_wiring", func(t *testing.T) {
		if seam := oidcJIT(authconfig.Config{}, slog.Default(), nil, nil, authapi.SSOSessions{}); seam != nil {
			t.Fatal("the OIDC seam is built with AUTH_SSO_JIT off")
		}
	})
	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		e := newSAMLJITEnv(t, ctx, false)
		before := e.tableCounts(t, ctx)
		e.wantStatus(t, e.acs(t, "jit-off-"+uuid.NewString()), http.StatusForbidden)
		wantSameJITCounts(t, before, e.tableCounts(t, ctx))
	})
}

func TestJITAutoProvisionOffRefusesAJoin(t *testing.T) {
	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		e := newSAMLJITEnv(t, ctx, true)
		if _, err := e.admin.Exec(ctx, `UPDATE public.sso_providers SET auto_provision_users = false WHERE id = $1`, e.provider.ID); err != nil {
			t.Fatal(err)
		}
		nameID := "jit-auto-off-" + uuid.NewString()
		user := e.account(t, ctx, nameID)
		before := e.tableCounts(t, ctx)
		e.wantStatus(t, e.acs(t, nameID), http.StatusForbidden)
		wantSameJITCounts(t, before, e.tableCounts(t, ctx))
		e.wantNoMembership(t, ctx, user)
	})
}

func TestJITRefusesAMemberOfAnotherOrg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSAMLJITEnv(t, ctx, true)
	for _, shape := range []string{"public", "auth suspended"} {
		t.Run("saml/"+shape, func(t *testing.T) {
			nameID := "jit-other-" + uuid.NewString()
			user := e.account(t, ctx, nameID)
			other := e.otherOrg(t, ctx)
			if shape == "public" {
				if _, err := e.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`,
					uuid.New(), user, other); err != nil {
					t.Fatal(err)
				}
			} else {
				e.seedAuthMembership(t, ctx, user, other, "suspended")
			}
			before := e.tableCounts(t, ctx)
			e.wantStatus(t, e.acs(t, nameID), http.StatusForbidden)
			wantSameJITCounts(t, before, e.tableCounts(t, ctx))
		})
	}
}

func TestJITOtherOrgCommittedAfterThePlanRead(t *testing.T) {
	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		e := newSAMLJITEnv(t, ctx, true)
		nameID := "jit-parked-" + uuid.NewString()
		user := e.account(t, ctx, nameID)
		other := e.otherOrg(t, ctx)
		got := e.whileHeld(t, ctx, nameID, func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SELECT 1 FROM public.users WHERE id = $1 FOR UPDATE`, user); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, uuid.New(), user, other)
			return err
		})
		e.wantStatus(t, got, http.StatusForbidden)
		e.wantNoMembership(t, ctx, user)
	})
}

func TestJITRoleTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSAMLJITEnv(t, ctx, true)
	for _, row := range []struct{ name, row, want string }{
		{"empty", "", "member"}, {"viewer", "viewer", "viewer"}, {"member", "member", "member"}, {"admin", "admin", "admin"},
		{"owner", "owner", ""}, {"unknown", "root", ""},
	} {
		t.Run("saml/"+row.name, func(t *testing.T) {
			e.setDefaultRole(t, ctx, row.row)
			newID, joinID := "jit-role-"+uuid.NewString(), "jit-role-join-"+uuid.NewString()
			joiner := e.account(t, ctx, joinID)
			before := e.tableCounts(t, ctx)
			newAnswer, joinAnswer := e.acs(t, newID), e.acs(t, joinID)
			if row.want == "" {
				e.wantStatus(t, newAnswer, http.StatusForbidden)
				e.wantStatus(t, joinAnswer, http.StatusForbidden)
				wantSameJITCounts(t, before, e.tableCounts(t, ctx))
				return
			}
			e.wantStatus(t, newAnswer, http.StatusOK)
			e.wantStatus(t, joinAnswer, http.StatusOK)
			e.wantOneMembership(t, ctx, e.principalOf(t, ctx, newID), row.want)
			e.wantOneMembership(t, ctx, joiner, row.want)
		})
	}
}

func TestJITSuspendedOrgRefused(t *testing.T) {
	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		e := newSAMLJITEnv(t, ctx, true)
		nameID := "jit-suspended-" + uuid.NewString()
		user := e.account(t, ctx, nameID)
		if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET is_active = false WHERE id = $1`, e.org); err != nil {
			t.Fatal(err)
		}
		e.wantStatus(t, e.acs(t, nameID), http.StatusForbidden)
		e.wantNoMembership(t, ctx, user)
	})
}

// The account is disabled after the plan read: the check under the lock refuses it.
func TestJITDisabledAccountRefused(t *testing.T) {
	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		e := newSAMLJITEnv(t, ctx, true)
		nameID := "jit-disabled-" + uuid.NewString()
		user := e.account(t, ctx, nameID)
		got := e.whileHeld(t, ctx, nameID, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, user)
			return err
		})
		e.wantNoMembership(t, ctx, user)
		e.wantStatus(t, got, http.StatusForbidden)
	})
}

// A sign-in never undoes a status: the auth row stays as it was.
func TestJITRemovedMemberStaysOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSAMLJITEnv(t, ctx, true)
	for _, status := range []string{"removed", "suspended", "invited"} {
		t.Run("saml/"+status, func(t *testing.T) {
			nameID := "jit-out-" + uuid.NewString()
			user := e.account(t, ctx, nameID)
			e.seedAuthMembership(t, ctx, user, e.org, status)
			before := e.tableCounts(t, ctx)
			e.wantStatus(t, e.acs(t, nameID), http.StatusForbidden)
			wantSameJITCounts(t, before, e.tableCounts(t, ctx))
			if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE principal_id = $1 AND organization_id = $2 AND status = $3`,
				user, e.org, status); n != 1 {
				t.Fatalf("%d rows with status %s, want 1", n, status)
			}
		})
	}
}

func (e *samlJITEnv) twoPosts(t *testing.T, nameID string) [2]answer {
	t.Helper()
	bodies := [2]string{e.body(t, nameID), e.body(t, nameID)}
	type result struct {
		got answer
		err error
	}
	out := [2]chan result{make(chan result, 1), make(chan result, 1)}
	for i := range bodies {
		go func() {
			got, err := e.post(bodies[i])
			out[i] <- result{got, err}
		}()
	}
	var answers [2]answer
	for i := range out {
		r := <-out[i]
		if r.err != nil {
			t.Fatal(r.err)
		}
		answers[i] = r.got
	}
	return answers
}

func TestJITTwoFirstSignInsOneRow(t *testing.T) {
	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		e := newSAMLJITEnv(t, ctx, true)
		nameID := "jit-two-" + uuid.NewString()
		won := 0
		for _, got := range e.twoPosts(t, nameID) {
			switch got.status {
			case http.StatusOK:
				won++
			case http.StatusConflict:
			default:
				t.Fatalf("ACS = %d %s, want a success or 409", got.status, got.body)
			}
		}
		if won == 0 {
			t.Fatal("no sign-in won")
		}
		e.wantOneMembership(t, ctx, e.principalOf(t, ctx, nameID), "member")
	})
}

func TestJITTwoJoinsOneRow(t *testing.T) {
	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		e := newSAMLJITEnv(t, ctx, true)
		nameID := "jit-two-join-" + uuid.NewString()
		user := e.account(t, ctx, nameID)
		for _, got := range e.twoPosts(t, nameID) {
			e.wantStatus(t, got, http.StatusOK)
		}
		e.wantOneMembership(t, ctx, user, "member")
	})
}

// The account became a member after the plan read: a session, no second row.
func TestJITAlreadyMemberGetsASessionOnly(t *testing.T) {
	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		e := newSAMLJITEnv(t, ctx, true)
		nameID := "jit-already-" + uuid.NewString()
		user := e.account(t, ctx, nameID)
		if err := e.owned.ProjectOrganization(ctx, e.org); err != nil {
			t.Fatal(err)
		}
		got := e.whileHeld(t, ctx, nameID, func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SELECT 1 FROM public.users WHERE id = $1 FOR UPDATE`, user); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'admin')`, uuid.New(), user, e.org)
			return err
		})
		e.wantStatus(t, got, http.StatusOK)
		if !strings.Contains(string(got.body), `"role":"admin"`) && !strings.Contains(string(got.body), `"role": "admin"`) {
			t.Fatalf("ACS body %s, want role admin", got.body)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE principal_id = $1`, user); n != 0 {
			t.Fatalf("%d auth rows, want none", n)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user); n != 1 {
			t.Fatalf("%d sessions, want 1", n)
		}
	})
}

func (e *samlJITEnv) auditRows(t *testing.T, ctx context.Context, user uuid.UUID, eventType string) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = $2`, user, eventType)
}

func TestJITAuditRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSAMLJITEnv(t, ctx, true)

	t.Run("saml", func(t *testing.T) {
		nameID := "jit-audit-" + uuid.NewString()
		e.wantStatus(t, e.acs(t, nameID), http.StatusOK)
		user := e.principalOf(t, ctx, nameID)
		for _, event := range []string{"auth.principal.provisioned", "auth.membership.added", authapi.EventLoginSucceeded} {
			if n := e.auditRows(t, ctx, user, event); n != 1 {
				t.Errorf("%d %s rows, want 1", n, event)
			}
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE action = 'sso_login' AND status = 'success' AND user_id = $1`, user); n != 1 {
			t.Errorf("%d go-api success records, want 1", n)
		}
	})

	t.Run("oidc", func(t *testing.T) {
		sessions, err := session.New(session.Config{Pool: e.runtime, Schema: authschemaIdent(t), SuccessorKey: contracts.NewSecretString(testSuccessorKey),
			UsersOwned: true})
		if err != nil {
			t.Fatal(err)
		}
		issuer, err := authapi.NewIssuer(testAccessSecret, "dev-health-ops", "dev-health-api")
		if err != nil {
			t.Fatal(err)
		}
		var cfg authconfig.Config
		cfg.SSO.JIT = true
		sso, err := authapi.NewSSOSessions(authapi.CompatDeps{Sessions: sessions, Users: pgUsers(cfg, e.runtime, authschemaIdent(t)), Issuer: issuer,
			Projector: e.owned, AppendAudit: func(ctx context.Context, event audit.AuditEvent) error {
				return audit.Append(ctx, e.runtime, authschemaIdent(t), event)
			}}, signInRecord(e.runtime))
		if err != nil {
			t.Fatal(err)
		}
		org := e.org
		member := oidc.SessionRequest{PrincipalID: e.member, OrgID: &org, Role: "member", AMR: oidc.AMR}
		if _, err := sso.OIDC().Issue(ctx, member); err != nil {
			t.Fatalf("the member path refused the session: %v", err)
		}
		seam := oidcJIT(cfg, slog.Default(), e.owned, sessions, sso)
		if seam == nil {
			t.Fatal("AUTH_SSO_JIT on built no OIDC seam")
		}
		row := uuid.New()
		if _, err := e.admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status, config)
VALUES ($1, $2, $3, 'oidc', 'active', '{}'::json)`, row, org, "oidc-"+row.String()); err != nil {
			t.Fatal(err)
		}
		subject := "jit-audit-oidc-" + uuid.NewString()
		request := oidc.SessionRequest{OrgID: &org, Role: "member", AMR: oidc.AMR}
		principal, tokens, err := seam.ProvisionJoinAndIssue(ctx, oidc.FederatedAccount{
			Key:   oidc.ExternalKey{Issuer: "https://idp.example.test", Provider: row.String(), Subject: subject},
			Email: jitEmail(subject)}, org, request)
		if err != nil {
			t.Fatal(err)
		}
		if tokens.AccessToken == "" || tokens.RefreshToken == "" {
			t.Fatalf("tokens %+v", tokens)
		}
		for _, event := range []string{"auth.principal.provisioned", "auth.membership.added", authapi.EventLoginSucceeded} {
			if n := e.auditRows(t, ctx, principal.ID, event); n != 1 {
				t.Errorf("%d %s rows, want 1", n, event)
			}
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions s JOIN public.users u ON u.id = s.principal_id
WHERE s.principal_id = $1 AND s.organization_id = $2 AND u.last_login_at = s.issued_at`, principal.ID, org); n != 1 {
			t.Errorf("%d sessions with the sign-in stamp, want 1", n)
		}
	})
}

// A SAML sign-in reaches the join through the service configure builds.
func TestJITReachableFromRuntime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSAMLJITEnv(t, ctx, true)
	nameID := "jit-reach-" + uuid.NewString()
	e.wantStatus(t, e.acs(t, nameID), http.StatusOK)
	e.wantOneMembership(t, ctx, e.principalOf(t, ctx, nameID), "member")
}

func (e *samlJITEnv) setAllowedDomains(t *testing.T, ctx context.Context, list any) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `UPDATE public.sso_providers SET allowed_domains = $2::json WHERE id = $1`, e.provider.ID, list); err != nil {
		t.Fatal(err)
	}
}

func TestJITNewAccountNeedsAListedDomain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSAMLJITEnv(t, ctx, true)
	for name, list := range map[string]any{"empty": "[]", "null": nil} {
		t.Run("saml/"+name, func(t *testing.T) {
			e.setAllowedDomains(t, ctx, list)
			before := e.tableCounts(t, ctx)
			e.wantStatus(t, e.acs(t, "jit-list-"+uuid.NewString()), http.StatusBadRequest)
			wantSameJITCounts(t, before, e.tableCounts(t, ctx))
			if n := e.count(t, ctx, `SELECT count(*) FROM public.sso_providers WHERE id = $1 AND last_error = $2`, e.provider.ID,
				"User not found and auto-provisioning disabled"); n != 1 {
				t.Fatalf("%d provider rows with the auto-provision failure, want 1", n)
			}
		})
	}
	t.Run("saml/listed", func(t *testing.T) {
		e.setAllowedDomains(t, ctx, `["example.com"]`)
		nameID := "jit-list-" + uuid.NewString()
		e.wantStatus(t, e.acs(t, nameID), http.StatusOK)
		e.wantOneMembership(t, ctx, e.principalOf(t, ctx, nameID), "member")
	})
	t.Run("saml/unlisted", func(t *testing.T) {
		e.setAllowedDomains(t, ctx, `["other.example"]`)
		before := e.tableCounts(t, ctx)
		e.wantStatus(t, e.acs(t, "jit-list-"+uuid.NewString()), http.StatusForbidden)
		wantSameJITCounts(t, before, e.tableCounts(t, ctx))
	})
	t.Run("saml/switch_off", func(t *testing.T) {
		off := newSAMLJITEnv(t, ctx, false)
		off.setAllowedDomains(t, ctx, "[]")
		off.wantStatus(t, off.acs(t, "jit-list-"+uuid.NewString()), http.StatusForbidden)
	})
}

// A key the row holds decides the account: the email and the empty list do not.
func TestJITKnownKeyIgnoresAnEmptyList(t *testing.T) {
	t.Run("saml", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		e := newSAMLJITEnv(t, ctx, true)
		nameID := "jit-known-" + uuid.NewString()
		user := e.account(t, ctx, nameID)
		e.setAllowedDomains(t, ctx, "[]")
		e.wantStatus(t, e.acs(t, nameID), http.StatusOK)
		e.wantOneMembership(t, ctx, user, "member")
	})
}
