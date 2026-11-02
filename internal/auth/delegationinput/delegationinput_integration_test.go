//go:build integration

package delegationinput_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/delegationinput"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type liveInner struct{}

func (liveInner) PrincipalStatus() authorization.Status { return authorization.StatusActive }
func (liveInner) PlatformRole() string                  { return "" }
func (liveInner) OrganizationStatus(string) authorization.Status {
	return authorization.StatusActive
}
func (liveInner) Membership(id string) (authorization.Membership, bool) {
	return authorization.Membership{OrgID: id, Role: "member", Status: authorization.StatusActive}, true
}

// L1: the adapter over the real session and platform role stores, connected
// as the auth runtime role.
func TestLiveRoleReadAsTheRuntimeRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	inst, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cc()
		if err := inst.Close(c); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("delegationinput_runtime", inst)
	if err != nil {
		t.Fatal(err)
	}
	const password = "delegationinput-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		t.Fatal(err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	sessions, err := session.New(session.Config{Pool: runtime, Schema: schema, UsersOwned: true, PlatformRolesOwned: true,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(raw))})
	if err != nil {
		t.Fatal(err)
	}
	roles, err := platformrole.New(platformrole.Config{Pool: runtime, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	owners, err := platformrole.New(platformrole.Config{Pool: admin, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	id, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}

	user := func() uuid.UUID {
		u := uuid.New()
		if _, err := admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 7, now(), now())`, u, "d-"+u.String()[:8]+"@example.test"); err != nil {
			t.Fatal(err)
		}
		if err := id.Project(ctx, u); err != nil {
			t.Fatal(err)
		}
		return u
	}
	actor, target, org := user(), user(), uuid.New()
	if _, err := admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'l1')`, org, "org-"+org.String()[:8]); err != nil {
		t.Fatal(err)
	}
	if err := id.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, uuid.New(), target, org); err != nil {
		t.Fatal(err)
	}
	subject := actor.String()
	adminRow := uuid.New()
	if err := audit.Commit(ctx, admin, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.platform_role_assignments (id, principal_id, role_key, granted_by) VALUES ($1, $2, $3, $2)`,
				adminRow, actor, platformrole.PlatformAdmin)
			return err
		},
		Audit: audit.AuditEvent{EventType: "auth.platform_role.granted", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "role_granted", "target_kind": "principal", "target_id": subject}},
		Events: []audit.OutboxEvent{{AggregateType: "platform_role_assignment",
			AggregateID: provisioning.WireID("platform_role_assignment", adminRow.String()), EventType: "platform_role_assignment.granted",
			Change: map[string]any{"status": "active"}}},
	}); err != nil {
		t.Fatalf("grant the admin row: %v", err)
	}
	if _, err := owners.GrantOwner(ctx, actor); err != nil {
		t.Fatalf("GrantOwner: %v", err)
	}
	base, err := sessions.Create(ctx, actor, nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := sessions.StartDelegation(ctx, session.StartInput{BaseSessionID: base.Session.ID, TargetPrincipalID: target, OrganizationID: org, Reason: "l1"})
	if err != nil {
		t.Fatalf("StartDelegation: %v", err)
	}
	ad, err := delegationinput.New(delegationinput.Config{Sessions: sessions, Roles: delegationinput.StoreRoles(roles)})
	if err != nil {
		t.Fatal(err)
	}
	in := delegationinput.Input{Inner: liveInner{}, SessionID: rec.ID, PrincipalID: target, ClaimedActor: &actor}
	read := func() (authorization.Delegation, bool) {
		s, err := ad.Snapshot(ctx, in)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		ds, ok := s.(authorization.DelegationSnapshot)
		if !ok {
			t.Fatal("not a delegation snapshot")
		}
		d, _ := ds.Delegation()
		return d, len(d.Chain) == 1
	}
	expire := func(role string) {
		var rowID uuid.UUID
		if err := admin.QueryRow(ctx, `SELECT id FROM auth.platform_role_assignments WHERE principal_id = $1 AND role_key = $2`, actor, role).Scan(&rowID); err != nil {
			t.Fatalf("read the %s row: %v", role, err)
		}
		err := audit.Commit(ctx, admin, schema, audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				_, err := tx.Exec(ctx, `UPDATE auth.platform_role_assignments SET expires_at = statement_timestamp() - interval '1 minute' WHERE id = $1`, rowID)
				return err
			},
			Audit: audit.AuditEvent{EventType: "auth.platform_role.revoked", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
				Attributes: map[string]any{"reason_code": "role_revoked", "target_kind": "principal", "target_id": subject}},
			Events: []audit.OutboxEvent{{AggregateType: "platform_role_assignment",
				AggregateID: provisioning.WireID("platform_role_assignment", rowID.String()), EventType: "platform_role_assignment.revoked",
				Change: map[string]any{"status": "revoked"}}},
		})
		if err != nil {
			t.Fatalf("expire %s: %v", role, err)
		}
	}

	d, valid := read()
	if !valid || d.ActorPlatformRole != platformrole.PlatformOwner || d.Chain[0].ActorPrincipalID != actor.String() ||
		d.TargetOrganizationID != org.String() || d.Assurance != "aal1" {
		t.Fatalf("owner read: valid=%v %+v", valid, d)
	}
	expire(platformrole.PlatformOwner)
	if d, valid = read(); !valid || d.ActorPlatformRole != "" {
		t.Fatalf("owner expired: valid=%v %+v", valid, d)
	}
	expire(platformrole.PlatformAdmin)
	if _, valid = read(); valid {
		t.Fatal("the last role with the power expired: the delegation must be invalid")
	}
}
