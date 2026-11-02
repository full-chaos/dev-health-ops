//go:build integration

package session

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/delegationend"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const (
	eventStarted = "auth.session.delegation_started"
	eventEnded   = "auth.session.delegation_ended"
)

type delegationEnv struct {
	*env
	owned    *Store
	identity *identity.Store
	platform *platformrole.Store
}

func newDelegationEnv(t *testing.T, ctx context.Context) *delegationEnv {
	t.Helper()
	e := newEnvOwned(t, ctx, true)
	id, err := identity.New(identity.Config{Pool: e.runtime, Schema: e.schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	platform, err := platformrole.New(platformrole.Config{Pool: e.runtime, Schema: e.schema})
	if err != nil {
		t.Fatal(err)
	}
	d := &delegationEnv{env: e, identity: id, platform: platform}
	d.owned = e.newStore(t, e.keyText, Config{UsersOwned: true, PlatformRolesOwned: true})
	return d
}

func (d *delegationEnv) user(t *testing.T, ctx context.Context, platformAdmin bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	mail := "d-" + id.String()[:8] + "@example.test"
	if _, err := d.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 7, now(), now())`, id, mail); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	if err := d.identity.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	if platformAdmin {
		d.makeAdmin(t, ctx, id)
	}
	return id
}

func (d *delegationEnv) makeAdmin(t *testing.T, ctx context.Context, id uuid.UUID) {
	t.Helper()
	if _, err := d.identity.BackfillUser(ctx, identity.BackfillTarget{ID: id, Email: "d-" + id.String()[:8] + "@example.test",
		DisplayName: "d", Active: true, Verified: true, Superuser: true}, false); err != nil {
		t.Fatalf("BackfillUser: %v", err)
	}
}

func (d *delegationEnv) org(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := d.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'delegation-test')`,
		id, "org-"+id.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := d.identity.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

func (d *delegationEnv) member(t *testing.T, ctx context.Context, org, user uuid.UUID, role string) {
	t.Helper()
	if _, err := d.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, $4)`,
		uuid.New(), user, org, role); err != nil {
		t.Fatalf("seed public.memberships: %v", err)
	}
}

type delegationCase struct {
	actor, target, org uuid.UUID
	base               Issued
}

func (d *delegationEnv) fixture(t *testing.T, ctx context.Context) delegationCase {
	t.Helper()
	c := delegationCase{actor: d.user(t, ctx, true), target: d.user(t, ctx, false), org: d.org(t, ctx)}
	d.member(t, ctx, c.org, c.target, "member")
	base, err := d.owned.Create(ctx, c.actor, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	c.base = base
	return c
}

func (c delegationCase) input() StartInput {
	return StartInput{BaseSessionID: c.base.Session.ID, TargetPrincipalID: c.target, OrganizationID: c.org, Reason: "support ticket"}
}

func (d *delegationEnv) start(t *testing.T, ctx context.Context, c delegationCase) Record {
	t.Helper()
	record, err := d.owned.StartDelegation(ctx, c.input())
	if err != nil {
		t.Fatalf("StartDelegation: %v", err)
	}
	return record
}

func (d *delegationEnv) rowsOf(t *testing.T, ctx context.Context, actor uuid.UUID) int {
	return d.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE actor_principal_id = $1`, actor.String())
}

func (d *delegationEnv) liveRowsOf(t *testing.T, ctx context.Context, actor uuid.UUID) int {
	return d.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE actor_principal_id = $1 AND revoked_at IS NULL`, actor.String())
}

func (d *delegationEnv) revokedReason(t *testing.T, ctx context.Context, id uuid.UUID) string {
	t.Helper()
	var reason *string
	if err := d.admin.QueryRow(ctx, `SELECT revoked_reason FROM auth.sessions WHERE id = $1`, id.String()).Scan(&reason); err != nil {
		t.Fatalf("revoked_reason: %v", err)
	}
	if reason == nil {
		return ""
	}
	return *reason
}

// wantEnded checks one delegation_ended row naming the real actor and the target.
func (d *delegationEnv) wantEnded(t *testing.T, ctx context.Context, row Record, c delegationCase) {
	t.Helper()
	got := d.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
		WHERE event_type = $1 AND attributes->>'target_id' = $2 AND actor_principal_id::text = $3 AND subject_principal_id::text = $4`,
		eventEnded, row.ID.String(), c.actor.String(), c.target.String())
	if got != 1 {
		t.Fatalf("delegation_ended rows of %s = %d, want 1", row.ID, got)
	}
}

// refuseAudit makes the database refuse one audit event type, so a mutation
// fails at its audit insert, after its state writes.
func (d *delegationEnv) refuseAudit(t *testing.T, ctx context.Context, eventType string) {
	t.Helper()
	if _, err := d.admin.Exec(ctx, fmt.Sprintf(`ALTER TABLE auth.security_audit_events
		ADD CONSTRAINT refuse_event CHECK (event_type <> '%s') NOT VALID`, eventType)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := d.admin.Exec(context.Background(), `ALTER TABLE auth.security_audit_events DROP CONSTRAINT refuse_event`); err != nil {
			t.Errorf("drop constraint: %v", err)
		}
	})
}

func sessionEvent(eventType string) audit.AuditEvent {
	return audit.AuditEvent{EventType: eventType, Outcome: audit.OutcomeAllowed}
}

func (d *delegationEnv) inCommit(ctx context.Context, apply func(context.Context, audit.TxOps) error) error {
	return audit.Commit(ctx, d.runtime, d.schema, audit.Mutation{
		Apply: apply, Audit: sessionEvent("auth.session.revoked_all"), OffStream: audit.OffStreamSessionLifecycle,
	})
}

// The row Start writes: the target's session in the organization, with the
// actor, the base, the reason, the target's pin and the catalog's actions.
func TestDelegationRowIsTheTargetsSessionWithTheActorOnIt(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	record := d.start(t, ctx, c)
	if record.PrincipalID != c.target || record.OrganizationID == nil || *record.OrganizationID != c.org {
		t.Fatalf("record = %+v", record)
	}
	if record.LegacyTokenVersion == nil || *record.LegacyTokenVersion != 7 {
		t.Fatalf("pin = %v, want the target's token_version", record.LegacyTokenVersion)
	}
	want, err := policycatalog.Default().DelegatedActions("member")
	if err != nil {
		t.Fatal(err)
	}
	if record.Delegation == nil || record.Delegation.Actor != c.actor || record.Delegation.Reason != "support ticket" ||
		!slices.Equal(record.Delegation.PermittedActions, want) {
		t.Fatalf("delegation = %+v", record.Delegation)
	}
	if got := d.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND delegated_from_session_id = $2`,
		record.ID.String(), c.base.Session.ID.String()); got != 1 {
		t.Fatalf("delegated_from_session_id not the base: %d", got)
	}
	got := d.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
		WHERE event_type = $1 AND attributes->>'target_id' = $2 AND actor_principal_id::text = $3
		AND subject_principal_id::text = $4 AND organization_id::text = $5`,
		eventStarted, record.ID.String(), c.actor.String(), c.target.String(), c.org.String())
	if got != 1 {
		t.Fatalf("delegation_started rows = %d, want 1", got)
	}
	if rows := d.outboxRows(t, ctx, record.ID.String()); rows != 0 {
		t.Fatalf("outbox rows = %d, want 0", rows)
	}
	if wire := record.Wire(); wire.Delegation == nil || wire.Delegation.ActorPrincipalID != WirePrincipalID(c.actor) {
		t.Fatalf("wire delegation = %+v", wire.Delegation)
	}
}

func TestDelegationTwoStartsLeaveOneRow(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	first := d.start(t, ctx, c)
	second := d.start(t, ctx, c)
	if got := d.liveRowsOf(t, ctx, c.actor); got != 1 {
		t.Fatalf("live rows = %d, want 1", got)
	}
	if got := d.revokedReason(t, ctx, first.ID); got != delegationend.ReasonSuperseded {
		t.Fatalf("first row reason = %q", got)
	}
	if d.revokedReason(t, ctx, second.ID) != "" {
		t.Fatal("the second row is revoked")
	}
	d.wantEnded(t, ctx, first, c)
}

func TestRaceDelegationParallelStartsLeaveOneLiveRow(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	markRace(t)
	errs := make([]error, 8)
	parallel(8, func(i int) {
		_, errs[i] = d.owned.StartDelegation(ctx, c.input())
	})
	wins := 0
	for _, err := range errs {
		switch {
		case err == nil:
			wins++
		case !errors.Is(err, ErrBusy):
			t.Fatalf("a parallel Start: %v", err)
		}
	}
	if wins == 0 {
		t.Fatal("no Start won")
	}
	if got := d.liveRowsOf(t, ctx, c.actor); got != 1 {
		t.Fatalf("live rows = %d, want 1", got)
	}
}

func TestDelegationAuditFailureWritesNoRow(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	d.refuseAudit(t, ctx, eventStarted)
	if _, err := d.owned.StartDelegation(ctx, c.input()); err == nil {
		t.Fatal("Start succeeded with its audit row refused")
	}
	if got := d.rowsOf(t, ctx, c.actor); got != 0 {
		t.Fatalf("rows = %d, want 0", got)
	}
}

func TestDelegationHasNoRefreshCredential(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	record := d.start(t, ctx, c)
	if got := d.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1`, record.ID.String()); got != 0 {
		t.Fatalf("refresh credentials = %d, want 0", got)
	}
}

func TestDelegationLifeIsTheFirstEnd(t *testing.T) {
	life := func(r Record) time.Duration { return r.ExpiresAt.Sub(r.CreatedAt) }
	t.Run("fifteen_minutes", func(t *testing.T) {
		ctx := testContext(t)
		d := newDelegationEnv(t, ctx)
		record := d.start(t, ctx, d.fixture(t, ctx))
		if got := life(record); got != MaxDelegation {
			t.Fatalf("life = %s, want %s", got, MaxDelegation)
		}
	})
	t.Run("clipped_by_the_base", func(t *testing.T) {
		ctx := testContext(t)
		d := newDelegationEnv(t, ctx)
		c := d.fixture(t, ctx)
		d.guarded(t, ctx, `UPDATE auth.refresh_credentials SET expires_at = clock_timestamp() + interval '5 minutes' WHERE session_id = $1`,
			c.base.Session.ID.String())
		if got := life(d.start(t, ctx, c)); got > 5*time.Minute {
			t.Fatalf("life = %s, want at most the base's 5 minutes", got)
		}
	})
	t.Run("clipped_by_the_assignment", func(t *testing.T) {
		ctx := testContext(t)
		d := newDelegationEnv(t, ctx)
		c := d.fixture(t, ctx)
		granter := d.user(t, ctx, true)
		short := d.user(t, ctx, false)
		until := time.Now().Add(5 * time.Minute)
		if _, err := d.platform.Grant(ctx, platformrole.GrantInput{Actor: granter, Principal: short, Role: platformrole.PlatformAdmin, ExpiresAt: &until}); err != nil {
			t.Fatalf("Grant: %v", err)
		}
		base, err := d.owned.Create(ctx, short, nil, []string{"pwd"})
		if err != nil {
			t.Fatal(err)
		}
		c.actor, c.base = short, base
		if got := life(d.start(t, ctx, c)); got > 5*time.Minute {
			t.Fatalf("life = %s, want at most the assignment's 5 minutes", got)
		}
	})
}

func TestDelegationRefusals(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	cases := []struct {
		name string
		want error
		edit func(t *testing.T, c *delegationCase, in *StartInput)
	}{
		{"actor_holds_no_authority", ErrActorNotAuthorized, func(t *testing.T, c *delegationCase, in *StartInput) {
			plain := d.user(t, ctx, false)
			base, err := d.owned.Create(ctx, plain, nil, []string{"pwd"})
			if err != nil {
				t.Fatal(err)
			}
			c.actor, in.BaseSessionID = plain, base.Session.ID
		}},
		{"actor_disabled", ErrActorUnavailable, func(t *testing.T, c *delegationCase, in *StartInput) {
			if _, err := d.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, c.actor); err != nil {
				t.Fatal(err)
			}
			if err := d.identity.Project(ctx, c.actor); err != nil {
				t.Fatal(err)
			}
		}},
		{"target_holds_a_platform_role", ErrTargetPlatformRole, func(t *testing.T, c *delegationCase, in *StartInput) {
			d.makeAdmin(t, ctx, c.target)
		}},
		{"target_is_the_actor", ErrTargetIsActor, func(t *testing.T, c *delegationCase, in *StartInput) {
			in.TargetPrincipalID = c.actor
		}},
		{"target_disabled", ErrTargetUnavailable, func(t *testing.T, c *delegationCase, in *StartInput) {
			if _, err := d.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, c.target); err != nil {
				t.Fatal(err)
			}
			if err := d.identity.Project(ctx, c.target); err != nil {
				t.Fatal(err)
			}
		}},
		{"target_inactive", ErrTargetUnavailable, func(t *testing.T, c *delegationCase, in *StartInput) {
			if _, err := d.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, c.target); err != nil {
				t.Fatal(err)
			}
		}},
		{"target_unknown", ErrTargetUnavailable, func(t *testing.T, c *delegationCase, in *StartInput) {
			in.TargetPrincipalID = uuid.New()
		}},
		{"target_is_not_a_user_principal", ErrTargetUnavailable, func(t *testing.T, c *delegationCase, in *StartInput) {
			if _, err := d.admin.Exec(ctx, `UPDATE auth.principals SET kind = 'workload' WHERE id = $1`, c.target); err != nil {
				t.Fatal(err)
			}
		}},
		{"target_is_an_external_source", ErrTargetUnavailable, func(t *testing.T, c *delegationCase, in *StartInput) {
			if _, err := d.admin.Exec(ctx, `UPDATE auth.principals SET kind = 'external_source' WHERE id = $1`, c.target); err != nil {
				t.Fatal(err)
			}
		}},
		{"target_not_a_member", ErrTargetNotMember, func(t *testing.T, c *delegationCase, in *StartInput) {
			in.OrganizationID = d.org(t, ctx)
		}},
		{"target_role_not_in_the_catalog", ErrTargetRoleUnknown, func(t *testing.T, c *delegationCase, in *StartInput) {
			if _, err := d.admin.Exec(ctx, `UPDATE public.memberships SET role = 'custom' WHERE user_id = $1`, c.target); err != nil {
				t.Fatal(err)
			}
		}},
		{"target_role_is_a_platform_role", ErrTargetRoleUnknown, func(t *testing.T, c *delegationCase, in *StartInput) {
			if _, err := d.admin.Exec(ctx, `UPDATE public.memberships SET role = 'platform_admin' WHERE user_id = $1`, c.target); err != nil {
				t.Fatal(err)
			}
		}},
		{"organization_unknown", ErrOrganizationUnavailable, func(t *testing.T, c *delegationCase, in *StartInput) {
			in.OrganizationID = uuid.New()
		}},
		{"organization_disabled", ErrOrganizationUnavailable, func(t *testing.T, c *delegationCase, in *StartInput) {
			d.guarded(t, ctx, `UPDATE auth.organizations SET disabled_at = clock_timestamp() WHERE id = $1`, c.org)
		}},
		{"base_absent", ErrBaseNotLive, func(t *testing.T, c *delegationCase, in *StartInput) {
			in.BaseSessionID = uuid.New()
		}},
		{"base_revoked", ErrBaseNotLive, func(t *testing.T, c *delegationCase, in *StartInput) {
			if _, err := d.owned.Revoke(ctx, c.base.Session.ID, Actor{}); err != nil {
				t.Fatal(err)
			}
		}},
		{"base_expired", ErrBaseNotLive, func(t *testing.T, c *delegationCase, in *StartInput) {
			d.guarded(t, ctx, `UPDATE auth.refresh_credentials SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`,
				c.base.Session.ID.String())
		}},
		{"base_is_delegated", ErrNestedDelegation, func(t *testing.T, c *delegationCase, in *StartInput) {
			delegated := d.start(t, ctx, *c)
			d.makeAdmin(t, ctx, c.target)
			third := d.user(t, ctx, false)
			d.member(t, ctx, c.org, third, "member")
			c.actor, in.BaseSessionID, in.TargetPrincipalID = c.target, delegated.ID, third
		}},
		{"reason_blank", ErrInvalidReason, func(t *testing.T, c *delegationCase, in *StartInput) { in.Reason = "   " }},
		{"reason_over_the_cap", ErrInvalidReason, func(t *testing.T, c *delegationCase, in *StartInput) {
			in.Reason = strings.Repeat("x", 501)
		}},
	}
	for _, row := range cases {
		t.Run(row.name, func(t *testing.T) {
			c := d.fixture(t, ctx)
			in := c.input()
			row.edit(t, &c, &in)
			before := d.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE actor_principal_id IS NOT NULL`)
			if _, err := d.owned.StartDelegation(ctx, in); !errors.Is(err, row.want) {
				t.Fatalf("StartDelegation = %v, want %v", err, row.want)
			}
			if got := d.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE actor_principal_id IS NOT NULL`); got != before {
				t.Fatalf("a refusal wrote a row: %d -> %d", before, got)
			}
		})
	}
}

// A prior row that expired unrevoked still holds the actor's one slot.
func TestDelegationStartEndsAnExpiredUnrevokedPrior(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	prior := d.start(t, ctx, c)
	d.guarded(t, ctx, `UPDATE auth.sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, prior.ID.String())
	d.start(t, ctx, c)
	if got := d.revokedReason(t, ctx, prior.ID); got != delegationend.ReasonSuperseded {
		t.Fatalf("expired prior reason = %q", got)
	}
	if got := d.liveRowsOf(t, ctx, c.actor); got != 1 {
		t.Fatalf("live rows = %d, want 1", got)
	}
}

func TestDelegationRefusedUnlessBothSwitchesAreOn(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	for name, cfg := range map[string]Config{
		"platform_roles_off": {UsersOwned: true},
		"users_off":          {PlatformRolesOwned: true},
		"both_off":           {},
	} {
		t.Run(name, func(t *testing.T) {
			store := d.newStore(t, d.keyText, cfg)
			if _, err := store.StartDelegation(ctx, c.input()); !errors.Is(err, ErrNotOwned) {
				t.Fatalf("StartDelegation = %v, want ErrNotOwned", err)
			}
			if got := d.rowsOf(t, ctx, c.actor); got != 0 {
				t.Fatalf("rows = %d, want 0", got)
			}
		})
	}
}

// Every way the base ends takes the child with it, with its audit row.
func TestDelegationEndsWithItsBase(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	cases := map[string]func(t *testing.T, c delegationCase){
		"revoke": func(t *testing.T, c delegationCase) {
			if ok, err := d.owned.Revoke(ctx, c.base.Session.ID, Actor{}); err != nil || !ok {
				t.Fatalf("Revoke = %v, %v", ok, err)
			}
		},
		"revoke_all": func(t *testing.T, c delegationCase) {
			if _, err := d.owned.RevokeAll(ctx, c.actor, Actor{}); err != nil {
				t.Fatalf("RevokeAll: %v", err)
			}
		},
		"revoke_by_credential": func(t *testing.T, c delegationCase) {
			if ok, err := d.owned.RevokeByCredential(ctx, c.base.Credential, Actor{}); err != nil || !ok {
				t.Fatalf("RevokeByCredential = %v, %v", ok, err)
			}
		},
		"end_all_in_tx": func(t *testing.T, c delegationCase) {
			if err := d.inCommit(ctx, func(ctx context.Context, tx audit.TxOps) error {
				_, err := d.owned.EndAllInTx(ctx, tx, c.actor)
				return err
			}); err != nil {
				t.Fatalf("EndAllInTx: %v", err)
			}
		},
		"reuse": func(t *testing.T, c delegationCase) {
			d.rotatedAgo(t, ctx, c.base, GraceWindow+15*time.Second)
			if got := d.exchange(t, ctx, d.owned, c.base.Credential); got.Reason != ReasonRefreshReused {
				t.Fatalf("Exchange = %s, want refresh_reused", got.Reason)
			}
		},
		"supersede": func(t *testing.T, c delegationCase) {
			if _, err := d.owned.CreateSupersedingAudited(ctx, c.actor, nil, []string{"pwd"}, c.base.Session.ID, sessionEvent("auth.session.issued")); err != nil {
				t.Fatalf("CreateSupersedingAudited: %v", err)
			}
		},
	}
	for name, end := range cases {
		t.Run(name, func(t *testing.T) {
			c := d.fixture(t, ctx)
			row := d.start(t, ctx, c)
			end(t, c)
			if got := d.revokedReason(t, ctx, row.ID); got != delegationend.ReasonBaseEnded {
				t.Fatalf("child reason = %q, want %q", got, delegationend.ReasonBaseEnded)
			}
			d.wantEnded(t, ctx, row, c)
		})
	}
	for name, end := range map[string]func(ctx context.Context, tx audit.TxOps, c delegationCase, org uuid.UUID) error{
		"end_org_in_tx": func(ctx context.Context, tx audit.TxOps, c delegationCase, org uuid.UUID) error {
			_, err := d.owned.EndOrgInTx(ctx, tx, c.actor, org)
			return err
		},
		"end_organization_in_tx": func(ctx context.Context, tx audit.TxOps, _ delegationCase, org uuid.UUID) error {
			_, err := d.owned.EndOrganizationInTx(ctx, tx, org)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := d.fixture(t, ctx)
			baseOrg := d.org(t, ctx)
			base, err := d.owned.Create(ctx, c.actor, &baseOrg, []string{"pwd"})
			if err != nil {
				t.Fatal(err)
			}
			c.base = base
			row := d.start(t, ctx, c)
			if err := d.inCommit(ctx, func(ctx context.Context, tx audit.TxOps) error { return end(ctx, tx, c, baseOrg) }); err != nil {
				t.Fatalf("end: %v", err)
			}
			if got := d.revokedReason(t, ctx, row.ID); got != delegationend.ReasonBaseEnded {
				t.Fatalf("child reason = %q", got)
			}
			d.wantEnded(t, ctx, row, c)
		})
	}
}

// The target's own revoke-all ends the row and leaves the actor's base live.
func TestDelegationTargetRevokeAllEndsTheRowAndKeepsTheBase(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	row := d.start(t, ctx, c)
	own, err := d.owned.Create(ctx, c.target, nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := d.owned.RevokeAll(ctx, c.target, Actor{PrincipalID: &c.target}); err != nil || n != 2 {
		t.Fatalf("RevokeAll = %d, %v; want 2", n, err)
	}
	if !d.sessionRevoked(t, ctx, row.ID) || !d.sessionRevoked(t, ctx, own.Session.ID) {
		t.Fatal("the target's sessions are not all revoked")
	}
	if d.sessionRevoked(t, ctx, c.base.Session.ID) {
		t.Fatal("the target's revoke-all ended the actor's base")
	}
	if got := d.liveCredentials(t, ctx, c.base.Session.ID); got != 1 {
		t.Fatalf("base credentials = %d, want 1", got)
	}
	d.wantEnded(t, ctx, row, c)
}

func TestDelegationIsNotOnTheTargetsOwnSession(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	row := d.start(t, ctx, c)
	own, err := d.owned.Create(ctx, c.target, nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	record, err := d.owned.Current(ctx, own.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.ID == row.ID || record.Delegation != nil {
		t.Fatalf("the target's Current = %+v", record)
	}
}

func TestDelegationEndDelegationEndsOnlyADelegatedRow(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	row := d.start(t, ctx, c)
	if _, err := d.owned.EndDelegation(ctx, c.base.Session.ID, Actor{}); !errors.Is(err, ErrNotDelegated) {
		t.Fatalf("EndDelegation of the base = %v, want ErrNotDelegated", err)
	}
	if ok, err := d.owned.EndDelegation(ctx, row.ID, Actor{PrincipalID: &c.actor}); err != nil || !ok {
		t.Fatalf("EndDelegation = %v, %v", ok, err)
	}
	if d.sessionRevoked(t, ctx, c.base.Session.ID) {
		t.Fatal("EndDelegation ended the base")
	}
	d.wantEnded(t, ctx, row, c)
	if _, err := d.owned.EndDelegation(ctx, uuid.New(), Actor{}); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("EndDelegation of nothing = %v", err)
	}
}

// An audit failure on a cascaded end commits no end at all (rulings D3).
func TestDelegationAuditFailureOnACascadeCommitsNoEnd(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	row := d.start(t, ctx, c)
	d.refuseAudit(t, ctx, eventEnded)
	if _, err := d.owned.Revoke(ctx, c.base.Session.ID, Actor{}); err == nil {
		t.Fatal("Revoke succeeded with the delegation_ended row refused")
	}
	if d.sessionRevoked(t, ctx, c.base.Session.ID) || d.sessionRevoked(t, ctx, row.ID) {
		t.Fatal("a failed audit row left an end committed")
	}
}

// Under the organization verbs the cascade does not wait (rulings D6).
func TestDelegationCascadeUnderAnOrganizationVerbIsBusy(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	baseOrg := d.org(t, ctx)
	base, err := d.owned.Create(ctx, c.actor, &baseOrg, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	c.base = base
	row := d.start(t, ctx, c)
	holder, err := d.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := holder.Exec(ctx, `SELECT 1 FROM auth.sessions WHERE id = $1 FOR UPDATE`, row.ID.String()); err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	end := func(ctx context.Context) error {
		return d.inCommit(ctx, func(ctx context.Context, tx audit.TxOps) error {
			_, err := d.owned.EndOrgInTx(ctx, tx, c.actor, baseOrg)
			return err
		})
	}
	if err := end(bounded); !errors.Is(err, ErrBusy) {
		t.Fatalf("EndOrgInTx with the child held = %v, want ErrBusy", err)
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := end(ctx); err != nil {
		t.Fatalf("EndOrgInTx after the child was released: %v", err)
	}
	if !d.sessionRevoked(t, ctx, row.ID) {
		t.Fatal("the retry did not end the child")
	}
}

// An attempt that queued its audit row and then got ErrBusy leaves no row for
// the retry: one delegation_ended, not two (rulings A1).
func TestDelegationRetryAfterBusyWritesOneEndedRow(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	row := d.start(t, ctx, c)
	if _, err := d.admin.Exec(ctx, `INSERT INTO public.refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at, created_at)
		VALUES (gen_random_uuid(), $1, $2, 'delegation-busy', gen_random_uuid(), now() + interval '1 hour', now())`, c.target, c.org); err != nil {
		t.Fatal(err)
	}
	holder, err := d.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := holder.Exec(ctx, `SELECT 1 FROM public.refresh_tokens WHERE token_hash = 'delegation-busy' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	end := func() error {
		return d.inCommit(ctx, func(ctx context.Context, tx audit.TxOps) error {
			_, err := d.owned.EndOrgInTx(ctx, tx, c.target, c.org)
			return err
		})
	}
	if err := end(); !errors.Is(err, ErrBusy) {
		t.Fatalf("attempt 1 = %v, want ErrBusy", err)
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := end(); err != nil {
		t.Fatalf("attempt 2: %v", err)
	}
	d.wantEnded(t, ctx, row, c)
}

func TestDelegationEndsWhenTheActorLosesTheRole(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	other := d.user(t, ctx, true)
	row := d.start(t, ctx, c)
	if err := d.platform.Revoke(ctx, platformrole.RevokeInput{Actor: other, Principal: c.actor, Role: platformrole.PlatformAdmin}); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if got := d.revokedReason(t, ctx, row.ID); got != delegationend.ReasonActorEnded {
		t.Fatalf("reason = %q, want %q", got, delegationend.ReasonActorEnded)
	}
	d.wantEnded(t, ctx, row, c)
}

func TestDelegationEndsWhenTheActorIsDisabled(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	row := d.start(t, ctx, c)
	if _, err := d.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, c.actor); err != nil {
		t.Fatal(err)
	}
	if err := d.identity.Project(ctx, c.actor); err != nil {
		t.Fatalf("Project: %v", err)
	}
	if got := d.revokedReason(t, ctx, row.ID); got != delegationend.ReasonActorEnded {
		t.Fatalf("reason = %q, want %q", got, delegationend.ReasonActorEnded)
	}
	d.wantEnded(t, ctx, row, c)
}
