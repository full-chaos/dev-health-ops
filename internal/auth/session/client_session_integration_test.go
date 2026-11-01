//go:build integration

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

const eventClientSession = "oauth.client_session.created"

type clientCase struct {
	*delegationEnv
	person, organization uuid.UUID
	web                  Issued
}

func newClientCase(t *testing.T, ctx context.Context) *clientCase {
	t.Helper()
	d := newDelegationEnv(t, ctx)
	c := &clientCase{delegationEnv: d, person: d.user(t, ctx, false), organization: d.org(t, ctx)}
	d.member(t, ctx, c.organization, c.person, "member")
	c.web = c.signIn(t, ctx, &c.organization)
	return c
}

func (c *clientCase) signIn(t *testing.T, ctx context.Context, org *uuid.UUID) Issued {
	t.Helper()
	web, err := c.owned.Create(ctx, c.person, org, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return web
}

// client writes a client session under web, as approve does.
func (c *clientCase) client(ctx context.Context, web uuid.UUID, linked bool) (Record, error) {
	id := uuid.New()
	var out Record
	err := audit.Commit(ctx, c.runtime, c.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var err error
			out, err = c.owned.InsertClientSession(ctx, tx, ClientSessionInput{ID: id, WebSession: web, Linked: linked})
			return err
		},
		Audit: audit.AuditEvent{EventType: eventClientSession, Outcome: audit.OutcomeAllowed,
			Attributes: map[string]any{"reason_code": "accepted", "target_kind": "session", "target_id": id.String()}},
		OffStream: audit.OffStreamSessionLifecycle,
		Refusals:  []error{ErrWebSessionNotLive, ErrWebSessionDelegated, ErrWebSessionNoOrganization},
	})
	return out, err
}

func (c *clientCase) mustClient(t *testing.T, ctx context.Context, linked bool) Record {
	t.Helper()
	out, err := c.client(ctx, c.web.Session.ID, linked)
	if err != nil {
		t.Fatalf("client session: %v", err)
	}
	return out
}

// live is the read every consumer takes: Current, the state answer's source.
func (c *clientCase) live(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	r, err := c.owned.Current(ctx, id)
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	var now time.Time
	if err := c.admin.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return r.RevokedAt == nil && r.ExpiresAt.After(now)
}

func (c *clientCase) idleExpire(t *testing.T, ctx context.Context, session uuid.UUID) {
	t.Helper()
	c.guarded(t, ctx, `UPDATE auth.refresh_credentials SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, session.String())
}

// credential gives a session a live refresh credential, as O4 will, and returns it.
func (c *clientCase) credential(t *testing.T, ctx context.Context, record Record) contracts.SecretString {
	t.Helper()
	secret, err := newCredential()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.admin.Exec(ctx, `INSERT INTO auth.refresh_credentials (id, session_id, principal_id, token_hash, family_id, expires_at)
		VALUES ($1, $2, $3, $4, $5, clock_timestamp() + interval '30 days')`,
		uuid.NewString(), record.ID.String(), record.PrincipalID.String(), hashCredential(secret), uuid.NewString()); err != nil {
		t.Fatalf("seed credential: %v", err)
	}
	return secret
}

// Each of the five ends the browser client with no code on its path (acp, CHAOS-12251
// Q2), and an MCP client's session outlives the three web-only ones (ADR-13 decision
// 5). The positive control is the same pair live before the end.
func TestWhatEndsWhatOverABrowserAndAnMCPClientSession(t *testing.T) {
	ctx := testContext(t)
	for _, tc := range []struct {
		name          string
		end           func(t *testing.T, ctx context.Context, c *clientCase)
		mcpEndsToo    bool
		webStaysAlive bool
	}{
		{name: "a web logout", end: func(t *testing.T, ctx context.Context, c *clientCase) {
			if ok, err := c.owned.Revoke(ctx, c.web.Session.ID, Actor{}); err != nil || !ok {
				t.Fatalf("Revoke: %v, %v", ok, err)
			}
		}},
		{name: "an organization switch", end: func(t *testing.T, ctx context.Context, c *clientCase) {
			event := audit.AuditEvent{EventType: "auth.session.issued", Outcome: audit.OutcomeAllowed}
			if _, err := c.owned.CreateSupersedingAudited(ctx, c.person, &c.organization, []string{"pwd"}, c.web.Session.ID, event); err != nil {
				t.Fatalf("CreateSupersedingAudited: %v", err)
			}
		}},
		{name: "web idle expiry", end: func(t *testing.T, ctx context.Context, c *clientCase) {
			c.idleExpire(t, ctx, c.web.Session.ID)
		}},
		{name: "revoke-all", mcpEndsToo: true, end: func(t *testing.T, ctx context.Context, c *clientCase) {
			if _, err := c.owned.RevokeAll(ctx, c.person, Actor{}); err != nil {
				t.Fatalf("RevokeAll: %v", err)
			}
		}},
		{name: "a password change", mcpEndsToo: true, end: func(t *testing.T, ctx context.Context, c *clientCase) {
			c.guarded(t, ctx, `UPDATE auth.users SET password_updated_at = clock_timestamp() WHERE principal_id = $1`, c.person.String())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClientCase(t, ctx)
			browser, mcp := c.mustClient(t, ctx, true), c.mustClient(t, ctx, false)
			if !c.live(t, ctx, browser.ID) || !c.live(t, ctx, mcp.ID) || !c.live(t, ctx, c.web.Session.ID) {
				t.Fatal("positive control: a session is not live before the end")
			}
			tc.end(t, ctx, c)
			if c.live(t, ctx, c.web.Session.ID) {
				t.Fatal("the web session is still live: the end did not happen")
			}
			if c.live(t, ctx, browser.ID) {
				t.Error("the browser client's session outlived the web session")
			}
			if got := c.live(t, ctx, mcp.ID); got == tc.mcpEndsToo {
				t.Errorf("the MCP client's session: live = %v, want %v", got, !tc.mcpEndsToo)
			}
		})
	}
}

// The parent's whole effective revoke is read over the parent's row (acp, 46638678):
// a password change after the parent's creation and before the child's voids the
// parent, and the child's own stamp cannot see it.
func TestAStampBetweenTheTwoCreationsEndsTheChild(t *testing.T) {
	ctx := testContext(t)
	c := newClientCase(t, ctx)
	child := c.mustClient(t, ctx, true)
	var between time.Time
	if err := c.admin.QueryRow(ctx, `SELECT w.created_at + (s.created_at - w.created_at) / 2
		FROM auth.sessions s JOIN auth.sessions w ON w.id = s.web_session_id WHERE s.id = $1`, child.ID.String()).Scan(&between); err != nil {
		t.Fatal(err)
	}
	c.guarded(t, ctx, `UPDATE auth.users SET password_updated_at = $2 WHERE principal_id = $1`, c.person.String(), between)
	record, err := c.owned.Current(ctx, child.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.RevokedAt == nil {
		t.Fatal("the child reads as live after its parent was voided")
	}
}

// A browser client's own end stays its own: revoking it leaves the web session live,
// and its credential and its use move nothing on the web session.
func TestABrowserClientDoesNotMoveItsWebSession(t *testing.T) {
	ctx := testContext(t)
	c := newClientCase(t, ctx)
	child := c.mustClient(t, ctx, true)
	before, err := c.owned.Current(ctx, c.web.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	seen := func() *time.Time {
		var at *time.Time
		if err := c.admin.QueryRow(ctx, `SELECT last_seen_at FROM auth.sessions WHERE id = $1`, c.web.Session.ID.String()).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	seenBefore := seen()
	got := c.exchange(t, ctx, c.owned, c.credential(t, ctx, child))
	if !got.Reason.Success() {
		t.Fatalf("positive control: the child's exchange = %s", got.Reason)
	}
	after, err := c.owned.Current(ctx, c.web.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ExpiresAt.Equal(before.ExpiresAt) {
		t.Errorf("the web session's expiry moved: %v to %v", before.ExpiresAt, after.ExpiresAt)
	}
	if seenAfter := seen(); (seenBefore == nil) != (seenAfter == nil) || (seenBefore != nil && !seenBefore.Equal(*seenAfter)) {
		t.Errorf("the web session's last_seen_at moved: %v to %v", seenBefore, seenAfter)
	}
	if ok, err := c.owned.Revoke(ctx, child.ID, Actor{}); err != nil || !ok {
		t.Fatalf("Revoke: %v, %v", ok, err)
	}
	if c.live(t, ctx, child.ID) {
		t.Error("the revoked client session is live")
	}
	if !c.live(t, ctx, c.web.Session.ID) {
		t.Error("revoking a browser client ended the web session")
	}
}

// A linked session never outlives its parent in any read: the record's expiry, the
// exchange's cap, and the expiry of a new credential.
func TestALinkedSessionNeverOutlivesItsParentInAnyRead(t *testing.T) {
	ctx := testContext(t)

	t.Run("the record", func(t *testing.T) {
		c := newClientCase(t, ctx)
		child := c.mustClient(t, ctx, true)
		c.credential(t, ctx, child)
		parent, err := c.owned.Current(ctx, c.web.Session.ID)
		if err != nil {
			t.Fatal(err)
		}
		record, err := c.owned.Current(ctx, child.ID)
		if err != nil {
			t.Fatal(err)
		}
		if record.ExpiresAt.After(parent.ExpiresAt) {
			t.Fatalf("the child's expiry %v is after the parent's %v", record.ExpiresAt, parent.ExpiresAt)
		}
	})

	t.Run("the exchange cap", func(t *testing.T) {
		c := newClientCase(t, ctx)
		child := c.mustClient(t, ctx, true)
		secret := c.credential(t, ctx, child)
		c.idleExpire(t, ctx, c.web.Session.ID)
		got := c.exchange(t, ctx, c.owned, secret)
		if got.Reason.Success() {
			t.Fatal("a child was exchanged past its parent")
		}
		if got.Reason != ReasonSessionExpired {
			t.Fatalf("reason = %s, want %s", got.Reason, ReasonSessionExpired)
		}
	})

	t.Run("a new credential", func(t *testing.T) {
		c := newClientCase(t, ctx)
		child := c.mustClient(t, ctx, true)
		secret := c.credential(t, ctx, child)
		c.guarded(t, ctx, `UPDATE auth.refresh_credentials SET expires_at = clock_timestamp() + interval '1 hour' WHERE session_id = $1`, c.web.Session.ID.String())
		got := c.exchange(t, ctx, c.owned, secret)
		if !got.Reason.Success() {
			t.Fatalf("positive control: %s", got.Reason)
		}
		parent, err := c.owned.Current(ctx, c.web.Session.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.ExpiresAt.After(parent.ExpiresAt) {
			t.Fatalf("the new credential's expiry %v is after the parent's %v", got.ExpiresAt, parent.ExpiresAt)
		}
	})
}

func TestAClientSessionCarriesItsWebSessionsPinOrganizationAndLogin(t *testing.T) {
	ctx := testContext(t)
	c := newClientCase(t, ctx)
	for name, linked := range map[string]bool{"browser": true, "mcp": false} {
		t.Run(name, func(t *testing.T) {
			got := c.mustClient(t, ctx, linked)
			if got.ID == c.web.Session.ID {
				t.Fatal("a client session took the web sid")
			}
			web := c.web.Session
			if got.PrincipalID != web.PrincipalID || got.OrganizationID == nil || *got.OrganizationID != c.organization ||
				!got.AuthTime.Equal(web.AuthTime) || got.LegacyTokenVersion == nil || *got.LegacyTokenVersion != *web.LegacyTokenVersion {
				t.Fatalf("client session = %+v, web %+v", got, web)
			}
			var link *string
			var seconds float64
			if err := c.admin.QueryRow(ctx, `SELECT web_session_id::text, extract(epoch FROM expires_at - created_at)
				FROM auth.sessions WHERE id = $1`, got.ID.String()).Scan(&link, &seconds); err != nil {
				t.Fatal(err)
			}
			if (link != nil) != linked {
				t.Fatalf("link = %v, linked = %v", link, linked)
			}
			if time.Duration(seconds)*time.Second != 90*24*time.Hour {
				t.Fatalf("absolute life = %vs, want 90 days", seconds)
			}
		})
	}
}

// A client session is refused under a web session that cannot stand behind it.
func TestAClientSessionIsRefusedUnderASessionThatCannotConsent(t *testing.T) {
	ctx := testContext(t)
	for _, tc := range []struct {
		name string
		web  func(t *testing.T, ctx context.Context, c *clientCase) uuid.UUID
		want error
	}{
		{"missing", func(*testing.T, context.Context, *clientCase) uuid.UUID { return uuid.New() }, ErrWebSessionNotLive},
		{"revoked", func(t *testing.T, ctx context.Context, c *clientCase) uuid.UUID {
			if _, err := c.owned.Revoke(ctx, c.web.Session.ID, Actor{}); err != nil {
				t.Fatal(err)
			}
			return c.web.Session.ID
		}, ErrWebSessionNotLive},
		{"expired", func(t *testing.T, ctx context.Context, c *clientCase) uuid.UUID {
			c.idleExpire(t, ctx, c.web.Session.ID)
			return c.web.Session.ID
		}, ErrWebSessionNotLive},
		{"a disabled principal", func(t *testing.T, ctx context.Context, c *clientCase) uuid.UUID {
			c.guarded(t, ctx, `UPDATE auth.principals SET disabled_at = clock_timestamp() WHERE id = $1`, c.person.String())
			return c.web.Session.ID
		}, ErrWebSessionNotLive},
		{"a linked client session", func(t *testing.T, ctx context.Context, c *clientCase) uuid.UUID {
			return c.mustClient(t, ctx, true).ID
		}, ErrWebSessionNotLive},
		{"no organization", func(t *testing.T, ctx context.Context, c *clientCase) uuid.UUID {
			return c.signIn(t, ctx, nil).Session.ID
		}, ErrWebSessionNoOrganization},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClientCase(t, ctx)
			web := tc.web(t, ctx, c)
			for name, linked := range map[string]bool{"browser": true, "mcp": false} {
				before := c.count(t, ctx, `SELECT count(*) FROM auth.sessions`)
				got, err := c.client(ctx, web, linked)
				if !errors.Is(err, tc.want) {
					t.Fatalf("%s: err = %v, want %v, session %+v", name, err, tc.want, got)
				}
				if after := c.count(t, ctx, `SELECT count(*) FROM auth.sessions`); after != before {
					t.Fatalf("%s: %d rows became %d", name, before, after)
				}
			}
		})
	}
}

// The link's shape is the schema's, for any writer: self-reference, depth two, a
// change of the link, an actor on either end, another principal, another organization
// and no organization are each refused. The positive control is the plain link.
func TestTheSchemaRefusesEveryBadWebLink(t *testing.T) {
	ctx := testContext(t)
	c := newClientCase(t, ctx)
	other := c.user2(t, ctx)
	otherOrg := c.org2(t, ctx)
	insert := func(id, principal string, org *uuid.UUID, link string) error {
		_, err := c.admin.Exec(ctx, `INSERT INTO auth.sessions (id, principal_id, organization_id, token_hash, expires_at, legacy_token_version, web_session_id)
			VALUES ($1, $2, $3, $4, clock_timestamp() + interval '1 hour', 0, $5)`, id, principal, org, []byte(uuid.NewString()), link)
		return err
	}
	web, user := c.web.Session.ID.String(), c.person.String()
	child := uuid.NewString()
	if err := insert(child, user, &c.organization, web); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	self := uuid.NewString()
	if err := insert(self, user, &c.organization, self); err == nil {
		t.Error("a self-reference was accepted")
	}
	if err := insert(uuid.NewString(), user, &c.organization, child); err == nil {
		t.Error("depth two was accepted")
	}
	if err := insert(uuid.NewString(), other.String(), &c.organization, web); err == nil {
		t.Error("another principal's link was accepted")
	}
	if err := insert(uuid.NewString(), user, &otherOrg, web); err == nil {
		t.Error("another organization's link was accepted")
	}
	if err := insert(uuid.NewString(), user, nil, web); err == nil {
		t.Error("a link with no organization was accepted")
	}
	second := c.signIn(t, ctx, &c.organization).Session.ID.String()
	if _, err := c.admin.Exec(ctx, `UPDATE auth.sessions SET web_session_id = $2 WHERE id = $1`, child, second); err == nil {
		t.Error("a change of the link was accepted")
	}
	if _, err := c.admin.Exec(ctx, `UPDATE auth.sessions SET web_session_id = NULL WHERE id = $1`, child); err == nil {
		t.Error("clearing the link was accepted")
	}
	if _, err := c.admin.Exec(ctx, `UPDATE auth.sessions SET web_session_id = $2 WHERE id = $1`, second, web); err == nil {
		t.Error("linking an unlinked row was accepted")
	}
	delegated := c.fixture(t, ctx)
	start, err := c.owned.StartDelegation(ctx, delegated.input())
	if err != nil {
		t.Fatalf("StartDelegation: %v", err)
	}
	if err := insert(uuid.NewString(), delegated.target.String(), &delegated.org, start.ID.String()); err == nil {
		t.Error("a link to a delegated session was accepted")
	}
}

// The delegated side of the actor check: a delegated session, and the real actor of
// a live delegation with a plain session of their own. A target with its own session
// is not refused, and nor is an actor after the delegation ended. A missing session
// row is an error, never false.
func TestActorInDelegationReadsTheDelegatedSideOnly(t *testing.T) {
	ctx := testContext(t)
	c := newClientCase(t, ctx)
	d := c.fixture(t, ctx)
	start, err := c.owned.StartDelegation(ctx, d.input())
	if err != nil {
		t.Fatalf("StartDelegation: %v", err)
	}
	targetOwn, err := c.owned.Create(ctx, d.target, &d.org, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	read := func(principal, session uuid.UUID) bool {
		t.Helper()
		got, err := c.owned.ActorInDelegation(ctx, principal, session)
		if err != nil {
			t.Fatalf("ActorInDelegation: %v", err)
		}
		return got
	}
	if !read(d.actor, d.base.Session.ID) {
		t.Error("an actor with a plain session and a live delegation passed")
	}
	if !read(d.target, start.ID) {
		t.Error("a delegated session passed")
	}
	if read(d.target, targetOwn.Session.ID) {
		t.Error("a target with its own session was refused")
	}
	if read(c.person, c.web.Session.ID) {
		t.Error("a principal on neither side was refused: the positive control")
	}
	if _, err := c.owned.ActorInDelegation(ctx, d.actor, uuid.New()); err == nil {
		t.Error("a missing session row read as false")
	}
	if ok, err := c.owned.EndDelegation(ctx, start.ID, Actor{}); err != nil || !ok {
		t.Fatalf("EndDelegation: %v, %v", ok, err)
	}
	if read(d.actor, d.base.Session.ID) {
		t.Error("an actor whose delegation ended was refused")
	}
}

// A delegation that expired or lost its base through the effective expressions no
// longer counts, as every other read of it.
func TestActorInDelegationReadsThroughTheEffectiveExpiry(t *testing.T) {
	ctx := testContext(t)
	c := newClientCase(t, ctx)
	d := c.fixture(t, ctx)
	start, err := c.owned.StartDelegation(ctx, d.input())
	if err != nil {
		t.Fatalf("StartDelegation: %v", err)
	}
	c.guarded(t, ctx, `UPDATE auth.sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, start.ID.String())
	got, err := c.owned.ActorInDelegation(ctx, d.actor, d.base.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Error("an expired delegation still counts")
	}
}

func (c *clientCase) user2(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	other := c.user(t, ctx, false)
	return other
}

func (c *clientCase) org2(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	return c.delegationEnv.org(t, ctx)
}
