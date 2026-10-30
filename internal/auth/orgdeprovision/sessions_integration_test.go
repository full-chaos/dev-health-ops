//go:build integration

package orgdeprovision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

func (e *env) open(t *testing.T, ctx context.Context, principal uuid.UUID, orgID *uuid.UUID) uuid.UUID {
	t.Helper()
	issued, err := e.sessions.Create(ctx, principal, orgID, []string{"pwd"})
	if err != nil {
		t.Fatalf("session Create: %v", err)
	}
	return issued.Session.ID
}

// goToken seeds a live go-api refresh_tokens row and returns its id and jti.
func (e *env) goToken(t *testing.T, ctx context.Context, user uuid.UUID, orgID *uuid.UUID) (uuid.UUID, string) {
	t.Helper()
	id := uuid.New()
	jti := id.String()
	digest := sha256.Sum256([]byte(jti))
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at, created_at)
		VALUES ($1, $2, $3, $4, gen_random_uuid(), now() + interval '1 day', now())`, id, user, orgID, hex.EncodeToString(digest[:])); err != nil {
		t.Fatalf("seed go-api refresh token: %v", err)
	}
	return id, jti
}

// sessionEnded reports whether the session is revoked with no credential of it left unrevoked.
func (e *env) sessionEnded(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	revoked := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NOT NULL AND revoked_reason = 'session_revoked'`, id)
	live := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1 AND revoked_at IS NULL`, id)
	return revoked == 1 && live == 0
}

func (e *env) sessionLive(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NULL`, id) == 1 &&
		e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1 AND revoked_at IS NULL`, id) > 0
}

func (e *env) tokenRevoked(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM public.refresh_tokens WHERE id = $1 AND revoked_at IS NOT NULL`, id) == 1
}

func (e *env) auditRows(t *testing.T, ctx context.Context, event string, orgID uuid.UUID) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND organization_id = $2`, event, orgID.String())
}

// hold locks one row FOR UPDATE in an open runtime transaction and returns its release.
func (e *env) hold(t *testing.T, ctx context.Context, lock string, id uuid.UUID) func() {
	t.Helper()
	tx, err := e.runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	var held int
	if err := tx.QueryRow(ctx, lock, id).Scan(&held); err != nil {
		t.Fatalf("hold the row: %v", err)
	}
	return func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Fatal(err)
		}
	}
}

const (
	holdSession = `SELECT 1 FROM auth.sessions WHERE id = $1 FOR UPDATE`
	holdToken   = `SELECT 1 FROM public.refresh_tokens WHERE id = $1 FOR UPDATE`
)

// orgCredentials is one credential of each kind the verbs must end or leave.
type orgCredentials struct {
	org, other                                            uuid.UUID
	member, exMember, superuser                           uuid.UUID
	memberSession, exMemberSession, superuserSession      uuid.UUID
	memberOtherOrg, memberNoOrg                           uuid.UUID
	tokenInOrg, tokenExMember, tokenOtherOrg, tokenNoOrgs uuid.UUID
}

func (e *env) credentials(t *testing.T, ctx context.Context) orgCredentials {
	t.Helper()
	c := orgCredentials{org: e.seedOrg(t, ctx), other: e.seedOrg(t, ctx)}
	owner, _ := e.seedUser(t, ctx)
	c.member, _ = e.seedUser(t, ctx)
	c.exMember, _ = e.seedUser(t, ctx)
	c.superuser, _ = e.seedUser(t, ctx)
	e.add(t, ctx, c.org, owner, "owner")
	e.add(t, ctx, c.other, owner, "owner")
	e.add(t, ctx, c.org, c.member, "member")
	e.add(t, ctx, c.other, c.member, "member")
	e.add(t, ctx, c.org, c.exMember, "member")
	// e.members ends no session: the ex-member's session outlives the removal.
	if _, err := e.members.Remove(ctx, membership.Change{OrgID: c.org, PrincipalID: c.exMember, Authorize: allowMembers}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	c.memberSession = e.open(t, ctx, c.member, &c.org)
	c.exMemberSession = e.open(t, ctx, c.exMember, &c.org)
	c.superuserSession = e.open(t, ctx, c.superuser, &c.org)
	c.memberOtherOrg = e.open(t, ctx, c.member, &c.other)
	c.memberNoOrg = e.open(t, ctx, c.member, nil)
	c.tokenInOrg, _ = e.goToken(t, ctx, c.member, &c.org)
	c.tokenExMember, _ = e.goToken(t, ctx, c.exMember, &c.org)
	c.tokenOtherOrg, _ = e.goToken(t, ctx, c.member, &c.other)
	c.tokenNoOrgs, _ = e.goToken(t, ctx, c.member, nil)
	return c
}

func (e *env) wantEnded(t *testing.T, ctx context.Context, c orgCredentials) {
	t.Helper()
	for name, id := range map[string]uuid.UUID{"member": c.memberSession, "ex-member": c.exMemberSession, "superuser": c.superuserSession} {
		if !e.sessionEnded(t, ctx, id) {
			t.Errorf("the %s's session of the organization is not ended", name)
		}
	}
	for name, id := range map[string]uuid.UUID{"other organization": c.memberOtherOrg, "no organization": c.memberNoOrg} {
		if !e.sessionLive(t, ctx, id) {
			t.Errorf("the session of the %s did not stay live", name)
		}
	}
	for name, id := range map[string]uuid.UUID{"member": c.tokenInOrg, "ex-member": c.tokenExMember} {
		if !e.tokenRevoked(t, ctx, id) {
			t.Errorf("the %s's go-api refresh token of the organization is not revoked", name)
		}
	}
	for name, id := range map[string]uuid.UUID{"other organization": c.tokenOtherOrg, "no organization": c.tokenNoOrgs} {
		if e.tokenRevoked(t, ctx, id) {
			t.Errorf("the go-api refresh token of the %s was revoked", name)
		}
	}
}

func (e *env) wantUntouched(t *testing.T, ctx context.Context, c orgCredentials) {
	t.Helper()
	for name, id := range map[string]uuid.UUID{"member": c.memberSession, "ex-member": c.exMemberSession, "superuser": c.superuserSession} {
		if !e.sessionLive(t, ctx, id) {
			t.Errorf("the %s's session was ended", name)
		}
	}
	for name, id := range map[string]uuid.UUID{"member": c.tokenInOrg, "ex-member": c.tokenExMember} {
		if e.tokenRevoked(t, ctx, id) {
			t.Errorf("the %s's go-api refresh token was revoked", name)
		}
	}
}

// A deprovision ends every credential of the organization, whoever holds it, in its one commit, with its one audit row.
// The same principal's sessions and tokens of another organization or of none stay live.
func TestDeprovisionEndsEverySessionOfTheOrganizationOnly(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	c := e.credentials(t, ctx)
	if _, err := e.service.Deprovision(ctx, Input{OrgID: c.org, Authorize: allowOrg}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	e.wantEnded(t, ctx, c)
	if n := e.auditRows(t, ctx, EventDeprovisioned, c.org); n != 1 {
		t.Fatalf("deprovision audit rows = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type LIKE 'auth.session.%' AND subject_principal_id = $1`,
		c.member.String()); n != 0 {
		t.Fatalf("session audit rows = %d, want 0: the deprovision's row is the audit row", n)
	}
}

// The end of the sessions is in the deprovision's transaction: a failure after it, in the suspension, leaves them live.
func TestAFailedDeprovisionLeavesTheSessionsLive(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	c := e.credentials(t, ctx)
	fn := "deprovision_sessions_fail_" + uuid.NewString()[:8]
	if _, err := e.admin.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.id = '%s' THEN RAISE EXCEPTION 'planted failure'; END IF; RETURN NEW; END $$`, fn, c.org)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE ON auth.organizations FOR EACH ROW EXECUTE FUNCTION public.%s()`, fn, fn)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = e.admin.Exec(context.Background(), fmt.Sprintf(`DROP TRIGGER %s ON auth.organizations`, fn))
		_, _ = e.admin.Exec(context.Background(), fmt.Sprintf(`DROP FUNCTION public.%s()`, fn))
	})
	if _, err := e.service.Deprovision(ctx, Input{OrgID: c.org, Authorize: allowOrg}); !errors.Is(err, ErrStore) {
		t.Fatalf("Deprovision with a planted failure = %v, want ErrStore", err)
	}
	e.wantUntouched(t, ctx, c)
}

func TestARefusedActorEndsNoSession(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	c := e.credentials(t, ctx)
	if _, err := e.service.Deprovision(ctx, Input{OrgID: c.org, Authorize: denyOrg}); !errors.Is(err, ErrDenied) {
		t.Fatalf("Deprovision denied = %v", err)
	}
	if _, err := e.orgs.SuspendAuthorized(ctx, c.org, nil, org.Actor{}, func(context.Context, audit.TxOps, uuid.UUID) error { return org.ErrDenied }); !errors.Is(err, org.ErrDenied) {
		t.Fatalf("Suspend denied = %v", err)
	}
	e.wantUntouched(t, ctx, c)
}

// A row another writer holds is never waited for while the deprovision holds the organization: it rolls back, and after
// its last attempt it is ErrStore with nothing written. The repeat after the release ends everything.
func TestADeprovisionThatFindsACredentialLockedGivesUpWithNothingWritten(t *testing.T) {
	for name, held := range map[string]struct {
		lock string
		row  func(orgCredentials) uuid.UUID
	}{
		"a session":              {holdSession, func(c orgCredentials) uuid.UUID { return c.exMemberSession }},
		"a go-api refresh token": {holdToken, func(c orgCredentials) uuid.UUID { return c.tokenInOrg }},
	} {
		t.Run(name, func(t *testing.T) {
			e := getEnv(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			c := e.credentials(t, ctx)
			before := e.live(t, ctx, c.org)
			release := e.hold(t, ctx, held.lock, held.row(c))
			_, err := e.service.Deprovision(ctx, Input{OrgID: c.org, Authorize: allowOrg})
			if !errors.Is(err, ErrStore) || !errors.Is(err, errSessionsBusy) {
				t.Fatalf("Deprovision with the row held = %v, want ErrStore after the busy attempts", err)
			}
			if got := e.live(t, ctx, c.org); got != before {
				t.Fatalf("after the refused deprovision = %+v, want %+v", got, before)
			}
			if n := e.auditRows(t, ctx, EventDeprovisioned, c.org); n != 0 {
				t.Fatalf("audit rows = %d, want 0", n)
			}
			e.wantUntouched(t, ctx, c)
			release()
			if _, err := e.service.Deprovision(ctx, Input{OrgID: c.org, Authorize: allowOrg}); err != nil {
				t.Fatalf("Deprovision after the release: %v", err)
			}
			e.wantEnded(t, ctx, c)
		})
	}
}

// A retry inside the attempts: the row is released between two attempts, and the deprovision succeeds.
func TestADeprovisionRetriesARowLockedForAMoment(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c := e.credentials(t, ctx)
	release := e.hold(t, ctx, holdSession, c.memberSession)
	var attempts int
	authorize := func(context.Context, audit.TxOps, uuid.UUID) error {
		// The first attempt runs with the row held; the release comes before the second.
		attempts++
		if attempts == 2 {
			release()
		}
		return nil
	}
	if _, err := e.service.Deprovision(ctx, Input{OrgID: c.org, Authorize: authorize}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	e.wantEnded(t, ctx, c)
}

// waiters counts the backends of this database that wait on a lock.
func (e *env) waiters(t *testing.T, ctx context.Context) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
WHERE NOT l.granted AND a.datname = current_database()`)
}

// The cycle NOWAIT exists for, on the real statements: a legacy exchange holds its go-api refresh token and waits for a
// key-share lock on the organization row (its new session's foreign key), which the verb holds FOR UPDATE. The verb
// never waits for the token: it rolls back without a deadlock, the exchange completes, and the next attempt ends the
// session the exchange made.
func TestAVerbHoldingTheOrganizationRowNeverWaitsOnALegacyExchange(t *testing.T) {
	verbs := map[string]func(ctx context.Context, e *env, orgID uuid.UUID, authorize func(context.Context, audit.TxOps, uuid.UUID) error) error{
		"deprovision": func(ctx context.Context, e *env, orgID uuid.UUID, authorize func(context.Context, audit.TxOps, uuid.UUID) error) error {
			_, err := e.service.Deprovision(ctx, Input{OrgID: orgID, Authorize: authorize})
			return err
		},
		"suspend": func(ctx context.Context, e *env, orgID uuid.UUID, authorize func(context.Context, audit.TxOps, uuid.UUID) error) error {
			_, err := e.orgs.SuspendAuthorized(ctx, orgID, nil, org.Actor{}, authorize)
			return err
		},
	}
	for name, verb := range verbs {
		t.Run(name, func(t *testing.T) {
			e := getEnv(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			orgID := e.seedOrg(t, ctx)
			user, _ := e.seedUser(t, ctx)
			e.add(t, ctx, orgID, user, "owner")
			token, jti := e.goToken(t, ctx, user, &orgID)
			type answer struct {
				result session.Result
				err    error
			}
			exchanged := make(chan answer, 1)
			var once sync.Once
			// The exchange runs on the test's context: the verb's own ends with its commit.
			authorize := func(context.Context, audit.TxOps, uuid.UUID) error {
				once.Do(func() {
					go func() {
						result, err := e.sessions.ExchangeLegacy(ctx, session.LegacyRefresh{PrincipalID: user, OrganizationID: &orgID, JTI: contracts.NewSecretString(jti)})
						exchanged <- answer{result, err}
					}()
					for e.waiters(t, context.Background()) != 1 {
						select {
						case got := <-exchanged:
							t.Errorf("the exchange answered %q (%v) without waiting for the organization row", got.result.Reason, got.err)
							exchanged <- got
							return
						case <-ctx.Done():
							return
						default:
						}
					}
				})
				return nil
			}
			if err := verb(ctx, e, orgID, authorize); err != nil {
				t.Fatalf("the verb: deadlock=%v err=%v, want it done with no deadlock", isDeadlock(err), err)
			}
			got := <-exchanged
			if got.err != nil || got.result.Reason != session.ReasonAccepted {
				t.Fatalf("ExchangeLegacy = %q, %v", got.result.Reason, got.err)
			}
			if !e.sessionEnded(t, ctx, got.result.Issued.Session.ID) {
				t.Fatal("the session the exchange made outlived the verb")
			}
			if !e.tokenRevoked(t, ctx, token) {
				t.Fatal("the go-api refresh token is not revoked")
			}
		})
	}
}

// A suspend ends what a deprovision ends, in its one commit and with its one audit row. A repeat is a no-op: a session
// opened after the suspend stays. A reactivate brings nothing back.
func TestSuspendEndsEverySessionOfTheOrganizationOnly(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	c := e.credentials(t, ctx)
	changed, err := e.orgs.SuspendAuthorized(ctx, c.org, nil, org.Actor{}, allowOrg)
	if err != nil || !changed {
		t.Fatalf("Suspend: changed=%v err=%v", changed, err)
	}
	e.wantEnded(t, ctx, c)
	if n := e.auditRows(t, ctx, "auth.organization.suspended", c.org); n != 1 {
		t.Fatalf("suspend audit rows = %d, want 1", n)
	}
	late := e.open(t, ctx, c.member, &c.org)
	if changed, err := e.orgs.SuspendAuthorized(ctx, c.org, nil, org.Actor{}, allowOrg); err != nil || changed {
		t.Fatalf("repeat Suspend: changed=%v err=%v, want a no-op", changed, err)
	}
	if !e.sessionLive(t, ctx, late) {
		t.Fatal("a repeat suspend ended a session")
	}
	if n := e.auditRows(t, ctx, "auth.organization.suspended", c.org); n != 1 {
		t.Fatalf("suspend audit rows after a repeat = %d, want 1", n)
	}
	if changed, err := e.orgs.ReactivateAuthorized(ctx, c.org, nil, org.Actor{}, allowOrg); err != nil || !changed {
		t.Fatalf("Reactivate: changed=%v err=%v", changed, err)
	}
	e.wantEnded(t, ctx, c)
}

// A suspend that finds a row locked gives up with the organization active and nothing written; the repeat ends everything.
func TestASuspendThatFindsACredentialLockedGivesUpWithNothingWritten(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c := e.credentials(t, ctx)
	release := e.hold(t, ctx, holdSession, c.exMemberSession)
	_, err := e.orgs.SuspendAuthorized(ctx, c.org, nil, org.Actor{}, allowOrg)
	if !errors.Is(err, org.ErrStore) {
		t.Fatalf("Suspend with the row held = %v, want ErrStore", err)
	}
	if got := e.live(t, ctx, c.org); !got.active {
		t.Fatal("the organization was suspended")
	}
	if n := e.auditRows(t, ctx, "auth.organization.suspended", c.org); n != 0 {
		t.Fatalf("audit rows = %d, want 0", n)
	}
	e.wantUntouched(t, ctx, c)
	release()
	if _, err := e.orgs.SuspendAuthorized(ctx, c.org, nil, org.Actor{}, allowOrg); err != nil {
		t.Fatalf("Suspend after the release: %v", err)
	}
	e.wantEnded(t, ctx, c)
}

// A deprovision of a suspended organization with only leftover sessions writes: it ends them, and the repeat audit row
// and organization.suspended event are the accepted cost of that repeat.
func TestARepeatDeprovisionEndsLeftoverSessionsWithItsRow(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	c := e.credentials(t, ctx)
	if _, err := e.service.Deprovision(ctx, Input{OrgID: c.org, Authorize: allowOrg}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	late := e.open(t, ctx, c.superuser, &c.org)
	res, err := e.service.Deprovision(ctx, Input{OrgID: c.org, Authorize: allowOrg})
	if err != nil {
		t.Fatalf("repeat Deprovision: %v", err)
	}
	if *res != (Result{}) {
		t.Fatalf("repeat result = %+v, want zero counts", res)
	}
	if !e.sessionEnded(t, ctx, late) {
		t.Fatal("the leftover session was not ended")
	}
	if n := e.auditRows(t, ctx, EventDeprovisioned, c.org); n != 2 {
		t.Fatalf("deprovision audit rows = %d, want 2", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'organization.suspended'
AND payload->>'organization_id' = $1`, "org_"+c.org.String()); n != 2 {
		t.Fatalf("organization.suspended events = %d, want 2", n)
	}
	if _, err := e.service.Deprovision(ctx, Input{OrgID: c.org, Authorize: allowOrg}); err != nil {
		t.Fatalf("third Deprovision: %v", err)
	}
	if n := e.auditRows(t, ctx, EventDeprovisioned, c.org); n != 2 {
		t.Fatalf("a repeat with nothing left wrote an audit row (%d rows)", n)
	}
}

// 2,000 sessions of one organization end in one transaction. Counts only (D6418).
func TestADeprovisionEndsTwoThousandSessionsInOneCommit(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	orgID := e.seedOrg(t, ctx)
	user, _ := e.seedUser(t, ctx)
	e.add(t, ctx, orgID, user, "owner")
	const sessions = 2000
	for range sessions {
		e.open(t, ctx, user, &orgID)
	}
	if _, err := e.service.Deprovision(ctx, Input{OrgID: orgID, Authorize: allowOrg}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE organization_id = $1 AND revoked_at IS NOT NULL`, orgID); n != sessions {
		t.Fatalf("ended sessions = %d, want %d", n, sessions)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials r JOIN auth.sessions s ON s.id = r.session_id
WHERE s.organization_id = $1 AND r.revoked_at IS NULL`, orgID); n != 0 {
		t.Fatalf("live credentials = %d, want 0", n)
	}
}
