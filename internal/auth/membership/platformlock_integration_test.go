//go:build integration

package membership

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var (
	platformActorOnce sync.Once
	platformActorID   uuid.UUID
	platformActorErr  error
)

// platformStore is the platform role store on the owner pool, and an administrator who may grant.
func (e *env) platformStore(t *testing.T, ctx context.Context) (*platformrole.Store, uuid.UUID) {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	roles, err := platformrole.New(platformrole.Config{Pool: e.admin, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	platformActorOnce.Do(func() {
		platformActorID = e.seedUser(t, ctx)
		_, platformActorErr = roles.Bootstrap(ctx, platformActorID)
	})
	if platformActorErr != nil {
		t.Fatalf("bootstrap the platform administrator: %v", platformActorErr)
	}
	return roles, platformActorID
}

// advisoryWaits reports the advisory lock requests that are granted and that wait, by mode.
func (e *env) advisoryWaits(t *testing.T, ctx context.Context) (sharedGranted, sharedWaiting, exclusiveWaiting int) {
	t.Helper()
	if err := e.admin.QueryRow(ctx, `SELECT
  count(*) FILTER (WHERE granted AND mode = 'ShareLock'),
  count(*) FILTER (WHERE NOT granted AND mode = 'ShareLock'),
  count(*) FILTER (WHERE NOT granted AND mode = 'ExclusiveLock')
FROM pg_locks WHERE locktype = 'advisory'`).Scan(&sharedGranted, &sharedWaiting, &exclusiveWaiting); err != nil {
		t.Fatalf("read pg_locks: %v", err)
	}
	return
}

// waitFor polls pg_locks until cond holds; ctx is a hang guard only.
func (e *env) waitFor(t *testing.T, ctx context.Context, what string, cond func() bool, early ...<-chan error) {
	t.Helper()
	for !cond() {
		select {
		case err := <-firstOf(early):
			t.Fatalf("%s: a goroutine ended first (err %v)", what, err)
		case <-ctx.Done():
			t.Fatalf("%s: hang guard expired", what)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func firstOf(chs []<-chan error) <-chan error {
	if len(chs) == 0 {
		return nil
	}
	return chs[0]
}

// heldAuthorize reads the target's platform role in the verb's transaction (the read go-api's precheck cannot make
// under the write's locks), then holds the verb there until release is closed.
func heldAuthorize(target uuid.UUID, read chan<- struct{}, release <-chan struct{}) Authorize {
	return func(ctx context.Context, tx audit.TxOps, l Locked) error {
		var held bool
		if err := tx.QueryRow(ctx, `SELECT `+rule.HoldsSQL(`"auth"`, "$1::uuid", "'platform_admin'"), target.String()).Scan(&held); err != nil {
			return err
		}
		close(read)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}
}

type verbCase struct {
	name string
	// setup seeds the rows and returns the target and a runner for the verb with the given Authorize.
	setup func(t *testing.T, ctx context.Context, e *env) (target uuid.UUID, run func(Authorize) error)
}

func verbCases() []verbCase {
	return []verbCase{
		{"Add", func(t *testing.T, ctx context.Context, e *env) (uuid.UUID, func(Authorize) error) {
			org, user := e.seedOrg(t, ctx), e.seedUser(t, ctx)
			return user, func(a Authorize) error {
				_, err := e.store.Add(ctx, Change{OrgID: org, PrincipalID: user, Role: "member", Authorize: a})
				return err
			}
		}},
		{"ChangeRole", func(t *testing.T, ctx context.Context, e *env) (uuid.UUID, func(Authorize) error) {
			org, user := e.seedOrg(t, ctx), e.seedUser(t, ctx)
			e.add(t, ctx, org, user, "member")
			return user, func(a Authorize) error {
				_, err := e.store.ChangeRole(ctx, Change{OrgID: org, PrincipalID: user, Role: "viewer", Authorize: a})
				return err
			}
		}},
		{"Remove", func(t *testing.T, ctx context.Context, e *env) (uuid.UUID, func(Authorize) error) {
			org, owner, user := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
			e.add(t, ctx, org, owner, "owner")
			e.add(t, ctx, org, user, "member")
			return user, func(a Authorize) error {
				_, err := e.store.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: a})
				return err
			}
		}},
		{"TransferOwnership", func(t *testing.T, ctx context.Context, e *env) (uuid.UUID, func(Authorize) error) {
			org, owner, user := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
			e.add(t, ctx, org, owner, "owner")
			e.add(t, ctx, org, user, "member")
			return user, func(a Authorize) error {
				return e.store.TransferOwnership(ctx, Transfer{OrgID: org, From: owner, To: user, Authorize: a})
			}
		}},
	}
}

// A verb holds the platform role write lock shared from before its locks to its commit: a grant of the platform role
// to the verb's target waits (one exclusive advisory request waiting in pg_locks) until the verb commits, so the role the
// verb read in its own transaction is the one it writes under.
func TestVerbsHoldThePlatformRoleWriteLockSharedUntilCommit(t *testing.T) {
	e := getEnv(t)
	for _, vc := range verbCases() {
		t.Run(vc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			roles, actor := e.platformStore(t, ctx)
			target, run := vc.setup(t, ctx, e)
			read, release := make(chan struct{}), make(chan struct{})
			verbErr, grantErr := make(chan error, 1), make(chan error, 1)
			go func() { verbErr <- run(heldAuthorize(target, read, release)) }()
			select {
			case <-read:
			case err := <-verbErr:
				t.Fatalf("the verb ended before its Authorize read: %v", err)
			case <-ctx.Done():
				t.Fatal("the verb never reached its Authorize read: hang guard expired")
			}
			go func() {
				_, err := roles.Grant(ctx, platformrole.GrantInput{Actor: actor, Principal: target, Role: platformrole.PlatformAdmin})
				grantErr <- err
			}()
			e.waitFor(t, ctx, "the grant waiting on the advisory lock", func() bool {
				_, _, excl := e.advisoryWaits(t, ctx)
				return excl == 1
			}, grantErr)
			if granted, _, _ := e.advisoryWaits(t, ctx); granted < 1 {
				t.Fatalf("shared advisory locks granted while the verb is held: got %d, want at least 1", granted)
			}
			close(release)
			for name, ch := range map[string]chan error{"verb": verbErr, "grant": grantErr} {
				select {
				case err := <-ch:
					if err != nil {
						t.Fatalf("%s: got error %v, want nil", name, err)
					}
				case <-ctx.Done():
					t.Fatalf("%s did not finish after the release: hang guard expired", name)
				}
			}
		})
	}
}

// A grant that already holds the exclusive write lock and then wants the principals' rows does not deadlock with a verb:
// the verb waits at the lock before it holds any principal row (step -1 of the auth lock order).
func TestVerbWaitsForAGrantBeforeItHoldsAnyPrincipalRow(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	target, run := verbCases()[1].setup(t, ctx, e)
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('auth.platform_role_assignments:write', 0))`); err != nil {
		t.Fatal(err)
	}
	verbErr := make(chan error, 1)
	go func() { verbErr <- run(allowAll) }()
	e.waitFor(t, ctx, "the verb waiting on the advisory lock", func() bool {
		_, shared, _ := e.advisoryWaits(t, ctx)
		return shared == 1
	}, verbErr)
	if _, err := tx.Exec(ctx, `SELECT 1 FROM auth.principals WHERE id = $1 FOR SHARE`, target); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
			t.Fatalf("the grant's principal lock deadlocked with the waiting verb: %v", err)
		}
		t.Fatalf("the grant's principal lock: got %v, want nil", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit the grant side: %v", err)
	}
	select {
	case err := <-verbErr:
		if err != nil {
			t.Fatalf("verb: got error %v, want nil", err)
		}
	case <-ctx.Done():
		t.Fatal("the verb did not finish after the grant side committed: hang guard expired")
	}
}

// Two verbs hold the lock at the same time: both are past it (two shared requests granted, none waiting).
func TestTwoVerbsHoldThePlatformRoleWriteLockTogether(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cases := verbCases()
	release := make(chan struct{})
	reads := make([]chan struct{}, 2)
	errs := make([]chan error, 2)
	for i := range 2 {
		target, run := cases[i].setup(t, ctx, e)
		reads[i], errs[i] = make(chan struct{}), make(chan error, 1)
		go func(i int) { errs[i] <- run(heldAuthorize(target, reads[i], release)) }(i)
	}
	for i := range 2 {
		e.waitFor(t, ctx, "both verbs past their lock", func() bool {
			select {
			case <-reads[i]:
				return true
			default:
				return false
			}
		}, errs[i])
	}
	granted, sharedWaiting, exclusiveWaiting := e.advisoryWaits(t, ctx)
	if granted != 2 || sharedWaiting != 0 || exclusiveWaiting != 0 {
		t.Fatalf("advisory requests with two verbs held: got granted=%d shared_waiting=%d exclusive_waiting=%d, want 2, 0, 0", granted, sharedWaiting, exclusiveWaiting)
	}
	close(release)
	for i := range 2 {
		select {
		case err := <-errs[i]:
			if err != nil {
				t.Fatalf("verb %d: got error %v, want nil", i, err)
			}
		case <-ctx.Done():
			t.Fatalf("verb %d did not finish: hang guard expired", i)
		}
	}
}

// A removal that finds a session of the member locked rolls back and plans again; every attempt is its own transaction
// and takes the shared platform role lock again, and a rolled-back attempt leaves none held: after the removal has given
// up (all attempts replanned, nothing written) no shared advisory lock is held and a grant of the platform role to the
// member completes at once.
func TestARemovalThatGivesUpOnABusySessionReleasesThePlatformRoleLock(t *testing.T) {
	s := newSessionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	roles, grantor := s.platformStore(t, ctx)
	org, user, owner := s.seedOrg(t, ctx), s.seedUser(t, ctx), s.seedUser(t, ctx)
	s.add(t, ctx, org, owner, "owner")
	s.add(t, ctx, org, user, "member")
	sess := s.open(t, ctx, user, &org)
	release := s.holdSession(t, ctx, sess)
	defer func() { _ = release(false) }()
	_, err := s.members.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll})
	if !errors.Is(err, ErrStore) || !errors.Is(err, errSessionsBusy) {
		t.Fatalf("Remove with the session held: got %v, want ErrStore after the busy attempts", err)
	}
	if got := s.replanned(t); got != planAttempts {
		t.Fatalf("replanned attempts: got %d, want %d", got, planAttempts)
	}
	if granted, waiting, exclusive := s.advisoryWaits(t, ctx); granted != 0 || waiting != 0 || exclusive != 0 {
		t.Fatalf("advisory requests after the removal gave up: got granted=%d shared_waiting=%d exclusive_waiting=%d, want 0, 0, 0", granted, waiting, exclusive)
	}
	if _, err := roles.Grant(ctx, platformrole.GrantInput{Actor: grantor, Principal: user, Role: platformrole.PlatformAdmin}); err != nil {
		t.Fatalf("grant after the removal gave up: got error %v, want nil", err)
	}
}
