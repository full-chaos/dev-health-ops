//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// writeBarrier holds the first statement whose SQL starts with prefix until
// Release: at its start (Arm), or after it ran (ArmEnd).
type writeBarrier struct {
	mu      sync.Mutex
	prefix  string
	atEnd   bool
	reached chan struct{}
	release chan struct{}
}

type heldAtEndKey struct{}

func (b *writeBarrier) Arm(prefix string) { b.arm(prefix, false) }

func (b *writeBarrier) ArmEnd(prefix string) { b.arm(prefix, true) }

func (b *writeBarrier) arm(prefix string, atEnd bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prefix, b.atEnd, b.reached, b.release = prefix, atEnd, make(chan struct{}), make(chan struct{})
}

func (b *writeBarrier) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.release != nil {
		close(b.release)
		b.release = nil
	}
}

func (b *writeBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	b.mu.Lock()
	hit := b.prefix != "" && strings.HasPrefix(strings.TrimSpace(data.SQL), b.prefix)
	var reached, release chan struct{}
	atEnd := b.atEnd
	if hit {
		b.prefix, reached, release = "", b.reached, b.release
	}
	b.mu.Unlock()
	if !hit {
		return ctx
	}
	if atEnd {
		return context.WithValue(ctx, heldAtEndKey{}, [2]chan struct{}{reached, release})
	}
	close(reached)
	<-release
	return ctx
}

func (b *writeBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if held, ok := ctx.Value(heldAtEndKey{}).([2]chan struct{}); ok {
		close(held[0])
		<-held[1]
	}
}

// TestASuperuserGrantCannotLandBetweenTheGuardAndTheWrite holds an org admin's
// write on a non-superuser target after its superuser check and before its
// first write, then sends a superuser's grant of is_superuser to that target.
// For every guarded write route, the grant must wait for the admin's write
// (check and write are one unit), so the admin's write never lands on a
// superuser and the grant is never lost.
func TestASuperuserGrantCannotLandBetweenTheGuardAndTheWrite(t *testing.T) {
	ctx := context.Background()
	pool, instance := migratedPostgres(t, ctx)
	barrier := &writeBarrier{}
	t.Cleanup(barrier.Release)
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Tracer = barrier
	gated, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gated.Close)

	const jwtKey = "superuser-grant-test-signing-key-32-bytes-long!!"
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.DiscardHandler)
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range admin.Routes(admin.Deps{Pool: gated, Guard: policy.NewGuard(auth, logger), Logger: logger}) {
		if strings.HasPrefix(route.Pattern, "/api/v1/admin/users") || strings.HasPrefix(route.Pattern, "/api/v1/admin/orgs/{org_id}/") {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, query)
		}
	}
	const adminPassword = "race admin own password 31"
	adminHash, err := bcrypt.GenerateFromPassword([]byte(adminPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	rootID := uuid.New()
	exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'race-root@example.com', true, true, true, 0, now(), now())`, rootID)
	token := func(id uuid.UUID, org string, superuser bool) string {
		t.Helper()
		signed, err := signer.Access(edgetoken.AccessClaims{UserID: id.String(), Email: id.String() + "@example.com",
			OrgID: org, Role: "admin", IsSuperuser: superuser}, time.Now(), uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	root := token(rootID, "", true)
	send := func(bearer, method, path, body string) int {
		request, err := http.NewRequestWithContext(ctx, method, server.URL+path, strings.NewReader(body))
		if err != nil {
			return -1
		}
		request.Header.Set("Authorization", "Bearer "+bearer)
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return -1
		}
		_ = response.Body.Close()
		return response.StatusCode
	}
	waitingOnALock := func() bool {
		var waiting int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		return waiting > 0
	}

	type write struct {
		name, method, path, body, firstWrite string
		targetIsMember                       bool
		// grantOwner sends the grant to the org's current owner, not the target.
		grantOwner bool
		// landed reports whether the admin's write is in the stored state.
		landed func(org, target uuid.UUID) bool
	}
	scalar := func(query string, args ...any) string {
		var out string
		if err := pool.QueryRow(ctx, query, args...).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	roleOf := func(org, user uuid.UUID) string {
		return scalar(`SELECT coalesce((SELECT role FROM memberships WHERE org_id = $1 AND user_id = $2), '-')`, org, user)
	}
	users := "/api/v1/admin/users/"
	orgs := "/api/v1/admin/orgs/"
	writes := []write{
		{name: "profile update", method: "PATCH", path: users + "{target}", body: `{"full_name":"edited during the grant"}`,
			firstWrite: "UPDATE users SET email", targetIsMember: true,
			landed: func(_, target uuid.UUID) bool {
				return scalar(`SELECT coalesce(full_name, '-') FROM users WHERE id = $1`, target) == "edited during the grant"
			}},
		{name: "password set", method: "POST", path: users + "{target}/password",
			body:       fmt.Sprintf(`{"admin_password":%q,"password":"Another Strong Pass 42!"}`, adminPassword),
			firstWrite: "UPDATE users SET password_hash", targetIsMember: true,
			landed: func(_, target uuid.UUID) bool {
				return scalar(`SELECT coalesce(password_hash, '-') FROM users WHERE id = $1`, target) != "-"
			}},
		{name: "user deletion", method: "DELETE", path: users + "{target}", firstWrite: "DELETE FROM users", targetIsMember: true,
			landed: func(_, target uuid.UUID) bool {
				return scalar(`SELECT count(*)::text FROM users WHERE id = $1`, target) == "0"
			}},
		{name: "member add", method: "POST", path: orgs + "{org}/members", body: `{"user_id":"{target}","role":"member"}`,
			firstWrite: "INSERT INTO memberships",
			landed:     func(org, target uuid.UUID) bool { return roleOf(org, target) == "member" }},
		{name: "member role change", method: "PATCH", path: orgs + "{org}/members/{target}", body: `{"role":"admin"}`,
			firstWrite: "UPDATE memberships SET role", targetIsMember: true,
			landed: func(org, target uuid.UUID) bool { return roleOf(org, target) == "admin" }},
		{name: "member removal", method: "DELETE", path: orgs + "{org}/members/{target}", firstWrite: "DELETE FROM memberships", targetIsMember: true,
			landed: func(org, target uuid.UUID) bool { return roleOf(org, target) == "-" }},
		{name: "ownership transfer", method: "POST", path: orgs + "{org}/transfer-ownership", body: `{"new_owner_user_id":"{target}"}`,
			firstWrite: "UPDATE memberships SET role", targetIsMember: true,
			landed: func(org, target uuid.UUID) bool { return roleOf(org, target) == "owner" }},
		{name: "ownership transfer away from the owner", method: "POST", path: orgs + "{org}/transfer-ownership", body: `{"new_owner_user_id":"{target}"}`,
			firstWrite: "UPDATE memberships SET role", targetIsMember: true, grantOwner: true,
			landed: func(org, target uuid.UUID) bool { return roleOf(org, target) == "owner" }},
	}
	for i, wr := range writes {
		t.Run(wr.name, func(t *testing.T) {
			orgID, adminID, ownerID, targetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, orgID, fmt.Sprintf("race-org-%d", i))
			exec(`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $4, $7, true, true, false, 0, now(), now()), ($2, $5, NULL, true, true, false, 0, now(), now()), ($3, $6, NULL, true, true, false, 0, now(), now())`,
				adminID, ownerID, targetID, adminID.String()+"@example.com", ownerID.String()+"@example.com", targetID.String()+"@example.com", string(adminHash))
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now()), ($4, $2, $5, 'owner', now(), now(), now())`, uuid.New(), orgID, adminID, uuid.New(), ownerID)
			if wr.targetIsMember {
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, targetID)
			}
			replace := strings.NewReplacer("{org}", orgID.String(), "{target}", targetID.String())

			barrier.Arm(wr.firstWrite)
			adminDone := make(chan int, 1)
			go func() {
				adminDone <- send(token(adminID, orgID.String(), false), wr.method, replace.Replace(wr.path), replace.Replace(wr.body))
			}()
			select {
			case <-barrier.reached:
			case status := <-adminDone:
				t.Fatalf("the admin's write finished with %d before its first write statement", status)
			case <-time.After(10 * time.Second):
				t.Fatal("the admin's write never reached its first write statement")
			}

			granted := targetID
			if wr.grantOwner {
				granted = ownerID
			}
			grantDone := make(chan int, 1)
			go func() { grantDone <- send(root, "PATCH", users+granted.String(), `{"is_superuser":true}`) }()
			grantFirst, grantHeld := false, false
			grantStatus := 0
			deadline := time.Now().Add(10 * time.Second)
			for !grantFirst && !grantHeld {
				select {
				case grantStatus = <-grantDone:
					grantFirst = true
				case <-time.After(20 * time.Millisecond):
					grantHeld = waitingOnALock()
				}
				if time.Now().After(deadline) {
					barrier.Release()
					t.Fatal("the grant neither finished nor waited on a lock")
				}
			}
			barrier.Release()
			adminStatus := <-adminDone
			if !grantFirst {
				grantStatus = <-grantDone
			}
			adminServed := adminStatus >= 200 && adminStatus < 300
			landed := wr.landed(orgID, targetID)
			superuser := scalar(`SELECT coalesce((SELECT is_superuser::text FROM users WHERE id = $1), 'absent')`, granted)
			t.Logf("grant finished first=%v held on a lock=%v; admin %d (landed=%v); grant %d; target superuser=%s",
				grantFirst, grantHeld, adminStatus, landed, grantStatus, superuser)

			if grantFirst && (adminServed || landed) {
				t.Errorf("an org admin's write landed on a target that was already a superuser: admin %d, landed=%v", adminStatus, landed)
			}
			if adminServed != landed {
				t.Errorf("admin status %d does not match the stored state (landed=%v)", adminStatus, landed)
			}
			if grantStatus == http.StatusOK && superuser != "true" {
				t.Errorf("the superuser grant was served and then lost: target superuser=%s", superuser)
			}
			if !grantHeld {
				t.Error("the grant did not wait for the admin's write: the check and the write are not one unit")
			}
		})
	}
}

// TestOppositeOwnershipTransfersBothFinish runs two ownership transfers that
// lock the same two users in opposite roles: the first moves org one from A
// to B, the second moves org two from B to A. The first is held after its
// superuser lock statement until the second waits on a lock. Both must
// finish: the targets are locked in one stable order, so neither transfer
// can hold one row while it waits for the other.
func TestOppositeOwnershipTransfersBothFinish(t *testing.T) {
	ctx := context.Background()
	pool, instance := migratedPostgres(t, ctx)
	barrier := &writeBarrier{}
	t.Cleanup(barrier.Release)
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Tracer = barrier
	gated, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gated.Close)

	const jwtKey = "superuser-grant-test-signing-key-32-bytes-long!!"
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.DiscardHandler)
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range admin.Routes(admin.Deps{Pool: gated, Guard: policy.NewGuard(auth, logger), Logger: logger}) {
		if strings.HasPrefix(route.Pattern, "/api/v1/admin/orgs/{org_id}/") {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	rootID, aID, bID, org1, org2 := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'xfer-root@example.com', true, true, true, 0, now(), now()), ($2, 'xfer-a@example.com', true, true, false, 0, now(), now()),
($3, 'xfer-b@example.com', true, true, false, 0, now(), now())`, []any{rootID, aID, bID}},
		{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'xfer-one', 'xfer-one', 'community', 'stripe', true, now(), now()), ($2, 'xfer-two', 'xfer-two', 'community', 'stripe', true, now(), now())`, []any{org1, org2}},
		{`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $3, 'owner', now(), now(), now()), (gen_random_uuid(), $1, $4, 'member', now(), now(), now()),
(gen_random_uuid(), $2, $4, 'owner', now(), now(), now()), (gen_random_uuid(), $2, $3, 'member', now(), now(), now())`, []any{org1, org2, aID, bID}},
	} {
		if _, err := pool.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	root, err := signer.Access(edgetoken.AccessClaims{UserID: rootID.String(), Email: "xfer-root@example.com", Role: "admin", IsSuperuser: true},
		time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	transfer := func(org, to uuid.UUID) int {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/api/v1/admin/orgs/"+org.String()+"/transfer-ownership",
			strings.NewReader(fmt.Sprintf(`{"new_owner_user_id":%q}`, to)))
		if err != nil {
			return -1
		}
		request.Header.Set("Authorization", "Bearer "+root)
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return -1
		}
		_ = response.Body.Close()
		return response.StatusCode
	}

	barrier.ArmEnd("SELECT is_superuser FROM users")
	first := make(chan int, 1)
	go func() { first <- transfer(org1, bID) }()
	select {
	case <-barrier.reached:
	case status := <-first:
		t.Fatalf("the first transfer finished with %d before its superuser lock statement", status)
	case <-time.After(10 * time.Second):
		t.Fatal("the first transfer never ran its superuser lock statement")
	}
	second := make(chan int, 1)
	go func() { second <- transfer(org2, aID) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			barrier.Release()
			t.Fatal("the second transfer never waited on a lock held by the first")
		}
		time.Sleep(20 * time.Millisecond)
	}
	barrier.Release()
	firstStatus, secondStatus := <-first, <-second
	owner := func(org uuid.UUID) uuid.UUID {
		var id uuid.UUID
		if err := pool.QueryRow(ctx, `SELECT user_id FROM memberships WHERE org_id = $1 AND role = 'owner'`, org).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	if firstStatus != http.StatusOK || secondStatus != http.StatusOK {
		t.Errorf("transfers returned %d and %d, want 200 and 200", firstStatus, secondStatus)
	}
	if owner(org1) != bID || owner(org2) != aID {
		t.Errorf("owners after both transfers: org one %v (want B), org two %v (want A)", owner(org1) == bID, owner(org2) == aID)
	}
}
