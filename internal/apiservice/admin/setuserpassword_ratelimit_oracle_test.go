//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// seedRateLimitVenue seeds one org, N admins (all role 'admin') and M
// targets (role 'member' of the same org, so _ensure_user_in_scope accepts
// every admin x target pair), returning their ids and venue.Tokens keys
// ("admin0".."adminN-1").
func seedRateLimitVenue(t *testing.T, ctx context.Context, admin *pgxpool.Pool, orgID uuid.UUID, adminIDs, targetIDs []uuid.UUID, adminPlaintextPassword string) map[string]map[string]any {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	adminHash, err := bcrypt.GenerateFromPassword([]byte(adminPlaintextPassword), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-pwd-ratelimit-org', 'Venue Password Rate Limit Org', 'community', 'stripe', true, now(), now())`, orgID)
	tokens := map[string]map[string]any{}
	for i, id := range adminIDs {
		email := fmt.Sprintf("venue-pwd-rl-admin-%d@example.com", i)
		exec(`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, true, true, false, 0, now(), now())`, id, email, string(adminHash))
		exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), orgID, id)
		tokens[fmt.Sprintf("admin%d", i)] = map[string]any{"user_id": id.String(), "email": email, "org_id": orgID.String(), "role": "admin"}
	}
	for i, id := range targetIDs {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, id, fmt.Sprintf("venue-pwd-rl-target-%d@example.com", i))
		// _ensure_user_in_scope needs the target to be a member of the
		// acting (non-superuser) admin's own org.
		exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, id)
	}
	return tokens
}

// TestSetUserPasswordRateLimitMatchesThePythonAPI is CHAOS-6335/CHAOS-6357's
// same-key exhaustion case: a fix PR after CHAOS-6304's own merge found
// set_user_password registered at 1 request/second, burst 10, where
// Python carries @limiter.limit(ADMIN_PASSWORD_LIMIT = "5/hour",
// key_func=get_admin_user_key). CHAOS-6357 replaces the mux-level,
// per-route Bucket with httpapi.KeyedLimiter, keyed by (admin, exact
// path) -- this proves ONE admin resetting ONE target's password six
// times in a row gets five 200s then a 429, on both planes, byte-for-byte.
func TestSetUserPasswordRateLimitMatchesThePythonAPI(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-password-rate-limit-32-byt"
	const adminPlaintextPassword = "correct horse battery staple rl"
	const attempts = 6

	orgID := uuid.New()
	adminID := uuid.New()
	targetID := uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			return seedRateLimitVenue(t, ctx, admin, orgID, []uuid.UUID{adminID}, []uuid.UUID{targetID}, adminPlaintextPassword)
		},
	})

	jsonHeaders := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin0"], "Content-Type": "application/json"}
	requests := make([]venueoracle.Request, attempts)
	for i := range requests {
		requests[i] = venueoracle.Request{
			Name: fmt.Sprintf("set password %d/%d", i+1, attempts), Method: "POST",
			Path:    "/api/v1/admin/users/" + targetID.String() + "/password",
			Headers: jsonHeaders,
			Body:    venueoracle.B64(fmt.Sprintf(`{"admin_password":%q,"password":"a new strong password %d"}`, adminPlaintextPassword, i)),
		}
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)

	// Diff fires each request against Go exactly once (Do, internally) and
	// compares it to the matching pre-fetched Python response, failing
	// loud on any status/body mismatch -- this is the whole proof: five
	// 200s then a 429, byte-identical on both planes, or the test fails
	// naming which numbered request diverged. Neither response shape
	// carries a wall-clock field (success:true, or slowapi's rate-limit
	// error body), so no Normalize redaction is needed.
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Log(receipt)
}

// TestSetUserPasswordRateLimitIsIndependentPerTargetAndAdmin is an
// adversarial-review repro, against real Python: a shared bucket per route
// pattern let one admin's resets for one target consume another target's
// (or another admin's) quota. Sends admin0->target0 to its full 5-request
// limit FIRST -- proving that bucket really is exhausted (request 6 is a
// 429) -- THEN admin0->target1 and admin1->target0, which must still be
// allowed. A review round on an earlier version of this test sent only 4
// requests against a limit of 5, so it passed even under a mutant that
// shared one global bucket for every caller and path; exhausting one
// bucket to its real limit before testing the other two closes that gap.
//
// Diffed against live Python request-by-request -- Python's own
// per-(admin,path) scoping already gets every one of these right; this
// proves the Go KeyedLimiter now agrees, not just that Python does.
func TestSetUserPasswordRateLimitIsIndependentPerTargetAndAdmin(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-password-rl-independence-32"
	const adminPlaintextPassword = "correct horse battery staple ind"

	orgID := uuid.New()
	admin0, admin1 := uuid.New(), uuid.New()
	target0, target1 := uuid.New(), uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			return seedRateLimitVenue(t, ctx, admin, orgID,
				[]uuid.UUID{admin0, admin1}, []uuid.UUID{target0, target1}, adminPlaintextPassword)
		},
	})

	body := func(n int) *string {
		return venueoracle.B64(fmt.Sprintf(`{"admin_password":%q,"password":"independence password %d"}`, adminPlaintextPassword, n))
	}
	headersFor := func(admin string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[admin], "Content-Type": "application/json"}
	}
	pathFor := func(target uuid.UUID) string { return "/api/v1/admin/users/" + target.String() + "/password" }

	var requests []venueoracle.Request
	for i := range 5 {
		requests = append(requests, venueoracle.Request{
			Name: fmt.Sprintf("admin0 -> target0 (own bucket, exhausting, %d/5)", i+1), Method: "POST",
			Path: pathFor(target0), Headers: headersFor("admin0"), Body: body(i),
		})
	}
	requests = append(requests,
		venueoracle.Request{Name: "admin0 -> target0, 6th (own bucket now exhausted)", Method: "POST", Path: pathFor(target0), Headers: headersFor("admin0"), Body: body(5)},
		venueoracle.Request{Name: "admin0 -> target1 (cross-target: must NOT be refused by target0's exhausted bucket)", Method: "POST", Path: pathFor(target1), Headers: headersFor("admin0"), Body: body(6)},
		venueoracle.Request{Name: "admin1 -> target0 (cross-admin: must NOT be refused by admin0's exhausted bucket)", Method: "POST", Path: pathFor(target0), Headers: headersFor("admin1"), Body: body(7)},
	)
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Log(receipt)
}

// TestSetUserPasswordRateLimitWindowDoesNotRollOverEarly is the
// codex-review pr2866-r1 P1's second finding: Go's OLD limiter was a
// token bucket that refilled continuously (one token every 12 minutes at
// "5/hour" == 1/300s), so it wrongly ADMITTED a 6th request at t=12m.
// Python's slowapi is a fixed window: a bucket that opened at t=0 for
// "5/hour" stays exhausted until t=60m, not eased open early. Proven live
// against a real slowapi instance during that review round:
//
//	request 1-5 at t=0: status=401 (five distinct admin_password checks,
//	    all past the rate limiter -- 401 here is the auth-dependency
//	    layer, irrelevant to this proof)
//	request 6 at t=12m: status=401 (admitted -- the bug)
//	SlowAPI fixed-window 5/hour at t=0: [True, True, True, True, True];
//	    at t=12m: False
//
// This test proves the FIX end-to-end through the real mounted Go route
// (real Postgres, real bcrypt, real HTTP, the actual admin.Routes wiring
// -- not just KeyedLimiter's own unit test) using apiservice.Deps.Now to
// drive the clock the exact same way the review's live Python
// reproduction did: five requests admitted at t=0, a 6th REFUSED at
// t=12m (never reaching the handler, so its answer is 429, not 401 --
// this is what changed), and a 7th ADMITTED once the full hour has
// elapsed. It does not also re-run live Python under a frozen clock:
// that ground truth is the review's own reproduction above, which this
// test's comment carries verbatim as its oracle.
func TestSetUserPasswordRateLimitWindowDoesNotRollOverEarly(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-password-rl-window-32-byte"
	const adminPlaintextPassword = "correct horse battery staple win"

	orgID := uuid.New()
	adminID := uuid.New()
	targetID := uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			return seedRateLimitVenue(t, ctx, admin, orgID, []uuid.UUID{adminID}, []uuid.UUID{targetID}, adminPlaintextPassword)
		},
	})

	now := time.Now()
	// The clock here is injected, so this test counts in the in-process
	// store (a shared Valkey store's windows are the server's real time).
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Now = func() time.Time { return now }
		deps.Limits = httpapi.NewMemoryStore(deps.Now)
	})

	headers := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin0"], "Content-Type": "application/json"}
	call := func(n int) int {
		body := venueoracle.B64(fmt.Sprintf(`{"admin_password":%q,"password":"window password %d"}`, adminPlaintextPassword, n))
		response := venueoracle.Do(t, goBase, venueoracle.Request{
			Name: fmt.Sprintf("window call %d", n), Method: "POST",
			Path:    "/api/v1/admin/users/" + targetID.String() + "/password",
			Headers: headers, Body: body,
		})
		return response.Status
	}

	for attempt := range 5 {
		if got := call(attempt); got != 200 {
			t.Fatalf("call %d at t=0 = %d, want 200", attempt+1, got)
		}
	}
	now = now.Add(12 * time.Minute)
	if got := call(5); got != 429 {
		t.Fatalf("call 6 at t=12m = %d, want 429 (Python's fixed window is still open until t=60m)", got)
	}
	now = now.Add(48*time.Minute + time.Second)
	if got := call(6); got != 200 {
		t.Fatalf("call 7 just past t=60m = %d, want 200 (fresh window)", got)
	}
	// This test compares Go against a recorded Python ground truth, not a
	// second live plane, so it has no Diff; the Go-only proof marks that all
	// seven calls ran.
	venueoracle.WriteGoOnlyProof(t, "Go's password rate-limit window against the recorded Python fixed window (7 calls over 60m)")
}

// TestSetUserPasswordValidationBeforeLimitVenueOracle is CHAOS-6435's class
// proof on the password route, diffed against live Python: FastAPI validates
// UserSetPassword's field constraints BEFORE the endpoint, and slowapi's
// decorator wraps the endpoint, so malformed bodies cost no allowance -- while
// what the endpoint itself refuses (the password policy) DOES count. One
// admin sends five malformed bodies (free), two policy-violating ones (each
// spends one of its five), then valid ones: three succeed and the next is
// limited, on both planes.
func TestSetUserPasswordValidationBeforeLimitVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-password-validate-limit-32"
	const adminPlaintextPassword = "correct horse battery staple vl"

	orgID, adminID, targetID := uuid.New(), uuid.New(), uuid.New()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			return seedRateLimitVenue(t, ctx, admin, orgID, []uuid.UUID{adminID}, []uuid.UUID{targetID}, adminPlaintextPassword)
		},
	})
	headers := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin0"], "Content-Type": "application/json"}
	path := "/api/v1/admin/users/" + targetID.String() + "/password"
	send := func(name, body string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: path, Headers: headers, Body: venueoracle.B64(body)}
	}
	var requests []venueoracle.Request
	for i := range 5 {
		requests = append(requests, send(fmt.Sprintf("malformed body %d/5 (validation, free)", i+1), `{"admin_password":"x","password":"y"}`))
	}
	requests = append(requests,
		send("missing field (validation, free)", fmt.Sprintf(`{"admin_password":%q}`, adminPlaintextPassword)),
		send("not an object (validation, free)", `[]`),
		send("password policy violation 1/2 (endpoint, counts)", fmt.Sprintf(`{"admin_password":%q,"password":"aaaaaaaa"}`, adminPlaintextPassword)),
		send("password policy violation 2/2 (endpoint, counts)", fmt.Sprintf(`{"admin_password":%q,"password":"12345678"}`, adminPlaintextPassword)),
	)
	for i := range 4 {
		requests = append(requests, send(fmt.Sprintf("valid %d/4", i+1),
			fmt.Sprintf(`{"admin_password":%q,"password":"a new strong password %d"}`, adminPlaintextPassword, i)))
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)
	t.Log(venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{}))
}
