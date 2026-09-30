package principal

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// fakeUsers is a policy.Store whose users row is whatever the test sets.
// Only UserState is used by the edge verifier; the other two methods fail the
// test's expectation of "never read" by returning an error.
type fakeUsers struct {
	state policy.UserState
	found bool
	err   error
	calls int
	last  uuid.UUID
}

func (f *fakeUsers) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	f.calls++
	f.last = id
	return f.state, f.found, f.err
}

func (f *fakeUsers) IsMember(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	return false, errors.New("fakeUsers: IsMember must not be read by the edge verifier")
}

func (f *fakeUsers) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, errors.New("fakeUsers: ActiveImpersonation must not be read by the edge verifier")
}

// activeUserStore is the users row of validEdgeClaims' subject: found,
// active, token_version 0.
func activeUserStore() *fakeUsers {
	return &fakeUsers{state: policy.UserState{IsActive: true}, found: true}
}

// edgeTestSubject is validEdgeClaims' sub.
const edgeTestSubject = "11111111-1111-1111-1111-111111111111"

func mustEdgeVerifierFor(t *testing.T, users policy.Store) *EdgeVerifier {
	t.Helper()
	v, err := NewEdgeVerifier(edgeTestSecret, edgeTestIssuer, edgeTestAudience, users)
	if err != nil {
		t.Fatalf("NewEdgeVerifier: %v", err)
	}
	return v
}

func verifyWithUsers(t *testing.T, users policy.Store, mutate func(map[string]any)) (*EdgeClaims, error) {
	t.Helper()
	v, err := NewEdgeVerifier(edgeTestSecret, edgeTestIssuer, edgeTestAudience, users)
	if err != nil {
		t.Fatalf("NewEdgeVerifier: %v", err)
	}
	claims := validEdgeClaims("org-42")
	if mutate != nil {
		mutate(claims)
	}
	return v.Verify(context.Background(), signEdgeToken(t, edgeTestSecret, claims))
}

// CHAOS-6290: a token whose JWT verifies is still refused when the live users
// row says so.
func TestEdgeVerifier_ServesActiveUser(t *testing.T) {
	users := activeUserStore()
	claims, err := verifyWithUsers(t, users, nil)
	if err != nil {
		t.Fatalf("Verify: unexpected error: %v", err)
	}
	if claims.OrgID != "org-42" {
		t.Fatalf("OrgID = %q, want org-42", claims.OrgID)
	}
	if users.calls != 1 || users.last.String() != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("UserState calls = %d for %s, want 1 for the token's sub", users.calls, users.last)
	}
}

func TestEdgeVerifier_RefusesInactiveUser(t *testing.T) {
	users := &fakeUsers{state: policy.UserState{IsActive: false}, found: true}
	claims, err := verifyWithUsers(t, users, nil)
	if err == nil || claims != nil {
		t.Fatalf("Verify = (%v, %v), want a refusal for a deactivated user", claims, err)
	}
	if !policy.IsRefusal(err) {
		t.Fatalf("err = %v, want policy.IsRefusal", err)
	}
}

func TestEdgeVerifier_RefusesUnknownUser(t *testing.T) {
	claims, err := verifyWithUsers(t, &fakeUsers{found: false}, nil)
	if err == nil || claims != nil || !policy.IsRefusal(err) {
		t.Fatalf("Verify = (%v, %v), want a refusal for a missing users row", claims, err)
	}
}

func TestEdgeVerifier_RefusesStaleTokenVersion(t *testing.T) {
	users := &fakeUsers{state: policy.UserState{IsActive: true, TokenVersion: 3}, found: true}
	claims, err := verifyWithUsers(t, users, nil) // token carries tv=0
	if err == nil || claims != nil || !policy.IsRefusal(err) {
		t.Fatalf("Verify = (%v, %v), want a refusal for a token_version mismatch", claims, err)
	}
	// The same row accepts the matching version, so the refusal above is the
	// version check and not a side effect of another field.
	if _, err := verifyWithUsers(t, users, func(c map[string]any) { c["tv"] = 3 }); err != nil {
		t.Fatalf("Verify with tv=3: unexpected error: %v", err)
	}
}

func TestEdgeVerifier_RefusesInvalidSubject(t *testing.T) {
	users := activeUserStore()
	claims, err := verifyWithUsers(t, users, func(c map[string]any) { c["sub"] = "not-a-uuid" })
	if err == nil || claims != nil || !policy.IsRefusal(err) {
		t.Fatalf("Verify = (%v, %v), want a refusal for a non-uuid sub", claims, err)
	}
	if users.calls != 0 {
		t.Fatalf("UserState called %d times for an unparseable sub, want 0", users.calls)
	}
}

// A failed lookup is never a pass, and it is not a credential refusal either:
// the caller answers it as the Go api does (503 unavailable / 500).
func TestEdgeVerifier_StoreErrorFailsClosed(t *testing.T) {
	for name, storeErr := range map[string]error{
		"unavailable": errors.Join(policy.ErrUnavailable, errors.New("connection lost")),
		"other":       errors.New("permission denied for table users"),
	} {
		t.Run(name, func(t *testing.T) {
			claims, err := verifyWithUsers(t, &fakeUsers{err: storeErr}, nil)
			if err == nil || claims != nil {
				t.Fatalf("Verify = (%v, %v), want an error and no claims", claims, err)
			}
			if policy.IsRefusal(err) {
				t.Fatalf("err = %v, a store failure must not read as a credential refusal", err)
			}
			if got, want := errors.Is(err, policy.ErrUnavailable), name == "unavailable"; got != want {
				t.Fatalf("errors.Is(err, ErrUnavailable) = %v, want %v", got, want)
			}
		})
	}
}

func TestEdgeVerifier_RequiresUsersStore(t *testing.T) {
	if _, err := NewEdgeVerifier(edgeTestSecret, edgeTestIssuer, edgeTestAudience, nil); err == nil {
		t.Fatal("NewEdgeVerifier accepted a nil users store; the token of a deactivated user would be served")
	}
}

// P1 of the r1 review: the users-row refusals log which refusal it was, never the subject.
func TestEdgeVerifier_UsersRowRefusalsLogTheReasonNotTheSubject(t *testing.T) {
	cases := map[string]struct {
		users *fakeUsers
		want  string
	}{
		"inactive":   {&fakeUsers{state: policy.UserState{IsActive: false}, found: true}, "user is deactivated"},
		"not found":  {&fakeUsers{found: false}, "user not found"},
		"stale":      {&fakeUsers{state: policy.UserState{IsActive: true, TokenVersion: 9}, found: true}, "stale session"},
		"store fail": {&fakeUsers{err: errors.New("permission denied for table users")}, "user lookup failed"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var logBuf bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
			defer slog.SetDefault(previous)
			if _, err := verifyWithUsers(t, tc.users, nil); err == nil {
				t.Fatal("Verify: expected a refusal")
			}
			out := logBuf.String()
			if !strings.Contains(out, tc.want) {
				t.Fatalf("log output lacks the refusal %q (an operator could not tell which check refused):\n%s", tc.want, out)
			}
			if strings.Contains(out, edgeTestSubject) || strings.Contains(out, "user_id") {
				t.Fatalf("log output carries the token subject:\n%s", out)
			}
		})
	}
}
