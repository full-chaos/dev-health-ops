package principal

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// fakeUsers is a policy.Store whose users row is whatever the test sets.
// UserState and Membership are read by the edge verifier; ActiveImpersonation
// fails the test's expectation of "never read" by returning an error.
type fakeUsers struct {
	state policy.UserState
	found bool
	err   error
	calls int
	last  uuid.UUID

	notMember   bool
	memberErr   error
	memberCalls int
}

func (f *fakeUsers) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	f.calls++
	f.last = id
	return f.state, f.found, f.err
}

func (f *fakeUsers) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	f.memberCalls++
	if f.memberErr != nil {
		return "", false, f.memberErr
	}
	return "admin", !f.notMember, nil
}

func (f *fakeUsers) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := f.Membership(ctx, user, org)
	return role, member, true, err
}

func (f *fakeUsers) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, errors.New("fakeUsers: ActiveImpersonation must not be read by the edge verifier")
}

func (*fakeUsers) ReadsPlatformRoles() bool { return false }

func (f *fakeUsers) AccessUser(ctx context.Context, id uuid.UUID) (policy.AccessUserRow, bool, error) {
	state, found, err := f.UserState(ctx, id)
	return policy.AccessUserRow{State: state}, found, err
}

// activeUserStore is an active users row at token_version 0.
func activeUserStore() *fakeUsers {
	return &fakeUsers{state: policy.UserState{IsActive: true}, found: true}
}

// mustEdgeVerifier is an EdgeVerifier over an active users row; it accepts no
// token until RouteAccess.
func mustEdgeVerifier(t *testing.T) *EdgeVerifier {
	t.Helper()
	v, err := NewEdgeVerifier(activeUserStore())
	if err != nil {
		t.Fatalf("NewEdgeVerifier: unexpected error: %v", err)
	}
	return v
}

// An edge verifier without a users store would accept the token of a
// deactivated user (CHAOS-6290): nil is refused at construction.
func TestEdgeVerifier_RequiresUsersStore(t *testing.T) {
	if _, err := NewEdgeVerifier(nil); err == nil {
		t.Fatal("NewEdgeVerifier accepted a nil users store")
	}
}

// The routed users-row refusals log which refusal it was, never the subject:
// the edge verifier hands policy a logger without the user_id attribute
// (edgeLogger).
func TestEdgeVerifier_RoutedUsersRowRefusalsLogTheReasonNotTheSubject(t *testing.T) {
	subject := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	wire := "prn_" + base64.RawURLEncoding.EncodeToString(subject[:])
	token := headerToken(`{"alg":"EdDSA","typ":"at+jwt","kid":"k"}`)
	cases := map[string]struct {
		users *fakeUsers
		want  string
	}{
		"inactive":   {&fakeUsers{state: policy.UserState{IsActive: false}, found: true}, "user not found or deactivated"},
		"not found":  {&fakeUsers{found: false}, "user not found or deactivated"},
		"store fail": {&fakeUsers{err: errors.New("permission denied for table users")}, "user lookup failed"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var logBuf bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
			defer slog.SetDefault(previous)
			v, err := NewEdgeVerifier(tc.users)
			if err != nil {
				t.Fatal(err)
			}
			if err := v.RouteAccess(stubValidator(t, stubEdDSA{claims: &authverify.AccessClaims{Subject: wire, SessionID: "ses_y"}}), "svc"); err != nil {
				t.Fatal(err)
			}
			if _, err := v.VerifyRouted(context.Background(), "GET /r", token); err == nil {
				t.Fatal("VerifyRouted: expected a refusal")
			}
			out := logBuf.String()
			if !strings.Contains(out, tc.want) {
				t.Fatalf("log output lacks the refusal %q (an operator could not tell which check refused):\n%s", tc.want, out)
			}
			if strings.Contains(out, subject.String()) || strings.Contains(out, "user_id") {
				t.Fatalf("log output carries the token subject:\n%s", out)
			}
		})
	}
}
