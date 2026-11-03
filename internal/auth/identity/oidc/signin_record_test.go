package oidc

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
)

type noSignInRecord struct{}

func (noSignInRecord) Succeeded(context.Context, ssoaudit.Success) error { return nil }

// signInSpy records each call; steps is shared with orderedSessions so a test reads the order of the two.
type signInSpy struct {
	got   []ssoaudit.Success
	err   error
	steps *[]string
}

func (s *signInSpy) Succeeded(_ context.Context, success ssoaudit.Success) error {
	s.got = append(s.got, success)
	if s.steps != nil {
		*s.steps = append(*s.steps, "record")
	}
	return s.err
}

type orderedSessions struct {
	steps *[]string
	err   error
}

func (s orderedSessions) Issue(context.Context, SessionRequest) (Tokens, error) {
	*s.steps = append(*s.steps, "issue")
	return Tokens{AccessToken: "a"}, s.err
}

func (orderedSessions) Preflight(context.Context) error { return nil }

func TestNewRefusesAConfigWithoutASignInRecord(t *testing.T) {
	if _, err := New(Config{Providers: &fakeProviders{}, Flows: &scriptedFlows{}, Identities: &orderIdentities{},
		Memberships: resolveMembers{}, Entitlements: &countingEntitlements{}, Sessions: &resolveSessions{}}); err == nil ||
		err.Error() != "oidc: a required dependency is missing" {
		t.Fatalf("err = %v", err)
	}
}

// A completed OAuth sign-in writes one record, after the session, naming the admitted provider, its organization and
// the provider row's protocol.
func TestOAuthSignInRecordsTheSuccessAfterTheSession(t *testing.T) {
	var steps []string
	spy := &signInSpy{steps: &steps}
	got, row, failure := runProviderCase(t, providerCase{signIn: spy, sessions: orderedSessions{steps: &steps}}, nil, nil)
	if failure != nil {
		t.Fatalf("sign-in refused: %+v", got)
	}
	if len(steps) != 2 || steps[0] != "issue" || steps[1] != "record" {
		t.Fatalf("steps %v, want issue then record", steps)
	}
	if len(spy.got) != 1 {
		t.Fatalf("records %+v", spy.got)
	}
	record := spy.got[0]
	if record.OrgID != row.OrgID || record.ProviderID != row.ID || record.Protocol != "oauth_gitlab" || record.PrincipalID.String() == "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("record %+v, row %s org %s", record, row.ID, row.OrgID)
	}
}

// A record that cannot be written is the 500 store_unavailable: the tokens are withheld.
func TestOAuthSignInRecordFailureIsThe500(t *testing.T) {
	_, _, failure := runProviderCase(t, providerCase{signIn: &signInSpy{err: errors.New("permission denied")}}, nil, nil)
	if failure == nil || failure.Status != http.StatusInternalServerError || failure.Reason != ReasonStoreUnavailable {
		t.Fatalf("failure %+v, want 500 store_unavailable", failure)
	}
}

// A sign-in whose session could not be issued writes no record.
func TestOAuthSignInWithoutASessionWritesNoRecord(t *testing.T) {
	var steps []string
	spy := &signInSpy{steps: &steps}
	_, _, failure := runProviderCase(t, providerCase{signIn: spy,
		sessions: orderedSessions{steps: &steps, err: errors.New("no session")}}, nil, nil)
	if failure == nil || failure.Status != http.StatusInternalServerError || len(spy.got) != 0 {
		t.Fatalf("failure %+v, records %+v", failure, spy.got)
	}
}
