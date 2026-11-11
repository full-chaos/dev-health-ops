package acrrevoke

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/acrlisting"
)

const organizationPath = "/internal/v1/organization/credentials/revoke"

type organizationReply struct {
	status int
	body   string
}

// orgACR is acr's token endpoint and organization revoke route: each exchange mints a new access token, and each
// revoke call takes the next reply.
type orgACR struct {
	srv *httptest.Server

	mu        sync.Mutex
	exchanges int
	revokes   []call
	replies   []organizationReply
}

func newOrgACR(t *testing.T, replies ...organizationReply) *orgACR {
	t.Helper()
	f := &orgACR{replies: replies}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *orgACR) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, _ := io.ReadAll(r.Body)
	if r.URL.Path == tokenPath {
		f.exchanges++
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": fmt.Sprintf("access-%d", f.exchanges), "token_type": "Bearer", "expires_in": 600})
		return
	}
	f.revokes = append(f.revokes, call{method: r.Method, path: r.URL.EscapedPath(), auth: r.Header.Get("Authorization"), contentType: r.Header.Get("Content-Type"), body: string(raw)})
	reply := organizationReply{status: http.StatusInternalServerError}
	if len(f.replies) > 0 {
		reply, f.replies = f.replies[0], f.replies[1:]
	}
	w.WriteHeader(reply.status)
	_, _ = w.Write([]byte(reply.body))
}

func (f *orgACR) client(t *testing.T) *Client {
	t.Helper()
	base, _ := url.Parse(f.srv.URL)
	endpoint, _ := url.Parse(f.srv.URL + tokenPath)
	c, err := New(Config{BaseURL: base, TokenEndpoint: endpoint, SubjectTokenFile: subjectFile(t), HTTPClient: f.srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func revoked(revoked, caller int, more bool) organizationReply {
	return organizationReply{status: http.StatusOK, body: fmt.Sprintf(`{"schema_version":"credential_revoke_organization_response.v1","revoked":%d,"caller_revoked":%d,"more":%t}`+"\n", revoked, caller, more)}
}

// C1: one exchange per call, a new access token each time, until acr answers more false; the counts are summed.
func TestRevokeOrganizationExchangesANewTokenForEveryCallUntilNoneIsLeft(t *testing.T) {
	f := newOrgACR(t, revoked(500, 0, true), revoked(500, 0, true), revoked(7, 1, false))

	got, err := f.client(t).RevokeOrganization(context.Background(), orgOne, actorOne, ReasonOrganizationDeleted)

	if err != nil {
		t.Fatalf("RevokeOrganization = %v", err)
	}
	if got != (OrganizationRevocation{Revoked: 1007, CallerRevoked: 1, Calls: 3}) {
		t.Fatalf("revocation = %+v, want 1007 revoked, 1 caller, 3 calls", got)
	}
	if f.exchanges != 3 || len(f.revokes) != 3 {
		t.Fatalf("%d exchanges, %d revoke calls; want 3 and 3", f.exchanges, len(f.revokes))
	}
	wantBody := `{"schema_version":"credential_revoke_organization_request.v1","actor_id":"` + actorOne + `","reason":"organization_deleted"}`
	for i, c := range f.revokes {
		if c.method != http.MethodPost || c.path != organizationPath || c.contentType != "application/json" || c.body != wantBody {
			t.Fatalf("call %d = %+v, want POST %s with %s", i, c, organizationPath, wantBody)
		}
		if want := fmt.Sprintf("Bearer access-%d", i+1); c.auth != want {
			t.Fatalf("call %d bearer = %q, want the new token %q", i, c.auth, want)
		}
	}
}

// C2: every answer that is not acr's 200 body is a failure, and the loop stops there.
func TestRevokeOrganizationClassifiesEveryOtherAnswer(t *testing.T) {
	for name, tc := range map[string]struct {
		reply organizationReply
		want  func(error) bool
	}{
		"409 organization_live":    {organizationReply{http.StatusConflict, acrError("organization_live", http.StatusConflict)}, func(err error) bool { return errors.Is(err, ErrOrganizationLive) }},
		"409 without the envelope": {organizationReply{http.StatusConflict, "conflict"}, kindIs(acrlisting.KindRejected)},
		"404 route not served":     {organizationReply{http.StatusNotFound, acrError("not_found", http.StatusNotFound)}, kindIs(acrlisting.KindRejected)},
		"503":                      {organizationReply{http.StatusServiceUnavailable, acrError("upstream_unavailable", http.StatusServiceUnavailable)}, kindIs(acrlisting.KindUnavailable)},
		"400":                      {organizationReply{http.StatusBadRequest, acrError("invalid_request", http.StatusBadRequest)}, kindIs(acrlisting.KindRejected)},
		"401":                      {organizationReply{http.StatusUnauthorized, acrError("invalid_token", http.StatusUnauthorized)}, kindIs(acrlisting.KindUnauthorized)},
		"403":                      {organizationReply{http.StatusForbidden, acrError("insufficient_scope", http.StatusForbidden)}, kindIs(acrlisting.KindForbidden)},
		"200 another schema":       {organizationReply{http.StatusOK, `{"schema_version":"x","revoked":1,"caller_revoked":0,"more":false}`}, kindIs(acrlisting.KindRejected)},
		"200 an unknown field":     {organizationReply{http.StatusOK, `{"schema_version":"credential_revoke_organization_response.v1","revoked":1,"caller_revoked":0,"more":false,"x":1}`}, kindIs(acrlisting.KindRejected)},
		"200 no more field":        {organizationReply{http.StatusOK, `{"schema_version":"credential_revoke_organization_response.v1","revoked":1,"caller_revoked":0}`}, kindIs(acrlisting.KindRejected)},
		"204":                      {organizationReply{http.StatusNoContent, ""}, kindIs(acrlisting.KindRejected)},
	} {
		t.Run(name, func(t *testing.T) {
			f := newOrgACR(t, tc.reply, revoked(1, 1, false))

			_, err := f.client(t).RevokeOrganization(context.Background(), orgOne, actorOne, ReasonOrganizationDisabled)

			if err == nil || !tc.want(err) {
				t.Fatalf("RevokeOrganization = %v", err)
			}
			if len(f.revokes) != 1 {
				t.Fatalf("%d revoke calls, want 1: a failure ends the loop", len(f.revokes))
			}
		})
	}
}

func kindIs(kind acrlisting.Kind) func(error) bool {
	return func(err error) bool {
		var failure *Error
		return errors.As(err, &failure) && failure.Kind == kind
	}
}

// C2: at the bound the revoke stops with ErrOrganizationIncomplete; a repeat continues and ends.
func TestRevokeOrganizationStopsAtTheBoundAndARepeatContinues(t *testing.T) {
	replies := make([]organizationReply, 0, MaxOrganizationCalls+1)
	for i := 0; i < MaxOrganizationCalls; i++ {
		replies = append(replies, revoked(500, 0, true))
	}
	f := newOrgACR(t, append(replies, revoked(3, 1, false))...)
	c := f.client(t)

	first, err := c.RevokeOrganization(context.Background(), orgOne, actorOne, ReasonOrganizationDisabled)

	if !errors.Is(err, ErrOrganizationIncomplete) || first.Calls != MaxOrganizationCalls || first.Revoked != 500*MaxOrganizationCalls {
		t.Fatalf("first = %+v, %v; want %d calls and ErrOrganizationIncomplete", first, err, MaxOrganizationCalls)
	}
	again, err := c.RevokeOrganization(context.Background(), orgOne, actorOne, ReasonOrganizationDisabled)
	if err != nil || again != (OrganizationRevocation{Revoked: 3, CallerRevoked: 1, Calls: 1}) {
		t.Fatalf("repeat = %+v, %v; want it to finish", again, err)
	}
}

// C3: a bad input sends nothing.
func TestRevokeOrganizationRefusesBadInputBeforeAnyRequest(t *testing.T) {
	for name, in := range map[string][3]string{
		"unknown reason": {orgOne, actorOne, "organization_suspended"},
		"no reason":      {orgOne, actorOne, ""},
		"bad org":        {"not-an-org", actorOne, ReasonOrganizationDeleted},
		"no actor":       {orgOne, "", ReasonOrganizationDeleted},
		"padded actor":   {orgOne, " " + actorOne, ReasonOrganizationDeleted},
		"long actor":     {orgOne, strings.Repeat("a", maxActorID+1), ReasonOrganizationDeleted},
	} {
		f := newOrgACR(t, revoked(1, 1, false))
		if _, err := f.client(t).RevokeOrganization(context.Background(), in[0], in[1], in[2]); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%s: %v, want ErrInvalidRequest", name, err)
		}
		if f.exchanges != 0 || len(f.revokes) != 0 {
			t.Fatalf("%s: %d exchanges, %d calls; want none", name, f.exchanges, len(f.revokes))
		}
	}
}

// The operator actor "operator" is a valid actor id.
func TestRevokeOrganizationAcceptsTheOperatorActor(t *testing.T) {
	f := newOrgACR(t, revoked(0, 1, false))
	if _, err := f.client(t).RevokeOrganization(context.Background(), orgOne, "operator", ReasonOrganizationDeleted); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.revokes[0].body, `"actor_id":"operator"`) {
		t.Fatalf("body = %s", f.revokes[0].body)
	}
}
