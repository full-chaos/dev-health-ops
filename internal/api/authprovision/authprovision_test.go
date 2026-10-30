package authprovision

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const testToken = "svc_authsess_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

type seen struct {
	method, path, auth, contentType string
	body                            map[string]any
}

func newClient(t *testing.T, answer func(w http.ResponseWriter, got seen)) (*Client, *seen) {
	t.Helper()
	got := &seen{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.method, got.path, got.auth, got.contentType = r.Method, r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		got.body = nil
		_ = json.Unmarshal(raw, &got.body)
		answer(w, *got)
	}))
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(base, testToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client, got
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func membershipAnswer(org, principal uuid.UUID, role, status string) string {
	return `{"membership_id":"` + uuid.NewString() + `","organization_id":"` + org.String() + `","principal_id":"` + principal.String() +
		`","role":"` + role + `","status":"` + status + `","joined_at":"2026-10-29T09:00:00Z","revision":2}`
}

func TestPathsAreTheAuthProvisioningRoutes(t *testing.T) {
	for got, want := range map[string]string{
		PathMembershipAdd:      "/internal/v1/memberships/add",
		PathMembershipRole:     "/internal/v1/memberships/role",
		PathMembershipRemove:   "/internal/v1/memberships/remove",
		PathMembershipTransfer: "/internal/v1/memberships/transfer",
		PathInviteCreate:       "/internal/v1/invites/create",
		PathInviteAccept:       "/internal/v1/invites/accept",
		PathOrgDeprovision:     "/internal/v1/organizations/deprovision",
	} {
		if got != want {
			t.Fatalf("path %q, want %q", got, want)
		}
	}
}

func TestNewRefusesAnythingButAnAuthSessionIssuerCredential(t *testing.T) {
	base, _ := url.Parse("http://auth-service:8095")
	for _, token := range []string{"", "svc_acr_" + strings.Repeat("A", 43), "svc_authsess_short", "Bearer " + testToken} {
		_, err := New(base, token, time.Second)
		if err == nil {
			t.Fatalf("New accepted %q", token)
		}
		if token != "" && strings.Contains(err.Error(), token) {
			t.Fatalf("the error carries the token: %v", err)
		}
	}
	if _, err := New(nil, testToken, time.Second); err == nil {
		t.Fatal("New accepted no URL")
	}
}

// Each verb posts exactly the route's body with the service credential.
func TestEveryVerbSendsItsRouteBodyWithTheServiceCredential(t *testing.T) {
	org, principal, actor, inviter, other := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	invite := uuid.New()
	metadata := json.RawMessage(`{"ip_address":"10.0.0.1"}`)
	for _, tc := range []struct {
		name, path string
		answer     string
		call       func(c *Client) error
		want       map[string]any
	}{
		{"add", PathMembershipAdd, membershipAnswer(org, principal, "admin", "active"),
			func(c *Client) error {
				_, err := c.AddMember(context.Background(), org, principal, actor, "admin", &inviter)
				return err
			},
			map[string]any{"organization_id": org.String(), "principal_id": principal.String(), "actor_principal_id": actor.String(),
				"role": "admin", "invited_by_principal_id": inviter.String()}},
		{"add, no inviter", PathMembershipAdd, membershipAnswer(org, principal, "member", "active"),
			func(c *Client) error {
				_, err := c.AddMember(context.Background(), org, principal, actor, "member", nil)
				return err
			},
			map[string]any{"organization_id": org.String(), "principal_id": principal.String(), "actor_principal_id": actor.String(), "role": "member"}},
		{"role", PathMembershipRole, membershipAnswer(org, principal, "viewer", "active"),
			func(c *Client) error {
				_, err := c.ChangeRole(context.Background(), org, principal, actor, "viewer")
				return err
			},
			map[string]any{"organization_id": org.String(), "principal_id": principal.String(), "actor_principal_id": actor.String(), "role": "viewer"}},
		{"remove", PathMembershipRemove, membershipAnswer(org, principal, "member", "removed"),
			func(c *Client) error { return c.RemoveMember(context.Background(), org, principal, actor) },
			map[string]any{"organization_id": org.String(), "principal_id": principal.String(), "actor_principal_id": actor.String()}},
		{"transfer", PathMembershipTransfer,
			`{"organization_id":"` + org.String() + `","from_principal_id":"` + principal.String() + `","owner_principal_id":"` + other.String() + `"}`,
			func(c *Client) error { return c.TransferOwnership(context.Background(), org, principal, other, actor) },
			map[string]any{"organization_id": org.String(), "from_principal_id": principal.String(), "to_principal_id": other.String(),
				"actor_principal_id": actor.String()}},
		{"invite create", PathInviteCreate,
			`{"invite_id":"` + invite.String() + `","token":"tok.en","expires_at":"2026-11-01T09:00:00Z"}`,
			func(c *Client) error {
				_, err := c.CreateInvite(context.Background(), InviteRequest{OrgID: org, Email: "a@example.com", Role: "member",
					TTL: 72 * time.Hour, Actor: actor, Description: "Organization invite created", RequestMetadata: metadata})
				return err
			},
			map[string]any{"organization_id": org.String(), "email": "a@example.com", "role": "member", "ttl_seconds": float64(259200),
				"actor_principal_id": actor.String(), "description": "Organization invite created",
				"request_metadata": map[string]any{"ip_address": "10.0.0.1"}}},
		{"invite accept", PathInviteAccept,
			`{"invite_id":"` + invite.String() + `","organization_id":"` + org.String() + `","membership_id":"` + uuid.NewString() + `","role":"member"}`,
			func(c *Client) error {
				_, err := c.AcceptInvite(context.Background(), AcceptRequest{Token: "tok.en", PrincipalID: principal, Description: "Invite accepted"})
				return err
			},
			map[string]any{"token": "tok.en", "principal_id": principal.String(), "description": "Invite accepted"}},
		{"deprovision", PathOrgDeprovision,
			`{"organization_id":"` + org.String() + `","removed_memberships":2,"revoked_invites":0,"suspended":true}`,
			func(c *Client) error { return c.Deprovision(context.Background(), org, actor) },
			map[string]any{"organization_id": org.String(), "actor_principal_id": actor.String()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, got := newClient(t, func(w http.ResponseWriter, _ seen) { writeJSON(w, http.StatusOK, tc.answer) })
			if err := tc.call(client); err != nil {
				t.Fatalf("call = %v", err)
			}
			if got.method != http.MethodPost || got.path != tc.path || got.auth != "Bearer "+testToken || got.contentType != "application/json" {
				t.Fatalf("request = %s %s auth=%v type=%q", got.method, got.path, got.auth == "Bearer "+testToken, got.contentType)
			}
			wantJSON, _ := json.Marshal(tc.want)
			gotJSON, _ := json.Marshal(got.body)
			if string(gotJSON) != string(wantJSON) {
				t.Fatalf("body = %s, want %s", gotJSON, wantJSON)
			}
		})
	}
}

// An invite's request metadata over the route's 2048-byte bound is left out
// (auth-service writes "{}"), so a long User-Agent never refuses the invite.
func TestOversizedInviteMetadataIsLeftOut(t *testing.T) {
	org, actor, invite := uuid.New(), uuid.New(), uuid.New()
	client, got := newClient(t, func(w http.ResponseWriter, _ seen) {
		writeJSON(w, http.StatusOK, `{"invite_id":"`+invite.String()+`","token":"tok.en","expires_at":"2026-11-01T09:00:00Z"}`)
	})
	big := json.RawMessage(`{"user_agent":"` + strings.Repeat("u", 2100) + `"}`)
	if _, err := client.CreateInvite(context.Background(), InviteRequest{OrgID: org, Email: "a@example.com", Role: "member",
		TTL: time.Hour, Actor: actor, Description: "d", RequestMetadata: big}); err != nil {
		t.Fatal(err)
	}
	if _, present := got.body["request_metadata"]; present {
		t.Fatalf("an oversized request_metadata was sent")
	}
}

// An empty description is left out, not sent as "": the routes refuse an
// empty text member as malformed (authapi optionalText), so an invite create
// with no description would answer 400 malformed and go-api 503.
func TestAnEmptyDescriptionIsLeftOut(t *testing.T) {
	org, actor, principal, invite := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	client, got := newClient(t, func(w http.ResponseWriter, got seen) {
		switch got.path {
		case PathInviteCreate:
			writeJSON(w, http.StatusOK, `{"invite_id":"`+invite.String()+`","token":"tok.en","expires_at":"2026-11-01T09:00:00Z"}`)
		case PathInviteAccept:
			writeJSON(w, http.StatusOK, `{"invite_id":"`+invite.String()+`","organization_id":"`+org.String()+`","membership_id":"`+uuid.NewString()+`","role":"member"}`)
		}
	})
	if _, err := client.CreateInvite(context.Background(), InviteRequest{OrgID: org, Email: "a@example.com", Role: "member", TTL: time.Hour, Actor: actor}); err != nil {
		t.Fatal(err)
	}
	if _, present := got.body["description"]; present {
		t.Fatalf("invite create sent an empty description: %v", got.body)
	}
	if _, err := client.AcceptInvite(context.Background(), AcceptRequest{Token: "tok.en", PrincipalID: principal}); err != nil {
		t.Fatal(err)
	}
	if _, present := got.body["description"]; present {
		t.Fatalf("invite accept sent an empty description: %v", got.body)
	}
}

func TestSuccessAnswersAreReadBack(t *testing.T) {
	org, principal, actor, invite, membership := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	client, _ := newClient(t, func(w http.ResponseWriter, got seen) {
		switch got.path {
		case PathInviteCreate:
			writeJSON(w, http.StatusOK, `{"invite_id":"`+invite.String()+`","token":"tok.en","expires_at":"2026-11-01T09:00:00.5Z"}`)
		case PathInviteAccept:
			writeJSON(w, http.StatusOK, `{"invite_id":"`+invite.String()+`","organization_id":"`+org.String()+`","membership_id":"`+membership.String()+`","role":"admin"}`)
		}
	})
	created, err := client.CreateInvite(context.Background(), InviteRequest{OrgID: org, Email: "a@example.com", Role: "member", TTL: time.Hour, Actor: actor})
	if err != nil || created.ID != invite || created.Token != "tok.en" || !created.ExpiresAt.Equal(time.Date(2026, 11, 1, 9, 0, 0, 5e8, time.UTC)) {
		t.Fatalf("CreateInvite = %+v, %v", created, err)
	}
	accepted, err := client.AcceptInvite(context.Background(), AcceptRequest{Token: "tok.en", PrincipalID: principal})
	if err != nil || accepted.InviteID != invite || accepted.OrgID != org || accepted.MembershipID != membership || accepted.Role != "admin" {
		t.Fatalf("AcceptInvite = %+v, %v", accepted, err)
	}
}

// A 4xx is a Refused carrying its status and the error.v1 reason_code; every
// other failure is ErrUnavailable. A reason that is not a plain code is dropped.
func TestFailuresNameTheirClassAndReason(t *testing.T) {
	org, principal, actor := uuid.New(), uuid.New(), uuid.New()
	for name, tc := range map[string]struct {
		status     int
		body       string
		wantStatus int
		wantReason string
	}{
		"409 last_owner":        {409, `{"schema_version":"error.v1","status":409,"reason_code":"last_owner"}`, 409, "last_owner"},
		"404 not found":         {404, `{"reason_code":"membership_not_found"}`, 404, "membership_not_found"},
		"403 no body":           {403, ``, 403, ""},
		"400 junk reason":       {400, `{"reason_code":"Bad Reason <script>"}`, 400, ""},
		"401":                   {401, `{}`, 401, ""},
		"500":                   {500, `{"reason_code":"store_unavailable"}`, 0, ""},
		"503":                   {503, `{}`, 0, ""},
		"302":                   {302, ``, 0, ""},
		"204":                   {204, ``, 0, ""},
		"200 not JSON":          {200, `nope`, 0, ""},
		"200 another principal": {200, membershipAnswer(org, uuid.New(), "admin", "active"), 0, ""},
		"200 another org":       {200, membershipAnswer(uuid.New(), principal, "admin", "active"), 0, ""},
		"200 not active":        {200, membershipAnswer(org, principal, "admin", "suspended"), 0, ""},
		"200 another role":      {200, membershipAnswer(org, principal, "viewer", "active"), 0, ""},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := newClient(t, func(w http.ResponseWriter, _ seen) {
				w.Header().Set("Location", "http://127.0.0.1:1/x")
				writeJSON(w, tc.status, tc.body)
			})
			_, err := client.AddMember(context.Background(), org, principal, actor, "admin", nil)
			if tc.wantStatus == 0 {
				if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrRefused) {
					t.Fatalf("AddMember = %v, want ErrUnavailable", err)
				}
				return
			}
			var refused *Refused
			if !errors.As(err, &refused) || !errors.Is(err, ErrRefused) || errors.Is(err, ErrUnavailable) {
				t.Fatalf("AddMember = %v, want a Refused", err)
			}
			if refused.Status != tc.wantStatus || refused.Reason != tc.wantReason || ReasonOf(err) != tc.wantReason {
				t.Fatalf("refused = %+v (ReasonOf %q), want %d %q", refused, ReasonOf(err), tc.wantStatus, tc.wantReason)
			}
		})
	}
}

func TestAnswersThatNameOtherRowsAreUnavailable(t *testing.T) {
	org, principal, actor, other := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for name, tc := range map[string]struct {
		answer string
		call   func(c *Client) error
	}{
		"remove still active": {membershipAnswer(org, principal, "member", "active"),
			func(c *Client) error { return c.RemoveMember(context.Background(), org, principal, actor) }},
		"transfer to another owner": {`{"organization_id":"` + org.String() + `","from_principal_id":"` + principal.String() + `","owner_principal_id":"` + uuid.NewString() + `"}`,
			func(c *Client) error { return c.TransferOwnership(context.Background(), org, principal, other, actor) }},
		"deprovision another org": {`{"organization_id":"` + uuid.NewString() + `","removed_memberships":0,"revoked_invites":0,"suspended":false}`,
			func(c *Client) error { return c.Deprovision(context.Background(), org, actor) }},
		"invite without token": {`{"invite_id":"` + uuid.NewString() + `","token":"","expires_at":"2026-11-01T09:00:00Z"}`,
			func(c *Client) error {
				_, err := c.CreateInvite(context.Background(), InviteRequest{OrgID: org, Email: "a@example.com", Role: "member", TTL: time.Hour, Actor: actor})
				return err
			}},
		"accept without organization": {`{"invite_id":"` + uuid.NewString() + `","organization_id":"x","membership_id":"` + uuid.NewString() + `","role":"member"}`,
			func(c *Client) error {
				_, err := c.AcceptInvite(context.Background(), AcceptRequest{Token: "t", PrincipalID: principal})
				return err
			}},
		"accept without role": {`{"invite_id":"` + uuid.NewString() + `","organization_id":"` + org.String() + `","membership_id":"` + uuid.NewString() + `","role":""}`,
			func(c *Client) error {
				_, err := c.AcceptInvite(context.Background(), AcceptRequest{Token: "t", PrincipalID: principal})
				return err
			}},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := newClient(t, func(w http.ResponseWriter, _ seen) { writeJSON(w, http.StatusOK, tc.answer) })
			if err := tc.call(client); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("= %v, want ErrUnavailable", err)
			}
		})
	}
}

func TestATimeoutAndAnUnreachableServerAreUnavailable(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	t.Cleanup(slow.Close)
	base, _ := url.Parse(slow.URL)
	client, err := New(base, testToken, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Deprovision(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("timeout = %v, want ErrUnavailable", err)
	}
	closed := httptest.NewServer(http.NotFoundHandler())
	base, _ = url.Parse(closed.URL)
	closed.Close()
	client, _ = New(base, testToken, time.Second)
	if err := client.Deprovision(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unreachable = %v, want ErrUnavailable", err)
	}
}

func TestNoErrorCarriesTheCredentialOrTheAnswerBody(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusInternalServerError} {
		client, _ := newClient(t, func(w http.ResponseWriter, _ seen) {
			writeJSON(w, status, `{"detail":"SENTINEL-ANSWER-TEXT","reason_code":"x"}`)
		})
		err := client.RemoveMember(context.Background(), uuid.New(), uuid.New(), uuid.New())
		if err == nil || strings.Contains(err.Error(), testToken) || strings.Contains(err.Error(), "SENTINEL-ANSWER-TEXT") {
			t.Fatalf("%d: error = %v", status, err)
		}
	}
}

func TestAnAnswerOverTheBoundIsUnavailable(t *testing.T) {
	client, _ := newClient(t, func(w http.ResponseWriter, _ seen) {
		writeJSON(w, http.StatusOK, `{"organization_id":"`+strings.Repeat("a", 128<<10)+`"}`)
	})
	if err := client.Deprovision(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("an oversized answer = %v, want ErrUnavailable", err)
	}
}

func TestTheClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	reached := make(chan struct{}, 4)
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached <- struct{}{}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer other.Close()
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer redirecting.Close()
	base, _ := url.Parse(redirecting.URL)
	core, err := NewCore(base, testToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if core.HTTP().CheckRedirect == nil || core.HTTP().CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the core's own CheckRedirect does not answer http.ErrUseLastResponse")
	}
	client := &Client{core: core}
	if err := client.Deprovision(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("redirect = %v, want ErrUnavailable", err)
	}
	if len(reached) != 0 {
		t.Fatalf("the redirect target saw %d request(s)", len(reached))
	}
}
