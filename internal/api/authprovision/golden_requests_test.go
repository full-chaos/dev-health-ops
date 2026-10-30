package authprovision_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
)

const goldenPath = "testdata/outgoing_requests.golden.json"

const goldenToken = "svc_authsess_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

var (
	goldenOrg       = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	goldenPrincipal = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	goldenActor     = uuid.MustParse("33333333-3333-4333-8333-333333333333")
	goldenTo        = uuid.MustParse("44444444-4444-4444-8444-444444444444")
	goldenInvitedBy = uuid.MustParse("55555555-5555-4555-8555-555555555555")
	goldenRow       = "66666666-6666-4666-8666-666666666666"
)

// goldenRequest is one outgoing request as the server saw it, with the bearer
// replaced by a placeholder that keeps its credential class prefix.
type goldenRequest struct {
	Verb    string              `json:"verb"`
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Headers map[string][]string `json:"headers"`
	Body    string              `json:"body"`
}

func bearerPlaceholder(value string) string {
	if strings.HasPrefix(value, "Bearer svc_authsess_") {
		return "Bearer svc_authsess_<PLACEHOLDER>"
	}
	return "Bearer <NOT-svc_authsess_>"
}

func goldenAnswer(path string) string {
	org, principal, actor, to := goldenOrg.String(), goldenPrincipal.String(), goldenActor.String(), goldenTo.String()
	member := func(role, status string) string {
		return `{"membership_id":"` + goldenRow + `","organization_id":"` + org + `","principal_id":"` + principal + `","role":"` + role + `","status":"` + status + `"}`
	}
	switch path {
	case authprovision.PathMembershipAdd:
		return member("member", "active")
	case authprovision.PathMembershipRole:
		return member("admin", "active")
	case authprovision.PathMembershipRemove:
		return member("member", "removed")
	case authprovision.PathMembershipTransfer:
		return `{"organization_id":"` + org + `","from_principal_id":"` + actor + `","owner_principal_id":"` + to + `"}`
	case authprovision.PathInviteCreate:
		return `{"invite_id":"` + goldenRow + `","token":"invite-token","expires_at":"2026-11-01T00:00:00Z"}`
	case authprovision.PathInviteAccept:
		return `{"invite_id":"` + goldenRow + `","organization_id":"` + org + `","membership_id":"` + goldenRow + `","role":"member"}`
	case authprovision.PathOrgDeprovision:
		return `{"organization_id":"` + org + `"}`
	case orgreactivate.Path:
		return `{"organization_id":"` + org + `","status":"active"}`
	case orgreactivate.SuspendPath:
		return `{"organization_id":"` + org + `","status":"suspended"}`
	}
	return `{}`
}

type goldenServer struct {
	mu   sync.Mutex
	last goldenRequest
	URL  *url.URL
	// exchange answers the workload token exchange; nil answers 404.
	exchange func() (int, string)
	// exchanges counts the exchange requests; provisioning is every other one.
	exchanges    int
	provisioning []rawRequest
}

// rawRequest is a provisioning request as received: its Authorization values
// and the whole of its header block and body as one string.
type rawRequest struct {
	Path string
	Auth []string
	Dump string
}

func newGoldenServer(t *testing.T) *goldenServer {
	t.Helper()
	g := &goldenServer{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if r.URL.Path == workloadtoken.ExchangePath {
			g.mu.Lock()
			g.exchanges++
			answer := g.exchange
			g.mu.Unlock()
			if answer == nil {
				http.NotFound(w, r)
				return
			}
			status, body := answer()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
			return
		}
		headers := map[string][]string{}
		for name, values := range r.Header {
			headers[name] = append([]string(nil), values...)
			if name == "Authorization" {
				headers[name] = []string{bearerPlaceholder(values[0])}
			}
		}
		g.mu.Lock()
		g.last = goldenRequest{Method: r.Method, Path: r.URL.Path, Headers: headers, Body: string(raw)}
		var dump strings.Builder
		for name, values := range r.Header {
			dump.WriteString(name + ": " + strings.Join(values, ",") + "\n")
		}
		dump.WriteString(string(raw))
		g.provisioning = append(g.provisioning, rawRequest{Path: r.URL.Path, Auth: append([]string(nil), r.Header.Values("Authorization")...), Dump: dump.String()})
		g.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, goldenAnswer(r.URL.Path))
	}))
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	g.URL = base
	return g
}

type goldenCall struct {
	verb string
	do   func() error
}

type lifecycle interface {
	orgreactivate.Reactivator
	orgreactivate.Suspender
}

// goldenCalls are the 9 outgoing verbs with fixed inputs, in a fixed order.
func goldenCalls(prov authprovision.Provisioner, life lifecycle) []goldenCall {
	ctx := context.Background()
	invitedBy := goldenInvitedBy
	metadata := json.RawMessage(`{"source":"golden"}`)
	return []goldenCall{
		{"memberships_add", func() error {
			_, err := prov.AddMember(ctx, goldenOrg, goldenPrincipal, goldenActor, "member", &invitedBy)
			return err
		}},
		{"memberships_role", func() error {
			_, err := prov.ChangeRole(ctx, goldenOrg, goldenPrincipal, goldenActor, "admin")
			return err
		}},
		{"memberships_remove", func() error { return prov.RemoveMember(ctx, goldenOrg, goldenPrincipal, goldenActor) }},
		{"memberships_transfer", func() error { return prov.TransferOwnership(ctx, goldenOrg, goldenActor, goldenTo, goldenActor) }},
		{"invites_create", func() error {
			_, err := prov.CreateInvite(ctx, authprovision.InviteRequest{OrgID: goldenOrg, Email: "golden@example.test", Role: "member",
				TTL: 72 * time.Hour, Actor: goldenActor, Description: "golden invite", RequestMetadata: metadata})
			return err
		}},
		{"invites_accept", func() error {
			_, err := prov.AcceptInvite(ctx, authprovision.AcceptRequest{Token: "invite-token", PrincipalID: goldenPrincipal,
				Description: "golden accept", RequestMetadata: metadata})
			return err
		}},
		{"organizations_deprovision", func() error { return prov.Deprovision(ctx, goldenOrg, goldenActor) }},
		{"organizations_reactivate", func() error { return life.Reactivate(ctx, goldenOrg, goldenActor) }},
		{"organizations_suspend", func() error { return life.Suspend(ctx, goldenOrg, goldenActor) }},
	}
}

// goldenVerbs drives the 9 verbs and returns the request each one sent.
func goldenVerbs(t *testing.T, g *goldenServer, prov authprovision.Provisioner, life lifecycle) []goldenRequest {
	t.Helper()
	calls := goldenCalls(prov, life)
	out := make([]goldenRequest, 0, len(calls))
	for _, call := range calls {
		if err := call.do(); err != nil {
			t.Fatalf("%s: %v", call.verb, err)
		}
		g.mu.Lock()
		got := g.last
		g.mu.Unlock()
		got.Verb = call.verb
		out = append(out, got)
	}
	return out
}

func renderGolden(requests []goldenRequest) []byte {
	for _, request := range requests {
		for _, values := range request.Headers {
			sort.Strings(values)
		}
	}
	raw, err := json.MarshalIndent(requests, "", "  ")
	if err != nil {
		panic(err)
	}
	return append(raw, '\n')
}

// The outgoing request bytes of every verb, with the selector unset (the
// session credential), are pinned by testdata/outgoing_requests.golden.json.
func TestOutgoingRequestsMatchTheGolden(t *testing.T) {
	g := newGoldenServer(t)
	prov, err := authprovision.New(g.URL, goldenToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	life, err := orgreactivate.New(g.URL, goldenToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got := renderGolden(goldenVerbs(t, g, prov, life))
	if os.Getenv("AUTHPROVISION_WRITE_GOLDEN") == "1" {
		if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("outgoing requests differ from %s\n--- got\n%s\n--- want\n%s", goldenPath, got, want)
	}
}
