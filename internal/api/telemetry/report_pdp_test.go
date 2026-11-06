package telemetry

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/pep/pepfake"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

const reportBearer = "aaaa.bbbb.cccc-dddd-eeee-ffff"

// denyingFake allows what its rules allow and answers every other request with the decision point's deny. pepfake
// alone answers an unmatched request with a local no_decision, which go-api must treat as no verdict (503), so a deny
// case built on it would pass through the no-verdict path.
type denyingFake struct {
	rules *pepfake.Fake
	deny  pep.Decision
}

func newDenyingFake(t *testing.T, rules ...pepfake.Rule) denyingFake {
	t.Helper()
	pdp := pdptest.New(t)
	pdp.Deny("platform_role_absent")
	client, err := pep.New(pep.Config{BaseURL: pdp.URL, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: "dev-health-ops-api", Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	deny := client.Decide(context.Background(), pep.Request{PrincipalReference: reportBearer, Action: rule.ActionAdminTelemetry, ResourceType: "platform", ResourceKey: "platform"})
	if deny.Allowed() || deny.Local() {
		t.Fatalf("the test decision point's deny: allowed=%v local=%v reason=%s", deny.Allowed(), deny.Local(), deny.Reason())
	}
	return denyingFake{rules: pepfake.New(rules...), deny: deny}
}

func (f denyingFake) Decide(ctx context.Context, req pep.Request) pep.Decision {
	if d := f.rules.Decide(ctx, req); d.Allowed() || d.Reason() != pep.ReasonNoDecision {
		return d
	}
	return f.deny
}

func (f denyingFake) DecideBatch(ctx context.Context, reqs []pep.Request) []pep.Decision {
	out := make([]pep.Decision, len(reqs))
	for i, req := range reqs {
		out[i] = f.Decide(ctx, req)
	}
	return out
}

// With the admin cell on, the instance-wide report is served by the decision point's admin.telemetry, not by
// go-api's own flag; an impersonating caller is refused before any question; no verdict is a 503.
func TestPDPAction_ReportIsTheDecisionPointsAdminTelemetry(t *testing.T) {
	allow := pepfake.Rule{PrincipalReference: reportBearer, Action: rule.ActionAdminTelemetry, ResourceType: "platform", ResourceKey: "platform"}
	bad := allow
	bad.MinimumAssurance = "bogus"
	h := handlers{logger: slog.New(slog.DiscardHandler)}
	served := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	for _, c := range []struct {
		name          string
		rules         []pepfake.Rule
		superuser     bool
		impersonating bool
		want          int
	}{
		{"pdp allows, flag off", []pepfake.Rule{allow}, false, false, 200},
		{"pdp denies, flag on", nil, true, false, 403},
		{"pdp allows, impersonating", []pepfake.Rule{allow}, true, true, 403},
		{"no verdict", []pepfake.Rule{bad}, true, false, 503},
	} {
		ctx := policy.WithPlatformDecider(context.Background(), newDenyingFake(t, c.rules...), reportBearer)
		if c.impersonating {
			ctx = policy.WithImpersonation(ctx, &policy.Impersonation{ID: uuid.New(), AdminUserID: uuid.New(), TargetUserID: uuid.New(),
				TargetOrgID: uuid.New(), TargetRole: "member", ExpiresAt: time.Now().Add(time.Hour)})
		}
		r := httptest.NewRequest(http.MethodPost, "/report", nil)
		r = r.WithContext(policy.WithUser(ctx, &policy.User{IsSuperuser: c.superuser}))
		w := httptest.NewRecorder()
		h.requirePlatformRole(served).ServeHTTP(w, r)
		if w.Code != c.want {
			t.Errorf("%s: status %d, want %d", c.name, w.Code, c.want)
		}
	}
}
