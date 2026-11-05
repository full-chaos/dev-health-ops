package telemetry

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep/pepfake"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

const reportBearer = "aaaa.bbbb.cccc-dddd-eeee-ffff"

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
		ctx := policy.WithPlatformDecider(context.Background(), pepfake.New(c.rules...), reportBearer)
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
