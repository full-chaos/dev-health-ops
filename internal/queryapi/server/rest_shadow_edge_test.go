package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// CHAOS-13073 r1: an HS256 edge token's delegation state and an org-required refusal both reach the REST shadow, on the
// routed and the unrouted path alike, and neither changes the answer.
func TestRESTShadowEdgeTokenDelegationAndOrgRequired(t *testing.T) {
	forbidden := answer{http.StatusForbidden, `{"detail":"Organization context required"}`}
	refused := shadowResultsMetric + "|reason=none,result=impersonation_refused"
	skipped := shadowResultsMetric + "|reason=hs256,result=skipped"
	delegate := uuid.NewString()
	for _, tc := range []struct {
		name      string
		routed    bool
		org       string
		delegated bool
		status    answer
		want      string
	}{
		{"routed, org, delegated", true, ecOrg.String(), true, answer{http.StatusOK, fmt.Sprintf(`{"org":%q}`, ecOrg.String())}, refused},
		{"routed, no org, delegated", true, "", true, forbidden, refused},
		{"routed, no org, not delegated", true, "", false, forbidden, skipped},
		{"unrouted, org, delegated", false, ecOrg.String(), true, answer{http.StatusOK, fmt.Sprintf(`{"org":%q}`, ecOrg.String())}, refused},
		{"unrouted, no org, delegated", false, "", true, forbidden, refused},
		{"unrouted, no org, not delegated", false, "", false, forbidden, skipped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			quietStdLog(t)
			shadowCounters(t)
			srv := pdptest.New(t)
			claims := ecEdgeClaims{sub: ecUser.String(), orgID: tc.org, tokenVersion: 5}
			if tc.delegated {
				claims.impersonatingUserID = delegate
			}
			var setup *accessSetup
			if tc.routed {
				setup = testAccess(newAccessMinter(t).keys, nil, accessTestNow)
			}
			f := newRESTFixture(t, newAccessStore(), setup, true)
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			gate := pdp.New(pdp.Config{Mode: pdp.ModeShadow, Decider: pepOf(t, srv.URL), Lookup: knobs(nil), Meter: provider.Meter("test"),
				Logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil))})
			handler := planeHandler(f.handler.(*http.ServeMux), gate)
			before := shadowCounters(t)
			req := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
			req.Header.Set("Authorization", "Bearer "+ecMintEdgeToken(t, claims))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			got := answer{rec.Code, strings.TrimSpace(rec.Body.String())}
			if got != tc.status {
				t.Fatalf("answer %+v, want %+v", got, tc.status)
			}
			want := map[string]int64{tc.want: 1}
			if d := settle(t, before, want); fmt.Sprint(d) != fmt.Sprint(want) {
				t.Fatalf("5.12 counters %v, want %v", d, want)
			}
			if n := len(srv.Requests()); n != 0 {
				t.Fatalf("PDP asked %d times for an HS256 token", n)
			}
		})
	}
}
