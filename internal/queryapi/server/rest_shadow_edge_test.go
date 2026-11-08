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
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// CHAOS-13073 r1 (acp ruling D6714 / 13106 option D): a delegated token is counted impersonation_refused before any skip
// class, on the routed and the unrouted path, with or without an organization, for the EdDSA user access token and the
// EdDSA envelope the PDP cannot judge, and an org-required refusal is observed. The retired HS256 class is the one
// generic-401 cell of the plane (TestQueryAPIHS256IsRefusedOnEveryRow). Every other cell keeps its legacy answer.
func TestRESTShadowDelegationAndOrgRequiredEveryPath(t *testing.T) {
	forbidden := answer{http.StatusForbidden, `{"detail":"Organization context required"}`}
	served := answer{http.StatusOK, fmt.Sprintf(`{"org":%q}`, ecOrg.String())}
	refused := shadowResultsMetric + "|reason=none,result=impersonation_refused"
	delegate := uuid.NewString()
	type cell struct {
		name      string
		routed    bool
		kind      string
		org       bool
		delegated bool
		status    answer
		want      string
	}
	envelopeOrgless := answer{http.StatusOK, `{"org":""}`}
	var cells []cell
	for _, routed := range []bool{true, false} {
		path := map[bool]string{true: "routed", false: "unrouted"}[routed]
		cells = append(cells,
			cell{path + " envelope org delegated", routed, "envelope", true, true, served, refused},
			cell{path + " envelope orgless delegated", routed, "envelope", false, true, envelopeOrgless, refused},
		)
	}
	cells = append(cells,
		cell{"routed user token org delegated", true, "user", true, true, served, refused},
		cell{"routed user token orgless delegated", true, "user", false, true, forbidden, refused},
		cell{"unrouted user token org delegated", false, "user", true, true, answer{http.StatusUnauthorized, restRefusal}, ""},
		cell{"unrouted user token orgless delegated", false, "user", false, true, answer{http.StatusUnauthorized, restRefusal}, ""},
	)
	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			quietStdLog(t)
			shadowCounters(t)
			srv := pdptest.New(t)
			m := newAccessMinter(t)
			var setup *accessSetup
			if tc.routed {
				setup = testAccess(m.keys, nil, accessTestNow)
			}
			f := newRESTFixture(t, newAccessStore(), setup)
			org := ""
			if tc.org {
				org = ecOrg.String()
			}
			var token string
			switch tc.kind {
			case "user":
				extra := map[string]any{"act": map[string]any{"sub": wirePrincipal(uuid.MustParse(delegate))}}
				if !tc.org {
					extra["org_id"] = nil
				}
				token = m.mint(ecUser, extra, nil)
			case "envelope":
				by := delegate
				token = iaEnvelope(t, f.priv, principal.Claims{OrgID: org, Role: "admin", ImpersonatedBy: &by})
			}
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			gate := pdp.New(pdp.Config{Mode: pdp.ModeShadow, Decider: pepOf(t, srv.URL), Lookup: knobs(nil), Meter: provider.Meter("test"),
				Logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil))})
			handler := planeHandler(f.handler.(*http.ServeMux), gate)
			before := shadowCounters(t)
			req := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if got := (answer{rec.Code, strings.TrimSpace(rec.Body.String())}); got != tc.status {
				t.Fatalf("answer %+v, want %+v", got, tc.status)
			}
			want := map[string]int64{tc.want: 1}
			if tc.want == "" {
				want = map[string]int64{}
			}
			if tc.want == "" {
				if d := settleQuiet(t, before); len(d) != 0 {
					t.Fatalf("5.12 counters %v, want none", d)
				}
				return
			}
			if d := settle(t, before, want); fmt.Sprint(d) != fmt.Sprint(want) {
				t.Fatalf("5.12 counters %v, want %v", d, want)
			}
			if n := len(srv.Requests()); n != 0 {
				t.Fatalf("PDP asked %d times", n)
			}
		})
	}
}
