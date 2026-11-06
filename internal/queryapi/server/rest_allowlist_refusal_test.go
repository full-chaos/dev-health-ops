package server

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep/pepfake"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
)

// CHAOS-13089 (acp Q5, Q6, Q9): an allowlist caller on a route class that does not narrow its reads is refused at the
// one gate point, the registry. A mixed (M) class is refused with allowlist_route_unsupported; the class name is in
// the log line, never in a metric label. A narrowable (N) class that is not yet narrowed stays unknown_obligation.
func TestAllowlistOnRoutesNotNarrowedIs503WithTheClassInTheLog(t *testing.T) {
	for _, route := range seamRoutes() {
		kind, ok := pdp.ClassKind(route.name)
		if !ok {
			t.Fatalf("%s is no route class", route.name)
		}
		if kind != pdp.KindMixed {
			// CHAOS-13090: the N classes carry the allowlist as a ceiling (rest_ceiling_test.go).
			continue
		}
		wantReason := pdp.ReasonAllowlistRouteUnsupported
		for name, keys := range map[string][]string{"allowlist []": {}, "allowlist [acme/a]": {"acme/a"}} {
			t.Run(route.name+"/"+name, func(t *testing.T) {
				logs := &bytes.Buffer{}
				previous := slog.Default()
				slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
				t.Cleanup(func() { slog.SetDefault(previous) })

				s := newSeamStack(t, route)
				rule := seamAllow(s, func(r *pepfake.Rule) { r.RepositoryScope, r.RepositoryAllowlist = "", keys })
				gate, reader := seamGate(t, pdp.ModeEnforce, namespaceOf(route.action), pepfake.New(rule))
				got := s.run(gate, s.user)
				if (answer{got.status, strings.TrimSpace(got.body)}) != gatedUnavailable {
					t.Fatalf("answered %d %q, want %+v", got.status, got.body, gatedUnavailable)
				}
				if len(got.statements) != 0 || len(s.seen) != 0 {
					t.Fatalf("a refused request reached the handler: %d statement(s)", len(got.statements))
				}
				line := logs.String()
				if !strings.Contains(line, "reason="+string(wantReason)) || !strings.Contains(line, "route_class="+route.name) {
					t.Fatalf("log %q, want reason=%s route_class=%s", line, wantReason, route.name)
				}
				if strings.Contains(line, "reason="+string(pdp.ReasonNotCarriedOut)) {
					t.Fatalf("a mixed route is refused as unknown_obligation: %q", line)
				}
				for label := range (&gatedREST{reader: reader}).counter(t, "dev_health_query_api_pdp_results_total") {
					if strings.Contains(label, route.name) || strings.Contains(label, "route_class") || strings.Contains(label, string(pdp.ReasonAllowlistRouteUnsupported)) {
						t.Fatalf("metric label %q carries the class or the reason", label)
					}
				}
			})
		}
	}
}
