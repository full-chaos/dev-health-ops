package pdp

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// CHAOS-13074 Q2: ONE shadow holds the REST rows and the extra (GraphQL) rows, so one in-flight cap and one sample
// counter serve both. The extra rows come from the caller: this package never imports the graph package.
func TestPDPExtraShadowRowsJoinTheOneShadow(t *testing.T) {
	srv := pdptest.New(t)
	row, err := policy.ShadowRowFor("analytics.read")
	if err != nil {
		t.Fatal(err)
	}
	g := New(Config{Mode: ModeShadow, Decider: pepClient(t, srv.URL), ExtraShadowRows: map[string]policy.ShadowRow{"Query.catalog": row}})
	shadow := g.Shadow()
	if shadow == nil {
		t.Fatal("no shadow with an extra row")
	}
	for _, pattern := range []string{"Query.catalog", testProfile} {
		shadow.Observe(context.Background(), policy.ShadowObservation{Pattern: pattern, Bearer: testBearer, OrgID: testOrg,
			Caller: policy.ShadowCallerMember, UserAccessToken: true, LegacyAllowed: true})
	}
	g.Wait()
	if got := len(srv.Requests()); got != 2 {
		t.Fatalf("PDP asked %d times, want 2 (one extra row, one REST row)", got)
	}
}

func TestPDPExtraShadowRowKeyCollisionIsAnErrorAndTheShadowIsOff(t *testing.T) {
	srv := pdptest.New(t)
	row, _ := policy.ShadowRowFor("analytics.read")
	var logs bytes.Buffer
	g := New(Config{Mode: ModeShadow, Decider: pepClient(t, srv.URL), Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		ExtraShadowRows: map[string]policy.ShadowRow{testProfile: row}})
	if g.Shadow() != nil {
		t.Fatal("a shadow was built over a colliding row key")
	}
	if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "variable=rows") {
		t.Fatalf("log %q, want an ERROR naming the variable rows", logs.String())
	}
}
