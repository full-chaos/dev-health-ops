package pdp

import (
	"context"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// Product rule 1 (D6773), CHAOS-13209 B3: an enforced row asked with no organization (only a superuser passes the
// verifier with none) keeps today's answer, the Legacy scope of no organization, as the same caller's HS256 token
// gets; the PDP cannot judge an organization action without one and is not asked.
func TestPDPEnforceNoOrganizationKeepsTheLegacyScope(t *testing.T) {
	srv := pdptest.New(t)
	_, cfg := newMeter(t)
	cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, pepClient(t, srv.URL), cells("analytics")
	scope, err := New(cfg).Authorize(context.Background(), testProfile, testBearer, "")
	if err != nil || !reflect.DeepEqual(scope, pepgate.Legacy("")) {
		t.Fatalf("no organization: scope %+v err %v, want the Legacy scope of no organization and no error", scope, err)
	}
	if n := len(srv.Requests()); n != 0 {
		t.Fatalf("%d PDP calls, want 0", n)
	}
}
