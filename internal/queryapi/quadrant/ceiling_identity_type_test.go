package quadrant

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// assignee of work_item_cycle_times is Nullable(String); a union arm that selects it bare makes the identity column
// Nullable(String), which resolvePersonIdentity scans into a *string.
func TestTheCeilingWorkArmSelectsAStringIdentity(t *testing.T) {
	ceiling, err := teamscope.ResolveCeiling(context.Background(), nil, pepgate.Allowlist("org-1", nil))
	if err != nil || !ceiling.Restricted() {
		t.Fatalf("ceiling restricted %v err %v", ceiling.Restricted(), err)
	}
	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &fixtureRowScanner{}, nil
	}}
	if _, err := resolvePersonIdentity(context.Background(), client, "person-1", "org-1", ceiling); err != nil {
		t.Fatalf("resolvePersonIdentity: %v", err)
	}
	if strings.Contains(captured, " assignee AS identity") || !strings.Contains(captured, "coalesce(assignee, '') AS identity") {
		t.Errorf("the ceiling work arm selects a Nullable assignee as the identity:\n%s", captured)
	}
}
