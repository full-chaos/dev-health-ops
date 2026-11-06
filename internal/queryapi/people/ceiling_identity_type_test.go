package people

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// assignee of work_item_cycle_times is Nullable(String); a union arm that selects it bare makes the identity column
// Nullable(String), which the reads scan into a *string.
func TestTheCeilingWorkArmsSelectAStringIdentity(t *testing.T) {
	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &scalarStringScanner{values: []string{"alice@example.com"}}, nil
	}}
	scope := readScope{ceiling: ceilingOf(t, "11111111-1111-1111-1111-111111111111")}
	if _, err := resolvePersonIdentity(context.Background(), client, "person-1", "org-1", scope); err != nil {
		t.Fatalf("resolvePersonIdentity: %v", err)
	}
	for name, query := range map[string]string{"resolvePersonIdentity": captured, "searchWorkArmLinked": searchWorkArmLinked("")} {
		if strings.Contains(query, " assignee AS identity") || !strings.Contains(query, "coalesce(assignee, '') AS identity") {
			t.Errorf("%s: the ceiling work arm selects a Nullable assignee as the identity:\n%s", name, query)
		}
	}
}
