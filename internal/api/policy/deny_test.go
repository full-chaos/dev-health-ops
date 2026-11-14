package policy

import (
	"net/http"
	"slices"
	"testing"
)

// Every *Deny mount records its route, even one with an empty DenyText, so the census asks that row for a legacy-body cell:
// a mount with no text answers the guard's own text and would otherwise pass every check unseen.
func TestAnEmptyDenyTextMountIsStillACensusRow(t *testing.T) {
	g := NewGuard(nil, nil)
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	g.WrapPlatformDeny(ActionAdminTelemetry, "POST /api/v1/census-probe/platform", DenyText{}, next)
	g.BodyFirstDeny("PATCH /api/v1/census-probe/body", Authenticated, DenyText{}, next)
	rows := DenyTextRows()
	for _, key := range []string{"POST /api/v1/census-probe/platform", "PATCH /api/v1/census-probe/body"} {
		if !slices.Contains(rows, key) {
			t.Errorf("%s is mounted with an empty DenyText but is not in DenyTextRows %v", key, rows)
		}
	}
}
