//go:build integration

package filteroptions

import (
	"context"
	"reflect"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// The developer list and the team list leave out a key whose newest row is a
// retraction (CHAOS-9135). The row of zeros of a person carries the team of the
// row it retracts, so a team that only a retracted person held is no longer
// listed from the user-level tables.
func TestTheListsLeaveOutRetractedPeople(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	crossorg.SeedRetractedPeople(ctx, t, admin, f, day)
	bindings := []dhclickhouse.Binding{{Name: "org_id", Value: f.OrgA}}
	developers, err := distinctValues(ctx, client, developersQuery, "developers", bindings)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"alice@example.com", "carol@example.com"}; !reflect.DeepEqual(developers, want) {
		t.Errorf("developers = %v, want %v", developers, want)
	}
}
