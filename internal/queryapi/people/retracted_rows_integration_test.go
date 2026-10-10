//go:build integration

package people

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// The two user-level tables hold a row of zeros with a retraction marker over a
// person-day that a recompute no longer produces (CHAOS-9135). Every read of the
// person pages takes the newest row of each key and leaves a key out when that
// row is a retraction. Three people, in each table:
//
//   - alice: one measured row                          -> shown
//   - bob:   a measured row, then a NEWER retraction   -> hidden
//   - carol: a retraction, then a NEWER measured row   -> shown
func TestAPersonWhoseNewestRowIsARetractionIsNoPersonOnThePeoplePages(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	crossorg.SeedRetractedPeople(ctx, t, admin, f, day)
	person := func(name string) string { return name + "@example.com" }
	personID := func(name string) string {
		sum := md5.Sum([]byte(person(name)))
		return hex.EncodeToString(sum[:])
	}

	for name, wantFound := range map[string]bool{"alice": true, "bob": false, "carol": true} {
		identity, err := resolvePersonIdentity(ctx, client, personID(name), f.OrgA)
		if err != nil {
			t.Fatal(err)
		}
		if found := identity != ""; found != wantFound {
			t.Errorf("resolvePersonIdentity(%s) found=%v, want %v: a person whose newest rows are retractions is no person", name, found, wantFound)
		}
		coverage, err := fetchIdentityCoverage(ctx, client, []string{person(name)}, f.OrgA)
		if err != nil {
			t.Fatal(err)
		}
		if (coverage > 0) != wantFound {
			t.Errorf("fetchIdentityCoverage(%s) = %d, want sources only for a person with a live row", name, coverage)
		}
		for _, table := range []struct{ table, column, identity string }{
			{"user_metrics_daily", "loc_touched", "identity_id"},
			{"work_item_user_metrics_daily", "items_completed", "user_identity"},
		} {
			value, hasData, err := fetchPersonMetricValue(ctx, client, table.table, table.column, "sum", table.identity,
				[]string{person(name)}, day, day.AddDate(0, 0, 1), "", f.OrgA)
			if err != nil {
				t.Fatal(err)
			}
			if wantFound && (value != 10 || !hasData) {
				t.Errorf("%s of %s in %s = %v has_data=%v, want 10 and true", table.column, name, table.table, value, hasData)
			}
			if !wantFound && (value != 0 || hasData) {
				t.Errorf("%s of %s in %s = %v has_data=%v, want no data: the newest row of the key is a retraction", table.column, name, table.table, value, hasData)
			}
		}
	}

	results, err := BuildSearchResponse(ctx, &Reader{client: client}, f.OrgA, SearchParams{Query: "@example.com", Limit: 50, Now: day.AddDate(0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, result := range results {
		found[result.PersonID] = true
	}
	if !found[personID("alice")] || !found[personID("carol")] || found[personID("bob")] {
		t.Errorf("search found %v, want alice and carol and not bob", found)
	}
}
