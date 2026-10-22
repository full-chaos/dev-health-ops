package investment

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
)

// §0.4j: a unit takes the ownership at its own end day. A unit that ended before the first ownership record of a
// repository takes the repository's first recorded owners (D5987 option C), and a unit that ended in a gap takes none.
func TestAUnitEndedBeforeTheFirstRecordTakesTheFirstOwners(t *testing.T) {
	day := func(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }
	repo := uuid.MustParse("a1000000-0000-4000-8000-000000000001")
	closed := day(time.April, 1)
	set := chquery.TeamRepoDonorSet{
		TeamsByWorkItem: map[string][]string{"issue-1": {"team-a"}},
		IntervalsByTeam: map[string][]chquery.TeamRepoInterval{
			"team-a": {{TeamID: "team-a", RepoID: repo, ValidFrom: day(time.March, 1), ValidTo: &closed}},
		},
		FirstByRepo: map[uuid.UUID]time.Time{repo: day(time.March, 1)},
	}
	runAsOf := day(time.September, 21)
	for _, tc := range []struct {
		name    string
		unitEnd time.Time
		want    int
	}{
		{"ended before the first record", day(time.January, 15), 1},
		{"ended at the first record", day(time.March, 1), 1},
		{"ended in a gap after the only record closed", day(time.April, 15), 0},
		{"ended after the run (clock ahead)", day(time.October, 30), 0},
	} {
		donors := set.DonorsAt("issue-1", unitOwnershipAsOf(tc.unitEnd, runAsOf))
		if len(donors) != tc.want {
			t.Errorf("%s: %d donor(s), want %d: %v", tc.name, len(donors), tc.want, donors)
		}
	}
}
