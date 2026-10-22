package chquery

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func firstOwnersSet() (TeamRepoDonorSet, uuid.UUID, uuid.UUID) {
	day := func(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }
	steady, moved := uuid.MustParse("a1000000-0000-4000-8000-000000000001"), uuid.MustParse("a1000000-0000-4000-8000-000000000002")
	closed := day(time.April, 1)
	share := func(v float64) *float64 { return &v }
	return TeamRepoDonorSet{
		TeamsByWorkItem: map[string][]string{"wi": {"team-a", "team-c"}},
		IntervalsByTeam: map[string][]TeamRepoInterval{
			"team-a": {
				{TeamID: "team-a", RepoID: steady, ValidFrom: day(time.March, 1), Share: share(0.6)},
				{TeamID: "team-a", RepoID: moved, ValidFrom: day(time.March, 1), ValidTo: &closed, Share: share(1)},
			},
			"team-c": {{TeamID: "team-c", RepoID: moved, ValidFrom: day(time.May, 1), Share: share(1)}},
		},
		FirstByRepo: map[uuid.UUID]time.Time{steady: day(time.March, 1), moved: day(time.March, 1)},
	}, steady, moved
}

func TestDonorsAtGivesTheFirstOwnersBeforeTheFirstRecord(t *testing.T) {
	set, steady, moved := firstOwnersSet()
	day := func(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }
	repos := func(at time.Time) map[uuid.UUID][]string {
		got := map[uuid.UUID][]string{}
		for _, donor := range set.DonorsAt("wi", at) {
			got[donor.RepoID] = append(got[donor.RepoID], donor.TeamID)
		}
		return got
	}
	for _, tc := range []struct {
		name string
		at   time.Time
		want map[uuid.UUID][]string
	}{
		{"before the first record", day(time.January, 15), map[uuid.UUID][]string{steady: {"team-a"}, moved: {"team-a"}}},
		{"at the first record", day(time.March, 1), map[uuid.UUID][]string{steady: {"team-a"}, moved: {"team-a"}}},
		{"in a gap between records", day(time.April, 15), map[uuid.UUID][]string{steady: {"team-a"}}},
		{"after the records", day(time.September, 21), map[uuid.UUID][]string{steady: {"team-a"}, moved: {"team-c"}}},
	} {
		if got := repos(tc.at); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: donors = %v, want %v", tc.name, got, tc.want)
		}
	}
	for _, donor := range set.DonorsAt("wi", day(time.January, 15)) {
		if donor.RepoID == steady && (donor.Share == nil || *donor.Share != 0.6) {
			t.Errorf("a donor before the first record keeps the share of the first record's row: %v", donor.Share)
		}
	}
}

func TestDonorsAtWithoutFirstRecordsReadsTheInstantAlone(t *testing.T) {
	set, _, _ := firstOwnersSet()
	set.FirstByRepo = nil
	if got := set.DonorsAt("wi", time.Date(2026, time.January, 15, 0, 0, 0, 0, time.UTC)); len(got) != 0 {
		t.Errorf("a repository with no first record is tested at the instant, which no row holds: %v", got)
	}
}
