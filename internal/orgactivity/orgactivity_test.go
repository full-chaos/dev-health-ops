package orgactivity

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func at(day int) *time.Time {
	value := time.Date(2026, 10, day, 12, 0, 0, 0, time.UTC)
	return &value
}

func member(n byte, joinedDay int) Membership {
	joined := time.Date(2026, 1, joinedDay, 0, 0, 0, 0, time.UTC)
	return Membership{OrgID: uuid.UUID{15: n}, JoinedAt: &joined, CreatedAt: joined}
}

func TestSelectRanksByDataThenNewestMetricsThenEarliestJoin(t *testing.T) {
	early, late, third := member(1, 1), member(2, 2), member(3, 3)
	memberships := []Membership{early, late, third}
	known := func(a ...Activity) map[uuid.UUID]Activity {
		out := map[uuid.UUID]Activity{}
		for index, m := range memberships {
			out[m.OrgID] = a[index]
		}
		return out
	}
	for name, test := range map[string]struct {
		activities map[uuid.UUID]Activity
		want       int
	}{
		"the earliest org has no data, a later one has": {known(Activity{}, Activity{HasData: true, Last: at(5)}, Activity{}), 1},
		"both have data: the newest metrics win":        {known(Activity{HasData: true, Last: at(3)}, Activity{HasData: true, Last: at(9)}, Activity{HasData: true, Last: at(4)}), 1},
		"equal metrics: the earliest join wins":         {known(Activity{HasData: true, Last: at(5)}, Activity{HasData: true, Last: at(5)}, Activity{HasData: true, Last: at(5)}), 0},
		"nobody has data: the earliest join wins":       {known(Activity{}, Activity{}, Activity{}), 0},
		"one unknown: activity ranks nobody":            {known(Activity{}, Activity{HasData: true, Last: at(9)}, Activity{Unknown: true}), 0},
		"an absent entry is unknown":                    {map[uuid.UUID]Activity{early.OrgID: {}, late.OrgID: {HasData: true, Last: at(9)}}, 0},
	} {
		t.Run(name, func(t *testing.T) {
			if got := Select(memberships, nil, test.activities); got != test.want {
				t.Fatalf("Select = %d, want %d", got, test.want)
			}
		})
	}
}

func TestSelectHonoursTheRequestedOrganization(t *testing.T) {
	a, b := member(1, 1), member(2, 2)
	activities := map[uuid.UUID]Activity{a.OrgID: {}, b.OrgID: {HasData: true, Last: at(9)}}
	if got := Select([]Membership{a, b}, &a.OrgID, activities); got != 0 {
		t.Fatalf("requested org a: got %d, want 0", got)
	}
	other := uuid.UUID{15: 9}
	if got := Select([]Membership{a, b}, &other, activities); got != -1 {
		t.Fatalf("a requested org the user does not hold: got %d, want -1", got)
	}
	if got := Select(nil, nil, nil); got != -1 {
		t.Fatalf("no memberships: got %d, want -1", got)
	}
}

func TestSelectFallsBackToCreatedAtWithoutJoinedAt(t *testing.T) {
	older := Membership{OrgID: uuid.UUID{15: 1}, CreatedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	newer := Membership{OrgID: uuid.UUID{15: 2}, CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	activities := map[uuid.UUID]Activity{older.OrgID: {}, newer.OrgID: {}}
	if got := Select([]Membership{newer, older}, nil, activities); got != 1 {
		t.Fatalf("got %d, want the older created_at (index 1)", got)
	}
}
