package session

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/orgactivity"
)

// The auth plane applies orgactivity.Select to the projection; go-api applies
// selectActiveMembership to its live read. On the same facts they choose the
// same membership: go-api is the oracle.
func TestOrgActivitySelectChoosesWhatGoAPIChooses(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	day := func(n int) *time.Time { value := base.AddDate(0, 0, n); return &value }
	orgs := []uuid.UUID{{15: 1}, {15: 2}, {15: 3}}
	rows := []membershipRow{
		{OrgID: orgs[0], JoinedAt: day(0), CreatedAt: *day(0)},
		{OrgID: orgs[1], JoinedAt: day(1), CreatedAt: *day(1)},
		{OrgID: orgs[2], CreatedAt: *day(2)},
	}
	candidates := make([]orgactivity.Membership, len(rows))
	for index, row := range rows {
		candidates[index] = orgactivity.Membership{OrgID: row.OrgID, JoinedAt: row.JoinedAt, CreatedAt: row.CreatedAt}
	}
	requested := orgs[2]
	for name, test := range map[string]struct {
		activities map[uuid.UUID]activity
		requested  *uuid.UUID
	}{
		"earliest has no data":  {map[uuid.UUID]activity{orgs[0]: {}, orgs[1]: {hasData: true, last: day(30)}, orgs[2]: {}}, nil},
		"newest metrics win":    {map[uuid.UUID]activity{orgs[0]: {hasData: true, last: day(5)}, orgs[1]: {hasData: true, last: day(9)}, orgs[2]: {hasData: true, last: day(7)}}, nil},
		"equal metrics":         {map[uuid.UUID]activity{orgs[0]: {hasData: true, last: day(5)}, orgs[1]: {hasData: true, last: day(5)}, orgs[2]: {hasData: true, last: day(5)}}, nil},
		"no data anywhere":      {map[uuid.UUID]activity{orgs[0]: {}, orgs[1]: {}, orgs[2]: {}}, nil},
		"one unknown":           {map[uuid.UUID]activity{orgs[0]: {}, orgs[1]: {hasData: true, last: day(9)}, orgs[2]: {unknown: true}}, nil},
		"requested":             {map[uuid.UUID]activity{orgs[0]: {hasData: true, last: day(9)}, orgs[1]: {}, orgs[2]: {}}, &requested},
		"late org has the data": {map[uuid.UUID]activity{orgs[0]: {}, orgs[1]: {}, orgs[2]: {hasData: true, last: day(1)}}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			want := -1
			if chosen := selectActiveMembership(rows, test.requested, test.activities); chosen != nil {
				for index := range rows {
					if rows[index].OrgID == chosen.OrgID {
						want = index
					}
				}
			}
			projected := map[uuid.UUID]orgactivity.Activity{}
			for id, a := range test.activities {
				projected[id] = orgactivity.Activity{HasData: a.hasData, Last: a.last, Unknown: a.unknown}
			}
			if got := orgactivity.Select(candidates, test.requested, projected); got != want {
				t.Fatalf("orgactivity.Select = %d, go-api selectActiveMembership = %d", got, want)
			}
		})
	}
}
