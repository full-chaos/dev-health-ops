package atlassianteams

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// touchCountingConn counts every read or write Write starts; any other call
// panics through the nil embedded Conn.
type touchCountingConn struct {
	driver.Conn
	touches *int
}

func (c touchCountingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	*c.touches++
	return nil, errors.New("no read expected")
}

func (c touchCountingConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	*c.touches++
	return nil, errors.New("no write expected")
}

func (c touchCountingConn) Exec(context.Context, string, ...any) error {
	*c.touches++
	return errors.New("no write expected")
}

func TestAtlassianWriteRefusesABareTeamID(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	good := "jira:aaaaaaaa-0000-4000-8000-000000000001"
	bare := "aaaaaaaa-0000-4000-8000-000000000001"
	base := func() Rows {
		return Rows{
			Teams:       []TeamRow{{ID: good, Name: "Platform", IsActive: 1, UpdatedAt: now, OrgID: "org-1", Provider: Provider}},
			Memberships: []MembershipRow{{OrgID: "org-1", Provider: Provider, TeamID: good, MemberID: "jira:alice"}},
			Ownership:   []OwnershipRow{{OrgID: "org-1", Provider: Provider, TeamID: good, ProjectID: jiraProjectIDForTest(t, "10001")}},
		}
	}
	cases := map[string]func(*Rows){
		"team row":       func(r *Rows) { r.Teams[0].ID = bare },
		"membership row": func(r *Rows) { r.Memberships[0].TeamID = bare },
		"ownership row":  func(r *Rows) { r.Ownership[0].TeamID = bare },
		"other provider": func(r *Rows) { r.Ownership[0].TeamID = "linear:ENG" },
	}
	for name, plant := range cases {
		t.Run(name, func(t *testing.T) {
			rows := base()
			plant(&rows)
			touches := 0
			_, err := Write(context.Background(), touchCountingConn{touches: &touches}, "org-1", rows, Selections{Structure: true, Members: true, Projects: true})
			if !errors.Is(err, teamid.ErrBareTeamID) || !errors.Is(err, ErrConfiguration) {
				t.Fatalf("Write = %v, want a refusal of the bare team id", err)
			}
			if touches != 0 {
				t.Fatalf("Write touched ClickHouse %d times before it refused the row", touches)
			}
		})
	}
	touches := 0
	if _, err := Write(context.Background(), touchCountingConn{touches: &touches}, "org-1", base(), Selections{Structure: true}); errors.Is(err, teamid.ErrBareTeamID) || touches == 0 {
		t.Fatalf("Write of prefixed rows = %v after %d touches, want it to pass the refusal and read", err, touches)
	}
}

func jiraProjectIDForTest(t *testing.T, nativeID string) providersync.ProjectID {
	t.Helper()
	id, ok := providersync.JiraProjectID(nativeID)
	if !ok {
		t.Fatalf("no project id for %q", nativeID)
	}
	return id
}
