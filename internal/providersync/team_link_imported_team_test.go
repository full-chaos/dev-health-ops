package providersync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type unreadLinkTeams struct{ reads int }

func (q *unreadLinkTeams) Query(context.Context, string, ...any) (driver.Rows, error) {
	q.reads++
	return nil, errors.New("not read in this test")
}

// A link row of another organization is refused before any read, with an
// error and a Warn: it is never dropped as a link of an un-imported team.
func TestKeepLinksOfImportedTeamsRefusesARowOfAnotherOrganization(t *testing.T) {
	type link struct{ org, team string }
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	conn := &unreadLinkTeams{}
	rows := []link{{"org-1", "gh:a"}, {"org-2", "gh:b"}}
	_, err := KeepLinksOfImportedTeams(context.Background(), conn, "org-1", "github", "team_memberships", rows,
		func(r link) string { return r.org }, func(r link) string { return r.team }, func(link) bool { return true })
	if !errors.Is(err, ErrLinkRowsOfAnotherOrganization) || !strings.Contains(err.Error(), "1 team_memberships rows") {
		t.Fatalf("err = %v, want the row of another organization refused", err)
	}
	if conn.reads != 0 {
		t.Errorf("the store was read %d times before the refusal", conn.reads)
	}
	if !strings.Contains(logged.String(), `"msg":"team_links_of_another_organization_refused"`) || !strings.Contains(logged.String(), `"rows":1`) {
		t.Errorf("no Warn with the count:\n%s", logged.String())
	}
}
