package teamprovider_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
)

// The column lists of the sync and push writers of teams: the provider
// catalogs (github, gitlab, jira, linear, atlassian) and the team.v1 push.
var (
	catalogTeamsColumns = []string{"id", "team_uuid", "name", "description", "manual_members", "project_keys", "repo_patterns", "is_active", "updated_at", "org_id", "provider", "native_team_key", "parent_team_id", "created_at"}
	pushTeamsColumns    = []string{"id", "team_uuid", "name", "description", "manual_members", "project_keys", "repo_patterns", "is_active", "updated_at", "last_synced", "org_id", "provider", "native_team_key", "parent_team_id", "source_id", "created_at"}
)

// CHAOS-14094 (acp C4): the admin writes of a team read more of the stored
// row inside their insert; the statement a sync or push writer sends must not
// change with them. Each case pins the sha256 of the statement text and of its
// parameter names, for every roster rule a sync writer uses.
func TestASyncWritersTeamsStatementIsPinned(t *testing.T) {
	cases := []struct {
		name    string
		columns []string
		rows    []teamprovider.Write
		want    string
	}{
		{"catalog_kept", catalogTeamsColumns, []teamprovider.Write{pinRow(catalogTeamsColumns, "gh:a", teamprovider.RosterKept)},
			"88a66837bd1ece00e78477011e2aa3be7e1d181e875a6334d4bd34d1aaa82f32"},
		{"catalog_stated", catalogTeamsColumns, []teamprovider.Write{pinRow(catalogTeamsColumns, "gh:a", teamprovider.RosterStated)},
			"59d8b9494d73bcf38ae6516be192ed23fc24646f213f5ff4269234c638ecd772"},
		{"push_kept", pushTeamsColumns, []teamprovider.Write{pinRow(pushTeamsColumns, "gh:a", teamprovider.RosterKept)},
			"3a49e8bd893eb7988b4126bbe14f538082f355a4274ef3e84a1db932e1332da7"},
		{"carry_kept_and_source", pushTeamsColumns, []teamprovider.Write{
			pinRow(pushTeamsColumns, "gh:a", teamprovider.RosterKept),
			func() teamprovider.Write {
				row := pinRow(pushTeamsColumns, "gh:b", teamprovider.RosterSource)
				row.RosterSource = "gh:old"
				return row
			}(),
		}, "6e069fbbb5fd66514c073c5e09f769534e0b981d04e85487c0812b683cc2f0cf"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conn := &recordingExec{}
			if err := teamprovider.Insert(context.Background(), conn, "org-1", c.columns, c.rows); err != nil {
				t.Fatal(err)
			}
			names := make([]string, 0, len(conn.args))
			for _, arg := range conn.args {
				names = append(names, arg.(driver.NamedValue).Name)
			}
			sum := sha256.Sum256([]byte(conn.query + "\n" + strings.Join(names, ",")))
			if got := hex.EncodeToString(sum[:]); got != c.want {
				t.Errorf("PIN %s %s: the sync statement changed (want %s)", c.name, got, c.want)
			}
		})
	}
}

func pinRow(columns []string, id string, roster teamprovider.Roster) teamprovider.Write {
	at := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	values := make([]any, len(columns))
	for c, column := range columns {
		switch column {
		case "id":
			values[c] = id
		case "team_uuid", "source_id":
			values[c] = uuid.Nil
		case "org_id":
			values[c] = "org-1"
		case "manual_members", "project_keys", "repo_patterns":
			values[c] = []string{}
		case "is_active":
			values[c] = uint8(1)
		case "updated_at", "last_synced", "created_at":
			values[c] = at
		default:
			values[c] = "x"
		}
	}
	return teamprovider.Write{Values: values, Gone: teamprovider.GoneKept, Roster: roster}
}
