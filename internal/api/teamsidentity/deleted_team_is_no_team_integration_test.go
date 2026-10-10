//go:build integration

package teamsidentity

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestATeamAnAdminDeletedIsNoTeam is the acceptance case of the admin delete,
// for a team of each provider and for an admin's custom team, on the schema of
// the migration chain: a deleted team takes nothing at the next compute.
//
// The team row is written by the admin store (the writer of an admin team and
// of an imported provider team). The admin delete keeps the row and writes it
// INACTIVE with the time of the delete. The rows that NAME the team stay: its
// repository ownership rows, and the rows a daily family stored under it. The
// inactive team row is what every resolver already reads as "takes nothing".
//
// While the team is there it owns its repositories and holds the person's
// row and landscape points. After the delete:
//
//   - the repository it shared goes to the other owner, which ranked lower;
//   - the repository only it owned has no owner;
//   - the person's row of the day is written again as unassigned (id and
//     name), the person's points are under unassigned, and each point that
//     was stored under the deleted id holds a retraction row and no measure.
//
// The file uses no symbol of the delete, so the same file runs on a tree
// where the delete removes the row (there the team keeps everything).
func TestATeamAnAdminDeletedIsNoTeam(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	store := Store{Conn: conn}

	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	owned := day.AddDate(0, 0, -30)
	live := teamkeytables.ICLandscapeRolling30d.LiveRow("")
	for index, origin := range []string{"github", "gitlab", "jira", "linear", teamid.Custom} {
		t.Run(origin, func(t *testing.T) {
			org := fmt.Sprintf("00000000-0000-4000-8000-00000091190%d", index)
			gone, stays := teamid.Of(origin, "platform"), teamid.Of(origin, "apps")
			for id, name := range map[string]string{gone: "Platform", stays: "Apps"} {
				if _, err := store.CreateOrUpdateTeam(ctx, org, TeamWrite{Origin: origin, TeamID: id, Name: name}); err != nil {
					t.Fatalf("create team %s: %v", id, err)
				}
			}
			shared := uuid.NewSHA1(uuid.NameSpaceURL, []byte("shared:"+origin))
			onlyGone := uuid.NewSHA1(uuid.NameSpaceURL, []byte("only:"+origin))
			for _, claim := range []struct {
				team    string
				repo    uuid.UUID
				primary uint8
			}{{gone, shared, 1}, {stays, shared, 0}, {gone, onlyGone, 1}} {
				if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, updated_at)
    VALUES (?, ?, ?, ?, ?, 'exact', 'native', ?, 10, ?, ?)`,
					org, teamid.StoredProvider(origin), claim.team, claim.repo, "acme/"+claim.repo.String(), claim.primary, owned, owned); err != nil {
					t.Fatal(err)
				}
			}
			// The person is a member of the team (the table the finalize reads a
			// person's teams from), and a daily family stored a row for the
			// person under the team.
			const person = "dev@example.com"
			if err := conn.Exec(ctx, `INSERT INTO team_memberships
    (org_id, provider, team_id, member_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at)
    VALUES (?, ?, ?, 'dev', ?, [?], 'native', 1, 100, 10, ?, ?)`,
				org, teamid.StoredProvider(origin), gone, person, person, owned, owned); err != nil {
				t.Fatal(err)
			}
			if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, loc_deleted,
     prs_authored, prs_merged, loc_touched, delivery_units, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, 'Platform', 3, 40, 10, 1, 1, 50, 1, ?, ?)`,
				shared, day, person, person, gone, day.Add(30*time.Hour), org); err != nil {
				t.Fatal(err)
			}

			type state struct {
				Owners map[string]string
				Row    string
				Points []string
			}
			read := func() state {
				t.Helper()
				owners, err := teamownership.AuthoritativeOwnerByRepo(ctx, conn, org, day.Add(36*time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				named := map[string]string{}
				for repo, team := range owners {
					switch repo {
					case shared.String():
						named["shared"] = team
					case onlyGone.String():
						named["only"] = team
					default:
						named[repo] = team
					}
				}
				if _, err := daily.NewICFinalizeExecutor(conn).ComputeFinalizeFamily(ctx, daily.Run{
					ID: uuid.NewString(), OrganizationID: org, TargetDay: day,
				}); err != nil {
					t.Fatalf("ic_finalize: %v", err)
				}
				answer := state{Owners: named}
				if err := conn.QueryRow(ctx, `SELECT concat(toString(team_id), ' / ', toString(team_name)) FROM user_metrics_daily
WHERE org_id = ? AND day = ? AND author_email = ? ORDER BY computed_at DESC LIMIT 1`, org, day, person).Scan(&answer.Row); err != nil {
					t.Fatal(err)
				}
				if err := conn.QueryRow(ctx, `SELECT arraySort(groupUniqArray(toString(team_id))) FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND as_of_day = ? AND identity_id = ? AND `+live, org, day, person).Scan(&answer.Points); err != nil {
					t.Fatal(err)
				}
				sort.Strings(answer.Points)
				return answer
			}

			before := read()
			if want := (state{Owners: map[string]string{"shared": gone, "only": gone}, Row: gone + " / Platform", Points: []string{gone}}); !reflect.DeepEqual(before, want) {
				t.Fatalf("while the team is there: %+v, want %+v", before, want)
			}

			deleted, err := store.DeleteTeam(ctx, org, gone)
			if err != nil || !deleted {
				t.Fatalf("delete team %s: deleted = %v, err = %v", gone, deleted, err)
			}
			var rowsLeft, activeRows, markedRows uint64
			if err := conn.QueryRow(ctx, `SELECT count(), countIf(is_active = 1), countIf(deleted_at IS NOT NULL) FROM teams FINAL WHERE org_id = ? AND id = ?`,
				org, gone).Scan(&rowsLeft, &activeRows, &markedRows); err != nil {
				t.Fatal(err)
			}
			if rowsLeft != 1 || activeRows != 0 || markedRows != 1 {
				t.Fatalf("after the delete the team has %d rows, %d active, %d with the time of the delete; want one inactive row that carries it",
					rowsLeft, activeRows, markedRows)
			}

			after := read()
			if want := (state{Owners: map[string]string{"shared": stays}, Row: "unassigned / Unassigned", Points: []string{"unassigned"}}); !reflect.DeepEqual(after, want) {
				t.Errorf("after the delete: %+v, want %+v", after, want)
			}
			var liveUnderGone, retractedUnderGone uint64
			if err := conn.QueryRow(ctx, `SELECT countIf(`+live+`), countIf(NOT `+live+`) FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND as_of_day = ? AND identity_id = ? AND team_id = ?`, org, day, person, gone).Scan(&liveUnderGone, &retractedUnderGone); err != nil {
				t.Fatal(err)
			}
			if liveUnderGone != 0 || retractedUnderGone != 3 {
				t.Errorf("the points stored under the deleted team: %d live, %d retracted; want 0 and the 3 maps retracted", liveUnderGone, retractedUnderGone)
			}
		})
	}
}
