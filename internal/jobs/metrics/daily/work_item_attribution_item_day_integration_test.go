//go:build integration

package daily

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

// The two writers of work_item_team_attributions give one item the same row:
// the team of the ownership valid on the ITEM's day.
//
// The backstop runs when an ownership row changes and rederives every item of
// the changed repository, of any age. It read the ownership at the run's time,
// so each change put today's owner on the whole history of the repository,
// and an item older than the first known ownership of its repository got a
// team. The daily family reads the ownership at the start of the partition's
// day. Each case seeds the same item and the same ownership rows in two
// organizations, runs the backstop on one and the daily family (the partition
// of the item's day) on the other, and holds both against the owner on the
// item's day and against each other.
func TestTheBackstopAndTheDailyFamilyGiveAnItemTheOwnerOfItsOwnDay(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	daysAgo := func(n int) time.Time { return today.AddDate(0, 0, -n).Add(10 * time.Hour) }
	anHourAgo := now.Add(-time.Hour)

	type ownership struct {
		team      string
		validFrom time.Time
		validTo   *time.Time
	}
	cases := []struct {
		name      string
		status    string
		completed *time.Time
		ownership []ownership
		// itemDay is the partition the daily family resolves the item in.
		itemDay  time.Time
		wantTeam string
	}{
		{
			name:   "completed long before the ownership of its repository was first known",
			status: "done", completed: itemDayTime(daysAgo(60)),
			ownership: []ownership{{team: "team-new", validFrom: anHourAgo}},
			itemDay:   today.AddDate(0, 0, -60), wantTeam: "",
		},
		{
			name:   "completed under the owner before a change of owner",
			status: "done", completed: itemDayTime(daysAgo(60)),
			ownership: []ownership{
				{team: "team-old", validFrom: daysAgo(90), validTo: &anHourAgo},
				{team: "team-new", validFrom: anHourAgo},
			},
			itemDay: today.AddDate(0, 0, -60), wantTeam: "team-old",
		},
		{
			// The change lies between the item's day and today: the facts of
			// today and the facts of the item's day name different teams.
			name:   "completed long before a change of owner of some days ago",
			status: "done", completed: itemDayTime(daysAgo(60)),
			ownership: []ownership{
				{team: "team-old", validFrom: daysAgo(90), validTo: itemDayTime(daysAgo(5))},
				{team: "team-new", validFrom: daysAgo(5)},
			},
			itemDay: today.AddDate(0, 0, -60), wantTeam: "team-old",
		},
		{
			name:   "completed long before the ownership was first known some days ago",
			status: "done", completed: itemDayTime(daysAgo(60)),
			ownership: []ownership{{team: "team-new", validFrom: daysAgo(5)}},
			itemDay:   today.AddDate(0, 0, -60), wantTeam: "",
		},
		{
			// The facts are those at the START of the completion day: a change
			// later on that day, before or after the completion, is not in them.
			name:   "the owner changed on the completion day, before the completion",
			status: "done", completed: itemDayTime(daysAgo(60)),
			ownership: []ownership{
				{team: "team-old", validFrom: daysAgo(90), validTo: itemDayTime(daysAgo(60).Add(-5 * time.Hour))},
				{team: "team-new", validFrom: daysAgo(60).Add(-5 * time.Hour)},
			},
			itemDay: today.AddDate(0, 0, -60), wantTeam: "team-old",
		},
		{
			name:   "the owner changed on the completion day, after the completion",
			status: "done", completed: itemDayTime(daysAgo(60)),
			ownership: []ownership{
				{team: "team-old", validFrom: daysAgo(90), validTo: itemDayTime(daysAgo(60).Add(8 * time.Hour))},
				{team: "team-new", validFrom: daysAgo(60).Add(8 * time.Hour)},
			},
			itemDay: today.AddDate(0, 0, -60), wantTeam: "team-old",
		},
		{
			// A completion time on an item that is not done (it was opened
			// again) is not its day: the item is in each new day's partition.
			name:   "opened again after a completion, the owner changed some days ago",
			status: "in_progress", completed: itemDayTime(daysAgo(60)),
			ownership: []ownership{
				{team: "team-old", validFrom: daysAgo(90), validTo: itemDayTime(daysAgo(5))},
				{team: "team-new", validFrom: daysAgo(5)},
			},
			itemDay: today, wantTeam: "team-new",
		},
		{
			// A completion time after today (a provider's clock, a planned
			// date) is not a day that exists yet: the item is in today's
			// partition, with today's facts, not the facts of that later day.
			name:   "a completion time in the future, the owner changes tomorrow",
			status: "done", completed: itemDayTime(today.AddDate(0, 0, 2).Add(10 * time.Hour)),
			ownership: []ownership{
				{team: "team-old", validFrom: daysAgo(90), validTo: itemDayTime(today.AddDate(0, 0, 1))},
				{team: "team-new", validFrom: today.AddDate(0, 0, 1)},
			},
			itemDay: today, wantTeam: "team-old",
		},
		{
			name:   "completed after a change of owner",
			status: "done", completed: itemDayTime(daysAgo(2)),
			ownership: []ownership{
				{team: "team-old", validFrom: daysAgo(90), validTo: itemDayTime(daysAgo(5))},
				{team: "team-new", validFrom: daysAgo(5)},
			},
			itemDay: today.AddDate(0, 0, -2), wantTeam: "team-new",
		},
		{
			name:   "not completed, the owner changed some days ago",
			status: "in_progress",
			ownership: []ownership{
				{team: "team-old", validFrom: daysAgo(90), validTo: itemDayTime(daysAgo(5))},
				{team: "team-new", validFrom: daysAgo(5)},
			},
			itemDay: today, wantTeam: "team-new",
		},
		{
			// The day of an item that is not completed is today, and its facts
			// are the facts at the start of today, as in the daily family: an
			// ownership that began within today reaches it with the next day.
			name:   "not completed, the owner changed within today",
			status: "in_progress",
			ownership: []ownership{
				{team: "team-old", validFrom: daysAgo(90), validTo: &now},
				{team: "team-new", validFrom: now},
			},
			itemDay: today, wantTeam: "team-old",
		},
	}

	for index, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			seed := func(writerName string) (string, uuid.UUID) {
				orgID := fmt.Sprintf("org-item-day-%d-%s-%s", index, writerName, uuid.NewString())
				repoID := uuid.New()
				seedItemDayWorkItem(t, ctx, conn, orgID, repoID, testCase.status, daysAgo(120), testCase.completed)
				for _, row := range testCase.ownership {
					seedItemDayRepoOwnership(t, ctx, conn, orgID, repoID, row.team, row.validFrom, row.validTo, now)
				}
				return orgID, repoID
			}

			backstopOrg, _ := seed("backstop")
			writer, err := remaining.NewWorkItemAttributionClickHouseWriter(conn)
			if err != nil {
				t.Fatalf("new writer: %v", err)
			}
			backstop, err := remaining.NewWorkItemAttributionExecutor(ctx, conn, writer)
			if err != nil {
				t.Fatalf("new backstop: %v", err)
			}
			outcome, err := backstop.ComputeOrg(ctx, backstopOrg)
			if err != nil {
				t.Fatalf("backstop: %v", err)
			}
			if outcome.SkippedNoop || outcome.ItemsSeen != 1 {
				t.Fatalf("the backstop did not rederive the item: %+v", outcome)
			}

			dailyOrg, dailyRepo := seed("daily")
			family, err := NewWorkItemAttributionExecutor(conn)
			if err != nil {
				t.Fatalf("new daily family: %v", err)
			}
			run := Run{ID: uuid.NewString(), OrganizationID: dailyOrg, TargetDay: testCase.itemDay}
			partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(dailyRepo.String())}}
			if _, err := family.ComputeFamily(ctx, run, partition); err != nil {
				t.Fatalf("daily family: %v", err)
			}

			fromBackstop := queryItemDayRows(t, ctx, conn, backstopOrg, "backstop")
			fromDaily := queryItemDayRows(t, ctx, conn, dailyOrg, "daily")
			if len(fromDaily) == 0 {
				t.Fatalf("the daily family wrote no row for the item in the partition of %s: the case does not hold the item's day", testCase.itemDay.Format(time.DateOnly))
			}
			if primary := primaryTeamOf(fromDaily); primary != testCase.wantTeam {
				t.Fatalf("the daily family gives team %q, want %q (rows %+v)", primary, testCase.wantTeam, fromDaily)
			}
			if primary := primaryTeamOf(fromBackstop); primary != testCase.wantTeam {
				t.Fatalf("the backstop gives team %q, want %q, the owner on the item's day (rows %+v)", primary, testCase.wantTeam, fromBackstop)
			}
			if fmt.Sprint(fromBackstop) != fmt.Sprint(fromDaily) {
				t.Fatalf("the two writers give the item different rows:\n  backstop %+v\n  daily    %+v", fromBackstop, fromDaily)
			}
		})
	}
}

// One run of the backstop holds items of different days. Each item gets the row
// of its own day and no row of another item's day.
func TestOneBackstopRunGivesEachItemTheOwnerOfItsOwnDay(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	changed := today.AddDate(0, 0, -5)
	completedLongAgo := today.AddDate(0, 0, -60).Add(10 * time.Hour)
	completedLately := today.AddDate(0, 0, -2).Add(10 * time.Hour)

	orgID := "org-item-day-two-items-" + uuid.NewString()
	repoID := uuid.New()
	for id, item := range map[string]itemDayItem{
		"item-long-ago":      {provider: "github", repoID: repoID, status: "done", createdAt: today.AddDate(0, 0, -120), completedAt: &completedLongAgo},
		"item-lately":        {provider: "github", repoID: repoID, status: "done", createdAt: today.AddDate(0, 0, -120), completedAt: &completedLately},
		"item-not-completed": {provider: "github", repoID: repoID, status: "in_progress", createdAt: today.AddDate(0, 0, -120)},
	} {
		seedItemDayItemWithID(t, ctx, conn, orgID, id, item)
	}
	seedItemDayOwnershipOf(t, ctx, conn, "repository", orgID, "github", repoID.String(), "team-old", today.AddDate(0, 0, -90), &changed, now)
	seedItemDayOwnershipOf(t, ctx, conn, "repository", orgID, "github", repoID.String(), "team-new", changed, nil, now)

	writer, err := remaining.NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	backstop, err := remaining.NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new backstop: %v", err)
	}
	outcome, err := backstop.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("backstop: %v", err)
	}
	if outcome.ItemsSeen != 3 {
		t.Fatalf("the backstop saw %d item(s), want 3: %+v", outcome.ItemsSeen, outcome)
	}

	rows, err := conn.Query(ctx, `
SELECT work_item_id, ifNull(team_id, ''), is_primary
FROM work_item_team_attributions FINAL
WHERE org_id = ? ORDER BY work_item_id, team_id`, orgID)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := map[string][]string{}
	for rows.Next() {
		var id, team string
		var primary uint8
		if err := rows.Scan(&id, &team, &primary); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = append(got[id], fmt.Sprintf("%s primary=%d", team, primary))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read rows: %v", err)
	}
	want := map[string][]string{
		"item-long-ago":      {"team-old primary=1"},
		"item-lately":        {"team-new primary=1"},
		"item-not-completed": {"team-new primary=1"},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("rows after one run:\n  got  %v\n  want %v", got, want)
	}
}

// A completed item with no completion time is in no partition of the daily
// family. The backstop still resolves it, on the day of the run.
func TestTheBackstopResolvesACompletedItemWithNoCompletionTimeOnTheDayOfTheRun(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	now := time.Now().UTC()
	orgID := "org-item-day-no-completion-" + uuid.NewString()
	repoID := uuid.New()
	seedItemDayWorkItem(t, ctx, conn, orgID, repoID, "done", now.AddDate(0, 0, -120), nil)
	seedItemDayRepoOwnership(t, ctx, conn, orgID, repoID, "team-old", now.AddDate(0, 0, -90), itemDayTime(now.AddDate(0, 0, -5)), now)
	seedItemDayRepoOwnership(t, ctx, conn, orgID, repoID, "team-new", now.AddDate(0, 0, -5), nil, now)

	writer, err := remaining.NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	backstop, err := remaining.NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new backstop: %v", err)
	}
	if _, err := backstop.ComputeOrg(ctx, orgID); err != nil {
		t.Fatalf("backstop: %v", err)
	}
	rows := queryItemDayRows(t, ctx, conn, orgID, "backstop")
	if primary := primaryTeamOf(rows); primary != "team-new" {
		t.Fatalf("team %q, want team-new, the owner on the day of the run (rows %+v)", primary, rows)
	}
}

func itemDayTime(value time.Time) *time.Time { return &value }

type itemDayRow struct {
	Team    string
	Source  string
	Primary uint8
}

func primaryTeamOf(rows []itemDayRow) string {
	for _, row := range rows {
		if row.Primary == 1 {
			return row.Team
		}
	}
	return ""
}

func seedItemDayWorkItem(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID string, repoID uuid.UUID, status string, createdAt time.Time, completedAt *time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx,
		`INSERT INTO work_items (repo_id, work_item_id, provider, project_id, org_id, status, created_at, completed_at, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(repoID, "the-item", "github", "", orgID, status, createdAt, completedAt, time.Now().UTC()); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

func seedItemDayRepoOwnership(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID string, repoID uuid.UUID, teamID string, validFrom time.Time, validTo *time.Time, updatedAt time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_repo_ownership (
		org_id, provider, team_id, repo_id, repo_full_name, match_type, source,
		is_primary, specificity, priority, valid_from, valid_to, updated_at
	)`)
	if err != nil {
		t.Fatalf("prepare team_repo_ownership batch: %v", err)
	}
	if err := batch.Append(
		orgID, "github", teamID, repoID, "acme/"+teamID, "exact", "native",
		uint8(1), uint16(100), int32(0), validFrom, validTo, updatedAt,
	); err != nil {
		t.Fatalf("append team_repo_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_repo_ownership batch: %v", err)
	}
}

// queryItemDayRows reads the rows of the item that the named writer left, and
// fails when another writer wrote one: each organization is run by one writer.
func queryItemDayRows(t *testing.T, ctx context.Context, conn driver.Conn, orgID, writerName string) []itemDayRow {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT ifNull(team_id, ''), source, is_primary, writer
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND work_item_id = 'the-item'
ORDER BY source, team_id`, orgID)
	if err != nil {
		t.Fatalf("query work_item_team_attributions: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var result []itemDayRow
	for rows.Next() {
		var row itemDayRow
		var written string
		if err := rows.Scan(&row.Team, &row.Source, &row.Primary, &written); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if written != writerName {
			t.Fatalf("a row of writer %q in the organization of writer %q", written, writerName)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read rows: %v", err)
	}
	return result
}

// The rule holds for each provider and for each kind of fact that gives an item
// a team: the ownership of its repository, the ownership of its project, and
// the membership of its assignee. One case for each cell: an item completed 60
// days ago, under the team that held the fact until one hour ago; the fact now
// names another team. Both writers give the team of the item's day.
//
// A Jira or Linear item has no repository, so the repository cell exists for
// the two code hosts only.
func TestTheOwnerOfAnItemsDayHoldsForEachProviderAndKindOfFact(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	completed := today.AddDate(0, 0, -60).Add(10 * time.Hour)
	itemDay := today.AddDate(0, 0, -60)
	began := today.AddDate(0, 0, -90)
	anHourAgo := now.Add(-time.Hour)

	kinds := map[string][]string{
		"repository ownership": {"github", "gitlab"},
		"project ownership":    {"github", "gitlab", "jira", "linear"},
		"membership":           {"github", "gitlab", "jira", "linear"},
	}
	cells := 0
	for kind, providers := range kinds {
		for _, provider := range providers {
			cells++
			t.Run(kind+"/"+provider, func(t *testing.T) {
				seed := func(writerName string) (string, uuid.UUID) {
					orgID := fmt.Sprintf("org-item-day-matrix-%s-%s-%s", provider, writerName, uuid.NewString())
					repoID := uuid.New()
					item := itemDayItem{provider: provider, repoID: repoID, status: "done", createdAt: today.AddDate(0, 0, -120), completedAt: &completed}
					switch kind {
					case "repository ownership":
						seedItemDayOwnershipOf(t, ctx, conn, "repository", orgID, provider, repoID.String(), "team-old", began, &anHourAgo, now)
						seedItemDayOwnershipOf(t, ctx, conn, "repository", orgID, provider, repoID.String(), "team-new", anHourAgo, nil, now)
					case "project ownership":
						item.projectID, item.projectKey = "project-1", "PRJ"
						seedItemDayOwnershipOf(t, ctx, conn, "project", orgID, provider, "project-1", "team-old", began, &anHourAgo, now)
						seedItemDayOwnershipOf(t, ctx, conn, "project", orgID, provider, "project-1", "team-new", anHourAgo, nil, now)
					case "membership":
						item.assignees = []string{"user-alice"}
						seedItemDayOwnershipOf(t, ctx, conn, "member", orgID, provider, "user-alice", "team-old", began, &anHourAgo, now)
						seedItemDayOwnershipOf(t, ctx, conn, "member", orgID, provider, "user-alice", "team-new", anHourAgo, nil, now)
					}
					seedItemDayItem(t, ctx, conn, orgID, item)
					return orgID, repoID
				}

				backstopOrg, _ := seed("backstop")
				writer, err := remaining.NewWorkItemAttributionClickHouseWriter(conn)
				if err != nil {
					t.Fatalf("new writer: %v", err)
				}
				backstop, err := remaining.NewWorkItemAttributionExecutor(ctx, conn, writer)
				if err != nil {
					t.Fatalf("new backstop: %v", err)
				}
				outcome, err := backstop.ComputeOrg(ctx, backstopOrg)
				if err != nil {
					t.Fatalf("backstop: %v", err)
				}
				if outcome.SkippedNoop || outcome.ItemsSeen != 1 {
					t.Fatalf("the backstop did not rederive the item: %+v", outcome)
				}

				dailyOrg, dailyRepo := seed("daily")
				family, err := NewWorkItemAttributionExecutor(conn)
				if err != nil {
					t.Fatalf("new daily family: %v", err)
				}
				run := Run{ID: uuid.NewString(), OrganizationID: dailyOrg, TargetDay: itemDay}
				partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(dailyRepo.String())}}
				if _, err := family.ComputeFamily(ctx, run, partition); err != nil {
					t.Fatalf("daily family: %v", err)
				}

				fromBackstop := queryItemDayRows(t, ctx, conn, backstopOrg, "backstop")
				fromDaily := queryItemDayRows(t, ctx, conn, dailyOrg, "daily")
				// CHAOS-9414: membership is provenance only, so an item whose only
				// team signal is the assignee's membership has NO primary team;
				// the member's team is a non-primary provenance row.
				wantTeam := "team-old"
				if kind == "membership" {
					wantTeam = ""
				}
				if primary := primaryTeamOf(fromDaily); primary != wantTeam {
					t.Fatalf("the daily family gives team %q, want %q: the seed does not give the item a team through %s (rows %+v)", primary, wantTeam, kind, fromDaily)
				}
				if primary := primaryTeamOf(fromBackstop); primary != wantTeam {
					t.Fatalf("the backstop gives team %q, want %q, the team of the item's day (rows %+v)", primary, wantTeam, fromBackstop)
				}
				if kind == "membership" && !strings.Contains(fmt.Sprint(fromDaily), "Team:team-old Source:assignee_membership Primary:0") {
					t.Fatalf("the daily family lost the membership provenance row (rows %+v)", fromDaily)
				}
				if fmt.Sprint(fromBackstop) != fmt.Sprint(fromDaily) {
					t.Fatalf("the two writers give the item different rows:\n  backstop %+v\n  daily    %+v", fromBackstop, fromDaily)
				}
			})
		}
	}
	if cells != 10 {
		t.Fatalf("%d cells ran, want 10", cells)
	}
}

type itemDayItem struct {
	provider              string
	repoID                uuid.UUID
	status                string
	createdAt             time.Time
	completedAt           *time.Time
	projectID, projectKey string
	assignees             []string
}

func seedItemDayItem(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, item itemDayItem) {
	t.Helper()
	seedItemDayItemWithID(t, ctx, conn, orgID, "the-item", item)
}

func seedItemDayItemWithID(t *testing.T, ctx context.Context, conn driver.Conn, orgID, workItemID string, item itemDayItem) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx,
		`INSERT INTO work_items (repo_id, work_item_id, provider, project_id, project_key, assignees, org_id, status, created_at, completed_at, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	assignees := item.assignees
	if assignees == nil {
		assignees = []string{}
	}
	if err := batch.Append(item.repoID, workItemID, item.provider, item.projectID, item.projectKey, assignees, orgID,
		item.status, item.createdAt, item.completedAt, time.Now().UTC()); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

// seedItemDayOwnershipOf writes one fact row of the named kind with a validity
// window: the ownership of a repository or of a project, or a membership.
func seedItemDayOwnershipOf(
	t *testing.T, ctx context.Context, conn driver.Conn,
	kind, orgID, provider, subject, teamID string, validFrom time.Time, validTo *time.Time, updatedAt time.Time,
) {
	t.Helper()
	var statement string
	var row []any
	switch kind {
	case "repository":
		statement = `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source,
			is_primary, specificity, priority, valid_from, valid_to, updated_at)`
		row = []any{orgID, provider, teamID, uuid.MustParse(subject), "acme/" + teamID, "exact", "native",
			uint8(1), uint16(100), int32(0), validFrom, validTo, updatedAt}
	case "project":
		key := "PRJ"
		statement = `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source,
			is_primary, specificity, priority, valid_from, valid_to, updated_at)`
		row = []any{orgID, provider, teamID, subject, &key, "native", uint8(1), uint16(100), int32(0), validFrom, validTo, updatedAt}
	case "member":
		statement = `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, source,
			is_primary, specificity, priority, valid_from, valid_to, updated_at)`
		row = []any{orgID, provider, teamID, subject, subject, nil, "native", uint8(1), uint16(100), int32(0), validFrom, validTo, updatedAt}
	default:
		t.Fatalf("unknown kind %q", kind)
	}
	batch, err := conn.PrepareBatch(ctx, statement)
	if err != nil {
		t.Fatalf("prepare %s batch: %v", kind, err)
	}
	if err := batch.Append(row...); err != nil {
		t.Fatalf("append %s row: %v", kind, err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send %s batch: %v", kind, err)
	}
}
