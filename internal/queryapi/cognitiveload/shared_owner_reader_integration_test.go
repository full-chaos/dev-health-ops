//go:build integration

package cognitiveload

import (
	"context"
	"slices"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-13685: the team + repository gate gives a repository the owner the
// shared owner reader (teamownership.AuthoritativeOwnersByRepo) gives it. An
// inactive team owns nothing, a row with an empty team id is skipped, and
// before the first record of a repository its first recorded owners apply
// (package ownershipwindow). Each cell states its answer and checks that the
// shared reader gives the same answer.
func TestTheRepoGateTakesTheOwnerOfTheSharedOwnerReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-shared-owner-reader"
	now := time.Now().UTC()
	past := now.Add(-time.Hour)
	repo := func(name string) uuid.UUID { return uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+name)) }
	inactiveOnly, inactiveFirst, emptyFirst, firstRecordLater, control :=
		repo("inactive-only"), repo("inactive-first"), repo("empty-first"), repo("first-record-later"), repo("control")
	for _, id := range []uuid.UUID{inactiveOnly, inactiveFirst, emptyFirst, firstRecordLater, control} {
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, 'github', ?)`,
			id, "acme/"+id.String(), org, past); err != nil {
			t.Fatalf("insert repo: %v", err)
		}
	}
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
		VALUES ('team-gone', generateUUIDv4(), 'Gone', [], ?, ?, ?, 'github', 0)`, past, past, org); err != nil {
		t.Fatalf("insert inactive team: %v", err)
	}
	for _, row := range []struct {
		team        string
		repo        uuid.UUID
		primary     uint8
		specificity uint16
		from        time.Time
	}{
		{"team-gone", inactiveOnly, 1, 150, past},
		{"team-gone", inactiveFirst, 1, 200, past},
		{"team-live", inactiveFirst, 0, 100, past},
		{"", emptyFirst, 1, 200, past},
		{"team-live", emptyFirst, 0, 100, past},
		{"team-live", firstRecordLater, 1, 150, now.Add(time.Hour)},
		{"team-live", control, 1, 150, past},
	} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, ?, 0, ?, NULL, ?)`,
			org, row.team, row.repo, "acme/"+row.repo.String(), row.primary, row.specificity, row.from, past); err != nil {
			t.Fatalf("insert ownership: %v", err)
		}
	}

	shared, err := teamownership.AuthoritativeOwnersByRepo(ctx, conn, org, now)
	if err != nil {
		t.Fatalf("shared owner reader: %v", err)
	}
	for _, tc := range []struct {
		name string
		team string
		repo uuid.UUID
		want bool
	}{
		{"InactiveTeamWithALiveOwnerRowOwnsNothing", "team-gone", inactiveOnly, false},
		{"InactiveTeamRowDoesNotOutrankTheActiveOwner", "team-live", inactiveFirst, true},
		{"EmptyTeamIDRowIsSkipped", "team-live", emptyFirst, true},
		{"FirstRecordedOwnerAppliesBeforeTheFirstRecord", "team-live", firstRecordLater, true},
		{"ControlActiveTeamWithAValidRowOwns", "team-live", control, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if sharedOwns := slices.Contains(shared[tc.repo.String()], tc.team); sharedOwns != tc.want {
				t.Fatalf("the shared owner reader gives %s owns %s = %v, the cell states %v", tc.team, tc.repo, sharedOwns, tc.want)
			}
			_, owned, err := resolveOwnedRepoID(ctx, client, org, tc.team, tc.repo.String(), now)
			if err != nil {
				t.Fatalf("resolveOwnedRepoID: %v", err)
			}
			if owned != tc.want {
				t.Errorf("%s owns %s = %v, want %v (the shared owner reader's answer)", tc.team, tc.repo, owned, tc.want)
			}
		})
	}
}

// The window edge through the whole gate, at one bound instant: a repository
// whose rows all start after the instant gives its first recorded owner the
// days (the first recorded owners apply before the first record), and an
// instant in a gap between two records has no owner, as in the shared owner
// reader.
func TestTheRepoGateGivesTheFirstRecordedOwnerTheDaysAndAGapNoOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-first-recorded-owner"
	asOf := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	measured := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	synced := asOf.AddDate(0, 0, -90)
	repo := func(name string) uuid.UUID { return uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+name)) }
	onlyLater, gap := repo("only-later"), repo("gap")
	gapEnd := asOf.AddDate(0, 0, -30)
	for _, id := range []uuid.UUID{onlyLater, gap} {
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, 'github', ?)`,
			id, "acme/"+id.String(), org, synced); err != nil {
			t.Fatalf("insert repo: %v", err)
		}
		if err := conn.Exec(ctx, `INSERT INTO team_metrics_daily
			(org_id, day, team_id, team_name, repo_id, commits_count, after_hours_commits_count, weekend_commits_count,
			 after_hours_commit_ratio, weekend_commit_ratio, computed_at)
			VALUES (?, ?, 'unassigned', '', ?, 4, 1, 0, 0.25, 0, ?)`,
			org, measured, id.String(), asOf.Add(-time.Hour)); err != nil {
			t.Fatalf("insert team metrics: %v", err)
		}
	}
	for _, row := range []struct {
		team string
		repo uuid.UUID
		from time.Time
		to   *time.Time
	}{
		{"team-a", onlyLater, asOf.AddDate(0, 0, 30), nil},
		{"team-a", gap, asOf.AddDate(0, 0, -60), &gapEnd},
		{"team-b", gap, asOf.AddDate(0, 0, 10), nil},
	} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 150, 0, ?, ?, ?)`,
			org, row.team, row.repo, "acme/"+row.repo.String(), row.from, row.to, synced); err != nil {
			t.Fatalf("insert ownership: %v", err)
		}
	}

	shared, err := teamownership.AuthoritativeOwnersByRepo(ctx, conn, org, asOf)
	if err != nil {
		t.Fatalf("shared owner reader: %v", err)
	}
	since, until := graphqldate.New(measured.AddDate(0, 0, -1)), graphqldate.New(measured.AddDate(0, 0, 1))
	for _, tc := range []struct {
		name string
		team string
		repo uuid.UUID
		days int
	}{
		{"RowsAllStartLaterFirstRecordedOwnerGetsTheDays", "team-a", onlyLater, 1},
		{"GapEndedOwnerGetsNoDays", "team-a", gap, 0},
		{"GapLaterOwnerGetsNoDays", "team-b", gap, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if sharedOwns := slices.Contains(shared[tc.repo.String()], tc.team); sharedOwns != (tc.days > 0) {
				t.Fatalf("the shared owner reader gives %s owns %s = %v, the cell states %v", tc.team, tc.repo, sharedOwns, tc.days > 0)
			}
			teamID, repoID := tc.team, tc.repo.String()
			got, err := resolve(ctx, client, org, since, until, &teamID, &repoID, asOf)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got.TotalDays != tc.days {
				t.Fatalf("%s on %s: %d days, want %d", tc.team, tc.repo, got.TotalDays, tc.days)
			}
			if tc.days > 0 && (got.Signals[0].Day.String() != measured.Format("2006-01-02") ||
				got.Signals[0].AfterHoursCommitRatio == nil || *got.Signals[0].AfterHoursCommitRatio != 0.25) {
				t.Errorf("%s on %s: signal %+v, want the measured day at after-hours ratio 0.25", tc.team, tc.repo, got.Signals[0])
			}
		})
	}
}
