//go:build integration

package investment

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The team ownership fallback gives a work unit the repositories its donor
// team owned at the END of the unit's own span, not at the time of the run.
//
// For each provider one donor team owned one repository until five days before
// the run and another one since; a second team's ownership was first known one
// day before the run. One run materializes four units of different end times:
//
//   - a unit that ended ten days before the run, of the first team: the
//     repository the team owned THEN;
//   - a unit that ended two days before the run, of the first team: the
//     repository the team owns since the change;
//   - a unit that ended ten days before the run, of the second team: nothing,
//     the team owned no repository yet (ownership starts when it was first
//     known);
//   - a unit that ended at the run's reference time, of the second team: its
//     repository.
func TestTheOwnershipFallbackTakesTheRepositoriesOfTheUnitsEndTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	materializer, err := NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}

	at := time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC)
	longBefore, changed, lately, firstKnown := at.AddDate(0, 0, -10), at.AddDate(0, 0, -5), at.AddDate(0, 0, -2), at.AddDate(0, 0, -1)

	type unitCase struct {
		name   string
		issues [2]string
		want   []string
	}
	var cases []unitCase
	for providerIndex, provider := range wtiProviders {
		host := repoHostFor[provider]
		attrSource := attributionSourceFor[provider]
		source := ownershipSourceFor[provider][0]
		team, lateTeam := provider+"-team", provider+"-team-late"
		rThen, rSince, rLate := matrixRepoID(providerIndex, 1), matrixRepoID(providerIndex, 2), matrixRepoID(providerIndex, 3)
		for id, name := range map[string]string{rThen: "then", rSince: "since", rLate: "late"} {
			mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id) VALUES (?, ?, ?, ?)`,
				id, "acme/"+provider+"-"+name, host, hierarchyCascadeTestOrg)
		}
		for _, id := range []string{team, lateTeam} {
			mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO teams (id, team_uuid, name, provider, is_active, updated_at, org_id) VALUES (?, generateUUIDv4(), ?, ?, 1, ?, ?)`,
				id, id, provider, at, hierarchyCascadeTestOrg)
		}
		seedExpiringRepoOwnership(t, ctx, conn, host, team, rThen, "acme/"+provider+"-then", source, at.AddDate(0, 0, -30), changed)
		seedRepoOwnershipByID(t, ctx, conn, host, team, rSince, "acme/"+provider+"-since", source, changed)
		seedRepoOwnershipByID(t, ctx, conn, host, lateTeam, rLate, "acme/"+provider+"-late", source, firstKnown)

		for _, unit := range []struct {
			name, key, team string
			ended           time.Time
			want            []string
		}{
			{"ended before the change of the team's repositories", "before", team, longBefore, []string{rThen}},
			{"ended after the change of the team's repositories", "after", team, lately, []string{rSince}},
			{"ended before the team's ownership was first known", "first", lateTeam, longBefore, nil},
			{"ended after the team's ownership was first known", "known", lateTeam, at, []string{rLate}},
		} {
			one, two := provider+":"+unit.key+"1", provider+":"+unit.key+"2"
			for _, issue := range []string{one, two} {
				seedMatrixWorkItemAt(t, ctx, conn, issue, provider, unit.ended)
				seedAttribution(t, ctx, conn, issue, provider, unit.team, attrSource, 1, at)
			}
			seedIssueEdge(t, ctx, conn, one, two, "", unit.ended)
			cases = append(cases, unitCase{name: provider + ": " + unit.name, issues: [2]string{one, two}, want: unit.want})
		}
	}

	if _, err := materializer.Run(ctx, Config{
		OrgID: hierarchyCascadeTestOrg, FromTS: at.AddDate(0, 0, -40), ToTS: at.Add(2 * time.Hour),
		RunID: "team-ownership-unit-time", ComputedAt: at.Add(2 * time.Hour), ProviderName: "mock",
	}); err != nil {
		t.Fatal(err)
	}

	if len(cases) != 4*len(wtiProviders) || len(wtiProviders) != 4 {
		t.Fatalf("%d cases for %d providers, want 16 for 4", len(cases), len(wtiProviders))
	}
	// The issue -> issue link is a reference, so each issue is its own
	// one-node unit with the same end time and team.
	for _, c := range cases {
		for _, issue := range c.issues {
			unit := units.WorkUnitID([]units.NodeKey{{Type: "issue", ID: issue}})
			got, _, weightSum, _ := readRepoEffort(t, ctx, conn, unit)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("%s (%s): repositories from the fallback = %v, want %v", c.name, issue, got, c.want)
			}
			if len(c.want) > 0 && (weightSum < 0.999999 || weightSum > 1.000001) {
				t.Errorf("%s (%s): the shares sum to %g, want 1", c.name, issue, weightSum)
			}
		}
	}

	// The run's record names the bound of the read and the first and the last
	// end time a unit took its ownership at.
	record := lastLogLine(t, logs.String(), "investment team repository fallback")
	for _, want := range []string{
		"ownership_as_of=" + at.Add(2*time.Hour).Format(time.RFC3339Nano),
		"ownership_as_of_first_unit=" + longBefore.Format(time.RFC3339Nano),
		"ownership_as_of_last_unit=" + at.Format(time.RFC3339Nano),
	} {
		if !strings.Contains(record, want) {
			t.Errorf("the fallback record does not hold %q: %s", want, record)
		}
	}
}

// seedMatrixWorkItemAt is seedMatrixWorkItem with a last_synced equal to the
// item's own time, so the item's time is the only time the unit's span has.
func seedMatrixWorkItemAt(t *testing.T, ctx context.Context, conn driver.Conn, issue, provider string, at time.Time) {
	t.Helper()
	seedWorkItem(t, ctx, conn, issue, "", at)
	mustTeamOwnershipExec(t, ctx, conn,
		`INSERT INTO work_items SELECT * REPLACE (? AS provider, ? AS last_synced) FROM work_items FINAL WHERE org_id = ? AND work_item_id = ?`,
		provider, at.Add(time.Second), hierarchyCascadeTestOrg, issue)
}
