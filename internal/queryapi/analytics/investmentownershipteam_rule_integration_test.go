//go:build integration

package analytics

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// D5987 option C (CHAOS-9404): the investment-path team (the `ut` source) is the owner of the repository at the instant,
// with the first recorded owners applying before the first record. It reads the one ownership rule
// (internal/ownershipwindow) through teamscope.OwnerShares; this runs it on a real engine, and runs each planted
// defect of the rule on the same data to show the expectations can fail.
func TestInvestmentTeamSourceAppliesTheFirstRecordedOwnersBackward(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := chquery.NewProductionClient(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const org = "org-10520"
	day := func(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }
	synced := day(time.September, 1)
	repos := map[string]string{
		"pair":    "10520001-1052-4052-8052-105210521052",
		"tie":     "10520002-1052-4052-8052-105210521052",
		"gap":     "10520003-1052-4052-8052-105210521052",
		"gone":    "10520004-1052-4052-8052-105210521052",
		"revoked": "10520005-1052-4052-8052-105210521052",
		"future":  "10520006-1052-4052-8052-105210521052",
	}
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for name, id := range repos {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, id, "acme/"+name, org, synced, synced)
	}
	exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES ('team-gone', generateUUIDv4(), 'Gone', [], [], [], 0, ?, ?, ?, 'github', 'team-gone')`, synced, synced, org)
	type row struct {
		team, repo string
		primary    uint8
		share      float64
		from       time.Time
		to         *time.Time
	}
	apr1, apr5, now := day(time.April, 1), day(time.April, 5), time.Now().UTC()
	for _, r := range []row{
		{"team-a", "pair", 1, 0.6, day(time.March, 1), nil},
		{"team-b", "pair", 2, 0.4, day(time.March, 1), nil},
		{"team-a", "tie", 1, 0.5, day(time.March, 1), nil},
		{"team-b", "tie", 1, 0.5, day(time.March, 1), nil},
		{"team-a", "gap", 1, 1, day(time.March, 1), &apr1},
		{"team-c", "gap", 1, 1, day(time.May, 1), nil},
		{"team-gone", "gone", 1, 1, day(time.February, 1), nil},
		{"team-a", "gone", 1, 1, day(time.March, 1), nil},
		{"team-a", "revoked", 1, 1, day(time.March, 1), &apr5},
		{"team-a", "future", 1, 1, now.AddDate(0, 0, 30), nil},
	} {
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, share, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, 100, 0, ?, ?, ?, ?)`,
			org, r.team, repos[r.repo], "acme/"+r.repo, r.primary, r.share, r.from, r.to, synced)
	}

	query := func(planted string, at time.Time) map[string]float64 {
		t.Helper()
		source, bindings := ownershipTeamSourceAt(org, nil, at)
		if planted != "" {
			variants := plantedRuleSources(source)
			source = variants[planted]
		}
		rows, err := client.Query(ctx, `SELECT r.repo, ut.team_id, ut.owner_share FROM `+source+` AS ut INNER JOIN (SELECT toString(id) AS id, repo FROM repos FINAL WHERE org_id = '`+org+`') AS r ON r.id = ut.owner_repo_id`, bindings)
		if err != nil {
			t.Fatalf("source at %s: %v", at.Format(time.DateOnly), err)
		}
		defer rows.Close()
		got := map[string]float64{}
		for rows.Next() {
			var repo, team string
			var share float64
			if err := rows.Scan(&repo, &team, &share); err != nil {
				t.Fatal(err)
			}
			got[repo[len("acme/"):]+"|"+team] = share
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return got
	}
	equal := func(got, want map[string]float64) bool {
		if len(got) != len(want) {
			return false
		}
		for k, w := range want {
			if g, ok := got[k]; !ok || math.Abs(g-w) > 1e-9 {
				return false
			}
		}
		return true
	}
	show := func(m map[string]float64) string {
		var keys []string
		for k, v := range m {
			keys = append(keys, fmt.Sprintf("%s=%.2f", k, v))
		}
		sort.Strings(keys)
		return fmt.Sprint(keys)
	}

	beforeAll, atFirst, inGap, after := day(time.February, 15), day(time.March, 1), day(time.April, 15), day(time.June, 1)
	want := map[string]map[string]float64{
		"before every record": {"pair|team-a": 0.6, "pair|team-b": 0.4, "tie|team-a": 0.5, "tie|team-b": 0.5, "gap|team-a": 1, "gone|team-a": 1, "revoked|team-a": 1, "future|team-a": 1},
		"at the first record": {"pair|team-a": 0.6, "pair|team-b": 0.4, "tie|team-a": 0.5, "tie|team-b": 0.5, "gap|team-a": 1, "gone|team-a": 1, "revoked|team-a": 1, "future|team-a": 1},
		"in a gap":            {"pair|team-a": 0.6, "pair|team-b": 0.4, "tie|team-a": 0.5, "tie|team-b": 0.5, "gone|team-a": 1, "future|team-a": 1},
		"after the records":   {"pair|team-a": 0.6, "pair|team-b": 0.4, "tie|team-a": 0.5, "tie|team-b": 0.5, "gap|team-c": 1, "gone|team-a": 1, "future|team-a": 1},
	}
	instants := map[string]time.Time{"before every record": beforeAll, "at the first record": atFirst, "in a gap": inGap, "after the records": after}
	for name, at := range instants {
		if got := query("", at); !equal(got, want[name]) {
			t.Errorf("%s: owners %s, want %s", name, show(got), show(want[name]))
		}
	}
	// The present: a repository whose only row starts after now has its first owners now; a revoked one has none.
	present := query("", now)
	if _, ok := present["future|team-a"]; !ok {
		t.Errorf("at the present the repository whose first record starts later has no owner: %s", show(present))
	}
	if _, ok := present["revoked|team-a"]; ok {
		t.Errorf("at the present a revoked ownership still owns: %s", show(present))
	}

	// Each planted defect of the rule gives a different answer on this data, so the expectations above can fail.
	for _, plant := range []string{"clamp removed", "clamp on every row", "inactive team counted", "inactive guard inverted"} {
		differs := false
		for name, at := range instants {
			if !equal(query(plant, at), want[name]) {
				differs = true
			}
		}
		if !differs {
			t.Errorf("planted defect %q gives the same owners at every instant: the data cannot catch it", plant)
		}
	}
}
