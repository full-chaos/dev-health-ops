//go:build integration

package compoundingrisk

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// sweepUUIDs maps the fixture's repository ids to the UUIDs the repos table
// needs. Filters are mapped on the way in and served scope ids on the way out,
// so the real variant compares with the same golden as the model.
func sweepUUIDs(world sweepWorld) (map[string]string, map[string]string) {
	in, out := map[string]string{}, map[string]string{}
	for i, r := range world.repos {
		u := fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1)
		in[r.id], out[u] = u, r.id
	}
	return in, out
}

func sweepSQLNum(v *float64) string {
	if v == nil {
		return "NULL"
	}
	return strconv.FormatFloat(*v, 'g', -1, 64)
}

func sweepSeed(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, world sweepWorld, uuidOf map[string]string) {
	t.Helper()
	for _, table := range []string{"compounding_risk_daily", "repos", "teams", "team_repo_ownership"} {
		exec(t, ctx, conn, "TRUNCATE TABLE %s", table)
	}
	nameOf := map[string]string{}
	var values []string
	for _, r := range world.repos {
		nameOf[r.id] = r.name
		values = append(values, fmt.Sprintf("('%s', '%s', 'github', '%s', now64(3), now64(3))", uuidOf[r.id], r.name, sweepOrg))
	}
	exec(t, ctx, conn, "INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES %s", strings.Join(values, ", "))
	values = nil
	var owned []string
	for _, tm := range world.teams {
		active := 0
		if tm.active {
			active = 1
		}
		values = append(values, fmt.Sprintf("('%s', '%s', '%s', [], %d, now64(6))", tm.id, tm.name, sweepOrg, active))
		for _, id := range tm.owns {
			owned = append(owned, fmt.Sprintf("('%s', 'github', '%s', '%s', '%s', 'exact', 'native', toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), now64(3))",
				sweepOrg, tm.id, uuidOf[id], nameOf[id]))
		}
	}
	exec(t, ctx, conn, "INSERT INTO teams (id, name, org_id, repo_patterns, is_active, updated_at) VALUES %s", strings.Join(values, ", "))
	if len(owned) > 0 {
		exec(t, ctx, conn, "INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, valid_from, updated_at) VALUES %s", strings.Join(owned, ", "))
	}
	values = nil
	for _, r := range world.risk {
		id := r.scopeID
		if r.scope == scopeRepo {
			id = uuidOf[id]
		}
		severity := r.severity
		if severity == "" {
			severity = "unknown"
		}
		values = append(values, fmt.Sprintf("('%s', '%s', '%s', '%s', %s, '%s', %s, %s, %s, %s, %v, %v, %v, %v, %v, %v, toDateTime('%s', 'UTC'))",
			sweepOrg, r.day.Format(dateLayout), r.scope, id, sweepSQLNum(r.score), severity,
			sweepSQLNum(r.churn), sweepSQLNum(r.complexity), sweepSQLNum(r.ownership), sweepSQLNum(r.review),
			r.wChurn, r.wComplexity, r.wOwnership, r.wReview, r.thresholdElevated, r.thresholdHigh,
			r.computedAt.UTC().Format("2006-01-02 15:04:05")))
	}
	exec(t, ctx, conn, `INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, compounding_risk, severity,
      churn_norm, complexity_norm, ownership_norm, review_norm, w_churn, w_complexity, w_ownership, w_review,
      threshold_elevated, threshold_high, computed_at) VALUES %s`, strings.Join(values, ", "))
}

func sweepMapIDs(ids []string, to map[string]string) []string {
	if ids == nil {
		return nil
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if v, ok := to[id]; ok {
			id = v
		}
		out = append(out, id)
	}
	return out
}

// CHAOS-14462: the equality sweep on real ClickHouse. Same fixture, same
// combinations, same golden and compare as TestCompoundingRiskEqualitySweep.
func TestRealClickHouse_CompoundingRiskEqualitySweep(t *testing.T) {
	_, conn, client := startStore(t)
	// The cap combinations read ownership once per listed team (about 500
	// reads each): the store context of startStore is too short for 66 runs.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	text := sweepTable(t, func(_ string, world sweepWorld, filter *model.CompoundingRiskFilterInput) (*model.CompoundingRiskResult, error) {
		uuidOf, idOf := sweepUUIDs(world)
		sweepSeed(t, ctx, conn, world, uuidOf)
		if filter != nil {
			f := *filter
			f.RepoIds = sweepMapIDs(f.RepoIds, uuidOf)
			filter = &f
		}
		got, err := Resolve(ctx, client, sweepOrg, filter, sweepNow())
		if got != nil {
			for i := range got.Rows {
				got.Rows[i].ScopeID = sweepMapIDs([]string{got.Rows[i].ScopeID}, idOf)[0]
			}
		}
		return got, err
	})
	sweepCompare(t, text)
}
