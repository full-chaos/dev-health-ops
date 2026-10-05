//go:build integration

package server

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTestOpsCoverageRoute_ExactRepoKeysReturnBranchCoverageOutsideIndependentTopN
// exercises the registered TestOps Coverage HTTP document over the production
// ClickHouse schema. It proves the CHAOS-8497 acceptance criterion: once the
// line-coverage result supplies its repository key, the web can ask for that
// repository's branch-coverage value even when branch coverage's independent
// top-N response does not contain it. It does not claim that a current web
// workflow already sends keys.
func TestTestOpsCoverageRoute_ExactRepoKeysReturnBranchCoverageOutsideIndependentTopN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	options, err := stdclickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	queryClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(instance.URI))
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	t.Cleanup(func() { _ = queryClient.Close() })

	const (
		orgID      = "org-8497"
		targetRepo = "84970000-0000-4000-8000-000000000001"
	)
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := day.Add(24 * time.Hour)
	seed := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed ClickHouse: %v\n%s", err, statement)
		}
	}
	seedCoverage := func(repo string, line, branch float64) {
		t.Helper()
		seed(`INSERT INTO testops_coverage_metrics_daily
			(repo_id, day, line_coverage_pct, branch_coverage_pct, uncovered_files_count, coverage_regression_count, org_id, computed_at)
			VALUES (?, ?, ?, ?, 0, 0, ?, ?)`, repo, day, line, branch, orgID, computedAt)
	}
	seedRepo := func(repo, name string) {
		t.Helper()
		seed(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
			VALUES (?, ?, ?, ?, ?, ?)`, repo, name, "github", orgID, computedAt, computedAt)
	}

	// The target is the line-coverage leader, but 100 other repositories
	// rank ahead of it for branch coverage. Thus a regular branch top-100
	// result omits it, while a branch request for the line result's exact key
	// must still return it.
	seedRepo(targetRepo, "acme/target")
	seedCoverage(targetRepo, 99, 1)
	for i := 0; i < 100; i++ {
		repo := fmt.Sprintf("84970000-0000-4000-8000-%012d", i+2)
		seedRepo(repo, fmt.Sprintf("acme/other-%03d", i+1))
		seedCoverage(repo, 1, float64(100+i))
	}

	pool := startTestRegistryPostgres(t)
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(writeTestJWKS(t, pub), itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	handler, _, _, _, err := newQueryHandler(analytics.PinInvestmentMembershipScope(queryClient), pool, verifier, itTestSchemaDigest, os.Getenv)
	if err != nil {
		t.Fatalf("build TestOps Coverage handler: %v", err)
	}
	token := signTestEnvelope(t, priv, orgID)

	variables := func(branchTopN int, branchKeys []string) map[string]any {
		branch := map[string]any{
			"dimension": "REPO",
			"measure":   "COVERAGE_BRANCH_PCT",
			"dateRange": map[string]any{
				"startDate": day.Format("2006-01-02"),
				"endDate":   day.Format("2006-01-02"),
			},
			"topN": branchTopN,
		}
		if branchKeys != nil {
			branch["keys"] = branchKeys
		}
		return map[string]any{
			"orgId": orgID,
			"batch": map[string]any{
				"breakdowns": []any{
					map[string]any{
						"dimension": "REPO",
						"measure":   "COVERAGE_LINE_PCT",
						"dateRange": map[string]any{
							"startDate": day.Format("2006-01-02"),
							"endDate":   day.Format("2006-01-02"),
						},
						"topN": 1,
					},
					branch,
				},
			},
		}
	}

	type item struct {
		Key   string   `json:"key"`
		Value *float64 `json:"value"`
	}
	type response struct {
		Data struct {
			Analytics struct {
				Breakdowns []struct {
					Measure string `json:"measure"`
					Items   []item `json:"items"`
				} `json:"breakdowns"`
			} `json:"analytics"`
		} `json:"data"`
		Errors json.RawMessage `json:"errors"`
	}
	decode := func(recBody []byte) response {
		t.Helper()
		var got response
		if err := json.Unmarshal(recBody, &got); err != nil {
			t.Fatalf("decode GraphQL response: %v\n%s", err, recBody)
		}
		if len(got.Errors) != 0 && string(got.Errors) != "null" {
			t.Fatalf("registered TestOps Coverage document returned GraphQL errors: %s", got.Errors)
		}
		return got
	}
	itemsFor := func(got response, measure string) []item {
		t.Helper()
		for _, breakdown := range got.Data.Analytics.Breakdowns {
			if breakdown.Measure == measure {
				return breakdown.Items
			}
		}
		t.Fatalf("response has no %s breakdown: %+v", measure, got.Data.Analytics.Breakdowns)
		return nil
	}
	containsKey := func(items []item, key string) bool {
		for _, item := range items {
			if item.Key == key {
				return true
			}
		}
		return false
	}

	independent := postGraphQLWithVariables(t, handler, registeredTestOpsCoverageDocument, token, variables(100, nil))
	if independent.Code != http.StatusOK {
		t.Fatalf("independent TestOps Coverage request: got HTTP %d, want 200: %s", independent.Code, independent.Body.String())
	}
	initial := decode(independent.Body.Bytes())
	lineItems := itemsFor(initial, "coverage_line_pct")
	if len(lineItems) != 1 || lineItems[0].Key != targetRepo || lineItems[0].Value == nil || *lineItems[0].Value != 99 {
		t.Fatalf("line top-1 = %+v, want the target repository at 99", lineItems)
	}
	lineKeys := []string{lineItems[0].Key} // derive the branch request from the actual line response.
	branchTop100 := itemsFor(initial, "coverage_branch_pct")
	if len(branchTop100) != 100 || containsKey(branchTop100, targetRepo) {
		t.Fatalf("independent branch top-100 must omit the line-result key %q, got %d rows: %+v", targetRepo, len(branchTop100), branchTop100)
	}

	exact := postGraphQLWithVariables(t, handler, registeredTestOpsCoverageDocument, token, variables(1, lineKeys))
	if exact.Code != http.StatusOK {
		t.Fatalf("exact-key TestOps Coverage request: got HTTP %d, want 200: %s", exact.Code, exact.Body.String())
	}
	exactBranch := itemsFor(decode(exact.Body.Bytes()), "coverage_branch_pct")
	if len(exactBranch) != 1 || exactBranch[0].Key != targetRepo || exactBranch[0].Value == nil || *exactBranch[0].Value != 1 {
		t.Fatalf("branch exact-key result with topN=1 = %+v, want the target repository at 1", exactBranch)
	}
}
