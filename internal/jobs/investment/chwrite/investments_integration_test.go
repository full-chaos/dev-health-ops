//go:build integration

package chwrite

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const testOrgID = "33333333-3333-4333-8333-333333333333"

func newTestWriter(t *testing.T) (*Writer, driver.Conn, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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

	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	return writer, conn, ctx
}

func scanString(t *testing.T, ctx context.Context, conn driver.Conn, query string, args ...any) string {
	t.Helper()
	row := conn.QueryRow(ctx, query, args...)
	var value string
	if err := row.Scan(&value); err != nil {
		t.Fatalf("scan failed: %v\nquery: %s", err, query)
	}
	return value
}

func scanUint64(t *testing.T, ctx context.Context, conn driver.Conn, query string, args ...any) uint64 {
	t.Helper()
	row := conn.QueryRow(ctx, query, args...)
	var value uint64
	if err := row.Scan(&value); err != nil {
		t.Fatalf("scan failed: %v\nquery: %s", err, query)
	}
	return value
}

// TestWriteInvestmentsRoundTripsMapColumnsAndNullableFields proves the
// property a mocked conn cannot express: theme_distribution_json and
// subcategory_distribution_json are real ClickHouse Map(String, Float64)
// columns, and a Nullable(UUID)/Nullable(String) column round-trips both a
// present and an absent value through the real engine, not just through Go's
// own type system.
func TestWriteInvestmentsRoundTripsMapColumnsAndNullableFields(t *testing.T) {
	writer, conn, ctx := newTestWriter(t)

	repoID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	workUnitType := "issue"
	computedAt := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	records := []InvestmentRecord{
		{
			// Present repo_id/provider/labels.
			WorkUnitID:                 "wu-with-repo",
			WorkUnitType:               &workUnitType,
			WorkUnitName:               strPtr("Fix the thing"),
			FromTS:                     computedAt.Add(-24 * time.Hour),
			ToTS:                       computedAt,
			RepoID:                     &repoID,
			Provider:                   strPtr("github"),
			EffortMetric:               "fte_days",
			EffortValue:                1.5,
			ThemeDistribution:          map[string]float64{"Feature Work": 0.8, "Bug Fix": 0.2},
			SubcategoryDistribution:    map[string]float64{"new-feature": 0.8, "regression": 0.2},
			StructuralEvidenceJSON:     `{"prs":1}`,
			EvidenceQuality:            0.9,
			EvidenceQualityBand:        "high",
			CategorizationStatus:       "ok",
			CategorizationErrorsJSON:   "[]",
			CategorizationModelVersion: "v1",
			CategorizationInputHash:    "abc123",
			CategorizationRunID:        "run-1",
			ComputedAt:                 computedAt,
		},
		{
			// Absent repo_id/provider/labels -- the NULL path.
			WorkUnitID:                 "wu-without-repo",
			WorkUnitType:               nil,
			WorkUnitName:               nil,
			FromTS:                     computedAt.Add(-24 * time.Hour),
			ToTS:                       computedAt,
			RepoID:                     nil,
			Provider:                   nil,
			EffortMetric:               "fte_days",
			EffortValue:                0.5,
			ThemeDistribution:          map[string]float64{},
			SubcategoryDistribution:    map[string]float64{},
			StructuralEvidenceJSON:     `{}`,
			EvidenceQuality:            0.1,
			EvidenceQualityBand:        "low",
			CategorizationStatus:       "invalid_llm_output",
			CategorizationErrorsJSON:   `["schema"]`,
			CategorizationModelVersion: "v1",
			CategorizationInputHash:    "def456",
			CategorizationRunID:        "run-1",
			ComputedAt:                 computedAt,
		},
	}

	written, err := writer.WriteInvestments(ctx, testOrgID, records)
	if err != nil {
		t.Fatalf("WriteInvestments: %v", err)
	}
	if written != 2 {
		t.Fatalf("written = %d, want 2", written)
	}

	// The Map column round-trips through arrayMap/toString rather than a typed
	// scan target, so this asserts the value ClickHouse actually stored, not a
	// value Go merely believes it sent.
	themeValue := scanString(t, ctx, conn,
		`SELECT toString(theme_distribution_json['Feature Work']) FROM work_unit_investments
		 WHERE org_id = ? AND work_unit_id = ?`, testOrgID, "wu-with-repo")
	if themeValue != "0.8" {
		t.Fatalf("theme_distribution_json['Feature Work'] = %q, want \"0.8\"", themeValue)
	}

	repoIsNull := scanUint64(t, ctx, conn,
		`SELECT toUInt64(isNull(repo_id)) FROM work_unit_investments
		 WHERE org_id = ? AND work_unit_id = ?`, testOrgID, "wu-with-repo")
	if repoIsNull != 0 {
		t.Fatalf("repo_id for wu-with-repo is NULL, want a value")
	}
	repoIsNullAbsent := scanUint64(t, ctx, conn,
		`SELECT toUInt64(isNull(repo_id)) FROM work_unit_investments
		 WHERE org_id = ? AND work_unit_id = ?`, testOrgID, "wu-without-repo")
	if repoIsNullAbsent != 1 {
		t.Fatalf("repo_id for wu-without-repo is NOT NULL, want NULL")
	}

	// org_id scoping: a second org must not see these rows -- the
	// discriminator every reader in this codebase relies on.
	otherOrgCount := scanUint64(t, ctx, conn,
		`SELECT count() FROM work_unit_investments WHERE org_id = ? AND work_unit_id = ?`,
		"99999999-9999-4999-8999-999999999999", "wu-with-repo")
	if otherOrgCount != 0 {
		t.Fatalf("a different org sees this org's row: count = %d", otherOrgCount)
	}
}

// TestWriteRepoEffortAndQuotesRoundTrip proves the other two tables write and
// scope by org_id the same way, and that an empty slice is a no-op rather
// than an error -- matching write_work_unit_repo_effort/
// write_work_unit_investment_quotes's `if not rows: return` (sinks/clickhouse/
// investment.py:150-188).
func TestWriteRepoEffortAndQuotesRoundTrip(t *testing.T) {
	writer, conn, ctx := newTestWriter(t)

	repoID := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	computedAt := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	effortWritten, err := writer.WriteRepoEffort(ctx, testOrgID, []RepoEffortRecord{{
		WorkUnitID:          "wu-1",
		RepoID:              &repoID,
		EffortMetric:        "fte_days",
		EffortValue:         1.0,
		AllocationWeight:    0.75,
		AllocationSource:    "structural",
		CategorizationRunID: "run-1",
		ComputedAt:          computedAt,
	}})
	if err != nil {
		t.Fatalf("WriteRepoEffort: %v", err)
	}
	if effortWritten != 1 {
		t.Fatalf("effortWritten = %d, want 1", effortWritten)
	}

	quotesWritten, err := writer.WriteQuotes(ctx, testOrgID, []QuoteRecord{{
		WorkUnitID:          "wu-1",
		Quote:               "fixes the login bug",
		SourceType:          "pr_body",
		SourceID:            "pr-42",
		ComputedAt:          computedAt,
		CategorizationRunID: "run-1",
	}})
	if err != nil {
		t.Fatalf("WriteQuotes: %v", err)
	}
	if quotesWritten != 1 {
		t.Fatalf("quotesWritten = %d, want 1", quotesWritten)
	}

	effortCount := scanUint64(t, ctx, conn,
		`SELECT count() FROM work_unit_repo_effort WHERE org_id = ? AND work_unit_id = ?`,
		testOrgID, "wu-1")
	if effortCount != 1 {
		t.Fatalf("work_unit_repo_effort count = %d, want 1", effortCount)
	}
	quoteCount := scanUint64(t, ctx, conn,
		`SELECT count() FROM work_unit_investment_quotes WHERE org_id = ? AND work_unit_id = ?`,
		testOrgID, "wu-1")
	if quoteCount != 1 {
		t.Fatalf("work_unit_investment_quotes count = %d, want 1", quoteCount)
	}

	// Empty slice is a no-op, not an error -- matches Python's `if not rows: return`.
	noopWritten, err := writer.WriteRepoEffort(ctx, testOrgID, nil)
	if err != nil || noopWritten != 0 {
		t.Fatalf("WriteRepoEffort(nil) = (%d, %v), want (0, nil)", noopWritten, err)
	}
}

// TestWriteRefusesEmptyOrganizationID pins the CHAOS-4341 org-scoping
// discipline (internal/jobs/metrics/daily/cicd.Writer.WriteResult): a write
// with no org id fails closed instead of writing an org_id="" row invisible
// to every org-scoped reader.
func TestWriteRefusesEmptyOrganizationID(t *testing.T) {
	writer, _, ctx := newTestWriter(t)

	if _, err := writer.WriteInvestments(ctx, "", []InvestmentRecord{{WorkUnitID: "x"}}); err == nil {
		t.Fatal("WriteInvestments with empty orgID: want error, got nil")
	}
	if _, err := writer.WriteRepoEffort(ctx, "", []RepoEffortRecord{{WorkUnitID: "x"}}); err == nil {
		t.Fatal("WriteRepoEffort with empty orgID: want error, got nil")
	}
	if _, err := writer.WriteQuotes(ctx, "", []QuoteRecord{{WorkUnitID: "x"}}); err == nil {
		t.Fatal("WriteQuotes with empty orgID: want error, got nil")
	}
}

func strPtr(value string) *string { return &value }
