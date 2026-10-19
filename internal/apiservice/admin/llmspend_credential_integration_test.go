//go:build integration

package admin

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The organization spend summary serves only the calls made with the
// organization's own credentials. A platform call (its model, tokens, run and
// failure classes) and a row written before credential_source existed are never
// served.
func TestSpendServesOnlyOrganizationCredentialRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	exec(`CREATE TABLE llm_token_usage (
    org_id String, run_id String DEFAULT '', provider LowCardinality(String), model String, source LowCardinality(String),
    use_case LowCardinality(String) DEFAULT 'legacy', input_tokens UInt64, output_tokens UInt64, calls UInt64, computed_at DateTime
) ENGINE = MergeTree PARTITION BY toYYYYMM(computed_at) ORDER BY (org_id, provider, model, source, computed_at)`)
	exec(`CREATE TABLE work_unit_investments (
    org_id String, categorization_run_id String, categorization_status String, categorization_errors_json String, computed_at DateTime
) ENGINE = MergeTree ORDER BY (org_id, computed_at)`)
	const legacyRow = `INSERT INTO llm_token_usage (org_id, run_id, provider, model, source, input_tokens, output_tokens, calls, computed_at)
VALUES ('org-1', 'run-before-column', 'openai', 'jev-1.13.0', 'investment_materialize', 5, 5, 5, now())`
	exec(legacyRow)
	body, err := os.ReadFile("../../chmigrate/sql/139_llm_token_usage_credential_source.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		exec(statement)
	}
	rows := []struct{ org, run, model, source, credential string }{
		{"org-1", "run-platform", "jev-1.13.0", "investment_materialize", "platform"},
		{"org-1", "", "jev-1.13.0", "investment_mix_explain", "platform"},
		{"org-1", "run-org", "claude-haiku-5-5", "investment_materialize", "org"},
		{"org-1", "", "claude-haiku-5-5", "work_unit_explain", "org"},
		{"org-2", "run-other-org", "claude-haiku-5-5", "investment_materialize", "org"},
	}
	for _, row := range rows {
		exec(`INSERT INTO llm_token_usage (org_id, run_id, provider, model, source, input_tokens, output_tokens, calls, computed_at, credential_source)
VALUES ('` + row.org + `', '` + row.run + `', 'x', '` + row.model + `', '` + row.source + `', 1, 1, 1, now(), '` + row.credential + `')`)
	}
	exec(`INSERT INTO work_unit_investments VALUES ('org-1', 'run-platform', 'invalid_llm_output', '[]', now()), ('org-1', 'run-org', 'repaired', '[]', now())`)

	summary, err := clickhouseSpend{conn: conn}.readSpend(ctx, "org-1", 20, time.Now().Add(-24*time.Hour).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.runs) != 1 || summary.runs[0].runID != "run-org" || summary.runs[0].model != "claude-haiku-5-5" {
		t.Fatalf("runs = %+v, want only run-org", summary.runs)
	}
	if got := summary.runs[0].failures; got == nil || got.Len() != 1 {
		t.Fatalf("failures of run-org = %v, want its own single class", got)
	}
	if len(summary.unattributed) != 1 || summary.unattributed[0].model != "claude-haiku-5-5" || summary.unattributed[0].source != "work_unit_explain" {
		t.Fatalf("unattributed = %+v, want only the org-credential explain row", summary.unattributed)
	}
}
