//go:build integration

package investment

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

func TestAnOrgTypeSafeCategorizeRunWritesOneAttemptRowToTheStoreWithTheProvider(t *testing.T) {
	h := newShadowHarness(t)
	t.Setenv("TYPESAFE_API_KEY", testPlatformJevKey)
	redact := strings.NewReplacer(testTypeSafeKey, "[org key]", testPlatformJevKey, "[platform key]")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "bytypesafeattempt")
	venue.RequireReady(t, ctx)
	serviceModelFlag := uuid.NewString()
	pgseed.FeatureFlag(ctx, t, venue.Admin, serviceModelFlag, llmorgsettings.ACRServiceModelFeatureKey, "community", true)
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	org := uuid.NewString()
	pgseed.Org(ctx, t, venue.Admin, org, "enterprise")
	pgseed.OrgOverride(ctx, t, venue.Admin, org, serviceModelFlag, true)
	seedOrgSettings(ctx, t, venue.Admin, cipher, org, map[string]string{
		llmorgsettings.KeyCategorizeBYOEnabled: "true", "purpose:categorize:provider": "typesafe",
		"purpose:categorize:model": testTypeSafeModel, llmbudget.LimitKey: "100000000",
	}, map[string]string{"purpose:categorize:api_key": testTypeSafeKey})

	network := newTypeSafeBYO(typeSafeAnswers(testTypeSafeModel)).network
	byo := newOrgCategorizeBYO(venue.Domain, cipher, testLogger(),
		func(categorize.ProviderKind, string, string, string) (categorize.Provider, error) {
			return nil, errors.New("the typesafe path builds no text provider")
		},
		func(categorize.ClefClientConfig) (*categorize.TypeSafeClient, error) {
			return nil, errors.New("the typesafe path builds no clef client")
		})
	byo.newTypeSafe = func(cfg categorize.OrgTypeSafeClientConfig) (*categorize.TypeSafeClient, error) {
		return newTypeSafeTestClient(cfg, network, testLogger())
	}
	served, err := byo.Build(ctx, org)
	if err != nil || served == nil {
		t.Fatalf("Build = %v, %v", served, err)
	}
	t.Cleanup(func() { _ = served.Close() })
	cfg := shadowTestConfig()
	cfg.OrgID = org
	m := &Materializer{logger: testLogger()}
	m.SetServed(served)
	if _, err := m.categorizeEntry(ctx, cfg, shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6))); err != nil {
		t.Fatalf("categorize: %s", redact.Replace(err.Error()))
	}
	if got := network.count(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
	writer, err := chwrite.NewWriter(h.conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	served.finish(h.ctx, writer, cfg)
	if got := h.count(t, `SELECT count() FROM llm_categorization_attempts`); got != 1 {
		t.Fatalf("attempt rows = %d, want 1", got)
	}
	ledger := ledgerRows(ctx, t, venue.Admin, org)
	if len(ledger) != 1 || ledger[0].status != "succeeded" || ledger[0].actual == nil || !ledger[0].reconciledSet {
		t.Fatalf("ledger rows = %+v, want one succeeded, settled reservation", ledger)
	}

	var provider, requested, returned, requestID, apiMode string
	var httpStatus uint16
	var inputTokens uint32
	if err := h.conn.QueryRow(h.ctx, `
		SELECT provider, model_requested, model_returned, http_status, request_id, api_mode, input_tokens
		FROM llm_categorization_attempts
		WHERE org_id = ? AND run_id = ?`, cfg.OrgID, cfg.RunID).
		Scan(&provider, &requested, &returned, &httpStatus, &requestID, &apiMode, &inputTokens); err != nil {
		t.Fatalf("read attempt for org %q run %q: %v", cfg.OrgID, cfg.RunID, err)
	}
	if provider != "byo-typesafe" || requested != testTypeSafeModel || returned != testTypeSafeModel ||
		httpStatus != http.StatusOK || requestID != "req-typesafe-1" || apiMode != "systemone" || inputTokens != testTypeSafeInTokens {
		t.Fatalf("attempt provider=%q model_requested=%q model_returned=%q http_status=%d request_id=%q api_mode=%q input_tokens=%d",
			redact.Replace(provider), redact.Replace(requested), redact.Replace(returned), httpStatus, redact.Replace(requestID), redact.Replace(apiMode), inputTokens)
	}

	rows, err := h.conn.Query(h.ctx, `
		SELECT name FROM system.columns
		WHERE database = currentDatabase() AND table = 'llm_categorization_attempts'
		ORDER BY position`)
	if err != nil {
		t.Fatalf("enumerate attempt columns: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("read attempt column: %v", err)
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("enumerate attempt columns: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close attempt columns: %v", err)
	}
	if len(columns) == 0 {
		t.Fatalf("key scan columns = %d, want at least 1", len(columns))
	}
	var total uint64
	for _, column := range columns {
		identifier := "`" + strings.ReplaceAll(column, "`", "``") + "`"
		for marker, key := range []string{testTypeSafeKey, testPlatformJevKey} {
			var matches uint64
			err := h.conn.QueryRow(h.ctx, `SELECT count() FROM llm_categorization_attempts WHERE positionCaseInsensitive(toString(`+identifier+`), ?) > 0`, key).Scan(&matches)
			if err != nil {
				t.Fatalf("key scan column %q marker %d: %s", column, marker, redact.Replace(err.Error()))
			}
			total += matches
		}
	}
	if total != 0 {
		t.Fatalf("key scan matches = %d across %d columns, want 0", total, len(columns))
	}
}
