package investmentexplain

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// quoteVersionClient answers the data-version query from two stores: the work-unit rows (fixed) and the quote rows
// (varied by the test). It answers with the quotes' stamp and count only when the statement reads the quotes table,
// so a version that ignores quotes cannot see them.
type quoteVersionClient struct {
	quotesLatest, quotesCount string
	statements                []string
}

func (c *quoteVersionClient) Query(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	if strings.Contains(statement, "work_unit_investment_quotes") {
		return &fixtureRowScanner{rows: [][]any{{"2026-10-13 10:00:00.000", "4", c.quotesLatest, c.quotesCount}}}, nil
	}
	return &fixtureRowScanner{rows: [][]any{{"2026-10-13 10:00:00.000", "4"}}}, nil
}

func versionKey(t *testing.T, client *quoteVersionClient) string {
	t.Helper()
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	version, err := reader.FetchInvestmentDataVersion(context.Background(), "org-9582")
	if err != nil {
		t.Fatal(err)
	}
	themes, subcategories := newOrderedFloatMap(), newOrderedFloatMap()
	key, err := ComputeCacheKey(CacheKeyInput{Filters: mixCacheFilters(explainOpts(), themes, subcategories, version), OrgID: "org-9582"})
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// CHAOS-9582: the work-unit rows are identical in both runs; only the quote rows differ.
func TestQuoteOnlyWriteChangesTheExplainCacheKey(t *testing.T) {
	base := versionKey(t, &quoteVersionClient{quotesLatest: "2026-10-13 09:00:00.000", quotesCount: "7"})
	if newer := versionKey(t, &quoteVersionClient{quotesLatest: "2026-10-13 11:00:00.000", quotesCount: "7"}); newer == base {
		t.Error("a quote written later (newer stamp, same count) must change the key")
	}
	if more := versionKey(t, &quoteVersionClient{quotesLatest: "2026-10-13 09:00:00.000", quotesCount: "8"}); more == base {
		t.Error("an added quote (same newest stamp, more rows) must change the key")
	}
	if same := versionKey(t, &quoteVersionClient{quotesLatest: "2026-10-13 09:00:00.000", quotesCount: "7"}); same != base {
		t.Error("unchanged quotes must keep the key")
	}
}

func TestDataVersionIsOneOrgScopedRoundTripOverBothTables(t *testing.T) {
	client := &quoteVersionClient{quotesLatest: "x", quotesCount: "1"}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.FetchInvestmentDataVersion(context.Background(), "org-9582"); err != nil {
		t.Fatal(err)
	}
	if len(client.statements) != 1 {
		t.Fatalf("data version took %d queries, want 1 round trip", len(client.statements))
	}
	statement := client.statements[0]
	for _, table := range []string{"work_unit_investments", "work_unit_investment_quotes"} {
		if !strings.Contains(statement, table) {
			t.Errorf("data version does not read %s", table)
		}
	}
	if got := strings.Count(statement, "org_id = {org_id:String}"); got < 2 {
		t.Errorf("org filter appears %d times, want it on each table read", got)
	}
}
