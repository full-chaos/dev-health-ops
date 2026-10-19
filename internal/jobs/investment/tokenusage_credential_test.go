package investment

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
)

type capturingBatch struct{ rows *[][]any }

func (b capturingBatch) Append(values ...any) error {
	*b.rows = append(*b.rows, values)
	return nil
}
func (b capturingBatch) AppendStruct(any) error        { return nil }
func (b capturingBatch) Column(int) driver.BatchColumn { return nil }
func (b capturingBatch) Flush() error                  { return nil }
func (b capturingBatch) Send() error                   { return nil }
func (b capturingBatch) IsSent() bool                  { return true }
func (b capturingBatch) Abort() error                  { return nil }
func (b capturingBatch) Rows() int                     { return 1 }
func (b capturingBatch) Columns() []column.Interface   { return nil }
func (b capturingBatch) Close() error                  { return nil }

type capturingConn struct{ rows *[][]any }

func (c capturingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errRefuseForTest
}

func (c capturingConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return capturingBatch{rows: c.rows}, nil
}

func TestTokenUsageRowCarriesTheCredentialTheRunUsed(t *testing.T) {
	for name, tc := range map[string]struct {
		org  bool
		want string
	}{
		"org credentials":      {org: true, want: "org"},
		"platform credentials": {org: false, want: "platform"},
	} {
		t.Run(name, func(t *testing.T) {
			var rows [][]any
			writer, err := chwrite.NewWriter(capturingConn{rows: &rows})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := chquery.NewReader(capturingConn{rows: &rows})
			if err != nil {
				t.Fatal(err)
			}
			materializer, err := NewMaterializer(reader, writer, categorize.MockProvider{}, testLogger())
			if err != nil {
				t.Fatal(err)
			}
			cfg := tokenUsageConfig()
			cfg.OrgCredentials = tc.org
			if err := materializer.flushTokenUsage(context.Background(), cfg, Stats{LLMCalls: 2, LLMInputTokens: 10, LLMOutputTokens: 5}); err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("rows = %d, want 1", len(rows))
			}
			got := rows[0][len(rows[0])-1]
			if got != tc.want {
				t.Fatalf("credential_source = %v, want %s", got, tc.want)
			}
		})
	}
}

func TestProviderForRunSourceReportsWhoseCredentialsServe(t *testing.T) {
	executor, _, _ := orgExecutor(t, &fakeOrgConfig{cfg: orgCfg("openai", "org-model", orgKeyValue)})
	provider, _, _, byo, err := executor.providerForRunSource(context.Background(), "org-1", "auto", "")
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	if !byo {
		t.Error("an org with its own config must report org credentials")
	}
	executor, _, _ = orgExecutor(t, nil)
	provider, _, _, byo, err = executor.providerForRunSource(context.Background(), "org-1", "auto", "")
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	if byo {
		t.Error("the platform environment must not report org credentials")
	}
}
