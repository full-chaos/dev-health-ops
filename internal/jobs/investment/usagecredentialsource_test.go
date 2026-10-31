package investment

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
)

type capturingBatch struct {
	recordingBatch
	rows *[][]any
}

func (b capturingBatch) Append(values ...any) error {
	*b.rows = append(*b.rows, values)
	return nil
}

type capturingConn struct{ rows *[][]any }

func (c capturingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errRefuseForTest
}

func (c capturingConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return capturingBatch{recordingBatch: recordingBatch{appended: new(int)}, rows: c.rows}, nil
}

// usageCredentialSource flushes one run's token usage and returns the
// credential_source column of the row the run wrote.
func usageCredentialSource(t *testing.T, served *ServedDecision) string {
	t.Helper()
	var rows [][]any
	writer, err := chwrite.NewWriter(capturingConn{rows: &rows})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := chquery.NewReader(capturingConn{rows: &rows})
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewMaterializer(reader, writer, categorize.MockProvider{}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if served != nil {
		m.SetServed(served)
	}
	if err := m.flushTokenUsage(context.Background(), tokenUsageConfig(), Stats{LLMCalls: 2, LLMInputTokens: 100, LLMOutputTokens: 7}); err != nil {
		t.Fatalf("flushTokenUsage: %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 11 {
		t.Fatalf("rows = %v", rows)
	}
	source, _ := rows[0][10].(string)
	return source
}

func TestAClefBYORunWritesItsUsageRowWithTheOrgCredentialSource(t *testing.T) {
	stub := newClefBYO(clefAnswers("clef"))
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = served.Close() }()
	if got := usageCredentialSource(t, served); got != chwrite.CredentialSourceOrg {
		t.Fatalf("credential_source = %q, want %q", got, chwrite.CredentialSourceOrg)
	}
}

func TestAChatBYORunWritesItsUsageRowWithTheOrgCredentialSource(t *testing.T) {
	served, err := newBYOSystemOne(&stubOrgModel{model: "org-model-1", reply: answerAllQuestions}, "openai", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = served.Close() }()
	if got := usageCredentialSource(t, served); got != chwrite.CredentialSourceOrg {
		t.Fatalf("credential_source = %q, want %q", got, chwrite.CredentialSourceOrg)
	}
}

func TestAPlatformServedRunWritesItsUsageRowWithThePlatformCredentialSource(t *testing.T) {
	served := newTestServed(t, newFakeJev(t, nil), &syncBuffer{})
	defer func() { _ = served.Close() }()
	if got := usageCredentialSource(t, served); got != chwrite.CredentialSourcePlatform {
		t.Fatalf("credential_source = %q, want %q", got, chwrite.CredentialSourcePlatform)
	}
}

func TestAGenerativeRunWritesItsUsageRowWithThePlatformCredentialSource(t *testing.T) {
	if got := usageCredentialSource(t, nil); got != chwrite.CredentialSourcePlatform {
		t.Fatalf("credential_source = %q, want %q", got, chwrite.CredentialSourcePlatform)
	}
}
