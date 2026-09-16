package remaining

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

// TestWriteAttributionsRefusesAnUnidentifiedProducer pins the fail-closed
// choice. work_item_team_attributions has three producers whose rows are
// otherwise identical, so writer/run_id are the only way to attribute a
// stored row to the path that wrote it. A batch that names no producer, or
// names a path outside the shared vocabulary, would put rows in the table
// that the discriminator cannot explain -- the exact condition it exists to
// remove. The write is refused before any batch is prepared.
func TestWriteAttributionsRefusesAnUnidentifiedProducer(t *testing.T) {
	rows := []WorkItemAttributionRow{
		{WorkItemID: "wi-1", Provider: "github", Source: "repo_ownership", IsPrimary: 1,
			Confidence: "high", Evidence: "{}", ComputedAt: time.Now().UTC(), OrgID: "org-42"},
	}
	cases := []struct {
		name     string
		producer WorkItemAttributionProducer
	}{
		{"no producer at all", WorkItemAttributionProducer{}},
		{"run id without a writer", WorkItemAttributionProducer{RunID: "run-1"}},
		{"writer without a run id", WorkItemAttributionProducer{Writer: WorkItemAttributionWriterDaily}},
		{"blank run id", WorkItemAttributionProducer{Writer: WorkItemAttributionWriterDaily, RunID: "  "}},
		{"a writer name outside the shared vocabulary",
			WorkItemAttributionProducer{Writer: "daily-metrics", RunID: "run-1"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			conn := &attributionSendFailingConn{}
			writer, err := NewWorkItemAttributionClickHouseWriter(conn)
			if err != nil {
				t.Fatal(err)
			}
			written, err := writer.WriteAttributions(context.Background(), testCase.producer, rows)
			if !errors.Is(err, ErrWorkItemAttributionProducerUnidentified) {
				t.Fatalf("err = %v, want ErrWorkItemAttributionProducerUnidentified -- a batch "+
					"whose producer cannot be named must not reach the table", err)
			}
			if written != 0 {
				t.Fatalf("written=%d, want 0 -- the refusal happens before any row is appended", written)
			}
		})
	}
}

// TestWriteAttributionsRefusesAnUnidentifiedProducerOnAnEmptyBatch pins that
// the producer check runs BEFORE the empty-rows shortcut. A caller that
// forgot its producer is broken whether or not this particular run happened
// to resolve zero rows, and letting the empty case through would hide the
// bug until the first run that had rows to write.
func TestWriteAttributionsRefusesAnUnidentifiedProducerOnAnEmptyBatch(t *testing.T) {
	conn := &attributionSendFailingConn{}
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteAttributions(
		context.Background(), WorkItemAttributionProducer{}, nil,
	); !errors.Is(err, ErrWorkItemAttributionProducerUnidentified) {
		t.Fatalf("err = %v, want ErrWorkItemAttributionProducerUnidentified on an empty batch", err)
	}
}

// TestProducerAcceptsEveryWriterInTheSharedVocabulary pins that this
// package's aliases and the shared list agree. If a fourth path is added to
// workitemcontract and this package's Validate stops accepting one of them,
// that path's writes would be refused at runtime rather than at build time.
func TestProducerAcceptsEveryWriterInTheSharedVocabulary(t *testing.T) {
	for _, name := range workitemcontract.AttributionWriters() {
		producer := WorkItemAttributionProducer{Writer: name, RunID: "run-1"}
		if err := producer.Validate(); err != nil {
			t.Fatalf("Validate(writer=%q) = %v, want nil -- every name in the shared "+
				"vocabulary must be writable", name, err)
		}
	}
	for _, alias := range []string{WorkItemAttributionWriterDaily, WorkItemAttributionWriterBackstop} {
		producer := WorkItemAttributionProducer{Writer: alias, RunID: "run-1"}
		if err := producer.Validate(); err != nil {
			t.Fatalf("Validate(alias=%q) = %v, want nil", alias, err)
		}
	}
}
