package remaining

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// stepFaultConn fails the first of PrepareBatch / Append / Send that it is told
// to, with an error carrying hostile text the cause must never repeat.
type stepFaultConn struct {
	failAt string // "prepare", "append" or "send"
}

const stepFaultText = "tenant-acme password=hunter2 SELECT * FROM work_items"

func (c *stepFaultConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errors.New("unused")
}

func (c *stepFaultConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	if c.failAt == "prepare" {
		return nil, errors.New(stepFaultText)
	}
	return &stepFaultBatch{failAt: c.failAt}, nil
}

type stepFaultBatch struct {
	attributionSendFailingBatch
	failAt string
}

func (batch *stepFaultBatch) Append(...any) error {
	if batch.failAt == "append" {
		return errors.New(stepFaultText)
	}
	return nil
}

func (batch *stepFaultBatch) Send() error {
	if batch.failAt == "send" {
		return errors.New(stepFaultText)
	}
	return nil
}

// TestEveryWriterSiteNamesItsOwnStep pins WHICH step each of the 9 writer sites
// names. The expected labels are written out here, not read from stepcause, so
// a call site that passes a neighbouring step (Send -> Prepare, org-wide marker
// -> scoped marker) fails: a wrong label points an operator at the wrong
// statement, and no other test would notice.
func TestEveryWriterSiteNamesItsOwnStep(t *testing.T) {
	now := time.Now().UTC()
	row := WorkItemAttributionRow{WorkItemID: "wi-1", Provider: "github", Source: "codeowners", IsPrimary: 1,
		Confidence: "high", Evidence: "{}", ComputedAt: now, OrgID: "org-42"}
	producer := WorkItemAttributionProducer{Writer: WorkItemAttributionWriterBackstop, RunID: "run-steps"}
	writes := map[string]func(*WorkItemAttributionClickHouseWriter) error{
		"attributions": func(w *WorkItemAttributionClickHouseWriter) error {
			_, err := w.WriteAttributions(context.Background(), producer, []WorkItemAttributionRow{row})
			return err
		},
		"run marker": func(w *WorkItemAttributionClickHouseWriter) error {
			return w.WriteAttributionRun(context.Background(),
				WorkItemAttributionRunRecord{OrgID: "org-42", RunID: "run-steps", CompletedAt: now})
		},
		"scoped markers": func(w *WorkItemAttributionClickHouseWriter) error {
			return w.WriteScopedAttributionRuns(context.Background(), []WorkItemAttributionScopedRunRecord{
				{OrgID: "org-42", ScopeKind: "repo", ScopeID: "r1", RunID: "run-steps", CompletedAt: now}})
		},
	}
	want := map[string]map[string]string{
		"attributions": {
			"prepare": "step=prepare work_item_team_attributions batch",
			"append":  "step=append work_item_team_attributions row",
			"send":    "step=send work_item_team_attributions batch",
		},
		"run marker": {
			"prepare": "step=prepare work_item_attribution_backstop_runs batch",
			"append":  "step=append work_item_attribution_backstop_runs row",
			"send":    "step=send work_item_attribution_backstop_runs batch",
		},
		"scoped markers": {
			"prepare": "step=prepare work_item_attribution_backstop_scoped_runs batch",
			"append":  "step=append work_item_attribution_backstop_scoped_runs row",
			"send":    "step=send work_item_attribution_backstop_scoped_runs batch",
		},
	}
	for site, write := range writes {
		for failAt, wantCause := range want[site] {
			t.Run(site+"/"+failAt, func(t *testing.T) {
				writer, err := NewWorkItemAttributionClickHouseWriter(&stepFaultConn{failAt: failAt})
				if err != nil {
					t.Fatal(err)
				}
				err = write(writer)
				if err == nil {
					t.Fatal("the forced failure did not surface")
				}
				cause, ok := jobruntime.SafeCause(err)
				if !ok || cause != wantCause {
					t.Fatalf("cause = %q ok=%v, want %q", cause, ok, wantCause)
				}
			})
		}
	}
}
