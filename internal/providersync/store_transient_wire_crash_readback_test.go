package providersync

import (
	"strings"
	"testing"
)

// The COMMIT reply is lost (57P01) and the first read-back is itself cut by a
// connection loss: the read is retried and finds the commit. 57P02 must behave
// like 57P01 here.
func TestCompleteReadBackRetriesAConnectionLossRead(t *testing.T) {
	for _, code := range []string{"57P02", "57P01"} {
		t.Run(code, func(t *testing.T) {
			w := &storeWire{faults: []*wireFault{
				{step: "commit", code: "57P01", times: 1},
				{step: "read_back", code: code, times: 1},
			}}
			run := runWireWrite(t, w, "complete", nil)
			if run.err != nil || w.updates != 1 || w.commits != 1 || w.reads != 2 {
				t.Fatalf("SQLSTATE %s at the read-back: err=%v updates=%d commits=%d reads=%d, want nil, 1, 1, 2 (the read is retried)",
					code, run.err, w.updates, w.commits, w.reads)
			}
		})
	}
}

// The cell reads the captured log, which the wire runner installs as the default
// logger, so it does not run in parallel.
// A connection loss at the needs-attention savepoint of a failing unit retries
// the whole terminalization instead of continuing on the lost connection.
func TestFailNeedsAttentionSavepointRetriesAConnectionLoss(t *testing.T) {
	for _, code := range []string{"57P02", "57P01"} {
		t.Run(code, func(t *testing.T) {
			w := &storeWire{faults: []*wireFault{{step: "savepoint", code: code, times: 1}}}
			run := runWireWrite(t, w, "fail", nil)
			stamped := strings.Count(run.logs, "providersync.needs_attention_stamped")
			if run.err != nil || w.updates != 2 || w.commits != 1 || stamped != 1 {
				t.Fatalf("SQLSTATE %s at the savepoint: err=%v updates=%d commits=%d stamped=%d, want nil, 2, 1, 1 (retried transaction)",
					code, run.err, w.updates, w.commits, stamped)
			}
		})
	}
}
