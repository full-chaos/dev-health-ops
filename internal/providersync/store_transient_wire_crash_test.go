package providersync

import (
	"testing"
)

// A server crash shutdown (SQLSTATE 57P02) is a lost connection like 57P01 and
// 57P03, which the store already retries in place. Complete is the write.
func TestCompleteRetriesAConnectionLossRefusalInPlace(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		code  string
		retry bool
	}{
		{"57P02 crash shutdown", "57P02", true},
		{"57P01 admin shutdown (control)", "57P01", true},
		{"28P01 authentication (control, stays final)", "28P01", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			w := &storeWire{faults: []*wireFault{{step: "unit_update", code: test.code, times: 1}}}
			run := runWireWrite(t, w, "complete", nil)
			if test.retry && (run.err != nil || w.updates != 2 || w.commits != 1) {
				t.Fatalf("SQLSTATE %s at the unit UPDATE: err=%v updates=%d commits=%d, want nil, 2, 1 (retried in place)",
					test.code, run.err, w.updates, w.commits)
			}
			if !test.retry && (run.err == nil || w.updates != 1 || w.commits != 0) {
				t.Fatalf("SQLSTATE %s at the unit UPDATE: err=%v updates=%d commits=%d, want an error, 1, 0 (final, no retry)",
					test.code, run.err, w.updates, w.commits)
			}
		})
	}
}

// The write committed and the connection was lost at COMMIT with 57P02: the
// write is read back and reported as done, once, like the same loss with 57P01.
func TestCompleteWithAConnectionLossAtCommitIsReadBack(t *testing.T) {
	t.Parallel()
	for _, code := range []string{"57P02", "57P01"} {
		t.Run(code, func(t *testing.T) {
			t.Parallel()
			w := &storeWire{faults: []*wireFault{{step: "commit", code: code, times: 1}}}
			run := runWireWrite(t, w, "complete", nil)
			if run.err != nil || w.updates != 1 || w.commits != 1 || w.reads != 1 {
				t.Fatalf("SQLSTATE %s at COMMIT: err=%v updates=%d commits=%d reads=%d, want nil, 1, 1, 1 (read back once, not written twice)",
					code, run.err, w.updates, w.commits, w.reads)
			}
		})
	}
}
