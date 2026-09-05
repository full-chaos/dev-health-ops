package icfinalize

import "sync"

// stringInputs records whether FloatValue ever received a string, and an
// example if so.
//
// This exists to answer a MEASUREMENT rather than to change behaviour: the
// Python `_float_value` has a string branch that parses via Python `float()`,
// and porting it faithfully would mean adding a Python-float() primitive to
// internal/pythonparity, which does not currently have one. That is only worth
// doing if the branch is actually reachable from the ClickHouse loader, whose
// SELECT returns typed columns.
//
// The alternative -- writing the primitive speculatively -- would add a parity
// surface nobody calls, and a parity helper that is never exercised is a
// liability: it looks like protection and has never been proven against the
// interpreter.
//
// This is a POSITIVE CONTROL for the corpus, not production telemetry. It is
// deliberately not wired to the observer.
type stringInputRecorder struct {
	mu      sync.Mutex
	seen    bool
	example string
}

var stringInputs stringInputRecorder

func (recorder *stringInputRecorder) observe(value string) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if !recorder.seen {
		recorder.seen = true
		recorder.example = value
	}
}

// StringInputSeen reports whether FloatValue has been handed a string, with
// the first example. A corpus run that returns false has PROVEN the branch
// unreachable for that corpus -- which is what licenses not writing the
// primitive, and is a stronger statement than "I did not see one".
func StringInputSeen() (bool, string) {
	stringInputs.mu.Lock()
	defer stringInputs.mu.Unlock()
	return stringInputs.seen, stringInputs.example
}

// ResetStringInputSeen exists so a test can assert the recorder DETECTS a
// string, not merely that it reports false. Without it, "no strings seen"
// could mean the recorder is broken.
func ResetStringInputSeen() {
	stringInputs.mu.Lock()
	defer stringInputs.mu.Unlock()
	stringInputs.seen = false
	stringInputs.example = ""
}
