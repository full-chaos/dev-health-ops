package pythonparity

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// comparePayload reports whether the frozen and freshly-rendered documents agree
// on the fields that carry DATA, ignoring everything else in the document.
//
// # WHY THE GUARDS NO LONGER COMPARE THE WHOLE FILE
//
// They used to, and it produced a false alarm with a confidently wrong
// diagnosis. A fixture froze `sys.version`, which embeds the build string, so a
// macOS-generated document could never match a Linux runner: the guard reported
// "has ROTTED", pointed the reader at recommendations/loader.py, and nothing had
// rotted. Recording the bare version instead narrowed the window without closing
// it, because the live interpreter is UNPINNED -- parityLivePython takes
// $PYTHON, else whatever `python3` resolves to -- so the next CPython patch
// reproduces the same incident on CPython's release schedule.
//
// Removing the metadata fixed the instance. Comparing the payload fixes the
// CLASS: a future field added to a document for provenance cannot re-create the
// tripwire, because nothing outside the named payload is looked at.
//
// The line worth keeping, from lane-ci-flakes: a frozen environment value is a
// DEFECT when the environment can drift without a decision, and a FEATURE when
// it cannot. This package holds one of each. `python_version` drifts on
// CPython's schedule with nobody deciding anything, so it lives outside the
// comparison. `clickhouse_connect_version_measured` in the ClickHouse decoder
// golden can only change when someone deliberately bumps a pinned requirement,
// and at that moment a decoder golden SHOULD demand re-verification -- so that
// one stays inside its whole-document comparison, deliberately.
//
// # WHY RawMessage RATHER THAN DECODE-AND-COMPARE
//
// The payload is compared as RAW BYTES, not as re-serialised values. Decoding
// into Go types and comparing those would silently forgive a formatting change,
// a duplicated key, or a number that round-trips differently -- and this is a
// BYTE-parity package, where those are exactly the differences that matter.
// json.RawMessage keeps the original bytes of each field, so the comparison
// stays as strict as the old whole-file one everywhere it still applies.
func comparePayload(frozen, rendered []byte, fields ...string) error {
	var frozenDoc, renderedDoc map[string]json.RawMessage
	if err := json.Unmarshal(frozen, &frozenDoc); err != nil {
		return fmt.Errorf("parse frozen document: %w", err)
	}
	if err := json.Unmarshal(rendered, &renderedDoc); err != nil {
		return fmt.Errorf("parse rendered document: %w", err)
	}

	for _, field := range fields {
		frozenValue, frozenPresent := frozenDoc[field]
		renderedValue, renderedPresent := renderedDoc[field]

		// A missing payload field is a failure, not a vacuous pass. Without
		// this, renaming `cases` would leave both sides absent, every named
		// field would compare equal by not existing, and the guard would go
		// green over a document it no longer understands.
		if !frozenPresent || !renderedPresent {
			return fmt.Errorf(
				"payload field %q missing (frozen present=%v, rendered present=%v) -- "+
					"the document shape changed; the guard cannot compare what it "+
					"cannot find, and must not pass by failing to look",
				field, frozenPresent, renderedPresent)
		}
		if !bytes.Equal(frozenValue, renderedValue) {
			return fmt.Errorf("payload field %q differs between frozen and live", field)
		}
	}
	return nil
}
