package joboutbox

import "github.com/full-chaos/dev-health-ops/internal/api/pyjson"

// StoredArgsText renders a canonical job envelope as the text
// worker_job_outbox.args holds: Python's json.dumps default text (", " and
// ": " separators, ensure_ascii, one line), with the envelope's key order kept.
// The Python producer stores the same envelope through SQLAlchemy's JSON type,
// which writes exactly that text, so a row reads the same whichever runtime
// produced it.
//
// It is the only place that spells the stored text: every Go writer of the
// column calls it, and pyjson is the one JSON writer behind it. The hash is a
// separate matter and stays over the canonical bytes (payload_hash is
// sha256(canonical) on both runtimes), so the stored text is never hashed and
// the relay, which decodes args and re-canonicalizes before comparing the hash,
// accepts either spelling.
func StoredArgsText(canonical []byte) (string, error) {
	value, err := pyjson.Decode(canonical)
	if err != nil {
		return "", err
	}
	return pyjson.Dumps(value)
}
