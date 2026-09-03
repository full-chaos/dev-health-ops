package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrorSurface names the error.v1 wire contract.
const ErrorSurface = "error.v1"

// MaxClockSkew is how far ahead of the reader's clock an ErrorEnvelope's
// OccurredAt may sit before the envelope is refused.
//
// Five minutes is the conventional allowance for unsynchronised hosts. The
// bound is ONE-DIRECTIONAL on purpose: an error stamped in the PAST is normal
// (queueing, retries, slow logs) and is never checked, while one stamped in
// the future cannot be explained by anything but skew or a wrong clock, and it
// reorders an audit trail silently once stored.
//
// A client policy, not a contract term. A deployment may reasonably pick a
// different tolerance; what it may not do is skip the check, because "no
// bound" makes the timestamp unusable for ordering.
const MaxClockSkew = 5 * time.Minute

// transientStatuses are the statuses TRD section 18 defines as transient, and
// therefore the ones the schema's if/then requires retry_after_seconds on.
//
// Duplicated from the schema only so IsTransient can answer without re-reading
// it. TestTransientStatusesMatchTheSchema asserts the two agree, so this
// cannot quietly become a second source of truth.
var transientStatuses = map[int]bool{429: true, 503: true}

// ErrorEnvelope is one error.v1 document, validated and parsed.
type ErrorEnvelope struct {
	Status            int       `json:"status"`
	ReasonCode        string    `json:"reason_code"`
	RequestID         string    `json:"request_id"`
	OccurredAt        Timestamp `json:"occurred_at"`
	RetryAfterSeconds *int      `json:"retry_after_seconds,omitempty"`
}

// IsTransient reports whether the caller may usefully retry.
//
// Reads the STATUS rather than the presence of RetryAfterSeconds. Those
// coincide today only because the schema's conditional makes them coincide,
// and keying off the field would silently invert this property if that
// conditional were ever relaxed.
func (e ErrorEnvelope) IsTransient() bool { return transientStatuses[e.Status] }

// refuseDuplicateMembers rejects a document containing the same object member
// twice, anywhere in the tree.
//
// WHY THIS RUNS BEFORE VALIDATION, AND WHY VALIDATION CANNOT DO IT. Every JSON
// parser collapses duplicate members to one survivor -- Go and Python both keep
// the LAST -- so by the time a validator is handed a decoded object the earlier
// value is gone. The schema then validates the survivor and reports success
// about a document whose bytes contained something else.
//
// That defeats the subtractive contract directly. `reason_code`'s pattern makes
// prose and addresses unrepresentable, but
//
//	{"reason_code":"credential_for_bob@example.com","reason_code":"grant_absent"}
//
// validates, because only `grant_absent` survives to be checked. The disclosure
// TRD section 18 forbids is sitting in the response body, and this is a WIRE
// contract -- the guarantee is about bytes, not about the object they decode to.
// Found by codex round 2.
//
// RFC 8259 permits duplicate names and says behaviour is unpredictable; that is
// exactly why a security contract refuses them rather than inheriting whichever
// survivor the parser picked.
func refuseDuplicateMembers(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := scanJSONValue(dec); err != nil {
		return err
	}
	// Trailing content is not this function's concern, but a second top-level
	// value means the caller was handed something other than one document.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%s: trailing content after the top-level value", ErrorSurface)
	}
	return nil
}

// scanJSONValue consumes exactly one value, recursing so that a duplicate
// nested inside an array or a sub-object is caught too. Written as a walk
// rather than a decode because the whole point is to see what a decode would
// have thrown away.
func scanJSONValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("%s: document is not valid JSON: %w", ErrorSurface, err)
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil // a scalar; nothing to descend into
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return fmt.Errorf("%s: document is not valid JSON: %w", ErrorSurface, err)
			}
			key, ok := keyTok.(string)
			if !ok {
				return fmt.Errorf("%s: object member name is not a string", ErrorSurface)
			}
			if seen[key] {
				return fmt.Errorf(
					"%s: duplicate object member %q. Parsers keep the last value, so an "+
						"earlier one would never reach validation -- a document can carry a "+
						"forbidden value on the wire and still validate",
					ErrorSurface, key)
			}
			seen[key] = true
			if err := scanJSONValue(dec); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := scanJSONValue(dec); err != nil {
				return err
			}
		}
	}
	if _, err := dec.Token(); err != nil { // the closing delimiter
		return fmt.Errorf("%s: document is not valid JSON: %w", ErrorSurface, err)
	}
	return nil
}

// utcInstantInRange refuses a timestamp whose UTC-normalised instant falls
// outside 0001-01-01..9999-12-31.
//
// COMPONENT BOUNDS DO NOT BOUND THE COMPOSITION. The schema pattern bounds year
// 0001-9999 of the LOCAL fields, but the instant is (fields x offset) and the
// offset can carry it outside the range the fields satisfy:
//
//	0001-01-01T00:00:00+23:59  ->  UTC year 0000
//	9999-12-31T23:59:59-23:59  ->  UTC year 10000
//
// Both are lexically valid under the pattern; a regex cannot see it, because
// the composition is arithmetic rather than lexical. Found by lane-auth-wave1
// enumerating the class after codex round 3.
//
// GO DOES NOT NEED THIS AND HAS IT ANYWAY. time.Time represents both instants
// without complaint, so nothing here would fail. Python's datetime cannot, and
// raised OverflowError on the caller's first astimezone -- an uncaught stdlib
// exception on a document the client had accepted. This check exists so the two
// clients agree about which documents are acceptable, which is the whole point
// of a wire contract: a Go service must not emit an envelope the Python client
// will choke on, and must not accept one a Python peer would refuse.
//
// It runs BEFORE the clock-skew check, matching Python. The high boundary is
// also far-future, so the skew bound would refuse it first -- but that is a
// COINCIDENCE: the skew bound is deliberately one-directional, and making it
// symmetric would silently reopen the 9999 case with no test failing.
func utcInstantInRange(moment time.Time) error {
	year := moment.UTC().Year()
	if year < 1 || year > 9999 {
		return fmt.Errorf(
			"%s: occurred_at %s normalises to UTC year %d, outside "+
				"0001-01-01..9999-12-31. The schema bounds the local fields; the offset "+
				"carries the instant past that bound, which a pattern cannot express",
			ErrorSurface, moment.Format(time.RFC3339), year)
	}
	return nil
}

// ParseErrorEnvelope validates and parses an error.v1 document received on an
// HTTP response whose status line said httpStatus.
//
// httpStatus is a REQUIRED parameter rather than an option: making it optional
// would let every caller skip the one check the schema cannot perform, and a
// check that is easy to omit is one that will be omitted. The schema is handed
// a document, never an exchange, so it cannot see the response line at all.
//
// now is injectable so the skew check is testable without mocking a clock.
// A zero now means time.Now().UTC().
func ParseErrorEnvelope(root string, raw []byte, httpStatus int, now time.Time) (ErrorEnvelope, error) {
	// BEFORE decoding: a decode is what destroys the evidence.
	if err := refuseDuplicateMembers(raw); err != nil {
		return ErrorEnvelope{}, err
	}

	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		return ErrorEnvelope{}, fmt.Errorf("%s: document is not valid JSON: %w", ErrorSurface, err)
	}
	if err := Validate(root, ErrorSurface, document); err != nil {
		return ErrorEnvelope{}, err
	}

	var envelope ErrorEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return ErrorEnvelope{}, fmt.Errorf("%s: decoding validated document: %w", ErrorSurface, err)
	}

	if envelope.Status != httpStatus {
		return ErrorEnvelope{}, fmt.Errorf(
			"%s: envelope status %d disagrees with the HTTP status %d it arrived on; "+
				"refusing rather than choosing one -- trusting the body would let a server "+
				"contradict its own response line, and on a 404 that discloses the existence "+
				"the status withholds",
			ErrorSurface, envelope.Status, httpStatus)
	}

	// Range check BEFORE the skew check -- see utcInstantInRange on why the
	// order is load-bearing rather than incidental.
	if err := utcInstantInRange(envelope.OccurredAt.Time); err != nil {
		return ErrorEnvelope{}, err
	}

	if now.IsZero() {
		now = time.Now().UTC()
	}
	if envelope.OccurredAt.After(now.Add(MaxClockSkew)) {
		return ErrorEnvelope{}, fmt.Errorf(
			"%s: occurred_at %s is more than %s ahead of %s; a future timestamp "+
				"reorders an audit trail silently",
			ErrorSurface, envelope.OccurredAt.Format(time.RFC3339Nano), MaxClockSkew,
			now.Format(time.RFC3339Nano))
	}

	return envelope, nil
}
