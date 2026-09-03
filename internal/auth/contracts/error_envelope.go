package contracts

import (
	"encoding/json"
	"fmt"
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
