package operational

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrMalformedAttributes is a permanent, data-shaped condition: a stored
// `billing_notifications.attributes` value that cannot be coerced to the type
// the email renderer needs. A stored value does not change between retries, so
// this is a permanent drop rather than a transient failure -- the same
// classification Python's `_PermanentBillingDrop("malformed_notification_
// attributes")` gave it.
var ErrMalformedAttributes = errors.New("operational billing notification attributes are malformed")

// DecodeBillingAttributes ports `system_ops.send_billing_notification`'s
// attribute coercion block: each field is read with the same default and the
// same `int(...)`/`str(...)` conversion Python applied, so an existing stored
// row renders identically under Go.
//
// Defaults (absent key) mirror Python exactly: amount_cents 0, currency "usd",
// attempt_count 1, days_remaining 0, and "" for the string fields.
func DecodeBillingAttributes(raw []byte) (BillingAttributes, error) {
	attributes := BillingAttributes{Currency: "usd", AttemptCount: 1}
	if len(raw) == 0 {
		// An absent/NULL attributes column reads as "no keys present", which
		// is exactly the all-defaults case above -- not an error. Python's
		// `dict(notification.attributes)` on an empty dict behaved the same.
		return attributes, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return BillingAttributes{}, fmt.Errorf("%w: not a JSON object: %v", ErrMalformedAttributes, err)
	}
	var err error
	if attributes.AmountCents, err = intField(fields, "amount_cents", 0); err != nil {
		return BillingAttributes{}, err
	}
	if attributes.AttemptCount, err = intField(fields, "attempt_count", 1); err != nil {
		return BillingAttributes{}, err
	}
	if attributes.DaysRemaining, err = intField(fields, "days_remaining", 0); err != nil {
		return BillingAttributes{}, err
	}
	if attributes.Currency, err = stringField(fields, "currency", "usd"); err != nil {
		return BillingAttributes{}, err
	}
	if attributes.InvoiceURL, err = stringField(fields, "invoice_url", ""); err != nil {
		return BillingAttributes{}, err
	}
	if attributes.OldTier, err = stringField(fields, "old_tier", ""); err != nil {
		return BillingAttributes{}, err
	}
	if attributes.NewTier, err = stringField(fields, "new_tier", ""); err != nil {
		return BillingAttributes{}, err
	}
	if attributes.Tier, err = stringField(fields, "tier", ""); err != nil {
		return BillingAttributes{}, err
	}
	if attributes.TrialEndDate, err = stringField(fields, "trial_end_date", ""); err != nil {
		return BillingAttributes{}, err
	}
	return attributes, nil
}

// intField ports `int(attributes.get(name, default))`, which accepts a JSON
// number (truncating toward zero, as int() does for a float), a decimal string
// (int("12")), and a boolean (int(True) == 1). Anything else raised TypeError
// or ValueError in Python and is malformed here.
func intField(fields map[string]json.RawMessage, name string, fallback int64) (int64, error) {
	raw, present := fields[name]
	if !present {
		return fallback, nil
	}
	trimmed := strings.TrimSpace(string(raw))
	switch trimmed {
	case "true":
		return 1, nil
	case "false":
		return 0, nil
	case "null":
		// Python: int(None) raises TypeError.
		return 0, fmt.Errorf("%w: %s is null", ErrMalformedAttributes, name)
	}
	if strings.HasPrefix(trimmed, `"`) {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, fmt.Errorf("%w: %s is not decodable: %v", ErrMalformedAttributes, name, err)
		}
		// Python's int(str) tolerates surrounding whitespace and a sign, and
		// rejects everything else -- including a decimal point.
		value, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %s is not an integer string", ErrMalformedAttributes, name)
		}
		return value, nil
	}
	if value, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return value, nil
	}
	// A JSON float: int() truncates toward zero.
	asFloat, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s is not numeric", ErrMalformedAttributes, name)
	}
	return int64(asFloat), nil
}

// stringField ports `str(attributes.get(name, default))`. A JSON string is
// itself; a number keeps its literal text; a boolean renders Python-style
// ("True"/"False"); null renders "None", which is what Python's str(None)
// produced. The producer only ever writes strings and integers here -- the
// other arms exist so a hand-edited row renders the way it used to rather
// than diverging silently.
func stringField(fields map[string]json.RawMessage, name string, fallback string) (string, error) {
	raw, present := fields[name]
	if !present {
		return fallback, nil
	}
	trimmed := strings.TrimSpace(string(raw))
	switch trimmed {
	case "true":
		return "True", nil
	case "false":
		return "False", nil
	case "null":
		return "None", nil
	}
	if strings.HasPrefix(trimmed, `"`) {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return "", fmt.Errorf("%w: %s is not decodable: %v", ErrMalformedAttributes, name, err)
		}
		return text, nil
	}
	if strings.HasPrefix(trimmed, "[") || strings.HasPrefix(trimmed, "{") {
		// Python would str() a list/dict into its repr, which no template
		// should ever want. Reject rather than invent a rendering.
		return "", fmt.Errorf("%w: %s is a composite value", ErrMalformedAttributes, name)
	}
	return trimmed, nil
}
