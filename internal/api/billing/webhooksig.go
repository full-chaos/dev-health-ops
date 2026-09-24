package billing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// stripeWebhookTolerance is stripe-python's Webhook.DEFAULT_TOLERANCE.
const stripeWebhookTolerance = 300

// errSignature is stripe-python's SignatureVerificationError: the webhook
// answers it with 400 "Invalid Stripe signature".
var errSignature = errors.New("billing: stripe signature verification failed")

// errWebhookCrash is any other exception stripe-python raises while it
// verifies a webhook (undecodable body, a signature it cannot compare):
// the route lets it escape, which is the bare 500.
var errWebhookCrash = errors.New("billing: stripe webhook verification raised")

// verifyStripeSignature is stripe-python's WebhookSignature.verify_header
// with the default tolerance, step for step:
//   - the body must decode as UTF-8 (else UnicodeDecodeError, the 500);
//   - an empty header is a signature error;
//   - the header splits on ",", each item on the first two "="; the first
//     "t" item's value goes through int() (anything int() refuses, or a
//     "t" item without "=", is a signature error); every "v1" item's value
//     is a signature;
//   - no v1 signature is a signature error;
//   - HMAC-SHA256 of "<int timestamp>.<body>" in lower hex is compared with
//     each signature in order; a non-ASCII signature reached before a match
//     makes hmac.compare_digest raise (the 500); no match is a signature
//     error;
//   - a timestamp older than now - 300 s is a signature error (a future one
//     is accepted).
//
// secret is never empty here: the route refuses an empty webhook secret
// before verifying (get_webhook_secret).
func verifyStripeSignature(payload []byte, header, secret string, now time.Time) error {
	if !utf8.Valid(payload) {
		return errWebhookCrash
	}
	if header == "" {
		return errSignature
	}
	// Starlette decodes header bytes as latin-1.
	header = latin1(header)
	type item struct {
		key, value string
		hasValue   bool
	}
	var items []item
	for _, part := range strings.Split(header, ",") {
		fields := strings.SplitN(part, "=", 3)
		entry := item{key: fields[0]}
		if len(fields) > 1 {
			entry.value, entry.hasValue = fields[1], true
		}
		items = append(items, entry)
	}
	var timestamp *big.Int
	found := false
	for _, entry := range items {
		if entry.key != "t" {
			continue
		}
		if !entry.hasValue {
			return errSignature
		}
		if !found {
			parsed, err := pythonparity.ParseInt(entry.value)
			if err != nil {
				return errSignature
			}
			timestamp, found = parsed, true
		}
	}
	if !found {
		return errSignature
	}
	var signatures []string
	for _, entry := range items {
		if entry.key == "v1" {
			if !entry.hasValue {
				return errSignature
			}
			signatures = append(signatures, entry.value)
		}
	}
	if len(signatures) == 0 {
		return errSignature
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp.String() + "."))
	mac.Write(payload)
	expected := hex.EncodeToString(mac.Sum(nil))
	matched := false
	for _, signature := range signatures {
		for _, r := range signature {
			if r >= 0x80 {
				return errWebhookCrash
			}
		}
		if hmac.Equal([]byte(expected), []byte(signature)) {
			matched = true
			break
		}
	}
	if !matched {
		return errSignature
	}
	// timestamp < time.time() - tolerance, with time.time() a float: at
	// the whole-second boundary a fraction of a second past it already
	// counts as too old.
	oldest := big.NewInt(now.Unix() - stripeWebhookTolerance)
	switch timestamp.Cmp(oldest) {
	case -1:
		return errSignature
	case 0:
		if now.Nanosecond() > 0 {
			return errSignature
		}
	}
	return nil
}

// latin1 is a header value as Starlette hands it over: each byte one code
// point (latin-1).
func latin1(raw string) string {
	runes := make([]rune, len(raw))
	for index := 0; index < len(raw); index++ {
		runes[index] = rune(raw[index])
	}
	return string(runes)
}
