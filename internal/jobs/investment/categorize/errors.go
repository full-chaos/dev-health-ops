package categorize

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// llmErrorKind is llm/errors.py's exception hierarchy, collapsed to one Go
// error type with a Kind discriminator instead of a class per branch --
// callers switch on Kind rather than a type hierarchy, but the branches and
// their meanings match the Python docstring one for one.
type llmErrorKind int

const (
	llmErrorGeneric llmErrorKind = iota
	llmErrorAuth
	llmErrorModelNotFound
	llmErrorInvalidRequest
	llmErrorRateLimit
	llmErrorContextLength
	llmErrorServer
	llmErrorTimeout
	llmErrorTransport
	llmErrorOutput
)

// llmError is llm/errors.py's LLMError base class.
type llmError struct {
	kind       llmErrorKind
	message    string
	provider   string
	model      string
	retryAfter time.Duration // only meaningful for llmErrorRateLimit
	cause      error
}

func (e *llmError) Error() string {
	parts := []string{sanitizeMessage(e.message)}
	if e.provider != "" {
		parts = append(parts, "provider="+e.provider)
	}
	if e.model != "" {
		parts = append(parts, "model="+e.model)
	}
	if e.cause != nil {
		parts = append(parts, fmt.Sprintf("cause_type=%T", e.cause))
	}
	return strings.Join(parts, " | ")
}

func (e *llmError) Unwrap() error { return e.cause }

// secretPatterns replaces an ENUMERATED-LABEL approach (round 1-3's own
// history: "api_key" widened to "api key", then to no-punctuation, then
// "client_secret" added as its own alternative -- each fix closing one
// specific shape while the underlying design, a fixed list of exact
// compound labels, kept producing new gaps) with a STRUCTURAL one, per
// lane-4441's peer read of #2178 (2026-09-03): (b) an Authorization-header
// value is redacted as ONE unit (scheme word + credential, base64 padding
// included), not just its first space-separated token; (a) the label
// match is a case-insensitive SUBSTRING on a short list of credential
// words, not a whole compound label, so "webhook_secret" (containing
// "secret") is covered without needing its own alternative; (c)/(d) catch
// a credential by its own SHAPE -- a known provider key prefix, or any
// long opaque base64/hex-looking run -- independent of any label at all,
// since a raw provider error body is not guaranteed to label its own
// secret.
var secretPatterns = []*regexp.Regexp{
	// (b) Authorization schemes. Applied FIRST: redacting the whole
	// scheme+credential here means pattern (a) below, which also matches
	// the bare word "authorization", only ever sees the already-redacted
	// text on its second pass (harmless -- re-matching "authorization:
	// <redacted>" just redacts it again, a no-op).
	regexp.MustCompile(`(?i)(authorization[\s:=]+)['"]?[A-Za-z0-9+/_.~-]+=*(?:\s+[A-Za-z0-9+/_.~-]+=*)?`),

	// (b2) URI userinfo credentials -- a `scheme://user:password@host` DSN
	// (Postgres, Redis, ClickHouse, an internal gateway URL, ...) quoted
	// verbatim in a raw provider/proxy diagnostic. Neither (a)'s label
	// match nor (d)'s length-24 opaque-run heuristic reliably catches this:
	// a real DB password is often short and carries no "password"/"secret"
	// label at all -- the '://' + '@' SHAPE is the only reliable signal.
	// Percent-encoded/special-character passwords (found via lane-4978's
	// codex round 1, CHAOS-4978 #2189 P1) are covered because the userinfo
	// class excludes only whitespace/'/'/'@', not '%'/'$'/'!'/etc. Go's
	// RE2 engine has no lookahead, so the trailing '@' is consumed as part
	// of the match rather than merely asserted -- the redacted output is
	// `scheme://<redacted>host` (the '@' separator itself is dropped along
	// with the credential; harmless, since only the credential's secrecy
	// matters here, not preserving the original delimiter).
	regexp.MustCompile(`\b([a-zA-Z][a-zA-Z0-9+.-]*://)[^\s/@]+@`),

	// (a) Generic credential-label SUBSTRING match -- deliberately not a
	// whole-word/whole-label match, so "api_key", "x-api-key",
	// "webhook_secret", "client_secret" all match via the bare keyword
	// they contain, without an enumerated compound-label list. Any run
	// of whitespace/`:`/`=` as the separator (round 1/2's own fixes,
	// preserved).
	regexp.MustCompile(`(?i)((?:secret|token|key|password|passwd|credential|authorization|signature)[\s:=]+)['"]?[^'"\s,;}]+`),

	// (c) Known provider key-prefix shapes, independent of any label --
	// covers a raw key appearing in an error body with nothing labeling
	// it at all.
	regexp.MustCompile(`\b(?:sk-|sk_live_|sk_test_|ghp_|gho_|github_pat_|xox[a-zA-Z0-9]+-|AKIA|AIza)[A-Za-z0-9_-]{6,}`),

	// (d) Any long, opaque base64/hex-shaped run -- the broadest net,
	// deliberately: a credential shape this list has never seen before
	// still gets caught if it is long and opaque enough to plausibly BE
	// one. Accepted tradeoff, stated once here rather than at each
	// finding: this can also redact a long hash or a dash-free UUID;
	// over-redaction is the safe failure mode for a sanitizer whose job
	// is preventing a leak, under-redaction is the one that already
	// shipped three P1s.
	regexp.MustCompile(`\b[A-Za-z0-9+/_-]{24,}={0,2}\b`),
}

// sanitizeMessage ports errors.py's _sanitize_message: strip newlines,
// redact anything that looks like a credential, cap the length. Error
// messages here can end up in logs, and a raw provider exception can quote
// the request that produced it -- including the Authorization header.
func sanitizeMessage(message string) string {
	if message == "" {
		message = "LLM provider error"
	}
	text := strings.ReplaceAll(message, "\n", " ")
	text = strings.ReplaceAll(text, "\r", " ")
	text = strings.TrimSpace(text)
	for _, pattern := range secretPatterns {
		text = pattern.ReplaceAllStringFunc(text, func(match string) string {
			sub := pattern.FindStringSubmatch(match)
			if len(sub) > 1 && sub[1] != "" {
				return sub[1] + "<redacted>"
			}
			return "<redacted>"
		})
	}
	if len(text) > 500 {
		text = text[:500]
	}
	return text
}

var llmRetryableKinds = map[llmErrorKind]struct{}{
	llmErrorRateLimit: {},
	llmErrorServer:    {},
	llmErrorTimeout:   {},
	llmErrorTransport: {},
	llmErrorOutput:    {},
}

// isRetryable ports errors.py's is_retryable.
func isRetryable(err *llmError) bool {
	if err == nil {
		return false
	}
	_, ok := llmRetryableKinds[err.kind]
	return ok
}

const (
	baseDelaySeconds = 0.5
	maxDelaySeconds  = 30.0
)

// retryDelay ports errors.py's retry_delay: exponential backoff, 0-indexed
// attempt, capped at maxDelaySeconds.
func retryDelay(attempt int) time.Duration {
	delay := baseDelaySeconds * float64(int64(1)<<uint(attempt))
	if delay > maxDelaySeconds {
		delay = maxDelaySeconds
	}
	return time.Duration(delay * float64(time.Second))
}

// maxRetryAfterSeconds clamps a provider-supplied Retry-After so a hostile
// or buggy provider (`Retry-After: 86400`) cannot pin a worker for a day --
// errors.py's _MAX_RETRY_AFTER_SECONDS.
const maxRetryAfterSeconds = 60.0

func retryAfterFromHeader(header http.Header) time.Duration {
	raw := header.Get("Retry-After")
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(raw, 64); err == nil {
		if seconds < 0 {
			seconds = 0
		}
		if seconds > maxRetryAfterSeconds {
			seconds = maxRetryAfterSeconds
		}
		return time.Duration(seconds * float64(time.Second))
	}
	if when, err := http.ParseTime(raw); err == nil {
		remaining := time.Until(when).Seconds()
		if remaining < 0 {
			remaining = 0
		}
		if remaining > maxRetryAfterSeconds {
			remaining = maxRetryAfterSeconds
		}
		return time.Duration(remaining * float64(time.Second))
	}
	return 0
}

// classifyProviderError ports errors.py's classify_provider_error: inspect
// an HTTP status code and the raw error text for well-known patterns from
// the OpenAI/local-server SDKs, and wrap the result in the canonical kind.
// statusCode is 0 when no HTTP response was ever received (a transport-level
// failure); header may be nil.
func classifyProviderError(err error, statusCode int, header http.Header, provider, model string) *llmError {
	if err == nil {
		return nil
	}
	msgLower := strings.ToLower(err.Error())

	base := func(kind llmErrorKind, message string) *llmError {
		return &llmError{kind: kind, message: message, provider: provider, model: model, cause: err}
	}

	// codex round 3 (#2178, bigboy) P2: substring-matching a fixed word
	// list ("connection refused", "dns error", ...) missed Go's own
	// ordinary DNS failure text ("no such host"), so a transient DNS
	// blip was classified generic (non-retryable) instead of transient.
	// httpTransportError is ONLY ever constructed when the HTTP client's
	// own Do() call failed outright -- no response was received at all --
	// which structurally IS a transport failure regardless of the
	// underlying OS/network error's exact wording. Checking the TYPE
	// covers every such failure shape (DNS, TLS, connection refused,
	// anything else net/http can return) in one place, instead of
	// growing an ever-incomplete substring list.
	var transportErr *httpTransportError
	if errors.As(err, &transportErr) {
		return base(llmErrorTransport, "LLM provider endpoint could not be reached.")
	}

	switch {
	case containsAnySubstring(msgLower, "insufficient_quota", "current quota"):
		return base(llmErrorAuth, "LLM quota exhausted. Check provider billing/quota or use a different API key.")
	case containsAnySubstring(msgLower, "model_not_found", "model not found", "model does not exist"):
		return base(llmErrorModelNotFound, fmt.Sprintf("LLM model not found for provider %q using configured model %q.", provider, model))
	case containsAnySubstring(msgLower, "401", "invalid_api_key", "authentication", "unauthorized", "missing api key", "api key missing", "api_key is required"):
		return base(llmErrorAuth, "Invalid or missing LLM API key.")
	case statusCode == http.StatusTooManyRequests || containsAnySubstring(msgLower, "429", "rate_limit", "rate limit", "too many requests"):
		e := base(llmErrorRateLimit, "Transient LLM rate limit from provider.")
		if header != nil {
			e.retryAfter = retryAfterFromHeader(header)
		}
		return e
	case containsAnySubstring(msgLower, "context_length_exceeded", "maximum context length", "too many tokens", "input too long", "reduce your prompt"):
		return base(llmErrorContextLength, "LLM prompt exceeds the model context window.")
	case statusCode == http.StatusBadRequest || statusCode == http.StatusUnprocessableEntity ||
		containsAnySubstring(msgLower, "unsupported parameter", "unsupported value", "invalid parameter", "invalid_request_error", "invalid request"):
		return base(llmErrorInvalidRequest, "LLM provider rejected an unsupported request parameter.")
	case containsAnySubstring(msgLower, "timeout", "timed out"):
		return base(llmErrorTimeout, "LLM provider request timed out.")
	case containsAnySubstring(msgLower, "connection error", "connection refused", "network is unreachable", "name or service not known", "dns error", "tls error"):
		return base(llmErrorTransport, "LLM provider endpoint could not be reached.")
	case statusCode >= 500 || containsAnySubstring(msgLower, "500", "502", "503", "504", "server error", "internal error"):
		return base(llmErrorServer, "Transient LLM provider server error.")
	default:
		return base(llmErrorGeneric, err.Error())
	}
}

func containsAnySubstring(haystack string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(haystack, needle) {
			return true
		}
	}
	return false
}
