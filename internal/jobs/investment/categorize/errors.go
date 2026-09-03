package categorize

import (
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

var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`sk-[A-Za-z0-9_-]{8,}`),
	// codex round 1 (#2178) P1: "bearer\s+" alone missed a colon/equals
	// separator ("Bearer: <token>", "Bearer=<token>") -- widened to any run
	// of whitespace/`:`/`=` between the word and the token.
	regexp.MustCompile(`(?i)(bearer[\s:=]+)[A-Za-z0-9._~+/=-]{8,}`),
	// codex round 1 (#2178) P1: "api[_-]?key" only allowed a SINGLE
	// underscore/hyphen between "api" and "key", so "api key: <secret>"
	// (a literal space, as a real 403 body used) reached this pattern
	// unredacted. Widened to any run of whitespace/underscore/hyphen.
	// codex round 2 (#2178, bigboy) P1: the label/value separator itself
	// still REQUIRED a literal `:`/`=` ("\s*[:=]\s*"), so a body reading
	// "api key <secret>" -- whitespace only, no punctuation -- still
	// passed through. Widened the separator to "any run of
	// whitespace/`:`/`=`" so a bare space alone is sufficient.
	regexp.MustCompile(`(?i)((?:api[\s_-]*key|authorization|x-api-key|token)[\s:=]+)['"]?[^'"\s,;}]+`),
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
