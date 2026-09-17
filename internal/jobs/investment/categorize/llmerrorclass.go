package categorize

import "errors"

// LLMErrorClass names the only distinctions an HTTP caller draws between
// this package's LLM failures. llmErrorKind (errors.go) stays unexported --
// it carries llm/errors.py's full ten-branch hierarchy, which a caller
// outside this package has no use for -- while api/main.py's
// _http_exception_from_llm_error only ever asks three questions of an
// LLMError: is it an auth failure, a rate limit, or a server error. This
// type is those three questions plus "none of them", and nothing more.
type LLMErrorClass int

const (
	// LLMErrorClassOther is every LLMError that is not one of the three
	// below -- model-not-found, invalid-request, context-length, timeout,
	// transport, output and the bare generic error all land here, matching
	// _http_exception_from_llm_error's own trailing `return` for any
	// LLMError its three isinstance checks miss. Python's hierarchy is flat
	// (every class derives directly from LLMError), so there is no subclass
	// relationship that would route one of these to a different branch.
	LLMErrorClassOther LLMErrorClass = iota
	// LLMErrorClassAuth is llm/errors.py's LLMAuthError.
	LLMErrorClassAuth
	// LLMErrorClassRateLimit is llm/errors.py's LLMRateLimitError.
	LLMErrorClassRateLimit
	// LLMErrorClassServer is llm/errors.py's LLMServerError.
	LLMErrorClassServer
)

// ClassifyLLMError reports which LLMErrorClass err belongs to. ok is false
// when err is not one of this package's LLM errors at all -- the caller
// must then treat it the way Python's handler does, where a non-LLMError
// never reaches `except LLMError` and falls through to the endpoint's own
// generic handler instead. A caller that collapses those two cases answers
// a provider-shaped status for an unrelated failure.
func ClassifyLLMError(err error) (class LLMErrorClass, ok bool) {
	var typed *llmError
	if !errors.As(err, &typed) {
		return LLMErrorClassOther, false
	}
	switch typed.kind {
	case llmErrorAuth:
		return LLMErrorClassAuth, true
	case llmErrorRateLimit:
		return LLMErrorClassRateLimit, true
	case llmErrorServer:
		return LLMErrorClassServer, true
	default:
		return LLMErrorClassOther, true
	}
}
