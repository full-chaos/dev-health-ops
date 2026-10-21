package providerfoundation

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

const (
	ReasonBodyRead   = "body_read"
	ReasonBodyDecode = "body_decode"
)

// routedBody carries the route template of the request that produced a 2xx
// response, so a read or decode failure after the client returned names it.
type routedBody struct {
	io.ReadCloser
	route string
}

func withResponseRoute(route string, response *http.Response) *http.Response {
	if route != "" && response != nil && response.Body != nil {
		response.Body = &routedBody{ReadCloser: response.Body, route: route}
	}
	return response
}

// ResponseRoute returns the route template the response was requested with,
// or "" when the call named none.
func ResponseRoute(response *http.Response) string {
	if response == nil {
		return ""
	}
	if body, ok := response.Body.(*routedBody); ok {
		return body.route
	}
	return ""
}

// ResponseFailure turns a failure that happens after a 2xx response (a body
// that fails to read or decode) into a ProviderError that names the route.
// errors.Is reaches cause; the cause text never reaches Error(). Read and
// decode failures stay permanent: a 2xx body failure is not retried here.
func ResponseFailure(response *http.Response, reason string, cause error) *ProviderError {
	return &ProviderError{
		Class:  ErrorPermanent,
		Route:  ResponseRoute(response),
		Reason: reason,
		cause:  cause,
	}
}

// ReadResponseBody reads at most limit+1 bytes. The caller keeps its own cap
// check; the bytes read so far come back with a read error, as io.ReadAll does.
// A read failure wraps ErrNormalizationInvalid.
func ReadResponseBody(response *http.Response, limit int64) ([]byte, error) {
	return ReadResponseBodyWith(response, limit, func(error) error { return ErrNormalizationInvalid })
}

// ReadResponseBodyTransport is ReadResponseBody for a route whose read failure
// is a transport failure (sanitised class text), not a malformed record.
func ReadResponseBodyTransport(response *http.Response, limit int64) ([]byte, error) {
	return ReadResponseBodyWith(response, limit, logging.TransportFailure)
}

// ReadResponseBodyWith is ReadResponseBody with the caller's cause for a read failure.
func ReadResponseBodyWith(response *http.Response, limit int64, cause func(error) error) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, ResponseFailure(response, ReasonBodyRead, ErrNormalizationInvalid)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return body, ResponseFailure(response, ReasonBodyRead, cause(err))
	}
	return body, nil
}

// ReadResponseBodyWithin reads a body of at most limit bytes. over > 0 means
// the body is above limit: the response's own Content-Length when that
// already says so (no byte is read), else limit+1 (the read stops there).
// A declared length only sizes the first buffer, never past limit+1, and the
// read stays capped at limit+1, so an under-reported length cannot widen it.
func ReadResponseBodyWithin(response *http.Response, limit int64, cause func(error) error) (body []byte, over int64, err error) {
	if response == nil || response.Body == nil || limit < 0 {
		return nil, 0, ResponseFailure(response, ReasonBodyRead, ErrNormalizationInvalid)
	}
	if response.ContentLength > limit {
		return nil, response.ContentLength, nil
	}
	initial := int64(bytes.MinRead)
	if response.ContentLength >= 0 {
		initial = response.ContentLength + 1
	}
	body = make([]byte, 0, min(initial, limit+1))
	reader := io.LimitReader(response.Body, limit+1)
	for {
		if len(body) == cap(body) {
			body = append(body, 0)[:len(body)]
		}
		n, readErr := reader.Read(body[len(body):cap(body)])
		body = body[:len(body)+n]
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return body, 0, ResponseFailure(response, ReasonBodyRead, cause(readErr))
		}
	}
	if int64(len(body)) > limit {
		return nil, int64(len(body)), nil
	}
	return body, 0, nil
}

// DecodeFlag selects decoder behaviour for DecodeResponseJSON.
type DecodeFlag int

const (
	// DecodeUseNumber decodes numbers into json.Number.
	DecodeUseNumber DecodeFlag = 1 << iota
	// DecodeStrict refuses data after the first JSON value.
	DecodeStrict
)

// DecodeResponseJSON decodes body into v. A failure names the response route
// and wraps ErrNormalizationInvalid.
func DecodeResponseJSON(response *http.Response, body []byte, v any, flags DecodeFlag) error {
	return DecodeResponseJSONWith(response, body, v, flags, func(error) error { return ErrNormalizationInvalid })
}

// DecodeResponseJSONWith is DecodeResponseJSON with the caller's cause for a decode failure.
func DecodeResponseJSONWith(response *http.Response, body []byte, v any, flags DecodeFlag, cause func(error) error) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if flags&DecodeUseNumber != 0 {
		decoder.UseNumber()
	}
	if err := decoder.Decode(v); err != nil {
		return ResponseFailure(response, ReasonBodyDecode, cause(err))
	}
	if flags&DecodeStrict != 0 && decoder.Decode(&struct{}{}) != io.EOF {
		return ResponseFailure(response, ReasonBodyDecode, cause(io.ErrUnexpectedEOF))
	}
	return nil
}

const (
	ReasonBodyTooLarge = "body_too_large"
	ReasonStatus       = "status"
	ReasonBodyShape    = "body_shape"
)

// ResponseInvalid is a post-2xx failure whose sentinel is ErrNormalizationInvalid.
func ResponseInvalid(response *http.Response, reason string) *ProviderError {
	return ResponseFailure(response, reason, ErrNormalizationInvalid)
}

// ResponseShape is a post-2xx failure of a decoded body whose fields do not
// have the shape the route needs (a missing mandatory field, a nil node, an
// unusable cursor). cause is the sentinel the caller already returned.
func ResponseShape(response *http.Response, cause error) *ProviderError {
	return ResponseFailure(response, ReasonBodyShape, cause)
}
