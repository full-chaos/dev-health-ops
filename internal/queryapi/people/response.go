package people

import "errors"

// RequestError carries an HTTP status the way Python's HTTPException does,
// so the route layer can answer the same status code build_person_summary_
// response/build_person_metric_response would raise for the same bad
// input, without this package importing net/http. A separate copy of
// internal/queryapi/quadrant/response.go's own RequestError -- same
// shape, different package, same "repeat, don't couple" posture this
// package's other files already follow.
type RequestError struct {
	Status  int
	Message string
}

func (e *RequestError) Error() string { return e.Message }

func badRequest(msg string) error { return &RequestError{Status: 400, Message: msg} }
func notFound(msg string) error   { return &RequestError{Status: 404, Message: msg} }

// AsRequestError extracts a *RequestError's status/message, or reports
// ok=false for any other error -- the route layer's 503 fallback
// (main.py's own `except Exception: raise HTTPException(503, "Data
// unavailable")`).
func AsRequestError(err error) (*RequestError, bool) {
	var reqErr *RequestError
	if errors.As(err, &reqErr) {
		return reqErr, true
	}
	return nil, false
}
