package decision

import (
	"context"
	"net/http"
)

// Transport sends one request to the decision backend. The signature holds
// standard-library types only, so a client in package categorize (where the
// shared retry, redaction and error-classification code lives) implements it
// with no import of this package.
type Transport interface {
	// PostSystemOne posts one /v1/systemone JSON body and returns the body and
	// the headers of the final HTTP 200 response. The retry policy belongs to
	// the transport (one request and one retry of the same body on 429, 529,
	// 5xx, a timeout or a transport error; no retry on 400, 401, 402, 403 or
	// 422). Every other end is an error:
	//
	//   - a cancelled or expired context: the context error;
	//   - anything else: an error that categorize.FailureClass and
	//     categorize.IsDeterministicFailure classify (401, 402, 403 and an
	//     unknown model are deterministic).
	//
	// A 200 with a body that is not the expected JSON is NOT an error of the
	// transport: the adapter decides request_failed:not_json from the body.
	PostSystemOne(ctx context.Context, body []byte) (response []byte, header http.Header, err error)
}
