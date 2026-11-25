package providerunit

import (
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// chunkBodyReadError marks a 2xx provider body whose read failed mid-stream on
// a chunked unit: a transport fault, so its cause class is provider_transient.
type chunkBodyReadError struct{ err error }

func (e chunkBodyReadError) Error() string { return e.err.Error() }

func (e chunkBodyReadError) Unwrap() error { return e.err }

// chunkedBodyReadFailure reports a post-response body_read failure. A body that
// arrived whole but does not decode (body_decode, body_shape) is not one.
func chunkedBodyReadFailure(err error) bool {
	var providerErr *providerfoundation.ProviderError
	return errors.As(err, &providerErr) && providerErr.AfterResponse() &&
		providerErr.Reason == providerfoundation.ReasonBodyRead
}
