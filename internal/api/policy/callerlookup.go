package policy

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrKeySourceUnavailable marks a token verifier whose key source (the JWKS)
// could not be read. It is an ErrUnavailable: the scope middlewares, which
// answer a store failure with the generic 500, answer it with the 503 of
// ErrUnavailable instead.
var ErrKeySourceUnavailable = fmt.Errorf("%w: token key source unavailable", ErrUnavailable)

// writeCallerLookupFailure answers a failed caller lookup: the generic 500,
// except an unreadable token key source, which is the 503 of ErrUnavailable.
func writeCallerLookupFailure(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrKeySourceUnavailable) {
		WriteAuthFailure(w, "", ErrUnavailable)
		return
	}
	WriteInternal(w)
}
