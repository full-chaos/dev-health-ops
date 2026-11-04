package policy

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrKeySourceUnavailable marks a token verifier whose key source (the JWKS)
// could not be read. It is an ErrUnavailable, so the scope middlewares answer
// it with the 503 of ErrUnavailable.
var ErrKeySourceUnavailable = fmt.Errorf("%w: token key source unavailable", ErrUnavailable)

// writeCallerLookupFailure answers a failed caller lookup: the generic 500,
// except an ErrUnavailable (a store outage or an unreadable token key
// source), which is the route guard's 503.
func writeCallerLookupFailure(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrUnavailable) {
		WriteAuthFailure(w, "", ErrUnavailable)
		return
	}
	WriteInternal(w)
}
