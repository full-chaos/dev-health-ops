package authruntime

import (
	"net/http"
	"time"
)

// firstLoginAnswer returns the first answer to the process's first login that
// is not the retryable 503. The first login pays one-time costs (pool connect,
// key-state read, the bcrypt dummy hash); on a loaded host under -race they can
// pass the request timeout, and the service then answers its retryable 503
// (Retry-After 1). A later request is warm. It waits pause between tries and
// stops at guard, returning the last 503. Any other status is returned at once.
func firstLoginAnswer(login func() int, guard, pause time.Duration, sleep func(time.Duration)) int {
	spent := time.Duration(0)
	for {
		status := login()
		if status != http.StatusServiceUnavailable || spent >= guard {
			return status
		}
		sleep(pause)
		spent += pause
	}
}
