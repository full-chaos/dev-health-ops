package providerfoundation

import (
	"net/http"
	"time"
)

func resetDelay(provider string, headers http.Header, now time.Time) time.Duration {
	return rateLimitRetryAfter(provider, headers, 0, now)
}
