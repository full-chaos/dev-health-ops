package providerfoundation

import (
	"net/http"
	"time"
)

func resetDelay(provider string, headers http.Header, now time.Time) time.Duration {
	_ = provider
	return rateLimitRetryAfter(headers, 0, now)
}
