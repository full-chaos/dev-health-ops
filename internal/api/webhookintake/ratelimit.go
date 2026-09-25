package webhookintake

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// pagerdutyLimitPerMinute is rate_limit.py's "60/minute" on pagerduty_webhook
// (@limiter.limit), keyed by get_forwarded_ip (IP-keyed -- see rate_limit.py's
// key_func=get_forwarded_ip; GitHub/GitLab/Jira/health carry no limit either,
// matching router.py, which has no @limiter.limit decorator on any of them).
const pagerdutyLimitPerMinute = 60

// pagerdutyLimit is the limit's identity in the shared store, in logs and on
// the operator /metrics store-error series.
var pagerdutyLimit = httpapi.Limit{ID: "webhook_pagerduty", Count: pagerdutyLimitPerMinute, Window: time.Minute}

// rateLimiters holds this area's limiters. The PagerDuty one is an
// httpapi.KeyedLimiter, the api's one keyed limiter: a fixed window per
// (client IP, exact request path) -- so per binding, as slowapi keys it -- in
// the shared counter store, so the limit holds across api replicas.
type rateLimiters struct {
	pagerduty *httpapi.KeyedLimiter
}

// newRateLimiters counts in store; nil means an in-process store on now
// (development and tests). The limiter's store-error series is declared at
// zero.
func newRateLimiters(store httpapi.CounterStore, now func() time.Time) *rateLimiters {
	if store == nil {
		store = httpapi.NewMemoryCounters(now)
	}
	limiter := httpapi.NewKeyedLimiter(store, pagerdutyLimit)
	limiter.Declare()
	return &rateLimiters{pagerduty: limiter}
}
