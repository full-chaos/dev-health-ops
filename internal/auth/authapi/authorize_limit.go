package authapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// The limit ids of the PDP caller buckets: the label values of
// dev_health_api_rate_limit_rejections_total. They are not in LimitIDs: the
// buckets live in the process, there is no counter store and so no store error.
const (
	pdpAuthorizeCallerLimitID      = "auth_svc_pdp_authorize_caller"
	pdpAuthorizeBatchCallerLimitID = "auth_svc_pdp_authorize_batch_caller"
)

const reasonRateLimited = "rate_limited"

// PDPCallerLimit is one caller workload's token bucket on one PDP route, per
// process. The zero value sets no per-caller limit.
type PDPCallerLimit struct {
	PerSecond float64
	Burst     int
	// Now is the bucket clock; nil is time.Now.
	Now func() time.Time
}

// callerLimiter holds one bucket per configured caller subject for one route.
// The subjects are the closed set AUTH_AUTHORIZE_CALLER_SUBJECTS names, so the
// map never grows with request data.
type callerLimiter struct {
	id      string
	buckets map[string]*httpapi.Bucket
}

func newCallerLimiter(id string, subjects []string, limit PDPCallerLimit) *callerLimiter {
	if limit.PerSecond <= 0 || limit.Burst <= 0 {
		return nil
	}
	c := &callerLimiter{id: id, buckets: make(map[string]*httpapi.Bucket, len(subjects))}
	for _, subject := range subjects {
		c.buckets[subject] = httpapi.NewBucket(limit.PerSecond, limit.Burst, limit.Now)
	}
	httpapi.DeclareRejections(id)
	return c
}

// take spends n tokens of the subject's bucket. A subject outside the set is
// refused: the admission gate never admits one.
func (c *callerLimiter) take(subject string, n int) (bool, time.Duration) {
	if c == nil {
		return true, 0
	}
	bucket, ok := c.buckets[subject]
	if !ok {
		return false, time.Second
	}
	return bucket.TakeN(n)
}

// chargeCaller spends n tokens of the admitted caller's bucket for op. On an
// empty bucket it answers the 429 and returns false.
func (h authorizeHandlers) chargeCaller(w http.ResponseWriter, r *http.Request, op, subject string, n int) bool {
	limiter := h.limiters[op]
	ok, wait := limiter.take(subject, n)
	if ok {
		return true
	}
	httpapi.CountRejection(limiter.id)
	h.rec.record(r.Context(), op, "denied", reasonRateLimited, nil)
	w.Header().Set("Retry-After", strconv.Itoa(httpapi.RetryAfterSeconds(wait)))
	httpapi.WriteError(w, r, httpapi.CodeRateLimited)
	return false
}

// callerSubjects are the configured caller workload subjects.
func (a *AuthorizeCallers) callerSubjects() []string {
	if a == nil {
		return nil
	}
	out := make([]string, 0, len(a.subjects))
	for subject := range a.subjects {
		out = append(out, subject)
	}
	return out
}
