package webhookintake

import (
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/ratelimit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// Per-minute limits, one fixed window each, counted in the shared store.
//
//   - pagerdutyLimitPerMinute is rate_limit.py's "60/minute" on pagerduty_webhook
//     (@limiter.limit), keyed by get_forwarded_ip: per (client IP, exact path),
//     so per binding. GitHub/GitLab/Jira/health carry no limit in Python.
//   - The three limits below are stricter than Python (D6129): a bound on
//     requests from one client address across the intake routes, and a bound
//     per (client address, route template) so ids in the path cannot mint new
//     buckets. They sit far above a provider's normal delivery rate, which
//     arrives from a few egress addresses.
const (
	pagerdutyLimitPerMinute         = 60
	intakeClientLimitPerMinute      = 3000
	pagerdutyTemplateLimitPerMinute = 600
	providerRouteLimitPerMinute     = 3000
)

var (
	pagerdutyLimit         = httpapi.Limit{ID: "webhook_pagerduty", Count: pagerdutyLimitPerMinute, Window: time.Minute}
	intakeClientLimit      = httpapi.Limit{ID: "webhook_intake_client", Count: intakeClientLimitPerMinute, Window: time.Minute}
	pagerdutyTemplateLimit = httpapi.Limit{ID: "webhook_pagerduty_route", Count: pagerdutyTemplateLimitPerMinute, Window: time.Minute}
	providerRouteLimit     = httpapi.Limit{ID: "webhook_intake_route", Count: providerRouteLimitPerMinute, Window: time.Minute}
)

const (
	providerGitHub    = "github"
	providerGitLab    = "gitlab"
	providerJira      = "jira"
	providerPagerDuty = "pagerduty"

	intakeClientBucket = "webhooks-intake"
	routePrefix        = "/api/v1/webhooks"
)

// routeTemplates are the registered route templates the route bucket keys on:
// fixed strings, never the request path.
var routeTemplates = map[string]string{
	providerGitHub:    routePrefix + "/github",
	providerGitLab:    routePrefix + "/gitlab",
	providerJira:      routePrefix + "/jira",
	providerPagerDuty: routePrefix + "/pagerduty/{binding_id}",
}

// rateLimiters holds this area's limiters, each an httpapi.KeyedLimiter: a
// fixed window in the shared counter store, so the limits hold across api
// replicas.
type rateLimiters struct {
	pagerduty         *httpapi.KeyedLimiter
	pagerdutyTemplate *httpapi.KeyedLimiter
	client            *httpapi.KeyedLimiter
	providerRoute     *httpapi.KeyedLimiter
}

// newRateLimiters counts in store; nil means an in-process store on now
// (development and tests). Every limiter's store-error series is declared at
// zero.
func newRateLimiters(store httpapi.CounterStore, now func() time.Time) *rateLimiters {
	if store == nil {
		store = httpapi.NewMemoryCounters(now)
	}
	limiters := &rateLimiters{
		pagerduty:         httpapi.NewKeyedLimiter(store, pagerdutyLimit),
		pagerdutyTemplate: httpapi.NewKeyedLimiter(store, pagerdutyTemplateLimit),
		client:            httpapi.NewKeyedLimiter(store, intakeClientLimit),
		providerRoute:     httpapi.NewKeyedLimiter(store, providerRouteLimit),
	}
	for _, limiter := range []*httpapi.KeyedLimiter{limiters.pagerduty, limiters.pagerdutyTemplate, limiters.client, limiters.providerRoute} {
		limiter.Declare()
	}
	return limiters
}

// Throttle reason classes, the only detail a refusal logs besides the provider.
const (
	throttleClient   = "client_bucket"
	throttleTemplate = "route_bucket"
	throttleExact    = "path_bucket"
)

// admit counts the request in the client bucket, the route-template bucket
// and, for PagerDuty, the exact-path bucket, in that order, before any binding
// or body read. A refusal is one 429 for every id. A store that cannot answer
// refuses a PagerDuty delivery with the unhandled-error 500 (never a request
// let through) and lets a GitHub, GitLab or Jira delivery through with a
// warning: those are verified by their own signature or token afterwards.
func (d Deps) admit(w http.ResponseWriter, r *http.Request, provider string) bool {
	if d.limiters == nil {
		return true
	}
	failClosed := provider == providerPagerDuty
	client := ratelimit.ForwardedIP(r)
	template := d.limiters.providerRoute
	if failClosed {
		template = d.limiters.pagerdutyTemplate
	}
	steps := []struct {
		limiter    *httpapi.KeyedLimiter
		path       string
		reason     string
		failClosed bool
	}{
		{d.limiters.client, intakeClientBucket, throttleClient, failClosed},
		{template, routeTemplates[provider], throttleTemplate, failClosed},
	}
	if failClosed {
		steps = append(steps, struct {
			limiter    *httpapi.KeyedLimiter
			path       string
			reason     string
			failClosed bool
		}{d.limiters.pagerduty, r.URL.Path, throttleExact, true})
	}
	for _, step := range steps {
		var allowed bool
		var err error
		if step.failClosed {
			allowed, err = step.limiter.AllowCounted(r.Context(), client, step.path)
		} else {
			allowed, err = step.limiter.Allow(r.Context(), client, step.path)
		}
		if err != nil {
			if step.failClosed {
				policy.WriteInternal(w)
				return false
			}
			d.logger().WarnContext(r.Context(), "webhook_intake.limiter_unavailable", "provider", provider, "reason", step.reason)
			continue
		}
		if !allowed {
			d.logger().WarnContext(r.Context(), "webhook_intake.throttled", "provider", provider, "reason", step.reason)
			policy.WriteDetail(w, http.StatusTooManyRequests, policy.ErrorDetail("Rate limit exceeded. Please try again later."), nil)
			return false
		}
	}
	return true
}

// limited runs admit before handler.
func (d Deps) limited(provider string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if d.admit(w, r, provider) {
			handler(w, r)
		}
	}
}
