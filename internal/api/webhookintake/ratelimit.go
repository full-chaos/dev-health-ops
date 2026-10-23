package webhookintake

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

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

// Gate counts intake deliveries before any other work on them. It runs ahead
// of the api's scope middlewares, so every shape of path an intake route
// answers is counted exactly once.
type Gate struct {
	limiters *rateLimiters
	logger   *slog.Logger
}

// NewGate counts in store; nil means an in-process store on now.
func NewGate(store httpapi.CounterStore, now func() time.Time, logger *slog.Logger) *Gate {
	if logger == nil {
		logger = slog.Default()
	}
	return &Gate{limiters: newRateLimiters(store, now), logger: logger}
}

// Provider names the intake route a request is for, from its method and its
// decoded path (the path the mux routes on): the three fixed routes, and every
// path under the PagerDuty prefix, whatever follows it, so no spelling of that
// route is left uncounted.
func Provider(method, path string) (string, bool) {
	if method != http.MethodPost {
		return "", false
	}
	for provider, template := range routeTemplates {
		if provider != providerPagerDuty && path == template {
			return provider, true
		}
	}
	if strings.HasPrefix(path, pagerdutyPathBase) {
		return providerPagerDuty, true
	}
	return "", false
}

// Admit returns nil when the request may go on, else the refusal to write: one
// 429 for every id. GitHub, GitLab and Jira deliveries count in the client
// bucket and the route-template bucket. A PagerDuty delivery first PEEKS its
// per-binding bucket without counting: over its limit, it is refused and
// nothing is charged, so no refusal spends a broader budget and a fresh id
// creates no counter before the route budget admits it. Under the limit it
// counts in the client bucket, the route-template bucket and then the
// per-binding bucket (whose final count still enforces the limit under races);
// an id that is not one UUID is charged only to the shared bucket for such ids,
// never to the client or route budget. A store that cannot answer refuses a
// PagerDuty delivery with the unhandled-error 500 (never a request let
// through) and lets a GitHub, GitLab or Jira delivery through with a warning:
// those are verified by their own signature or token afterwards.
func (g *Gate) Admit(r *http.Request, provider string) func(http.ResponseWriter) {
	client := ratelimit.ForwardedIP(r)
	type step struct {
		limiter *httpapi.KeyedLimiter
		path    string
		reason  string
	}
	throttled := func(reason string) func(http.ResponseWriter) {
		g.logger.WarnContext(r.Context(), "webhook_intake.throttled", "provider", provider, "reason", reason)
		return func(w http.ResponseWriter) {
			policy.WriteDetail(w, http.StatusTooManyRequests, policy.ErrorDetail("Rate limit exceeded. Please try again later."), nil)
		}
	}
	count := func(s step, failClosed bool) (refuse func(http.ResponseWriter), stop bool) {
		var allowed bool
		var err error
		if failClosed {
			allowed, err = s.limiter.AllowCounted(r.Context(), client, s.path)
		} else {
			allowed, err = s.limiter.Allow(r.Context(), client, s.path)
		}
		if err != nil {
			if failClosed {
				return policy.WriteInternal, true
			}
			g.logger.WarnContext(r.Context(), "webhook_intake.limiter_unavailable", "provider", provider, "reason", s.reason)
			return nil, false
		}
		if !allowed {
			return throttled(s.reason), true
		}
		return nil, false
	}

	if provider != providerPagerDuty {
		for _, s := range []step{
			{g.limiters.client, intakeClientBucket, throttleClient},
			{g.limiters.providerRoute, routeTemplates[provider], throttleTemplate},
		} {
			if refuse, stop := count(s, false); stop {
				return refuse
			}
		}
		return nil
	}

	exact := step{g.limiters.pagerduty, exactBucketPath(r.URL.Path), throttleExact}
	below, err := exact.limiter.TestCounted(r.Context(), client, exact.path)
	if err != nil {
		return policy.WriteInternal
	}
	if !below {
		return throttled(exact.reason)
	}
	if strings.HasSuffix(exact.path, "/"+malformedBucket) {
		refuse, _ := count(exact, true)
		return refuse
	}
	for _, s := range []step{
		{g.limiters.client, intakeClientBucket, throttleClient},
		{g.limiters.pagerdutyTemplate, routeTemplates[provider], throttleTemplate},
		exact,
	} {
		if refuse, stop := count(s, true); stop {
			return refuse
		}
	}
	return nil
}

// malformedBucket names the one exact-path bucket every PagerDuty path whose
// id is not a single UUID shares: caller text is never a store key.
const malformedBucket = "malformed"

// pagerdutyPathBase is the PagerDuty route up to its id.
const pagerdutyPathBase = routePrefix + "/pagerduty/"

// exactBucketPath is the per-binding bucket key of a PagerDuty path: the
// canonical (lowercase, hyphenated) form of the one UUID after the prefix, so
// every spelling of one id shares a bucket and distinct ids never do; any
// remainder that is not exactly one UUID (an extra or trailing slash, junk, a
// different length) is the shared malformed bucket. The prefix is the one the
// handler's uuid.Parse sees, so the gate and the handler read the same id.
func exactBucketPath(path string) string {
	id, _ := strings.CutPrefix(path, pagerdutyPathBase)
	if !strings.Contains(id, "/") {
		if parsed, err := uuid.Parse(id); err == nil {
			return pagerdutyPathBase + parsed.String()
		}
	}
	return pagerdutyPathBase + malformedBucket
}
