package providerfoundation

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const maxProviderRequestBody = 1 << 20
const maxProviderErrorBody = 64 << 10
const defaultReservationReleaseTimeout = 5 * time.Second

// RetryPolicy is an attempt budget, not a nested retry multiplier. A request
// gets at most MaxAttempts calls including the initial call.
type RetryPolicy struct {
	MaxAttempts int
	InitialWait time.Duration
	MaxWait     time.Duration
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, InitialWait: time.Second, MaxWait: 30 * time.Second}
}

func (p RetryPolicy) valid() bool {
	return p.MaxAttempts > 0 && p.InitialWait > 0 && p.MaxWait >= p.InitialWait
}

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Auth is constructed from a typed credential. It is the only place that may
// reveal a secret for an outbound request.
type Auth func(*http.Request) error

func TokenAuth(header, prefix string, token secrets.Value) Auth {
	return func(request *http.Request) error {
		if !token.Configured() {
			return ErrCredentialInvalid
		}
		request.Header.Set(header, prefix+token.Reveal())
		return nil
	}
}

type HTTPClient struct {
	// Ledger, when set, counts every wire request per (route family, dimension).
	Ledger *RequestLedger
	// GateWaitMax bounds how long, in total, one request sleeps in place at
	// the shared backoff gate before the try ends with the wait that remains
	// (CHAOS-8830; Python slept at most 300 s). Zero sleeps not at all, which
	// is the old behaviour: any gate wait ends the try. NewHTTPClient sets it
	// to RateLimitMaxHonoredDelay.
	GateWaitMax time.Duration
	Provider    string
	BaseURL     *url.URL
	Doer        HTTPDoer
	Auth        Auth
	Retry       RetryPolicy
	Lease       LeaseGuard
	Gate        BackoffGate
	Budget      BudgetStore
	BudgetKey   BudgetKey
	Metrics     *Metrics
	entropy     io.Reader

	// ReservationReleaseTimeout bounds cleanup independently from a cancelled
	// request. Zero uses defaultReservationReleaseTimeout.
	ReservationReleaseTimeout time.Duration
}

func NewHTTPClient(provider, base string, doer HTTPDoer, auth Auth, retry RetryPolicy, lease LeaseGuard) (*HTTPClient, error) {
	// Each refusal names its cause (a constructor argument that was nil, a base URL that is not absolute, a
	// retry policy that cannot run), never a value: a client built with a nil doer used to answer a bare
	// "provider credential is invalid" that the operator could not tell from a bad credential (CHAOS-7132).
	parsed, err := url.Parse(base)
	switch {
	case err != nil || parsed.Scheme == "" || parsed.Host == "":
		return nil, credentialInvalid("base_url_invalid")
	case doer == nil:
		return nil, credentialInvalid("http_client_missing")
	case auth == nil:
		return nil, credentialInvalid("auth_missing")
	case lease == nil:
		return nil, credentialInvalid("lease_missing")
	case !retry.valid():
		return nil, credentialInvalid("retry_policy_invalid")
	}
	if err := admitDoer(doer); err != nil {
		return nil, err
	}
	return &HTTPClient{Provider: strings.ToLower(provider), BaseURL: parsed, Doer: originGuardedAtConstruction(doer), Auth: auth, Retry: retry, Lease: lease, GateWaitMax: RateLimitMaxHonoredDelay, entropy: rand.Reader}, nil
}

// admitDoer is the ONE admission every constructor that sends a credential through a doer calls (NewHTTPClient, NewGitHubAppAuth,
// NewPagerDutyClientCredentialsAuth): the allow-list by reachability (httpguard.Guardable). A refused doer is a construction error
// with fixed text, and no request goes through it. nil when the doer is admitted.
func admitDoer(doer HTTPDoer) error {
	if httpguard.Guardable(doer) {
		return nil
	}
	return refuseUnguardableDoer()
}

// refuseUnguardableDoer is the construction error for a doer the redirect guard cannot reach (a decorator that is not an
// httpguard.Wrapper): loud, fixed text, and no request ever goes through it.
// unguardableDoerLog is the fixed text of the refusal line: no doer detail, no value.
const unguardableDoerLog = "provider client refused: the doer is not an *http.Client or an httpguard.Wrapper chain that ends at one"

func refuseUnguardableDoer() error {
	slog.Error(unguardableDoerLog)
	return credentialInvalid("http_doer_unguardable")
}

// resolveTarget is the URL a request path names under the client's base URL.
//
// A path (with or without a leading slash) is joined UNDER the base URL's own
// path, the way Python's httpx merges a base_url with a request path: a
// self-hosted GitLab at https://host/gitlab or a GitHub Enterprise base
// https://host/api/v3 keeps its prefix for every request. (url.URL.Parse would
// treat a leading-slash path as absolute and drop the prefix.) An absolute URL
// -- a Link rel="next" cursor, a page URL -- is taken as it is; either way the
// result must stay on the base URL's scheme and host, so a credential is never
// sent to another origin. A caller that needs a path outside the base path (a
// GraphQL endpoint beside a /api/v3 base) passes an absolute URL.
func (c *HTTPClient) resolveTarget(path string) (*url.URL, error) {
	ref, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	target := ref
	if ref.Scheme == "" && ref.Host == "" {
		joined := *c.BaseURL
		basePath, baseRaw := c.BaseURL.Path, c.BaseURL.EscapedPath()
		if ref.Path != "" {
			joined.Path = strings.TrimRight(basePath, "/") + "/" + strings.TrimLeft(ref.Path, "/")
			joined.RawPath = strings.TrimRight(baseRaw, "/") + "/" + strings.TrimLeft(ref.EscapedPath(), "/")
		}
		joined.RawQuery, joined.Fragment, joined.RawFragment = ref.RawQuery, ref.Fragment, ref.RawFragment
		target = &joined
	}
	if !sameOrigin(target, c.BaseURL) {
		httpguard.RecordLeftOrigin(context.Background(), httpguard.ActionRefused, "provider.follow_url", c.BaseURL, target)
		return nil, ErrCredentialInvalid
	}
	return target, nil
}

// sameOrigin is the one origin rule of internal/httpguard (scheme, host and port).
func sameOrigin(u, base *url.URL) bool { return httpguard.SameOrigin(u, base) }

// DropCredentialsOnHostChange is an http.Client CheckRedirect for a client that must follow redirects (it
// also makes unauthenticated reads) but must never replay a credential to another host: it follows like
// net/http (at most 10), and removes every credential header from a request whose scheme or host (with
// port) differs from the first request's. Go's own rule keeps Authorization for a SUBDOMAIN of the original
// host; this one does not.
func DropCredentialsOnHostChange(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	if len(via) > 0 && !sameOrigin(req.URL, via[0].URL) {
		httpguard.StripCredentials(req)
		httpguard.RecordLeftOrigin(req.Context(), httpguard.ActionStripped, "provider.drop_on_host_change", via[0].URL, req.URL)
	}
	return nil
}

// originKey carries, in the context of one request, the credential origin and the flag a refused redirect sets: the
// guarded client reads it from the redirect request (net/http gives a redirect the context of the request it follows), so
// the guard works however many route decorators sit between HTTPClient.Do and the client.
type originKey struct{}

type originState struct {
	target *url.URL
	left   *atomic.Bool
}

// originGuardedAtConstruction guards the supplied doer ONCE, where the origin is known, before any route decorates the
// client (a route's counting decorator wraps client.Doer: the guard is already inside it, so no decorator has to opt in).
// An *http.Client is copied with a CheckRedirect that stops at the first hop that leaves the request's credential origin
// (the original policy decides every same-origin hop); a Wrapper is rebuilt around its guarded inner doer; any other doer is
// a test's transport and is returned as it is.
func originGuardedAtConstruction(doer HTTPDoer) HTTPDoer {
	if wrapper, ok := doer.(httpguard.Wrapper); ok {
		if rebuilt, ok := wrapper.Rewrap(originGuardedAtConstruction(wrapper.Unwrap())).(HTTPDoer); ok {
			return rebuilt
		}
		return doer
	}
	hc, ok := doer.(*http.Client)
	if !ok || hc == nil {
		return doer
	}
	guarded := *hc
	inner := hc.CheckRedirect
	guarded.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		// The client's own policy decides first: a client that refuses the hop answers the 3xx to the caller (a route that
		// handles a Location itself, with no credential, relies on it) and nothing is sent to the other origin.
		if inner != nil {
			if err := inner(req, via); err != nil {
				return err
			}
		} else if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		state, known := req.Context().Value(originKey{}).(*originState)
		if !known {
			// A decorator dropped the request context, so the credential origin is lost: fail closed. The hop is followed (a
			// redirect to presigned storage must keep working) with EVERY credential header removed.
			httpguard.StripCredentials(req)
			req.Header.Set(strippedMarker, "1")
			slog.Warn("provider redirect without a credential origin in the request context: credential headers stripped")
			recordRedirectHop(req)
			return nil
		}
		// The hop would be followed: stop it where it leaves the credential origin and say so.
		if !sameOrigin(req.URL, state.target) {
			httpguard.RecordLeftOrigin(req.Context(), httpguard.ActionRefused, "provider.redirect", state.target, req.URL)
			state.left.Store(true)
			return http.ErrUseLastResponse
		}
		recordRedirectHop(req)
		return nil
	}
	return &guarded
}

// originGuardedDoer makes the credential origin a property of HTTPClient, not
// of whichever Doer was injected. A stock http.Client follows redirects and
// copies custom auth headers (GitLab's PRIVATE-TOKEN) to the new origin, so for
// an *http.Client the returned Doer is a shallow copy whose CheckRedirect stops
// at the first hop that leaves target's origin (the original policy still
// decides every same-origin hop) and sets the returned flag. Any other Doer is
// returned as is (a decorator is rebuilt around its guarded inner doer); responseLeftOrigin still detects a redirect it followed.
func originGuardedDoer(doer HTTPDoer, target *url.URL, hop func(*http.Request)) (HTTPDoer, *atomic.Bool) {
	if wrapper, ok := doer.(httpguard.Wrapper); ok { // a decorator cannot hide a following client from the origin guard
		inner, left := originGuardedDoer(wrapper.Unwrap(), target, hop)
		if rebuilt, ok := wrapper.Rewrap(inner).(HTTPDoer); ok {
			return rebuilt, left
		}
		return doer, left
	}
	left := new(atomic.Bool)
	hc, ok := doer.(*http.Client)
	if !ok || hc == nil {
		return doer, left
	}
	guarded := *hc
	inner := hc.CheckRedirect
	guarded.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		// the client's own policy decides first (a client that refuses the hop answers the 3xx to the caller)
		if inner != nil {
			if err := inner(req, via); err != nil {
				return err
			}
		} else if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		if !sameOrigin(req.URL, target) {
			httpguard.RecordLeftOrigin(req.Context(), httpguard.ActionRefused, "provider.redirect", target, req.URL)
			left.Store(true)
			return http.ErrUseLastResponse
		}
		if hop != nil {
			hop(req)
		}
		return nil
	}
	return &guarded, left
}

// strippedMarker is set on a redirect request whose credential headers the origin guard removed because the credential origin was
// lost (a decorator dropped the request context): the after-the-fact check lets such a followed hop through.
const strippedMarker = "X-Origin-Guard-Stripped"

// strippedOfCredentials: the last request a Doer sent is one the origin guard stripped.
func strippedOfCredentials(request *http.Request) bool {
	return request != nil && request.Header.Get(strippedMarker) != ""
}

// responseLeftOrigin is the after-the-fact check for a Doer that followed a
// redirect off the credential origin by itself: response.Request is the last
// request it sent. Only a request that a redirect created counts
// (response.Request.Response is the redirect response that caused it, which
// net/http sets): a transport that clones the request and rewrites its host
// (a proxy, a test stub) also changes response.Request.URL, with no redirect.
func responseLeftOrigin(response *http.Response, target *url.URL) bool {
	return response != nil && response.Request != nil && response.Request.Response != nil &&
		!sameOrigin(response.Request.URL, target)
}

type routeKey struct{}

// DoRoute is Do for a route whose path carries a member login or other
// personal data: a failure names the route by template (for example
// /users/{login}) instead of the path. The template must be a static literal,
// never built from a request value. It applies to this call only.
func (c *HTTPClient) DoRoute(ctx context.Context, method, template, path string, body io.Reader) (*http.Response, error) {
	return c.Do(context.WithValue(ctx, routeKey{}, template), method, path, body)
}

func routeTemplate(ctx context.Context) string {
	template, _ := ctx.Value(routeKey{}).(string)
	return template
}

func (c *HTTPClient) Do(ctx context.Context, method, path string, body io.Reader) (response *http.Response, err error) {
	if c == nil || c.BaseURL == nil {
		return nil, ErrCredentialInvalid
	}
	if err := c.Lease.Assert(ctx); err != nil {
		return nil, err
	}
	if err := c.awaitGate(ctx); err != nil {
		return nil, err
	}
	var requestBody []byte
	if body != nil {
		var err error
		requestBody, err = io.ReadAll(io.LimitReader(body, maxProviderRequestBody+1))
		if err != nil || len(requestBody) > maxProviderRequestBody {
			return nil, ErrCredentialInvalid
		}
	}
	var reservation Reservation
	if c.Budget != nil {
		reservation, err = c.Budget.Acquire(ctx, c.BudgetKey)
		if err != nil {
			if c.Metrics != nil {
				c.Metrics.RecordBudgetDenied(c.Provider)
			}
			return nil, err
		}
		if reservation == nil {
			if c.Metrics != nil {
				c.Metrics.RecordBudgetDenied(c.Provider)
			}
			return nil, ErrBudgetUnavailable
		}
		defer func() {
			releaseErr := c.releaseReservation(ctx, reservation)
			if releaseErr == nil {
				return
			}
			if c.Metrics != nil {
				c.Metrics.RecordBudgetReleaseError(c.Provider)
			}
			if err == nil {
				if response != nil && response.Body != nil {
					_ = response.Body.Close()
				}
				response = nil
			}
			err = errors.Join(err, releaseErr)
		}()
	}
	target, err := c.resolveTarget(path)
	if err != nil {
		return nil, ErrCredentialInvalid
	}
	var last *ProviderError
	for attempt := 1; attempt <= c.Retry.MaxAttempts; attempt++ {
		if err := c.Lease.Assert(ctx); err != nil {
			return nil, err
		}
		// The first attempt passed the gate above. A retry is a wire attempt too:
		// a sibling may have armed or extended the shared gate while this request
		// waited to retry.
		if attempt > 1 {
			if err := c.awaitGate(ctx); err != nil {
				return nil, err
			}
		}
		callLeft := new(atomic.Bool)
		// Each followed redirect hop counts as one more wire request: through the context for a guard behind a route's
		// decorator, and through the per-call guard for a decorator that drops the context (a hop is counted once).
		recorder := c.newHopRecorder(ctx, target.Path)
		request, err := http.NewRequestWithContext(withHopRecorder(context.WithValue(ctx, originKey{}, &originState{target: target, left: callLeft}), recorder), method, target.String(), bytes.NewReader(requestBody))
		if err != nil {
			return nil, ErrCredentialInvalid
		}
		if err := c.Auth(request); err != nil {
			return nil, err
		}
		var requestErr error
		guarded, leftOrigin := originGuardedDoer(c.Doer, target, recorder.record)
		c.recordWireRequest(ctx, target.Path)
		response, requestErr = guarded.Do(request)
		if callLeft.Load() || leftOrigin.Load() || (requestErr == nil && responseLeftOrigin(response, target) && !strippedOfCredentials(response.Request)) {
			// A redirect took the authenticated request off the credential's
			// origin. The header may already have been sent by the Doer, so
			// this is never retried and the response is never returned.
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			response = nil
			c.observe(ErrorPermanent)
			return nil, ErrCredentialInvalid
		}
		if requestErr != nil {
			last = c.transportFailure(ctx, requestErr, attempt)
			c.observe(last.Class)
			if attempt == c.Retry.MaxAttempts {
				return nil, last
			}
			if err := c.wait(ctx, c.retryDelay(attempt, 0, ErrorTransient)); err != nil {
				return nil, err
			}
			continue
		}
		classification := ClassifyHTTPForRequest(c.Provider, target.Path, response.StatusCode, response.Header, "")
		if classification == nil {
			c.observe("")
			return response, nil
		}
		message, _ := io.ReadAll(io.LimitReader(response.Body, maxProviderErrorBody))
		classification = ClassifyHTTPForRequest(c.Provider, target.Path, response.StatusCode, response.Header, string(message))
		// CHAOS-4582: message was already being read off the response body
		// (above) to let ClassifyHTTPWithMessage sniff GitHub/GitLab
		// rate-limit bodies -- it was then discarded rather than attached to
		// the classification returned to the caller, so every consumer saw
		// only the generic Class and never the provider's own rejection
		// reason. Path excludes the query string deliberately (see Path's
		// doc comment); Body is kept for classification parsers and never
		// reaches a log.
		classification.Path = target.Path
		classification.Route = routeTemplate(ctx)
		classification.Body = string(message)
		if dimension := budgetDimension(ctx); dimension != "" && classification.Class == ErrorRateLimited {
			classification.Dimension = dimension
		}
		retryDelay := c.retryDelay(attempt, classification.RetryAfter, classification.Class)
		if classification.Class == ErrorRateLimited && c.Gate != nil {
			// The gate is shared cross-runtime (Celery workers read the same
			// key) and accepts up to 300s, so arm it with the provider's own
			// delay rather than this client's truncated retry wait --
			// under-penalizing it sent Celery back at the provider early too.
			penalty := classification.RetryAfter
			if penalty <= 0 {
				penalty = retryDelay
			}
			if penalty > RateLimitMaxHonoredDelay {
				penalty = RateLimitMaxHonoredDelay
			}
			if err := c.Gate.Penalize(ctx, penalty); err != nil {
				_ = response.Body.Close()
				return nil, err
			}
		}
		_ = response.Body.Close()
		last = classification
		c.observe(classification.Class)
		if !classification.Retryable() || attempt == c.Retry.MaxAttempts {
			return nil, classification
		}
		if err := c.wait(ctx, retryDelay); err != nil {
			return nil, err
		}
	}
	if last == nil {
		last = &ProviderError{Class: ErrorTransient}
	}
	return nil, last
}

// DoUnauthenticated issues one absolute-URL request without applying provider
// authentication. It is intentionally narrow: GitHub Actions artifact
// downloads redirect to a short-lived pre-signed object URL, and forwarding
// the provider token to that different host would disclose a credential.
// The request still honors the live lease, backoff gate, and shared budget.
func (c *HTTPClient) DoUnauthenticated(
	ctx context.Context,
	method string,
	absoluteURL string,
) (response *http.Response, err error) {
	if c == nil || c.Doer == nil || c.Lease == nil {
		return nil, ErrCredentialInvalid
	}
	target, parseErr := url.Parse(absoluteURL)
	if parseErr != nil || target.Scheme == "" || target.Host == "" ||
		(target.Scheme != "http" && target.Scheme != "https") {
		return nil, ErrCredentialInvalid
	}
	if err := c.Lease.Assert(ctx); err != nil {
		return nil, err
	}
	if err := c.awaitGate(ctx); err != nil {
		return nil, err
	}
	var reservation Reservation
	if c.Budget != nil {
		reservation, err = c.Budget.Acquire(ctx, c.BudgetKey)
		if err != nil {
			if c.Metrics != nil {
				c.Metrics.RecordBudgetDenied(c.Provider)
			}
			return nil, err // Preserve ErrBudgetContended for attempt-neutral snooze.
		}
		if reservation == nil {
			if c.Metrics != nil {
				c.Metrics.RecordBudgetDenied(c.Provider)
			}
			return nil, ErrBudgetUnavailable
		}
		defer func() {
			releaseErr := c.releaseReservation(ctx, reservation)
			if releaseErr == nil {
				return
			}
			if c.Metrics != nil {
				c.Metrics.RecordBudgetReleaseError(c.Provider)
			}
			if err == nil {
				if response != nil && response.Body != nil {
					_ = response.Body.Close()
				}
				response = nil
			}
			err = errors.Join(err, releaseErr)
		}()
	}
	request, requestErr := http.NewRequestWithContext(withHopRecorder(ctx, c.newHopRecorder(ctx, target.Path)), method, target.String(), nil)
	if requestErr != nil {
		return nil, ErrCredentialInvalid
	}
	c.recordWireRequest(ctx, target.Path)
	response, err = c.Doer.Do(request)
	if err != nil {
		failure := c.transportFailure(ctx, err, 1)
		c.observe(failure.Class)
		return nil, failure
	}
	if classification := classifyResponseWithBody(c.Provider, target.Path, response); classification == nil {
		c.observe("")
	} else {
		c.observe(classification.Class)
	}
	return response, nil
}

// bodyPeekTimeout bounds how long classification waits for a body to start
// and finish within maxProviderErrorBody; a slower body keeps the
// status-and-headers classification.
const bodyPeekTimeout = 2 * time.Second

// peekedBody serves the bytes (and the read error, if any) of a background
// bounded read first and the rest of the original body after, so the caller
// sees exactly what the unwrapped body would have given it.
type peekedBody struct {
	done   chan struct{}
	peeked []byte
	err    error
	offset int
	rest   io.ReadCloser
}

func (b *peekedBody) Read(p []byte) (int, error) {
	<-b.done
	if b.offset < len(b.peeked) {
		n := copy(p, b.peeked[b.offset:])
		b.offset += n
		if b.offset == len(b.peeked) && b.err != nil {
			return n, b.err // the original body returned its data and error together
		}
		return n, nil
	}
	if b.err != nil {
		return 0, b.err
	}
	return b.rest.Read(p)
}

func (b *peekedBody) Close() error { return b.rest.Close() }

// classifyResponseWithBody classifies a response from status, headers and a
// bounded, time-bounded peek of the body, so a limit whose only signal is the
// body message (a GitHub secondary or abuse limit on a 403) is rate_limited
// with its dimension. Only that case consults the body (ClassifyHTTPWithMessage).
// The caller still reads the full body, with any read error it would have got,
// except after a peek timeout, which closes the body so no reader goroutine leaks.
func classifyResponseWithBody(provider, requestPath string, response *http.Response) *ProviderError {
	classification := ClassifyHTTPForRequest(provider, requestPath, response.StatusCode, response.Header, "")
	if classification == nil || response.Body == nil || classification.Class == ErrorRateLimited ||
		provider != "github" || response.StatusCode != http.StatusForbidden {
		return classification
	}
	body := &peekedBody{done: make(chan struct{}), rest: response.Body}
	go func() {
		body.peeked, body.err = io.ReadAll(io.LimitReader(body.rest, maxProviderErrorBody))
		close(body.done)
	}()
	response.Body = body
	timer := time.NewTimer(bodyPeekTimeout)
	defer timer.Stop()
	select {
	case <-body.done:
	case <-timer.C:
		_ = body.rest.Close() // ends the reader goroutine; the caller's later read fails
		logBodyPeekTimeout(provider, response.StatusCode, classification.Class)
		return classification
	}
	withBody := ClassifyHTTPForRequest(provider, requestPath, response.StatusCode, response.Header, string(body.peeked))
	if withBody == nil {
		return classification
	}
	if withBody.Class != classification.Class {
		logBodyReclassification(provider, response.StatusCode, classification.Class, withBody.Class, withBody.Dimension)
	}
	return withBody
}

func logBodyPeekTimeout(provider string, status int, class ErrorClass) {
	slog.Warn("provider error body not read in time, classified from status and headers; body closed",
		"provider", provider, "status", status, "class", string(class), "timeout_ms", bodyPeekTimeout.Milliseconds())
}

// logBodyReclassification logs only closed-set values (provider, status,
// class names, dimension name), never body or error text.
func logBodyReclassification(provider string, status int, from, to ErrorClass, dimension string) {
	slog.Debug("provider response reclassified from its body",
		"provider", provider, "status", status, "from_class", string(from), "to_class", string(to), "dimension", dimension)
}

func (c *HTTPClient) retryDelay(attempt int, retryAfter time.Duration, class ErrorClass) time.Duration {
	if retryAfter > 0 {
		// A rate limit is the provider telling us exactly how long to wait.
		// Truncating that to the generic 30s MaxWait and retrying anyway is
		// what escalates a primary limit into a secondary one, so honour it up
		// to Python's 300s ceiling. Every other class keeps MaxWait.
		honored := c.Retry.MaxWait
		if class == ErrorRateLimited && RateLimitMaxHonoredDelay > honored {
			honored = RateLimitMaxHonoredDelay
		}
		if retryAfter > honored {
			return honored
		}
		return retryAfter
	}
	wait := c.Retry.InitialWait
	for current := 1; current < attempt && wait < c.Retry.MaxWait; current++ {
		if wait > c.Retry.MaxWait/2 {
			wait = c.Retry.MaxWait
			break
		}
		wait *= 2
	}
	if wait < c.Retry.MaxWait {
		entropy := c.entropy
		if entropy == nil {
			entropy = rand.Reader
		}
		wait += retryJitter(entropy, wait/5+1)
	}
	if wait > c.Retry.MaxWait {
		return c.Retry.MaxWait
	}
	return wait
}

func retryJitter(entropy io.Reader, limit time.Duration) time.Duration {
	if entropy == nil || limit <= 1 {
		return 0
	}
	jitter, err := rand.Int(entropy, big.NewInt(int64(limit)))
	if err != nil {
		return 0
	}
	return time.Duration(jitter.Int64())
}

// awaitGate sleeps out the shared backoff gate in place, as Python's gate_call
// did, so a unit that another unit's rate limit paused continues its fetch
// instead of ending the try and restarting from zero. It sleeps only while the
// total stays within GateWaitMax and the context outlasts the wait; otherwise
// the try ends with ErrorRateLimited carrying the wait that remains, which the
// provider-unit handler turns into a persisted deferral.
func (c *HTTPClient) awaitGate(ctx context.Context) error {
	if c.Gate == nil {
		return nil
	}
	var slept time.Duration
	for {
		wait, err := c.Gate.Wait(ctx)
		if err != nil {
			return err
		}
		if wait <= 0 {
			if slept > 0 {
				slog.InfoContext(ctx, "provider_gate_wait_slept", "provider", c.Provider, "slept_ms", slept.Milliseconds())
			}
			return nil
		}
		if slept+wait > c.GateWaitMax {
			slog.InfoContext(ctx, "provider_gate_wait_deferred", "provider", c.Provider, "wait_ms", wait.Milliseconds(), "slept_ms", slept.Milliseconds(), "bound_ms", c.GateWaitMax.Milliseconds())
			return &ProviderError{Class: ErrorRateLimited, RetryAfter: wait}
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= wait {
			slog.InfoContext(ctx, "provider_gate_wait_deferred", "provider", c.Provider, "wait_ms", wait.Milliseconds(), "slept_ms", slept.Milliseconds(), "reason", "deadline")
			return &ProviderError{Class: ErrorRateLimited, RetryAfter: wait}
		}
		if err := c.wait(ctx, wait); err != nil {
			return err
		}
		slept += wait
		if err := c.Lease.Assert(ctx); err != nil {
			return err
		}
	}
}

func (c *HTTPClient) wait(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *HTTPClient) releaseReservation(ctx context.Context, reservation Reservation) error {
	timeout := c.ReservationReleaseTimeout
	if timeout <= 0 {
		timeout = defaultReservationReleaseTimeout
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	if err := reservation.Release(releaseCtx); err != nil {
		return ErrBudgetUnavailable
	}
	return nil
}

func (c *HTTPClient) observe(class ErrorClass) {
	if c.Metrics != nil {
		c.Metrics.RecordRequest(c.Provider, class)
	}
}

// ClassifyHTTP mirrors the current Python provider taxonomy without retaining
// response bodies or headers. GitHub's and GitLab's 403 rate-limit vocabularies
// are special; all other 403s remain authentication/permission failures.
func ClassifyHTTP(provider string, status int, headers http.Header) *ProviderError {
	return ClassifyHTTPWithMessage(provider, status, headers, "")
}

func ClassifyHTTPWithMessage(provider string, status int, headers http.Header, message string) *ProviderError {
	now := time.Now()
	retryAfter := ParseRetryAfter(headerValue(headers, "retry-after"), now)
	message = strings.ToLower(message)
	if isRateLimitedForbidden(provider, status, headers, message) {
		return &ProviderError{
			Class: ErrorRateLimited, StatusCode: status,
			RetryAfter: rateLimitRetryAfter(headers, retryAfter, now),
		}
	}
	switch status {
	case 0:
		return &ProviderError{Class: ErrorTransient}
	case http.StatusUnauthorized, http.StatusForbidden:
		return &ProviderError{Class: ErrorAuthentication, StatusCode: status}
	case http.StatusNotFound:
		return &ProviderError{Class: ErrorNotFound, StatusCode: status}
	case http.StatusConflict:
		return &ProviderError{Class: ErrorConflict, StatusCode: status}
	case http.StatusTooManyRequests:
		return &ProviderError{
			Class: ErrorRateLimited, StatusCode: status,
			RetryAfter: rateLimitRetryAfter(headers, retryAfter, now),
		}
	}
	if status >= 500 && status <= 599 {
		return &ProviderError{Class: ErrorTransient, StatusCode: status, RetryAfter: retryAfter}
	}
	if status >= 400 {
		return &ProviderError{Class: ErrorPermanent, StatusCode: status}
	}
	return nil
}

func isRateLimitedForbidden(provider string, status int, headers http.Header, message string) bool {
	if status != http.StatusForbidden {
		return false
	}
	if provider == "gitlab" &&
		(hasHeader(headers, "retry-after") || headerValue(headers, "ratelimit-remaining") == "0") {
		return true
	}
	return provider == "github" &&
		(headerValue(headers, "x-ratelimit-remaining") == "0" ||
			headerValue(headers, "retry-after") != "" ||
			strings.Contains(message, "rate limit") ||
			strings.Contains(message, "abuse") ||
			strings.Contains(message, "secondary"))
}

func headerValue(headers http.Header, wanted string) string {
	for key, values := range headers {
		if strings.EqualFold(key, wanted) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func hasHeader(headers http.Header, wanted string) bool {
	for key := range headers {
		if strings.EqualFold(key, wanted) {
			return true
		}
	}
	return false
}

// RateLimitMaxHonoredDelay bounds how long a provider-supplied rate-limit
// delay may park a request or the shared cooldown gate. It matches Python's
// RateLimitConfig.max_backoff_seconds (300s, connectors/utils/rate_limit_queue.py).
//
// The generic RetryPolicy.MaxWait (30s by default) stays the cap for ordinary
// transient errors: clamping a rate limit to it meant Go kept re-hitting a
// provider inside its own stated window, which is exactly what escalates
// GitHub from a primary to a secondary limit (CHAOS-3868).
const RateLimitMaxHonoredDelay = 300 * time.Second

// ParseRateLimitReset derives a delay from an absolute epoch-seconds reset
// header. GitHub primary limits usually carry only x-ratelimit-reset and no
// Retry-After; GitLab uses RateLimit-Reset. Python resolves both
// (providers/github/ratelimit.py github_retry_after_seconds); Go previously
// looked at retry-after alone and fell back to a generic backoff.
func ParseRateLimitReset(raw string, now time.Time) time.Duration {
	seconds, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0
	}
	delay := time.Duration((seconds - float64(now.UnixNano())/float64(time.Second)) * float64(time.Second))
	if delay < 0 {
		return 0
	}
	return delay
}

// rateLimitRetryAfter prefers an explicit Retry-After and falls back to the
// provider's reset header, mirroring Python's resolution order.
func rateLimitRetryAfter(headers http.Header, retryAfter time.Duration, now time.Time) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}
	for _, header := range []string{"x-ratelimit-reset", "ratelimit-reset"} {
		if delay := ParseRateLimitReset(headerValue(headers, header), now); delay > 0 {
			return delay
		}
	}
	return retryAfter
}

func ParseRetryAfter(raw string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil {
		if seconds < 0 {
			return 0
		}
		return time.Duration(seconds * float64(time.Second))
	}
	if date, err := http.ParseTime(raw); err == nil && date.After(now) {
		return date.Sub(now)
	}
	return 0
}

// PageFunc follows an opaque next cursor. Its source envelope is already
// normalized; the helper only enforces bounded pagination and cancellation.
type PageFunc[T any] func(context.Context, string) (items []T, next string, err error)

func CollectPages[T any](ctx context.Context, maxPages int, fetch PageFunc[T]) ([]T, error) {
	if maxPages < 1 || fetch == nil {
		return nil, ErrCredentialInvalid
	}
	var values []T
	cursor := ""
	for page := 0; page < maxPages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		items, next, err := fetch(ctx, cursor)
		if err != nil {
			return nil, err
		}
		values = append(values, items...)
		if next == "" {
			return values, nil
		}
		cursor = next
	}
	return nil, errors.New("provider pagination budget exhausted")
}

type budgetDimensionKey struct{}

// WithBudgetDimension names the dispatch budget dimension the requests made
// with ctx are accounted to, for a route whose dimension is not the one its
// request path implies: GitHub's file and blame blob reads go through GraphQL
// but their budget estimates are contents_blob. A rate limit on such a request
// carries this dimension, so the cooldown gate matches the units that budgeted
// it.
func WithBudgetDimension(ctx context.Context, dimension string) context.Context {
	return context.WithValue(ctx, budgetDimensionKey{}, dimension)
}

func budgetDimension(ctx context.Context) string {
	dimension, _ := ctx.Value(budgetDimensionKey{}).(string)
	return dimension
}

// ClassifyHTTPForRequest is ClassifyHTTPWithMessage for a response whose
// request path is known: a rate-limited classification also carries the
// budget dimension of that request (RateLimitDimension). Every code that
// classifies a provider response itself, rather than through HTTPClient.Do,
// must use it, or its rate limits are recorded without a dimension.
func ClassifyHTTPForRequest(provider, requestPath string, status int, headers http.Header, message string) *ProviderError {
	classification := ClassifyHTTPWithMessage(provider, status, headers, message)
	if classification != nil && classification.Class == ErrorRateLimited {
		classification.Dimension = RateLimitDimension(provider, requestPath, message)
	}
	return classification
}

// RateLimitDimension names the budget dimension a rate-limited request drew
// from, read from the request alone: a GitHub secondary or abuse limit is
// secondary_abuse_risk, the search API is search (also under a GitHub
// Enterprise Server /api/v3 prefix), a Jira JQL search under /rest/api/ is search,
// a path ending in /graphql is graphql_cost,
// anything else rest_core. These are the dimension names the dispatch budget
// buckets and the cooldown gate use.
func RateLimitDimension(provider, path, message string) string {
	if provider == "github" {
		lower := strings.ToLower(message)
		if strings.Contains(lower, "secondary") || strings.Contains(lower, "abuse") {
			return "secondary_abuse_risk"
		}
		if strings.HasPrefix(strings.TrimPrefix(path, "/api/v3"), "/search/") {
			return "search"
		}
	}
	if provider == "jira" && strings.Contains(path, "/rest/api/") && strings.Contains(path, "/search") {
		return "search"
	}
	if strings.HasSuffix(path, "/graphql") {
		return "graphql_cost"
	}
	return "rest_core"
}
