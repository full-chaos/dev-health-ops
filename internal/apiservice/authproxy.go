package apiservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/clientip"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// AuthAggregate is one routing unit of the /api/v1/auth/* proxy to
// auth-service (CHAOS-10231). Each has one flag; off, go-api serves it.
type AuthAggregate string

const (
	AuthSessions    AuthAggregate = "sessions"
	AuthLocalLogin  AuthAggregate = "local_login"
	AuthCredentials AuthAggregate = "credentials"
	AuthSocial      AuthAggregate = "social"
	AuthOIDC        AuthAggregate = "oidc"
	AuthSAML        AuthAggregate = "saml"
)

// AuthAggregates is every aggregate, in flag order.
var AuthAggregates = []AuthAggregate{AuthSessions, AuthLocalLogin, AuthCredentials, AuthSocial, AuthOIDC, AuthSAML}

// EnvAuthProxyURL is auth-service's base URL (scheme and host only).
const EnvAuthProxyURL = "AUTH_PROXY_URL"

// AuthProxyFlagEnv is the routing flag of an aggregate, AUTH_PROXY_<NAME>.
func AuthProxyFlagEnv(a AuthAggregate) string { return "AUTH_PROXY_" + strings.ToUpper(string(a)) }

// maxAuthProxyResponseBytes bounds one auth-service answer.
const maxAuthProxyResponseBytes = 1 << 20

// defaultAuthProxyTimeout bounds one upstream exchange when the route's own
// deadline is later.
const defaultAuthProxyTimeout = 10 * time.Second

// AuthProxyConfig is the proxy's routing state. The zero value serves every
// route in go-api.
type AuthProxyConfig struct {
	Upstream *url.URL
	On       map[AuthAggregate]bool
	// Timeout bounds one upstream exchange; zero means defaultAuthProxyTimeout.
	Timeout time.Duration
}

// Enabled reports whether a's routes go to auth-service.
func (c AuthProxyConfig) Enabled(a AuthAggregate) bool { return c.Upstream != nil && c.On[a] }

// ParseAuthProxyConfig reads AUTH_PROXY_URL and the AUTH_PROXY_<aggregate>
// flags. Every flag defaults off. It refuses a flag without a URL, a URL
// that is not a bare http(s) origin, and any aggregate that mints refresh
// credentials while sessions stay in go-api: the credential would have no
// owner able to rotate it.
func ParseAuthProxyConfig(lookup func(string) (string, bool)) (AuthProxyConfig, error) {
	cfg := AuthProxyConfig{On: map[AuthAggregate]bool{}}
	anyOn := false
	for _, aggregate := range AuthAggregates {
		name := AuthProxyFlagEnv(aggregate)
		raw, ok := lookup(name)
		if !ok || strings.TrimSpace(raw) == "" {
			continue
		}
		on, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return AuthProxyConfig{}, fmt.Errorf("%s must be true or false", name)
		}
		cfg.On[aggregate] = on
		anyOn = anyOn || on
	}
	raw, _ := lookup(EnvAuthProxyURL)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if anyOn {
			return AuthProxyConfig{}, fmt.Errorf("an AUTH_PROXY_* flag is on but %s is not set", EnvAuthProxyURL)
		}
		return cfg, nil
	}
	upstream, err := url.Parse(raw)
	if err != nil || (upstream.Scheme != "http" && upstream.Scheme != "https") || upstream.Host == "" || upstream.User != nil ||
		(upstream.Path != "" && upstream.Path != "/") || upstream.RawQuery != "" || upstream.Fragment != "" || upstream.Opaque != "" {
		return AuthProxyConfig{}, fmt.Errorf("%s must be an http(s) origin such as http://auth-service:8095, with no path, query or credentials", EnvAuthProxyURL)
	}
	cfg.Upstream = &url.URL{Scheme: upstream.Scheme, Host: upstream.Host}
	if !cfg.On[AuthSessions] {
		for _, aggregate := range AuthAggregates[1:] {
			if cfg.On[aggregate] {
				return AuthProxyConfig{}, fmt.Errorf("%s needs %s: the refresh credentials it issues are rotated only by the sessions owner",
					AuthProxyFlagEnv(aggregate), AuthProxyFlagEnv(AuthSessions))
			}
		}
	}
	return cfg, nil
}

// sessionIssuer is the client of auth-service's session issue route when
// auth-service owns sessions, built from AUTH_SESSION_ISSUE_TOKEN; nil when
// go-api owns them. Sessions on without a usable token is a start-up error:
// every sign-in would answer 503.
func sessionIssuer(cfg AuthProxyConfig, lookup func(string) (string, bool)) (sessionissue.Issuer, error) {
	if !cfg.Enabled(AuthSessions) {
		return nil, nil
	}
	token, _ := lookup(sessionissue.EnvToken)
	client, err := sessionissue.New(cfg.Upstream, strings.TrimSpace(token), cfg.Timeout)
	if err != nil {
		return nil, fmt.Errorf("%s is on: %w", AuthProxyFlagEnv(AuthSessions), err)
	}
	return client, nil
}

// logAuthProxyRouting states, at Info, the routing go-api starts with: each
// aggregate on or off and the upstream host (never a credential, path or
// query), whatever set the flags.
func logAuthProxyRouting(ctx context.Context, logger *slog.Logger, cfg AuthProxyConfig) {
	host := ""
	if cfg.Upstream != nil {
		host = cfg.Upstream.Host
	}
	attrs := []slog.Attr{slog.String("upstream_host", host)}
	for _, aggregate := range AuthAggregates {
		attrs = append(attrs, slog.Bool(string(aggregate), cfg.Enabled(aggregate)))
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "auth proxy routing", attrs...)
}

// sessionProxyRoutes are the internal/api/session routes the proxy takes over
// through session.Deps.Inner, so go-api's guard, body checks, validator and
// rate limiter still run before a forward. Keyed "METHOD pattern".
var sessionProxyRoutes = map[string]AuthAggregate{
	"POST /api/v1/auth/refresh":             AuthSessions,
	"POST /api/v1/auth/logout":              AuthSessions,
	"POST /api/v1/auth/login":               AuthLocalLogin,
	"POST /api/v1/auth/register":            AuthCredentials,
	"GET /api/v1/auth/verify":               AuthCredentials,
	"POST /api/v1/auth/resend-verification": AuthCredentials,
	"POST /api/v1/auth/forgot-password":     AuthCredentials,
	"POST /api/v1/auth/reset-password":      AuthCredentials,
	"POST /api/v1/auth/change-password":     AuthCredentials,
	"POST /api/v1/auth/social-login":        AuthSocial,
}

// routeProxyRoutes are the internal/api/sso sign-in routes, forwarded through
// sso.Deps.Inner behind go-api's guard and its body checks.
var routeProxyRoutes = map[string]AuthAggregate{
	"POST /api/v1/auth/oidc/{provider_id}/authorize": AuthOIDC,
	"POST /api/v1/auth/oidc/{provider_id}/callback":  AuthOIDC,
	"GET /api/v1/auth/saml/{provider_id}/metadata":   AuthSAML,
	"POST /api/v1/auth/saml/{provider_id}/initiate":  AuthSAML,
	"POST /api/v1/auth/saml/{provider_id}/acs":       AuthSAML,
}

// patternAggregate is both maps by pattern alone: every mapped pattern has
// one method.
var patternAggregate = func() map[string]AuthAggregate {
	out := map[string]AuthAggregate{}
	for _, routes := range []map[string]AuthAggregate{sessionProxyRoutes, routeProxyRoutes} {
		for key, aggregate := range routes {
			_, pattern, _ := strings.Cut(key, " ")
			out[pattern] = aggregate
		}
	}
	return out
}()

const (
	outcomeForwarded     = "forwarded"
	outcomeLocal         = "local"
	outcomeUpstreamError = "upstream_error"
)

var (
	authProxyMeter    = otel.Meter("github.com/full-chaos/dev-health-ops/internal/apiservice/authproxy")
	authProxyRequests = func() metric.Int64Counter {
		counter, err := authProxyMeter.Int64Counter("devhealth_api_auth_proxy_requests_total",
			metric.WithDescription("Requests to the proxied /api/v1/auth/* routes, by aggregate and outcome (forwarded, local, upstream_error)."))
		if err != nil {
			counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_api_auth_proxy_requests_total")
		}
		return counter
	}()
	authProxyDuration = func() metric.Float64Histogram {
		histogram, err := authProxyMeter.Float64Histogram("devhealth_api_auth_proxy_upstream_duration_seconds",
			metric.WithDescription("Time to the end of one auth-service exchange, by aggregate."))
		if err != nil {
			histogram, _ = otel.GetMeterProvider().Meter("noop").Float64Histogram("devhealth_api_auth_proxy_upstream_duration_seconds")
		}
		return histogram
	}()
)

// hopHeaders are never forwarded in either direction (RFC 9110 §7.6.1).
var hopHeaders = []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer",
	"Transfer-Encoding", "Upgrade"}

// clientAddressHeaders are dropped from the inbound request, with every
// X-Forwarded-* and X-Original-* field: auth-service learns the client only
// from the X-Forwarded-For go-api writes.
var clientAddressHeaders = []string{"Forwarded", "X-Real-Ip", "X-Client-Ip", "True-Client-Ip", "Cf-Connecting-Ip", "X-Cluster-Client-Ip",
	"Fastly-Client-Ip"}

var clientAddressPrefixes = []string{"X-Forwarded-", "X-Original-"}

// upstreamRequestIDHeader carries auth-service's own server-minted request id.
const upstreamRequestIDHeader = "X-Auth-Request-ID"

type authProxy struct {
	cfg       AuthProxyConfig
	logger    *slog.Logger
	transport http.RoundTripper
}

func newAuthProxy(cfg AuthProxyConfig, logger *slog.Logger) *authProxy {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultAuthProxyTimeout
	}
	// No proxy from the environment, no redirect following (a RoundTripper
	// follows none), and no shared connection pool with other clients.
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &authProxy{cfg: cfg, logger: logger, transport: transport}
}

// proxyState is one request's body as the client sent it and the outcome
// the forwarder decided; it travels from the route wrapper to the innermost
// handler through the context.
type proxyState struct {
	body    []byte
	outcome string
}

type proxyStateKey struct{}

// inner is session.Deps.Inner and sso.Deps.Inner: the innermost handler of a
// mapped route forwards when its aggregate's flag is on.
func (p *authProxy) inner(pattern string, local http.Handler) http.Handler {
	aggregate, mapped := patternAggregate[pattern]
	if !mapped || !p.cfg.Enabled(aggregate) {
		return local
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state, ok := r.Context().Value(proxyStateKey{}).(*proxyState)
		if !ok {
			// The route wrapper did not run: never fall back to the local handler.
			p.logger.ErrorContext(r.Context(), "auth proxy: forward without the route wrapper", slog.String("aggregate", string(aggregate)))
			policy.WriteInternal(w)
			return
		}
		p.forward(w, r, aggregate, state)
	})
}

// wrap puts the counting and body-keeping wrapper on every mapped route.
func (p *authProxy) wrap(routes []httpapi.Route) []httpapi.Route {
	for index := range routes {
		key := routes[index].Method + " " + routes[index].Pattern
		aggregate, ok := sessionProxyRoutes[key]
		if !ok {
			aggregate, ok = routeProxyRoutes[key]
		}
		if ok {
			routes[index].Handler = p.sessionRoute(aggregate, routes[index].Handler)
		}
	}
	return routes
}

// sessionRoute keeps the client's body bytes for the innermost forwarder and
// replays them to go-api's own guard, then counts the outcome.
func (p *authProxy) sessionRoute(aggregate AuthAggregate, local http.Handler) http.Handler {
	if !p.cfg.Enabled(aggregate) {
		return countLocal(aggregate, local)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := &proxyState{}
		raw, err := io.ReadAll(r.Body)
		state.body = raw
		// A read error (an over-bound body) replays after the bytes read, so
		// go-api's guard answers it exactly as it does with the flag off.
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(raw), errReader{err}))
		local.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), proxyStateKey{}, state)))
		outcome := state.outcome
		if outcome == "" {
			outcome = outcomeLocal
		}
		count(r.Context(), aggregate, outcome)
	})
}

func countLocal(aggregate AuthAggregate, local http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		local.ServeHTTP(w, r)
		count(r.Context(), aggregate, outcomeLocal)
	})
}

func count(ctx context.Context, aggregate AuthAggregate, outcome string) {
	authProxyRequests.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(
		attribute.String("aggregate", string(aggregate)), attribute.String("outcome", outcome)))
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) {
	if e.err == nil {
		return 0, io.EOF
	}
	return 0, e.err
}

// forward sends r to auth-service at the same path and writes its answer. A
// failure to get an answer is a refusal in go-api's shape: the request is
// never served by go-api instead.
func (p *authProxy) forward(w http.ResponseWriter, r *http.Request, aggregate AuthAggregate, state *proxyState) {
	ctx, cancel := context.WithTimeout(r.Context(), p.cfg.Timeout)
	defer cancel()
	requestID := httpapi.RequestIDFrom(r.Context())
	target := url.URL{Scheme: p.cfg.Upstream.Scheme, Host: p.cfg.Upstream.Host, Path: r.URL.Path, RawPath: r.URL.RawPath, RawQuery: r.URL.RawQuery}
	var body io.Reader = http.NoBody
	if len(state.body) > 0 {
		body = bytes.NewReader(state.body)
	}
	out, err := http.NewRequestWithContext(ctx, r.Method, target.String(), body)
	if err != nil {
		state.outcome = outcomeUpstreamError
		p.refuse(w, r, aggregate, http.StatusServiceUnavailable, "Authentication service unavailable", "build_request", err, "")
		return
	}
	out.Header = outboundHeader(r.Header)
	if address := clientip.FromRequest(r); address != "" {
		out.Header.Set("X-Forwarded-For", address)
	}
	if requestID != "" {
		out.Header.Set(httpapi.RequestIDHeader, requestID)
	}
	start := time.Now()
	response, err := p.transport.RoundTrip(out)
	if err != nil {
		authProxyDuration.Record(context.WithoutCancel(r.Context()), time.Since(start).Seconds(),
			metric.WithAttributes(attribute.String("aggregate", string(aggregate))))
		state.outcome = outcomeUpstreamError
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			p.refuse(w, r, aggregate, http.StatusGatewayTimeout, "Authentication service timed out", "timeout", err, "")
			return
		}
		p.refuse(w, r, aggregate, http.StatusServiceUnavailable, "Authentication service unavailable", "unreachable", err, "")
		return
	}
	defer response.Body.Close()
	upstreamID := loggableUpstreamID(response.Header.Get(upstreamRequestIDHeader))
	// The whole answer is read before any byte reaches the client, so an answer
	// cut short or over the bound is a refusal, never a partial body under the
	// upstream's status.
	answer, readErr := io.ReadAll(io.LimitReader(response.Body, maxAuthProxyResponseBytes+1))
	authProxyDuration.Record(context.WithoutCancel(r.Context()), time.Since(start).Seconds(),
		metric.WithAttributes(attribute.String("aggregate", string(aggregate))))
	if readErr == nil && len(answer) > maxAuthProxyResponseBytes {
		readErr = fmt.Errorf("answer exceeds %d bytes", maxAuthProxyResponseBytes)
	}
	if readErr != nil {
		state.outcome = outcomeUpstreamError
		if errors.Is(readErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			p.refuse(w, r, aggregate, http.StatusGatewayTimeout, "Authentication service timed out", "timeout", readErr, upstreamID)
			return
		}
		p.refuse(w, r, aggregate, http.StatusBadGateway, "Authentication service unavailable", "incomplete_answer", readErr, upstreamID)
		return
	}
	copyResponseHeader(w.Header(), response.Header)
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotModified {
		w.Header().Set("Content-Length", strconv.Itoa(len(answer)))
	}
	w.WriteHeader(response.StatusCode)
	if _, err := io.Copy(w, bytes.NewReader(answer)); err != nil {
		p.logger.WarnContext(r.Context(), "auth proxy: client went away",
			slog.String("aggregate", string(aggregate)), slog.String("route", r.Pattern),
			slog.String("request_id", httpapi.LoggableRequestID(r.Context())), slog.String("error", err.Error()))
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		// auth-service wrote the route's model; go-api's writer check reads it so.
		policy.MarkModelBody(w)
	}
	state.outcome = outcomeForwarded
	p.logger.InfoContext(r.Context(), "auth proxy forwarded",
		slog.String("aggregate", string(aggregate)), slog.String("route", r.Pattern),
		slog.Int("status", response.StatusCode), slog.String("request_id", httpapi.LoggableRequestID(r.Context())), slog.String("upstream_request_id", upstreamID),
		slog.Duration("duration", time.Since(start)))
}

func (p *authProxy) refuse(w http.ResponseWriter, r *http.Request, aggregate AuthAggregate, status int, detail, reason string, err error, upstreamID string) {
	p.logger.ErrorContext(r.Context(), "auth proxy: upstream failed; refused",
		slog.String("aggregate", string(aggregate)), slog.String("route", r.Pattern),
		slog.String("reason", reason), slog.Int("status", status), slog.String("request_id", httpapi.LoggableRequestID(r.Context())),
		slog.String("upstream_request_id", upstreamID), slog.String("error", upstreamErrorText(err)))
	policy.WriteDetail(w, status, detail, nil)
}

// upstreamRequestIDPattern is auth-service's request id wire form.
var upstreamRequestIDPattern = regexp.MustCompile(`^req_[A-Za-z0-9_-]{26}$`)

// loggableUpstreamID is auth-service's request id when it has the wire form,
// "invalid" for any other non-empty value.
func loggableUpstreamID(id string) string {
	if id == "" || upstreamRequestIDPattern.MatchString(id) {
		return id
	}
	return "invalid"
}

// upstreamErrorText is err without the request URL a *url.Error carries: the
// URL holds the client's path and query.
func upstreamErrorText(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Op + ": " + urlErr.Err.Error()
	}
	return err.Error()
}

// outboundHeader is the client's header minus hop-by-hop fields (including
// those Connection names), client-address claims and the request id, which
// forward sets itself.
func outboundHeader(in http.Header) http.Header {
	out := in.Clone()
	dropHop(out)
	for _, name := range clientAddressHeaders {
		out.Del(name)
	}
	for name := range out {
		canonical := textproto.CanonicalMIMEHeaderKey(name)
		for _, prefix := range clientAddressPrefixes {
			if strings.HasPrefix(canonical, prefix) {
				delete(out, name)
			}
		}
	}
	out.Del(httpapi.RequestIDHeader)
	out.Del("Host")
	return out
}

// copyResponseHeader copies auth-service's answer headers, except hop-by-hop
// fields, its correlation ids, its CORS fields (go-api's own CORS decides),
// and any field go-api's middleware already set on this response.
func copyResponseHeader(dst, src http.Header) {
	src = src.Clone()
	dropHop(src)
	src.Del(upstreamRequestIDHeader)
	src.Del(httpapi.RequestIDHeader)
	preset := map[string]bool{}
	for name := range dst {
		preset[textproto.CanonicalMIMEHeaderKey(name)] = true
	}
	for name, values := range src {
		canonical := textproto.CanonicalMIMEHeaderKey(name)
		if strings.HasPrefix(canonical, "Access-Control-") || preset[canonical] {
			continue
		}
		for _, value := range values {
			dst.Add(canonical, value)
		}
	}
}

func dropHop(header http.Header) {
	for _, field := range header.Values("Connection") {
		for _, name := range strings.Split(field, ",") {
			if name = strings.TrimSpace(name); name != "" {
				header.Del(name)
			}
		}
	}
	for _, name := range hopHeaders {
		header.Del(name)
	}
}
