package pushcli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// userAgent is http_client.py's USER_AGENT.
const userAgent = "dev-hops-push-cli"

// The retry budget of http_client.py's _RETRY_KWARGS: five attempts, the wait
// between them 1, 2, 4, 8 seconds (never above 30), or the server's Retry-After
// clamped to 30 seconds.
const (
	retryAttempts     = 5
	retryInitialDelay = 1.0
	retryMaxDelay     = 30.0
	retryBackoff      = 2.0
	requestTimeout    = 30 * time.Second
	schemaTimeout     = 10 * time.Second
	errorTextLimit    = 500
)

// clientConfig is IngestClientConfig.
type clientConfig struct{ apiURL, token, orgID string }

// apiError is IngestApiError: a 4xx contract error (or a 5xx other than 503) with
// the server's parsed {"error": {...}} envelope.
type apiError struct {
	status  int
	code    string
	message string
	errs    []pyjson.Value
}

func (e *apiError) Error() string { return e.message }

// transientError is IngestTransientError: a network error, a 429, or any 503.
type transientError struct {
	message    string
	retryAfter *float64
}

func (e *transientError) Error() string { return e.message }

// crashError stands for a Python traceback: the server answered with a shape the
// verb's code cannot handle (a body that is not JSON, a list where an object is
// read). The verb exits 1; the exact text is not Python's.
type crashError struct{ detail string }

func (e *crashError) Error() string { return e.detail }

func crash(format string, args ...any) *crashError {
	return &crashError{detail: fmt.Sprintf(format, args...)}
}

// spaceClass is the set re's \s matches in a str pattern (str.isspace).
const spaceClass = "\\t\\n\\v\\f\\r\\x1c-\\x20\\x{85}\\x{a0}\\x{1680}\\x{2000}-\\x{200a}\\x{2028}\\x{2029}\\x{202f}\\x{205f}\\x{3000}"

var (
	tokenPattern  = regexp.MustCompile(`fcpush_[A-Za-z0-9_-]+`)
	bearerPattern = regexp.MustCompile(`(?i)bearer[` + spaceClass + `]+[^` + spaceClass + `]+`)
)

// redactSecrets is redact_secrets: a token of the real fcpush_ shape and any
// bearer credential are replaced before server-supplied text is logged or printed.
func redactSecrets(text string) string {
	text = tokenPattern.ReplaceAllString(text, "fcpush_[REDACTED]")
	return bearerPattern.ReplaceAllString(text, "Bearer [REDACTED]")
}

// redactValue is _redact_value: every string anywhere in the value, dict keys
// included.
func redactValue(value pyjson.Value) pyjson.Value {
	switch typed := value.(type) {
	case string:
		return redactSecrets(typed)
	case *pyjson.Object:
		out := pyjson.NewObject()
		for _, key := range typed.Keys() {
			item, _ := typed.Get(key)
			out.Set(redactSecrets(key), redactValue(item))
		}
		return out
	case []pyjson.Value:
		out := make([]pyjson.Value, len(typed))
		for index, item := range typed {
			out[index] = redactValue(item)
		}
		return out
	}
	return value
}

// response is what an attempt got back.
type response struct {
	status int
	reason string
	header http.Header
	body   []byte
}

// text is response.text[:500]'s source: the body decoded as UTF-8 with
// replacement.
func (r *response) text() string { return pythonparity.DecodeUTF8Replace(string(r.body)) }

// json is response.json(): nil, false when the body is not JSON.
func (r *response) json() (pyjson.Value, bool) {
	value, err := pyjson.Decode(r.body)
	if err != nil {
		return nil, false
	}
	return value, true
}

// parseRetryAfter is _parse_retry_after: the delta-seconds form only, clamped to
// the CLI's own maximum delay; anything else is nil.
func parseRetryAfter(header http.Header) *float64 {
	values := header.Values("Retry-After")
	if len(values) == 0 {
		return nil
	}
	seconds, ok := pythonparity.ParseFloat(strings.Join(values, ", "))
	if !ok || math.IsInf(seconds, 0) || math.IsNaN(seconds) || seconds < 0 {
		return nil
	}
	seconds = math.Min(seconds, retryMaxDelay)
	return &seconds
}

// parseErrorEnvelope is parse_error_envelope: the {"error": {code, message,
// errors}} shape, or a generic code with the body's first 500 characters; every
// extracted string is redacted.
func parseErrorEnvelope(resp *response) (code, message string, errs []pyjson.Value) {
	body, _ := resp.json()
	if object, ok := body.(*pyjson.Object); ok {
		if errorValue, present := object.Get("error"); present {
			if inner, isObject := errorValue.(*pyjson.Object); isObject {
				codeValue, hasCode := inner.Get("code")
				if !hasCode {
					codeValue = "unknown_error"
				}
				code = redactSecrets(pyjson.Str(codeValue))
				messageValue, _ := inner.Get("message")
				if pyjson.Truthy(messageValue) {
					message = redactSecrets(pyjson.Str(messageValue))
				} else {
					message = redactSecrets(pythonparity.TruncateRunes(resp.text(), errorTextLimit))
				}
				errs = []pyjson.Value{}
				if rawErrors, ok := inner.Get("errors"); ok {
					if list, isList := rawErrors.([]pyjson.Value); isList {
						for _, item := range list {
							if _, isObject := item.(*pyjson.Object); isObject {
								errs = append(errs, redactValue(item))
							}
						}
					}
				}
				return code, message, errs
			}
		}
	}
	fallback := pythonparity.TruncateRunes(resp.text(), errorTextLimit)
	if fallback == "" {
		fallback = resp.reason
	}
	return "unknown_error", redactSecrets(fallback), []pyjson.Value{}
}

// raiseForResponse is _raise_for_response.
func raiseForResponse(resp *response) error {
	if resp.status < 400 {
		return nil
	}
	code, message, errs := parseErrorEnvelope(resp)
	if resp.status == http.StatusTooManyRequests || resp.status == http.StatusServiceUnavailable {
		return &transientError{message: fmt.Sprintf("%d %s: %s", resp.status, code, message), retryAfter: parseRetryAfter(resp.header)}
	}
	return &apiError{status: resp.status, code: code, message: message, errs: errs}
}

// ingestClient is the httpx.AsyncClient of the push verbs: no redirects followed,
// a 30 second limit on connecting and on waiting for the answer.
type ingestClient struct {
	http  *http.Client
	sleep func(seconds float64)
	// readTimeout is how long a response may stay silent (headers, then each read
	// of the body) before the attempt fails as a network error.
	readTimeout time.Duration
}

func newIngestClient() *ingestClient { return newIngestClientWithReadTimeout(requestTimeout) }

func newIngestClientWithReadTimeout(readTimeout time.Duration) *ingestClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: requestTimeout}).DialContext
	transport.TLSHandshakeTimeout = requestTimeout
	transport.ResponseHeaderTimeout = readTimeout
	return &ingestClient{
		readTimeout: readTimeout,
		http: &http.Client{
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		sleep: func(seconds float64) { pause(seconds) },
	}
}

// pause waits for the given seconds; a test replaces it to skip the retry waits.
var pause = func(seconds float64) { time.Sleep(secondsToDuration(seconds)) }

// secondsToDuration is a wait in seconds as a Duration, never overflowing.
func secondsToDuration(seconds float64) time.Duration {
	if seconds <= 0 || math.IsNaN(seconds) {
		return 0
	}
	if seconds >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds * float64(time.Second))
}

// schemeProblem is the text httpx raises for a URL it cannot send to, "" when the
// scheme is usable.
func schemeProblem(scheme string) string {
	switch scheme {
	case "http", "https":
		return ""
	case "":
		return "Request URL is missing an 'http://' or 'https://' protocol."
	}
	return fmt.Sprintf("Request URL has an unsupported protocol '%s://'.", scheme)
}

// bytesRepr is repr() of an ASCII bytes value.
func bytesRepr(value string) string {
	quote := byte('\'')
	if strings.Contains(value, "'") && !strings.Contains(value, `"`) {
		quote = '"'
	}
	var out strings.Builder
	out.WriteString("b")
	out.WriteByte(quote)
	for index := 0; index < len(value); index++ {
		c := value[index]
		switch {
		case c == quote || c == '\\':
			out.WriteByte('\\')
			out.WriteByte(c)
		case c == '\t':
			out.WriteString(`\t`)
		case c == '\n':
			out.WriteString(`\n`)
		case c == '\r':
			out.WriteString(`\r`)
		case c < 0x20 || c >= 0x7f:
			fmt.Fprintf(&out, `\x%02x`, c)
		default:
			out.WriteByte(c)
		}
	}
	out.WriteByte(quote)
	return out.String()
}

// checkHeadersASCII is what building the request does to its headers: a value
// with a character outside ASCII cannot be encoded, and the client raises before
// anything is sent (a crash).
func checkHeadersASCII(headers [][2]string) error {
	for _, header := range headers {
		for index := 0; index < len(header[1]); index++ {
			if header[1][index] >= 0x80 {
				return crash("the %s header value has a character outside ASCII", header[0])
			}
		}
	}
	return nil
}

// checkHeadersH11 is what writing the request does: a value h11 finds illegal (a
// control character, a leading or trailing space) is a protocol error, retried
// like any network failure.
func checkHeadersH11(headers [][2]string) error {
	for _, header := range headers {
		value := header[1]
		illegal := false
		for index := 0; index < len(value); index++ {
			c := value[index]
			if c < 0x20 && c != '\t' || c == 0x7f {
				illegal = true
			}
		}
		if value != "" && (value[0] == ' ' || value[0] == '\t' || value[len(value)-1] == ' ' || value[len(value)-1] == '\t') {
			illegal = true
		}
		if illegal {
			return &transientError{message: "transport error: Illegal header value " + bytesRepr(value)}
		}
	}
	return nil
}

// withBasicAuth is httpx's BasicAuth flow: the userinfo of the URL replaces the
// Authorization header the request carries.
func withBasicAuth(headers [][2]string, basic string) [][2]string {
	if basic == "" {
		return headers
	}
	out := make([][2]string, 0, len(headers)+1)
	replaced := false
	for _, header := range headers {
		if strings.EqualFold(header[0], "Authorization") {
			out = append(out, [2]string{"Authorization", basic})
			replaced = true
			continue
		}
		out = append(out, header)
	}
	if !replaced {
		out = append(out, [2]string{"Authorization", basic})
	}
	return out
}

// stallReader is httpx's per-read timeout on a response body: a read that gets
// no bytes for the timeout ends the request (its context is cancelled) with
// errReadTimeout.
type stallReader struct {
	body    io.Reader
	timeout time.Duration
	cancel  context.CancelFunc
}

var errReadTimeout = errors.New("read timeout")

func (r *stallReader) Read(p []byte) (int, error) {
	var fired atomic.Bool
	timer := time.AfterFunc(r.timeout, func() { fired.Store(true); r.cancel() })
	n, err := r.body.Read(p)
	timer.Stop()
	if err != nil && err != io.EOF && fired.Load() {
		return n, errReadTimeout
	}
	return n, err
}

// attempt is one request, without retries. A network failure is a
// *transientError; a status of 400 or more is raised as raiseForResponse does.
func (c *ingestClient) attempt(ctx context.Context, method, target string, headers [][2]string, body []byte) (*response, error) {
	parsed, err := parseHTTPXURL(target)
	if err != nil {
		return nil, err
	}
	if err := checkHeadersASCII(headers); err != nil {
		return nil, err
	}
	headers = withBasicAuth(headers, parsed.basic)
	if err := checkHeadersH11(headers); err != nil {
		var transient *transientError
		if errors.As(err, &transient) {
			if problem := schemeProblem(parsed.scheme); problem != "" {
				return nil, &transientError{message: "transport error: " + problem}
			}
		}
		return nil, err
	}
	if problem := schemeProblem(parsed.scheme); problem != "" {
		return nil, &transientError{message: "transport error: " + problem}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://placeholder/", reader)
	if err != nil {
		return nil, crash("invalid request: %v", err)
	}
	proxy, _ := http.ProxyFromEnvironment(req)
	req.URL = parsed.requestURL(proxy != nil)
	req.Host = ""
	for _, header := range headers {
		req.Header.Set(header[0], header[1])
	}
	resp, err := c.http.Do(req)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		// httpx's ReadTimeout has no text: waiting for the answer for the whole
		// limit is "transport error: " and nothing after it.
		if strings.Contains(err.Error(), "timeout awaiting response headers") {
			return nil, &transientError{message: "transport error: "}
		}
		return nil, &transientError{message: "transport error: " + err.Error()}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(&stallReader{body: resp.Body, timeout: c.readTimeout, cancel: cancel})
	if err != nil {
		if errors.Is(err, errReadTimeout) {
			return nil, &transientError{message: "transport error: "}
		}
		return nil, &transientError{message: "transport error: " + err.Error()}
	}
	reason := strings.TrimSpace(strings.TrimPrefix(resp.Status, fmt.Sprintf("%d", resp.StatusCode)))
	if reason == "" {
		reason = http.StatusText(resp.StatusCode)
	}
	result := &response{status: resp.StatusCode, reason: reason, header: resp.Header, body: data}
	if err := raiseForResponse(result); err != nil {
		return nil, err
	}
	return result, nil
}

// request is _request under retry_with_backoff: only a *transientError is
// retried, five attempts in all; the last failure is returned.
func (c *ingestClient) request(ctx context.Context, method, target string, headers [][2]string, body []byte) (*response, error) {
	delay := retryInitialDelay
	var last error
	for attempt := 0; attempt < retryAttempts; attempt++ {
		resp, err := c.attempt(ctx, method, target, headers, body)
		var transient *transientError
		if !errors.As(err, &transient) {
			return resp, err
		}
		last = err
		if attempt >= retryAttempts-1 {
			slog.Error("all attempts failed; giving up", "op", method, "attempts", retryAttempts)
			break
		}
		slog.Warn("attempt failed; retrying", "op", method, "attempt", attempt+1, "of", retryAttempts, "error", err.Error())
		if transient.retryAfter != nil {
			c.sleep(math.Max(0, *transient.retryAfter))
		} else {
			wait := math.Min(delay, retryMaxDelay)
			delay *= retryBackoff
			c.sleep(wait)
		}
	}
	return nil, last
}

func authHeaders(config clientConfig) [][2]string {
	return [][2]string{
		{"Authorization", "Bearer " + config.token},
		{"X-Org-Id", config.orgID},
		{"User-Agent", userAgent},
	}
}

// postBatch is post_batch: the status and the JSON body of the answer.
func (c *ingestClient) postBatch(ctx context.Context, config clientConfig, envelope []byte, idempotencyKey string) (int, pyjson.Value, error) {
	headers := append(authHeaders(config), [2]string{"Content-Type", "application/json"}, [2]string{"Idempotency-Key", idempotencyKey})
	resp, err := c.request(ctx, http.MethodPost, config.apiURL+"/api/v1/external-ingest/batches", headers, envelope)
	if err != nil {
		return 0, nil, err
	}
	body, ok := resp.json()
	if !ok {
		return 0, nil, crash("POST /batches answered with a body that is not JSON (HTTP %d)", resp.status)
	}
	return resp.status, body, nil
}

// getBatchStatus is get_batch_status.
func (c *ingestClient) getBatchStatus(ctx context.Context, config clientConfig, ingestionID string) (pyjson.Value, error) {
	resp, err := c.request(ctx, http.MethodGet, config.apiURL+"/api/v1/external-ingest/batches/"+ingestionID, authHeaders(config), nil)
	if err != nil {
		return nil, err
	}
	body, ok := resp.json()
	if !ok {
		return nil, crash("GET /batches answered with a body that is not JSON (HTTP %d)", resp.status)
	}
	return body, nil
}

// schemaDocument is get_schema_document: GET /schemas (public, one try, ten
// seconds) for its limits; nil on any failure, a non-2xx answer, or a body that
// is not a JSON object.
func (c *ingestClient) schemaDocument(ctx context.Context, apiURL string) *pyjson.Object {
	parsed, err := parseHTTPXURL(apiURL + "/api/v1/external-ingest/schemas")
	if err != nil || schemeProblem(parsed.scheme) != "" {
		slog.Debug("GET /schemas limits pre-check failed; using local defaults")
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, schemaTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://placeholder/", nil)
	if err != nil {
		return nil
	}
	proxy, _ := http.ProxyFromEnvironment(req)
	req.URL = parsed.requestURL(proxy != nil)
	req.Host = ""
	if parsed.basic != "" {
		req.Header.Set("Authorization", parsed.basic)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		slog.Debug("GET /schemas limits pre-check failed; using local defaults")
		return nil
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Debug("GET /schemas limits pre-check failed; using local defaults")
		return nil
	}
	body, err := pyjson.Decode(data)
	if err != nil {
		return nil
	}
	object, _ := body.(*pyjson.Object)
	return object
}
