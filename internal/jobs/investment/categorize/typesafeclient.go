package categorize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// TypeSafeClient sends one TypeSafe "System One" (Jev) evaluation request:
// POST https://api.typesafe.ai/v1/systemone with {model, state, questions}.
//
// It is a transport, not a categorizer, and it does NOT implement Provider:
// Jev returns typed answers (probabilities), not completion text, so there is
// nothing for CompletionRequest / the repair loop to carry. The decision
// adapter (a BundleCompleter) builds the body, calls SystemOne, and parses the
// answers from SystemOneResult.Body itself. Nothing in the served path
// constructs this client (ProviderKindTypeSafe is not in
// goImplementedProviderKinds).
//
// What is shared with OpenAIProvider (same code, not a copy): the hardened
// HTTP client (no redirects, no ambient proxy, bounded timeout), the
// no-redirect guard on the credentialed request, error classification
// (classifyProviderError), retryability (isRetryable), the exponential backoff
// (retryDelay), the Retry-After reader (retryAfterFromHeader, capped at 60 s),
// the sleep that honours cancellation (sleepForRetry), and the redaction of
// the key from every printed form. What is new: the request/response wire,
// the status-first class mapping for 402/403, the bounded response read, and
// per-attempt latency / request-id accounting.
type TypeSafeClient struct {
	cfg    TypeSafeClientConfig
	client *http.Client
	logger *slog.Logger
	// sleep waits out a retry backoff; false means ctx ended first. Tests
	// replace it; production uses sleepForRetry.
	sleep func(ctx context.Context, d time.Duration) bool
}

// TypeSafeClientConfig configures TypeSafeClient.
type TypeSafeClientConfig struct {
	APIKey secrets.Hidden `json:"-"`
	// BaseURL must be https://api.typesafe.ai (no port, userinfo, path or
	// query) so a key cannot leave for another host. Empty selects it. Any
	// other value is refused unless UnsafeAllowAnyBaseURLForTest is set.
	BaseURL string
	// Model must be a versioned id (jev-<major>.<minor>.<patch>); the moving
	// aliases jev-latest and jev-preview are refused. Empty selects
	// DefaultTypeSafeModel.
	Model string
	// Timeout bounds one HTTP attempt. Zero keeps the shared hardened client's
	// 60 s.
	Timeout    time.Duration
	HTTPClient *http.Client
	Logger     *slog.Logger
	// UnsafeAllowAnyBaseURLForTest lifts the host rule so a test can point the
	// client at an httptest server. It is a struct field, never an
	// environment variable: no deployment can switch it on.
	UnsafeAllowAnyBaseURLForTest bool
}

const (
	// DefaultTypeSafeBaseURL is the only host the client talks to.
	DefaultTypeSafeBaseURL = "https://api.typesafe.ai"
	// DefaultTypeSafeModel is the pinned Jev model of the evaluated design.
	DefaultTypeSafeModel = "jev-1.13.0"

	typeSafeSystemOnePath = "/v1/systemone"
	// typeSafeMaxRetries is 1 retry (2 attempts), the incumbent's
	// openAIMaxRetries.
	typeSafeMaxRetries = 1
	// maxSystemOneResponseBytes bounds one response read. A real answer set
	// is about 10 KB; this stops a hostile or broken peer from filling memory.
	maxSystemOneResponseBytes = 4 << 20
	typeSafeRequestIDHeader   = "x-typesafe-request-id"
	maxLoggedRequestIDLen     = 64
)

var typeSafeModelPattern = regexp.MustCompile(`^jev-[0-9]+\.[0-9]+\.[0-9]+$`)

// requestIDSafe keeps a peer-supplied request id loggable: id-shaped bytes
// only, bounded.
var requestIDUnsafe = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// NewTypeSafeClient validates cfg and builds the client. It sends nothing.
func NewTypeSafeClient(cfg TypeSafeClientConfig) (*TypeSafeClient, error) {
	if cfg.APIKey.Reveal() == "" {
		return nil, errors.New("typesafe client: an API key is required")
	}
	if cfg.Model == "" {
		cfg.Model = DefaultTypeSafeModel
	}
	if !typeSafeModelPattern.MatchString(cfg.Model) {
		// jev-latest and jev-preview move on a release; a pinned id keeps the
		// evidence and the stamp tied to one model.
		return nil, errors.New("typesafe client: model must be a versioned id such as jev-1.13.0 (jev-latest and jev-preview are refused)")
	}
	base, err := validateTypeSafeBaseURL(cfg.BaseURL, cfg.UnsafeAllowAnyBaseURLForTest)
	if err != nil {
		return nil, err
	}
	cfg.BaseURL = base
	client := cfg.HTTPClient
	if client == nil {
		client = newHardenedHTTPClient()
		if cfg.Timeout > 0 {
			client.Timeout = cfg.Timeout
		}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &TypeSafeClient{cfg: cfg, client: client, logger: logger, sleep: sleepForRetry}, nil
}

func validateTypeSafeBaseURL(raw string, allowAny bool) (string, error) {
	if raw == "" {
		return DefaultTypeSafeBaseURL, nil
	}
	trimmed := trimBaseURL(raw)
	if allowAny {
		return trimmed, nil
	}
	parsed, err := url.Parse(trimmed)
	// Only the shape is reported, never the configured value: it is an
	// operator-supplied URL and may carry a credential.
	refuse := errors.New("typesafe client: base URL must be " + DefaultTypeSafeBaseURL)
	if err != nil || parsed.Scheme != "https" || !strings.EqualFold(parsed.Host, "api.typesafe.ai") ||
		parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", refuse
	}
	return DefaultTypeSafeBaseURL, nil
}

// NewTypeSafeClientFromEnv builds the client from TYPESAFE_API_KEY,
// TYPESAFE_BASE_URL and TYPESAFE_MODEL, read through secrets.GetenvNamed (the
// key is registered for redaction). requestedModel, when non-empty, wins over
// TYPESAFE_MODEL. The generic LLM_MODEL / LLM_API_KEY / LLM_BASE_URL overrides
// are deliberately NOT read: they steer the served provider, and a shadow
// client built beside it must not inherit the served model or key.
func NewTypeSafeClientFromEnv(requestedModel string, logger *slog.Logger) (*TypeSafeClient, error) {
	apiKey := firstNonEmptyEnv("TYPESAFE_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("LLM provider %q is not configured: set TYPESAFE_API_KEY", ProviderKindTypeSafe)
	}
	model := requestedModel
	if model == "" {
		model = firstNonEmptyEnv("TYPESAFE_MODEL")
	}
	return NewTypeSafeClient(TypeSafeClientConfig{
		APIKey:  secrets.NewHidden(apiKey),
		BaseURL: firstNonEmptyEnv("TYPESAFE_BASE_URL"),
		Model:   model,
		Logger:  logger,
	})
}

// Model is the pinned model this client was built to request.
func (c *TypeSafeClient) Model() string { return c.cfg.Model }

// Close releases idle connections.
func (c *TypeSafeClient) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

// SystemOneRequest is the documented wire: {model, state, questions}. State
// and Questions are pre-rendered JSON so the caller owns key order (a choice
// question's option order is part of the evaluated behaviour) and the client
// never re-encodes them. Model empty means the client's model.
type SystemOneRequest struct {
	Model     string
	State     json.RawMessage
	Questions json.RawMessage
}

// Body renders the request as `{"model":..,"state":..,"questions":..}` with no
// HTML escaping.
func (r SystemOneRequest) Body(defaultModel string) ([]byte, error) {
	model := r.Model
	if model == "" {
		model = defaultModel
	}
	if !json.Valid(r.State) {
		return nil, errors.New("typesafe request: state is not valid JSON")
	}
	if !json.Valid(r.Questions) {
		return nil, errors.New("typesafe request: questions is not valid JSON")
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(model); err != nil {
		return nil, err
	}
	modelJSON := bytes.TrimRight(buf.Bytes(), "\n")
	var out bytes.Buffer
	out.WriteString(`{"model":`)
	out.Write(modelJSON)
	out.WriteString(`,"state":`)
	out.Write(r.State)
	out.WriteString(`,"questions":`)
	out.Write(r.Questions)
	out.WriteByte('}')
	return out.Bytes(), nil
}

// SystemOneUsage is the documented usage object. Reported is false when the
// response had none: a caller must not price such a response as zero tokens.
type SystemOneUsage struct {
	InputTokens  int64
	OutputTokens int64
	Reported     bool
}

// SystemOneAttempt describes one HTTP attempt. The slice is at most
// typeSafeMaxRetries+1 long.
type SystemOneAttempt struct {
	// StatusCode is 0 when no response arrived.
	StatusCode int
	// Class is "" for the attempt that succeeded, else a SystemOneClass.
	Class     string
	RequestID string
	// Latency is the request time of this attempt, not the wait before it.
	Latency time.Duration
	// WaitBefore is the backoff slept before this attempt (0 for the first).
	WaitBefore time.Duration
}

// SystemOneResult is one successful exchange. Body is the raw 200 body: the
// caller parses `answers` itself (duplicate-id and per-answer validity rules
// need the bytes), so the shipped parser and the evaluated parser can be one
// function.
type SystemOneResult struct {
	Body       []byte
	Model      string
	Usage      SystemOneUsage
	RequestID  string
	StatusCode int
	// Header is the header of the final 200 response.
	Header   http.Header
	Attempts []SystemOneAttempt
}

// AttemptCount is the number of HTTP requests sent.
func (r SystemOneResult) AttemptCount() int { return len(r.Attempts) }

// Latency is the request time of all attempts, backoff excluded.
func (r SystemOneResult) Latency() time.Duration { return attemptsLatency(r.Attempts) }

// RetryWait is the backoff slept between attempts.
func (r SystemOneResult) RetryWait() time.Duration { return attemptsRetryWait(r.Attempts) }

func attemptsLatency(attempts []SystemOneAttempt) (total time.Duration) {
	for _, a := range attempts {
		total += a.Latency
	}
	return total
}

func attemptsRetryWait(attempts []SystemOneAttempt) (total time.Duration) {
	for _, a := range attempts {
		total += a.WaitBefore
	}
	return total
}

// SystemOneClass is the closed set of failure classes a caller can switch on
// (and use as a bounded metric label).
type SystemOneClass string

const (
	SystemOneClassAuth          SystemOneClass = "auth"            // 401, 402, 403: stop; do not retry
	SystemOneClassRateLimit     SystemOneClass = "rate_limit"      // 429, retried once
	SystemOneClassServer        SystemOneClass = "server"          // 529 and 5xx, retried once
	SystemOneClassInvalid       SystemOneClass = "invalid_request" // 400, 422: not retried
	SystemOneClassTimeout       SystemOneClass = "timeout"         // retried once
	SystemOneClassTransport     SystemOneClass = "transport"       // retried once
	SystemOneClassUnexpected    SystemOneClass = "unexpected_status"
	SystemOneClassTooLarge      SystemOneClass = "response_too_large"
	SystemOneClassDecode        SystemOneClass = "decode" // 200 with a body that is not a JSON object
	SystemOneClassCanceled      SystemOneClass = "canceled"
	SystemOneClassBuildRequest  SystemOneClass = "build_request"
	SystemOneClassBodyReadError SystemOneClass = "body_read"
)

// SystemOneError is a failed exchange. It carries no request or response
// text. It unwraps to the package's *llmError, so ClassifyLLMError works, and
// a "canceled" error unwraps to the context error.
type SystemOneError struct {
	Class      SystemOneClass
	StatusCode int
	RequestID  string
	Attempts   []SystemOneAttempt
	cause      error
}

func (e *SystemOneError) Error() string {
	text := "typesafe systemone request failed: " + string(e.Class)
	if e.StatusCode != 0 {
		text += fmt.Sprintf(" status=%d", e.StatusCode)
	}
	if e.RequestID != "" {
		text += " request_id=" + e.RequestID
	}
	return fmt.Sprintf("%s attempts=%d", text, len(e.Attempts))
}

func (e *SystemOneError) Unwrap() error { return e.cause }

// StopsArm reports a class a caller should treat as "stop sending": a bad or
// unpaid key will fail every following request the same way.
func (e *SystemOneError) StopsArm() bool { return e.Class == SystemOneClassAuth }

// SystemOne renders req and sends it.
func (c *TypeSafeClient) SystemOne(ctx context.Context, req SystemOneRequest) (SystemOneResult, error) {
	body, err := req.Body(c.cfg.Model)
	if err != nil {
		return SystemOneResult{}, &SystemOneError{Class: SystemOneClassBuildRequest, cause: err}
	}
	return c.SendBody(ctx, body)
}

// SendBody sends a pre-rendered request body byte for byte. The decision
// adapter uses it so the bytes on the wire are the bytes the evaluation sent.
func (c *TypeSafeClient) SendBody(ctx context.Context, body []byte) (SystemOneResult, error) {
	return c.send(ctx, body, false)
}

// PostSystemOne is the transport the decision adapter calls: it posts one
// pre-rendered body and returns the body and header of the final 200 response.
// Unlike SendBody it does not judge the 200 body: a body that is not JSON is
// returned as it is, for the caller to classify (the adapter's
// request_failed:not_json). Every other end is a *SystemOneError, which unwraps
// to the package's *llmError (so FailureClass and IsDeterministicFailure work),
// or, for a cancel or deadline, to the context error.
func (c *TypeSafeClient) PostSystemOne(ctx context.Context, body []byte) ([]byte, http.Header, error) {
	res, err := c.send(ctx, body, true)
	if err != nil {
		return nil, nil, err
	}
	return res.Body, res.Header, nil
}

func (c *TypeSafeClient) send(ctx context.Context, body []byte, lenient bool) (SystemOneResult, error) {
	if !json.Valid(body) {
		return SystemOneResult{}, &SystemOneError{Class: SystemOneClassBuildRequest, cause: errors.New("body is not valid JSON")}
	}
	var attempts []SystemOneAttempt
	var wait time.Duration
	for attempt := 0; attempt <= typeSafeMaxRetries; attempt++ {
		result, att, err := c.once(ctx, body, lenient)
		att.WaitBefore = wait
		attempts = append(attempts, att)
		if err == nil {
			result.Attempts = attempts
			result.RequestID = att.RequestID
			result.StatusCode = att.StatusCode
			c.logSuccess(result)
			return result, nil
		}
		err.Attempts = attempts
		err.RequestID = att.RequestID
		retrying := false
		var delay time.Duration
		// A canceled error wraps ctx.Err(), not an *llmError, so it is never retried.
		var llm *llmError
		if errors.As(err, &llm) && isRetryable(llm) && attempt < typeSafeMaxRetries {
			retrying = true
			delay = typeSafeRetryDelay(err, attempt)
		}
		c.logFailure(err, attempt+1, retrying, att.Latency)
		if !retrying {
			return SystemOneResult{}, err
		}
		if !c.sleep(ctx, delay) {
			canceled := &SystemOneError{Class: SystemOneClassCanceled, RequestID: att.RequestID, Attempts: attempts, cause: ctx.Err()}
			c.logFailure(canceled, attempt+1, false, 0)
			return SystemOneResult{}, canceled
		}
		wait = delay
	}
	// Unreachable: the final iteration always returns.
	return SystemOneResult{}, &SystemOneError{Class: SystemOneClassTransport, Attempts: attempts}
}

// typeSafeRetryDelay honours the peer's Retry-After (capped at 60 s by
// retryAfterFromHeader) for any retryable status, else the shared backoff.
func typeSafeRetryDelay(err *SystemOneError, attempt int) time.Duration {
	var status *httpStatusError
	if errors.As(err, &status) {
		if d := retryAfterFromHeader(status.header); d > 0 {
			return d
		}
	}
	return retryDelay(attempt)
}

type systemOneProbe struct {
	Model string `json:"model"`
	Usage *struct {
		InputTokens  *int64 `json:"input_tokens"`
		OutputTokens *int64 `json:"output_tokens"`
	} `json:"usage"`
}

// once sends one attempt. On failure it returns a *SystemOneError that wraps
// the classified *llmError.
func (c *TypeSafeClient) once(ctx context.Context, body []byte, lenient bool) (SystemOneResult, SystemOneAttempt, *SystemOneError) {
	var att SystemOneAttempt
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+typeSafeSystemOnePath, bytes.NewReader(body))
	if err != nil {
		return SystemOneResult{}, att, &SystemOneError{Class: SystemOneClassBuildRequest, cause: logging.TransportFailure(err)}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey.Reveal())

	started := time.Now()
	resp, err := httpguard.NoRedirects(c.client).Do(req) // the API key rides this request
	att.Latency = time.Since(started)
	if err != nil {
		if ctx.Err() != nil {
			att.Class = string(SystemOneClassCanceled)
			return SystemOneResult{}, att, &SystemOneError{Class: SystemOneClassCanceled, cause: ctx.Err()}
		}
		transport := &httpTransportError{cause: logging.TransportFailure(err)}
		class, llm := c.classify(transport, 0, nil)
		// ctx is alive here, so a "deadline" is the client's own Timeout.
		if tc := logging.TransportClass(err); tc == "timeout" || tc == "deadline" {
			class, llm.kind = SystemOneClassTimeout, llmErrorTimeout
		}
		att.Class = string(class)
		return SystemOneResult{}, att, &SystemOneError{Class: class, cause: llm}
	}
	defer resp.Body.Close()
	att.StatusCode = resp.StatusCode
	att.RequestID = safeRequestID(resp.Header.Get(typeSafeRequestIDHeader))

	payload, readErr := io.ReadAll(io.LimitReader(resp.Body, maxSystemOneResponseBytes+1))
	if readErr == nil && len(payload) > maxSystemOneResponseBytes {
		att.Class = string(SystemOneClassTooLarge)
		return SystemOneResult{}, att, &SystemOneError{Class: SystemOneClassTooLarge, StatusCode: resp.StatusCode,
			cause: &llmError{kind: llmErrorGeneric, message: "response exceeded the size bound", provider: string(ProviderKindTypeSafe), model: c.cfg.Model}}
	}

	if resp.StatusCode != http.StatusOK {
		// The body is NOT carried: for a 422 it may quote the offending field
		// of the request, which holds source text. Classification reads the
		// status and headers only.
		class, llm := c.classify(&httpStatusError{statusCode: resp.StatusCode, header: resp.Header}, resp.StatusCode, resp.Header)
		att.Class = string(class)
		return SystemOneResult{}, att, &SystemOneError{Class: class, StatusCode: resp.StatusCode, cause: llm}
	}
	if readErr != nil {
		att.Class = string(SystemOneClassBodyReadError)
		return SystemOneResult{}, att, &SystemOneError{Class: SystemOneClassBodyReadError, StatusCode: resp.StatusCode,
			cause: &llmError{kind: llmErrorTransport, message: "response body could not be read", provider: string(ProviderKindTypeSafe), model: c.cfg.Model,
				cause: logging.TransportFailure(readErr)}}
	}

	var probe systemOneProbe
	if err := json.Unmarshal(payload, &probe); err != nil && lenient {
		probe = systemOneProbe{} // nothing in a body that is not JSON is trusted
	} else if err != nil {
		att.Class = string(SystemOneClassDecode)
		return SystemOneResult{}, att, &SystemOneError{Class: SystemOneClassDecode, StatusCode: resp.StatusCode,
			cause: &llmError{kind: llmErrorGeneric, message: "response is not a JSON object", provider: string(ProviderKindTypeSafe), model: c.cfg.Model,
				cause: logging.DecodeFailure(err)}}
	}
	out := SystemOneResult{Body: payload, Model: probe.Model, Header: resp.Header.Clone()}
	if probe.Usage != nil && probe.Usage.InputTokens != nil {
		out.Usage.Reported = true
		out.Usage.InputTokens = *probe.Usage.InputTokens
		if probe.Usage.OutputTokens != nil {
			out.Usage.OutputTokens = *probe.Usage.OutputTokens
		}
	}
	return out, att, nil
}

// classify maps a failure to a SystemOneClass and the shared *llmError. The
// shared classifier runs on the status alone (never on body text, which can
// contain digits or words that match its substring rules); 402 and 403 are
// then folded into auth because a rejected or unpaid key stops the arm.
func (c *TypeSafeClient) classify(err error, status int, header http.Header) (SystemOneClass, *llmError) {
	llm := classifyProviderError(err, status, header, string(ProviderKindTypeSafe), c.cfg.Model)
	if status == http.StatusPaymentRequired || status == http.StatusForbidden {
		llm.kind = llmErrorAuth
		llm.message = "Invalid or missing LLM API key."
	}
	switch llm.kind {
	case llmErrorAuth:
		return SystemOneClassAuth, llm
	case llmErrorRateLimit:
		return SystemOneClassRateLimit, llm
	case llmErrorServer:
		return SystemOneClassServer, llm
	case llmErrorInvalidRequest:
		return SystemOneClassInvalid, llm
	case llmErrorTimeout:
		return SystemOneClassTimeout, llm
	case llmErrorTransport:
		return SystemOneClassTransport, llm
	default:
		return SystemOneClassUnexpected, llm
	}
}

func safeRequestID(raw string) string {
	id := requestIDUnsafe.ReplaceAllString(raw, "")
	if len(id) > maxLoggedRequestIDLen {
		id = id[:maxLoggedRequestIDLen]
	}
	return id
}

// logFailure is the loud line of a failed attempt: status, class, request id,
// attempt number, whether a retry follows, and the measured latency. Scalars
// only; never the URL, a header, the request body or the response body.
func (c *TypeSafeClient) logFailure(err *SystemOneError, attempt int, retrying bool, latency time.Duration) {
	c.logger.Warn("typesafe systemone attempt failed",
		slog.String("provider", string(ProviderKindTypeSafe)),
		slog.String("model", c.cfg.Model),
		slog.String("class", string(err.Class)),
		slog.Int("status", err.StatusCode),
		slog.String("request_id", err.RequestID),
		slog.Int("attempt", attempt),
		slog.Int("max_attempts", typeSafeMaxRetries+1),
		slog.Bool("retrying", retrying),
		slog.Int64("latency_ms", latency.Milliseconds()),
		slog.Duration("timeout", c.client.Timeout),
	)
}

func (c *TypeSafeClient) logSuccess(r SystemOneResult) {
	if !r.Usage.Reported {
		c.logger.Warn("typesafe systemone response carried no usage",
			slog.String("provider", string(ProviderKindTypeSafe)),
			slog.String("model", c.cfg.Model),
			slog.String("request_id", r.RequestID),
			slog.Int("attempts", r.AttemptCount()),
		)
	}
	c.logger.Debug("typesafe systemone ok",
		slog.String("provider", string(ProviderKindTypeSafe)),
		slog.String("model", c.cfg.Model),
		slog.String("returned_model", r.Model),
		slog.String("request_id", r.RequestID),
		slog.Int("attempts", r.AttemptCount()),
		slog.Int64("input_tokens", r.Usage.InputTokens),
		slog.Int64("output_tokens", r.Usage.OutputTokens),
		slog.Int64("latency_ms", r.Latency().Milliseconds()),
		slog.Int64("retry_wait_ms", r.RetryWait().Milliseconds()),
	)
}

// errTypeSafeIsNotAProvider is what the Provider factories answer for the
// typesafe kind: it has no completion-text client by design.
var errTypeSafeIsNotAProvider = fmt.Errorf(
	"LLM provider kind %q is a decision backend, not a text completer: build it with NewTypeSafeClientFromEnv", ProviderKindTypeSafe)
