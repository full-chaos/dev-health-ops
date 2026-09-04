package workgraph

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"
)

const maxCompatibilityResponseBytes = 8 * 1024

// maxCompatibilityDetailBytes bounds the diagnostic slice attached to a
// classified failure. It is deliberately far below the ledger's own 1024
// bound so the sentinel text plus the status prefix always fit alongside it.
const maxCompatibilityDetailBytes = 200

// The bridge can fail in three ways that mean genuinely different things to
// the caller, and collapsing them into one sentinel is what wedged
// CHAOS-4970's request chain: every failure reached handler.work's
// releaseAmbiguous, and 'ambiguous' is a state Claim refuses and
// joboutbox's strand-repair sweep excludes by construction, so only a human
// /internal/worker/workgraph/v1/executions/{id}/repair call can ever move it
// again. Nothing in Go calls that endpoint, so "ambiguous" is in practice
// terminal.
//
// All three wrap ErrUnavailable so existing callers that only ask "was the
// dependency unavailable" keep working unchanged; a caller that cares about
// the distinction asks for the specific sentinel.
var (
	// ErrCompatibilityNotSent reports a failure that happened BEFORE any
	// byte reached the bridge -- a wiring fault, an unencodable request, or
	// a connection that was never established (DNS, refused dial, a TLS
	// handshake rejected before the request was written). The bridge cannot
	// have observed this attempt, so there is no side effect to reconcile
	// and no ambiguity to record.
	ErrCompatibilityNotSent = fmt.Errorf("work graph compatibility request was never sent: %w", ErrUnavailable)
	// ErrCompatibilityRefused reports a completed round trip that the bridge
	// declined: a 4xx other than 408/429, or a parsed response whose status
	// is not "success" or whose evidence fails validation. The bridge made a
	// decision about this request rather than being interrupted mid-flight,
	// so a retry is safe -- it will either be refused identically or succeed
	// once the refused condition clears.
	ErrCompatibilityRefused = fmt.Errorf("work graph compatibility request was refused by the bridge: %w", ErrUnavailable)
	// ErrCompatibilityUnknown reports a failure that MAY have left a side
	// effect: a deadline or reset after the request was written, a 408/429,
	// any 5xx, or a 2xx whose body could not be read or decoded. This is the
	// only class that still deserves the ambiguous release, and it is the
	// default for anything this package cannot positively place in one of
	// the other two -- the safe direction is the one that keeps recording an
	// ambiguity.
	ErrCompatibilityUnknown = fmt.Errorf("work graph compatibility execution outcome is unknown: %w", ErrUnavailable)
)

type HTTPCompatibilityConfig struct {
	Endpoint              string
	BearerToken           string
	AllowInsecureInternal bool
}

type HTTPCompatibilityExecutor struct {
	client *http.Client
	config HTTPCompatibilityConfig
}

func NewHTTPCompatibilityExecutor(client *http.Client, config HTTPCompatibilityConfig) (*HTTPCompatibilityExecutor, error) {
	if client == nil || !validCompatibilityEndpoint(config.Endpoint, config.AllowInsecureInternal) || len(config.BearerToken) == 0 || len(config.BearerToken) > 512 {
		return nil, ErrUnavailable
	}
	return &HTTPCompatibilityExecutor{client: client, config: config}, nil
}

func (executor *HTTPCompatibilityExecutor) Execute(ctx context.Context, claim Claim) ([]byte, error) {
	if executor == nil || executor.client == nil {
		return nil, compatibilityFailure(ErrCompatibilityNotSent, 0, "executor is not configured")
	}
	if !validRequest(claim.Request) || !validUUID(claim.Token) {
		return nil, compatibilityFailure(ErrCompatibilityNotSent, 0, "claim failed local validation")
	}
	body, err := json.Marshal(struct {
		RequestID  string `json:"request_id"`
		ClaimToken string `json:"claim_token"`
	}{RequestID: claim.Request.ID, ClaimToken: claim.Token})
	if err != nil {
		return nil, compatibilityFailure(ErrCompatibilityNotSent, 0, "request body could not be encoded")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, executor.config.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, compatibilityFailure(ErrCompatibilityNotSent, 0, "request could not be built")
	}
	request.Header.Set("Authorization", "Bearer "+executor.config.BearerToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := executor.client.Do(request)
	if err != nil {
		return nil, executor.fail(compatibilityTransportSentinel(err), 0, err.Error())
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxCompatibilityResponseBytes+1))
	if err != nil {
		// The status line arrived, so the bridge accepted and may have run
		// the request; losing the body says nothing about what it did.
		return nil, executor.fail(ErrCompatibilityUnknown, response.StatusCode, "response body could not be read")
	}
	if len(data) > maxCompatibilityResponseBytes {
		return nil, executor.fail(ErrCompatibilityUnknown, response.StatusCode, "response body exceeded its bound")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, executor.fail(compatibilityStatusSentinel(response.StatusCode), response.StatusCode, string(data))
	}
	var decoded struct {
		Status         string          `json:"status"`
		OutputEvidence json.RawMessage `json:"output_evidence"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		// A 2xx with an undecodable body is far more likely to be a bridge
		// that ran and a response this side could not read than a bridge
		// that declined -- a decline arrives as a non-2xx. Unknown, not
		// Refused.
		return nil, executor.fail(ErrCompatibilityUnknown, response.StatusCode, "success response body could not be decoded")
	}
	if decoded.Status != "success" {
		return nil, executor.fail(ErrCompatibilityRefused, response.StatusCode, "bridge reported status "+decoded.Status)
	}
	if !validEvidence(decoded.OutputEvidence) {
		return nil, executor.fail(ErrCompatibilityRefused, response.StatusCode, "bridge returned invalid output evidence")
	}
	return decoded.OutputEvidence, nil
}

// compatibilityStatusSentinel places a non-2xx status. 408 and 429 are pulled
// out of the 4xx range on purpose: neither is a decision about this request's
// content, and both can be returned after the bridge already started work.
func compatibilityStatusSentinel(status int) error {
	switch {
	case status == http.StatusRequestTimeout || status == http.StatusTooManyRequests:
		return ErrCompatibilityUnknown
	case status >= 400 && status < 500:
		return ErrCompatibilityRefused
	default:
		return ErrCompatibilityUnknown
	}
}

// compatibilityTransportSentinel places a client.Do error. Only errors that
// prove the request never left this process are NotSent; everything else --
// a deadline, a reset, an unexpected EOF -- may have been received and acted
// on before the connection broke, and is Unknown. The default is Unknown so
// an unrecognised transport error keeps today's ambiguous release rather than
// silently losing an executed attempt.
func compatibilityTransportSentinel(err error) error {
	if err == nil {
		return ErrCompatibilityUnknown
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return ErrCompatibilityNotSent
	}
	// A dial that failed never wrote a request byte. A read/write OpError
	// deliberately falls through to Unknown.
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return ErrCompatibilityNotSent
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return ErrCompatibilityNotSent
	}
	// A handshake rejected on either side ends before net/http writes the
	// request line.
	var certErr *tls.CertificateVerificationError
	if errors.As(err, &certErr) {
		return ErrCompatibilityNotSent
	}
	var hostnameErr x509.HostnameError
	if errors.As(err, &hostnameErr) {
		return ErrCompatibilityNotSent
	}
	var authorityErr x509.UnknownAuthorityError
	if errors.As(err, &authorityErr) {
		return ErrCompatibilityNotSent
	}
	var recordErr tls.RecordHeaderError
	if errors.As(err, &recordErr) {
		return ErrCompatibilityNotSent
	}
	return ErrCompatibilityUnknown
}

// fail is the only failure constructor used once a bearer token exists on
// this executor. It redacts that token from the diagnostic before the
// diagnostic can travel anywhere.
//
// This is not hypothetical: the detail is built from the bridge's own
// response body, and a bridge that echoes the Authorization header it was
// sent -- deliberately or by including request context in an error -- would
// otherwise write the credential straight into the ledger's failure_detail
// column, which is durable and operator-readable. Redacting here covers the
// transport-error path too, where the error text can quote request headers.
func (executor *HTTPCompatibilityExecutor) fail(sentinel error, status int, detail string) error {
	if executor != nil && executor.config.BearerToken != "" {
		detail = strings.ReplaceAll(detail, executor.config.BearerToken, "[redacted]")
	}
	return compatibilityFailure(sentinel, status, detail)
}

// compatibilityFailure wraps one of the three sentinels with the HTTP status
// (0 when no response was received) and a bounded, sanitized diagnostic.
// Callers that hold a bearer token must go through fail rather than calling
// this directly; the endpoint that can appear in a transport error's text is
// separately validated by validCompatibilityEndpoint to carry no userinfo and
// no query string.
func compatibilityFailure(sentinel error, status int, detail string) error {
	trimmed := sanitizeDetail(detail, maxCompatibilityDetailBytes)
	if trimmed == "" {
		return fmt.Errorf("%w: status=%d", sentinel, status)
	}
	return fmt.Errorf("%w: status=%d %s", sentinel, status, trimmed)
}

// sanitizeDetail makes an arbitrary string safe to carry in an error that may
// end up in the ledger's failure_detail column: control characters and
// newlines become single spaces, runs of whitespace collapse, and the result
// is cut to limit BYTES on a rune boundary so a multi-byte tail can never be
// split into invalid UTF-8.
func sanitizeDetail(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	var builder strings.Builder
	builder.Grow(min(len(value), limit))
	pendingSpace := false
	for _, symbol := range value {
		if symbol == utf8.RuneError || !unicode.IsPrint(symbol) {
			pendingSpace = builder.Len() > 0
			continue
		}
		if unicode.IsSpace(symbol) {
			pendingSpace = builder.Len() > 0
			continue
		}
		if pendingSpace {
			if builder.Len()+1+utf8.RuneLen(symbol) > limit {
				return builder.String()
			}
			builder.WriteByte(' ')
			pendingSpace = false
		}
		if builder.Len()+utf8.RuneLen(symbol) > limit {
			return builder.String()
		}
		builder.WriteRune(symbol)
	}
	return builder.String()
}

func validCompatibilityEndpoint(raw string, allowInsecure bool) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "/internal/worker/workgraph/v1/execute" {
		return false
	}
	if parsed.Scheme == "https" && parsed.Host != "" {
		return true
	}
	host := strings.ToLower(parsed.Hostname())
	if parsed.Scheme != "http" || parsed.Host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || (allowInsecure && ip.IsPrivate())
	}
	return allowInsecure && (!strings.Contains(host, ".") ||
		strings.HasSuffix(host, ".internal") || strings.HasSuffix(host, ".local"))
}

var _ CompatibilityExecutor = (*HTTPCompatibilityExecutor)(nil)
