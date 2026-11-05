// Package acrlisting is the read-only client of the acr credential lifecycle listing (GET /internal/v1/credentials/lifecycle,
// schema credential_lifecycle_list.v1) that the Auth Control Plane reconciliation reads (CHAOS-11199). One Client is one
// workload identity and one organization: the exchange names the organization (RFC 8693 resource), because acr can bind
// one identity in several.
//
// It sends exactly two kinds of request: the RFC 8693 token exchange (POST, the subject token is in the body and nowhere
// else) and the listing (GET, with the exchanged access token). It follows no redirect, retries nothing (the caller owns
// retry and pacing), and its errors name no token, no body and no credential id. A page that breaks the contract is an
// error, never an empty page.
package acrlisting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

const (
	listPath         = "/internal/v1/credentials/lifecycle"
	exchangeGrant    = "urn:ietf:params:oauth:grant-type:token-exchange"
	exchangeSubject  = "urn:ietf:params:oauth:token-type:jwt"
	listSchema       = "credential_lifecycle_list.v1"
	maxPageLimit     = 500
	defaultMaxPages  = 1000
	defaultTimeout   = 20 * time.Second
	refreshMargin    = 30 * time.Second
	maxSubjectBytes  = 16 << 10
	maxExchangeBytes = 16 << 10
	maxPageBytes     = 1 << 20
	maxRetryAfter    = 5 * time.Minute
)

var (
	idPattern       = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	revisionPattern = regexp.MustCompile(`^[1-9][0-9]{0,18}$`)
	orgPattern      = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

// Row is one listed credential. Status is one of active, revoked, expired.
type Row struct {
	ID       string
	Revision int64
	Status   string
}

// Kind is a closed failure class; its values are safe as metric labels.
type Kind string

const (
	KindExchange     Kind = "exchange"
	KindUnauthorized Kind = "unauthorized"
	KindForbidden    Kind = "forbidden"
	KindRateLimited  Kind = "rate_limited"
	KindUnavailable  Kind = "unavailable"
	KindRejected     Kind = "rejected"
	KindSchema       Kind = "schema"
	KindWalk         Kind = "walk"
	KindTransport    Kind = "transport"

	// KindInvalidRequest and KindInvalidTarget are acr's own refusals of the org selector: the identity is bound in
	// several organizations and no usable selector arrived, or it is not bound in the selected one.
	KindInvalidRequest Kind = "invalid_request"
	KindInvalidTarget  Kind = "invalid_target"
)

// Error is a failed listing. Its text is a fixed phrase and the status: no token, body or id. RetryAfter is the
// server's Retry-After in whole seconds, capped at five minutes, or zero.
type Error struct {
	Kind       Kind
	Status     int
	RetryAfter time.Duration
	cause      error
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return "acrlisting: " + string(e.Kind) + " (status " + strconv.Itoa(e.Status) + ")"
	}
	return "acrlisting: " + string(e.Kind)
}

// Unwrap exposes only a context error, so a caller can tell a cancelled run from a failed one.
func (e *Error) Unwrap() error { return e.cause }

func fail(kind Kind) *Error { return &Error{Kind: kind} }

func contextCause(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	}
	return nil
}

// Config builds a Client.
type Config struct {
	// BaseURL is the origin of acr's internal API. TokenEndpoint is acr's token endpoint (POST .../api/v1/oauth/token).
	// Each must be https, or http on a loopback host, and carry no user info, query or fragment on BaseURL.
	BaseURL       *url.URL
	TokenEndpoint *url.URL
	// SubjectTokenFile is the projected ServiceAccount token of the workload bound in acr. It is read again on every
	// exchange, never cached.
	SubjectTokenFile string
	// OrgID is the organization to list, a lowercase UUID. It is sent as the RFC 8693 resource urn:acr:org:<OrgID>,
	// because one workload identity can be bound in several organizations.
	OrgID string
	// HTTPClient is optional; a nil one is httpguard.NewClient. Whatever is given follows no redirect.
	HTTPClient *http.Client
	Timeout    time.Duration
	// PageLimit is 1..500, default 500. MaxPages bounds one walk, default 1000.
	PageLimit int
	MaxPages  int
	Now       func() time.Time
}

// ErrInvalidConfig reports an unusable Config.
var ErrInvalidConfig = errors.New("acrlisting: invalid configuration")

// Client lists one organization's credentials. It is safe for concurrent use.
type Client struct {
	base, endpoint *url.URL
	subjectFile    string
	org            string
	http           *http.Client
	limit, pages   int
	now            func() time.Time

	mu      sync.Mutex
	access  string
	expires time.Time
}

// ValidOrigin reports whether u is a usable acr origin: https, or http on a loopback host, with a host and no user
// info or fragment.
func ValidOrigin(u *url.URL) bool {
	if u == nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return false
	}
	switch u.Scheme {
	case "https":
		return true
	case "http":
		host := u.Hostname()
		if host == "localhost" {
			return true
		}
		ip := net.ParseIP(host)
		return ip != nil && ip.IsLoopback()
	}
	return false
}

// New returns a Client.
func New(cfg Config) (*Client, error) {
	switch {
	case !ValidOrigin(cfg.BaseURL) || cfg.BaseURL.RawQuery != "":
		return nil, fmt.Errorf("%w: base url", ErrInvalidConfig)
	case !ValidOrigin(cfg.TokenEndpoint):
		return nil, fmt.Errorf("%w: token endpoint", ErrInvalidConfig)
	case strings.TrimSpace(cfg.SubjectTokenFile) == "":
		return nil, fmt.Errorf("%w: subject token file", ErrInvalidConfig)
	case !ValidOrgID(cfg.OrgID):
		return nil, fmt.Errorf("%w: org id", ErrInvalidConfig)
	case cfg.PageLimit < 0 || cfg.PageLimit > maxPageLimit:
		return nil, fmt.Errorf("%w: page limit", ErrInvalidConfig)
	case cfg.MaxPages < 0:
		return nil, fmt.Errorf("%w: max pages", ErrInvalidConfig)
	}
	c := &Client{base: cfg.BaseURL, endpoint: cfg.TokenEndpoint, subjectFile: cfg.SubjectTokenFile, org: cfg.OrgID, limit: cfg.PageLimit, pages: cfg.MaxPages, now: cfg.Now}
	if c.limit == 0 {
		c.limit = maxPageLimit
	}
	if c.pages == 0 {
		c.pages = defaultMaxPages
	}
	if c.now == nil {
		c.now = time.Now
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if cfg.HTTPClient != nil {
		c.http = httpguard.NoRedirects(cfg.HTTPClient)
	} else {
		c.http = httpguard.NewClient(timeout)
	}
	return c, nil
}

// List walks the whole listing of the bound organization and returns its rows in credential id order.
func (c *Client) List(ctx context.Context) ([]Row, error) {
	token, err := c.accessToken(ctx)
	if err != nil {
		return nil, err
	}
	rows := []Row{}
	after := ""
	for page := 0; page < c.pages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, &Error{Kind: KindTransport, cause: contextCause(err)}
		}
		items, next, err := c.page(ctx, token, after)
		if err != nil {
			return nil, err
		}
		rows = append(rows, items...)
		if next == "" {
			return rows, nil
		}
		after = next
	}
	return nil, fail(KindWalk)
}

func (c *Client) page(ctx context.Context, token, after string) ([]Row, string, error) {
	query := url.Values{"limit": {strconv.Itoa(c.limit)}}
	if after != "" {
		query.Set("after", after)
	}
	target := c.base.JoinPath(listPath)
	target.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, "", fail(KindTransport)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", &Error{Kind: KindTransport, cause: contextCause(err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", statusError(resp)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPageBytes+1))
	if err != nil {
		return nil, "", &Error{Kind: KindTransport, cause: contextCause(err)}
	}
	if len(body) > maxPageBytes {
		return nil, "", fail(KindSchema)
	}
	return decodePage(body, after, c.limit)
}

func statusError(resp *http.Response) *Error {
	e := &Error{Status: resp.StatusCode}
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		e.Kind = KindUnauthorized
	case resp.StatusCode == http.StatusForbidden:
		e.Kind = KindForbidden
	case resp.StatusCode == http.StatusTooManyRequests:
		e.Kind = KindRateLimited
	case resp.StatusCode >= 500:
		e.Kind = KindUnavailable
	default:
		e.Kind = KindRejected
	}
	if e.Kind == KindRateLimited || e.Kind == KindUnavailable {
		if secs, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && secs > 0 {
			e.RetryAfter = min(time.Duration(secs)*time.Second, maxRetryAfter)
		}
	}
	return e
}

type wirePage struct {
	SchemaVersion string          `json:"schema_version"`
	Items         *[]wireItem     `json:"items"`
	NextAfter     json.RawMessage `json:"next_after"`
}

type wireItem struct {
	ID       string          `json:"credential_id"`
	Revision json.RawMessage `json:"revision"`
	Status   string          `json:"status"`
}

// decodePage holds the page to the schema and to the walk rules: ids ascend strictly and stay above the cursor, a page
// holds at most limit rows, and next_after is the last id of a non-empty page, so no id can be skipped or read twice.
func decodePage(body []byte, after string, limit int) ([]Row, string, error) {
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	var wire wirePage
	if err := dec.Decode(&wire); err != nil {
		return nil, "", fail(KindSchema)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, "", fail(KindSchema)
	}
	if wire.SchemaVersion != listSchema || wire.Items == nil || wire.NextAfter == nil || len(*wire.Items) > limit {
		return nil, "", fail(KindSchema)
	}
	rows := make([]Row, 0, len(*wire.Items))
	prev := after
	for _, it := range *wire.Items {
		revision, err := strconv.ParseInt(string(it.Revision), 10, 64)
		if err != nil || !revisionPattern.Match(it.Revision) {
			return nil, "", fail(KindSchema)
		}
		if !idPattern.MatchString(it.ID) || (it.Status != "active" && it.Status != "revoked" && it.Status != "expired") {
			return nil, "", fail(KindSchema)
		}
		if it.ID <= prev {
			return nil, "", fail(KindSchema)
		}
		prev = it.ID
		rows = append(rows, Row{ID: it.ID, Revision: revision, Status: it.Status})
	}
	if string(wire.NextAfter) == "null" {
		return rows, "", nil
	}
	var next string
	if err := json.Unmarshal(wire.NextAfter, &next); err != nil || !idPattern.MatchString(next) || len(rows) == 0 || next != rows[len(rows)-1].ID {
		return nil, "", fail(KindSchema)
	}
	return rows, next, nil
}

func (c *Client) accessToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if c.access != "" && now.Before(c.expires) {
		return c.access, nil
	}
	token, lifetime, err := c.exchange(ctx)
	if err != nil {
		return "", err
	}
	c.access, c.expires = token, now
	if lifetime > refreshMargin {
		c.expires = now.Add(lifetime - refreshMargin)
	}
	return token, nil
}

func (c *Client) exchange(ctx context.Context) (string, time.Duration, error) {
	return Exchange(ctx, c.http, c.endpoint, c.subjectFile, c.org)
}

// ValidOrgID reports whether orgID is a lowercase UUID, the form the exchange names.
func ValidOrgID(orgID string) bool { return orgPattern.MatchString(orgID) }

// Exchange is the RFC 8693 token exchange: the subject token read from subjectTokenFile (again on every call, never
// cached) for an access token of the organization orgID, sent as the resource urn:acr:org:<orgID>. httpClient must
// follow no redirect. It returns the access token and its lifetime; a failure is an *Error of KindExchange,
// KindInvalidRequest or KindInvalidTarget that names no token and no body.
func Exchange(ctx context.Context, httpClient *http.Client, tokenEndpoint *url.URL, subjectTokenFile, orgID string) (string, time.Duration, error) {
	if httpClient == nil || !ValidOrigin(tokenEndpoint) || !ValidOrgID(orgID) {
		return "", 0, fail(KindExchange)
	}
	subject, err := readSubject(subjectTokenFile)
	if err != nil {
		return "", 0, fail(KindExchange)
	}
	form := url.Values{"grant_type": {exchangeGrant}, "subject_token": {subject}, "subject_token_type": {exchangeSubject}, "resource": {"urn:acr:org:" + orgID}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenEndpoint.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, fail(KindExchange)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", 0, &Error{Kind: KindExchange, cause: contextCause(err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, &Error{Kind: exchangeRefusalKind(resp), Status: resp.StatusCode}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxExchangeBytes+1))
	if err != nil || len(body) > maxExchangeBytes {
		return "", 0, fail(KindExchange)
	}
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.ExpiresIn <= 0 || !printableToken(out.AccessToken) {
		return "", 0, fail(KindExchange)
	}
	return out.AccessToken, time.Duration(out.ExpiresIn) * time.Second, nil
}

func exchangeRefusalKind(resp *http.Response) Kind {
	if resp.StatusCode != http.StatusBadRequest {
		return KindExchange
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxExchangeBytes+1))
	if err != nil || len(body) > maxExchangeBytes {
		return KindExchange
	}
	var out struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &out) != nil {
		return KindExchange
	}
	switch out.Error {
	case "invalid_request":
		return KindInvalidRequest
	case "invalid_target":
		return KindInvalidTarget
	}
	return KindExchange
}

func printableToken(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r <= ' ' || r > '~' {
			return false
		}
	}
	return true
}

func readSubject(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("not a regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxSubjectBytes+1))
	if err != nil || len(raw) > maxSubjectBytes {
		return "", errors.New("unreadable or oversized")
	}
	subject := strings.TrimSpace(string(raw))
	if subject == "" {
		return "", errors.New("empty")
	}
	return subject, nil
}
