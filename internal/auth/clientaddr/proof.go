package clientaddr

// The client-address proof (CHAOS-11645): go-api signs the one X-Forwarded-For value it writes, and auth-service
// believes that value only with a valid proof, so the address no longer depends on which pod address a peer has.
//
// Wire form, one header line next to exactly one X-Forwarded-For line:
//
//	X-Forwarded-Client-Proof: v1.<kid>.<unix seconds>.<base64url HMAC-SHA256, no padding>
//
// The MAC covers the version, the key id, the time, the canonical address, the method, the request target
// (RequestURI) and the X-Request-Id value go-api sends, so one proof is good for one request only. The header name
// starts with X-Forwarded-, so go-api's proxy drops a client's copy before it writes its own.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const (
	// ProofHeader carries the proof of the X-Forwarded-For value.
	ProofHeader = "X-Forwarded-Client-Proof"
	// EnvMode selects how auth-service names the client: cidr, both or signed.
	EnvMode = "AUTH_CLIENT_ADDR_MODE"
	// EnvVerifyKey is auth-service's key list: one or two "kid:key" entries, comma-separated.
	EnvVerifyKey = "AUTH_CLIENT_ADDR_VERIFY_KEY"
	// EnvSigningKey is go-api's one "kid:key" entry.
	EnvSigningKey = "AUTH_CLIENT_ADDR_SIGNING_KEY"

	// MinProofKeyBytes is the floor of one decoded key.
	MinProofKeyBytes = 32
	// MaxVerifyKeys is the most keys auth-service holds: the current one and the next (or the retiring) one.
	MaxVerifyKeys = 2
	// ProofMaxAge is how old a proof may be (go-api's proxy timeout is 10 s).
	ProofMaxAge = 60 * time.Second
	// ProofMaxAhead is how far in the future a proof's time may be (clock skew).
	ProofMaxAhead = 5 * time.Second

	proofVersion   = "v1"
	proofDomain    = "dho-client-addr/v1"
	requestIDField = "X-Request-Id"
	macBytes       = sha256.Size
)

// The two well-known documents, the only routes where mode signed still reads the trusted-proxy list (the ingress
// controller serves them to the public and cannot sign). authapi.PatternJWKS and PatternDiscovery are the same
// strings (pinned by a test in authruntime).
const (
	WellKnownJWKSPath      = "/.well-known/jwks.json"
	WellKnownDiscoveryPath = "/.well-known/openid-configuration"
)

// Mode is how auth-service names the client.
type Mode string

const (
	// ModeCIDR is the trusted-proxy rule alone (Trust.Resolve). A proof is checked and counted only.
	ModeCIDR Mode = "cidr"
	// ModeBoth takes the address of a valid proof, else the trusted-proxy rule.
	ModeBoth Mode = "both"
	// ModeSigned takes the address of a valid proof, else the socket peer; the trusted-proxy rule is read only for
	// the well-known CIDR exception.
	ModeSigned Mode = "signed"
)

// ParseMode reads AUTH_CLIENT_ADDR_MODE; empty is cidr.
func ParseMode(value string) (Mode, error) {
	switch Mode(strings.TrimSpace(value)) {
	case "", ModeCIDR:
		return ModeCIDR, nil
	case ModeBoth:
		return ModeBoth, nil
	case ModeSigned:
		return ModeSigned, nil
	}
	return "", fmt.Errorf("%s must be cidr, both or signed", EnvMode)
}

// ProofKey is one key: its id and its secret bytes.
type ProofKey struct {
	id     string
	secret []byte
}

// ID is the key id.
func (k ProofKey) ID() string { return k.id }

var kidPattern = regexp.MustCompile(`^[a-z0-9-]{1,16}$`)

// ParseProofKeys reads a "kid:key" list (key = base64url without padding, at least MinProofKeyBytes decoded), at most
// limit entries, with distinct ids. Empty is no key. Errors name the variable and the entry index, never a value.
func ParseProofKeys(name, value string, limit int) ([]ProofKey, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	entries := strings.Split(value, ",")
	if len(entries) > limit {
		return nil, fmt.Errorf("%s holds %d entries; at most %d", name, len(entries), limit)
	}
	seen := map[string]bool{}
	keys := make([]ProofKey, 0, len(entries))
	for index, raw := range entries {
		kid, encoded, ok := strings.Cut(strings.TrimSpace(raw), ":")
		if !ok || !kidPattern.MatchString(kid) {
			return nil, fmt.Errorf("%s entry %d must be <kid>:<key> with a kid of 1-16 characters [a-z0-9-]", name, index+1)
		}
		secret, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
		if err != nil || len(secret) < MinProofKeyBytes {
			return nil, fmt.Errorf("%s entry %d must hold a key of at least %d bytes, base64url without padding", name, index+1, MinProofKeyBytes)
		}
		if seen[kid] {
			return nil, fmt.Errorf("%s entry %d repeats a kid", name, index+1)
		}
		seen[kid] = true
		keys = append(keys, ProofKey{id: kid, secret: secret})
	}
	return keys, nil
}

// ParseSigningKey reads go-api's one signing key; empty is none (go-api then sends no proof).
func ParseSigningKey(value string) (*ProofKey, error) {
	keys, err := ParseProofKeys(EnvSigningKey, value, 1)
	if err != nil || len(keys) == 0 {
		return nil, err
	}
	return &keys[0], nil
}

func proofMAC(secret []byte, kid string, ts int64, addr netip.Addr, method, requestURI, requestID string) []byte {
	mac := hmac.New(sha256.New, secret)
	for _, field := range []string{proofDomain, kid, strconv.FormatInt(ts, 10), addr.String(), method, requestURI} {
		mac.Write([]byte(field))
		mac.Write([]byte{'\n'})
	}
	mac.Write([]byte(requestID))
	return mac.Sum(nil)
}

// Prove is the proof header value for addr on one request: method, request target (URL.RequestURI) and the
// X-Request-Id value the request carries ("" for none).
func Prove(key ProofKey, now time.Time, addr netip.Addr, method, requestURI, requestID string) string {
	ts := now.Unix()
	addr = addr.WithZone("").Unmap()
	sum := proofMAC(key.secret, key.id, ts, addr, method, requestURI, requestID)
	return proofVersion + "." + key.id + "." + strconv.FormatInt(ts, 10) + "." + base64.RawURLEncoding.EncodeToString(sum)
}

// SetProof writes the X-Forwarded-For value and its proof on an outbound request. It writes nothing when key is nil
// or addr is not valid.
func SetProof(out *http.Request, key *ProofKey, now time.Time, addr netip.Addr) {
	if key == nil || !addr.IsValid() {
		return
	}
	addr = addr.WithZone("").Unmap()
	out.Header.Set("X-Forwarded-For", addr.String())
	out.Header.Set(ProofHeader, Prove(*key, now, addr, out.Method, out.URL.RequestURI(), out.Header.Get(requestIDField)))
}

// Outcome is the closed result of one proof check.
type Outcome string

const (
	OutcomeValid      Outcome = "valid"
	OutcomeAbsent     Outcome = "absent"
	OutcomeMalformed  Outcome = "malformed"
	OutcomeUnknownKid Outcome = "unknown_kid"
	OutcomeBadMAC     Outcome = "bad_mac"
	OutcomeStale      Outcome = "stale"
	OutcomeFuture     Outcome = "future"
)

// Verifier checks proofs against one or two keys.
type Verifier struct {
	keys map[string][]byte
	now  func() time.Time
}

// NewVerifier holds keys; nil for no key.
func NewVerifier(keys []ProofKey) *Verifier {
	if len(keys) == 0 {
		return nil
	}
	v := &Verifier{keys: map[string][]byte{}, now: time.Now}
	for _, key := range keys {
		v.keys[key.id] = key.secret
	}
	return v
}

// Check is the address the proof of r names, with its outcome; the address is valid only for OutcomeValid.
func (v *Verifier) Check(r *http.Request) (netip.Addr, Outcome) {
	proofs := r.Header.Values(ProofHeader)
	if len(proofs) == 0 {
		return netip.Addr{}, OutcomeAbsent
	}
	forwarded := r.Header.Values("X-Forwarded-For")
	ids := r.Header.Values(requestIDField)
	if len(proofs) != 1 || len(forwarded) != 1 || len(ids) > 1 {
		return netip.Addr{}, OutcomeMalformed
	}
	addr, err := netip.ParseAddr(forwarded[0])
	if err != nil || addr.Zone() != "" || addr.Is4In6() || addr.String() != forwarded[0] {
		return netip.Addr{}, OutcomeMalformed
	}
	parts := strings.Split(proofs[0], ".")
	if len(parts) != 4 || parts[0] != proofVersion || !kidPattern.MatchString(parts[1]) || !digitsOnly(parts[2]) {
		return netip.Addr{}, OutcomeMalformed
	}
	ts, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return netip.Addr{}, OutcomeMalformed
	}
	sent, err := base64.RawURLEncoding.Strict().DecodeString(parts[3])
	if err != nil || len(sent) != macBytes {
		return netip.Addr{}, OutcomeMalformed
	}
	secret, ok := v.keys[parts[1]]
	if !ok {
		return netip.Addr{}, OutcomeUnknownKid
	}
	requestID := ""
	if len(ids) == 1 {
		requestID = ids[0]
	}
	if !hmac.Equal(sent, proofMAC(secret, parts[1], ts, addr, r.Method, r.RequestURI, requestID)) {
		return netip.Addr{}, OutcomeBadMAC
	}
	now := v.now()
	at := time.Unix(ts, 0)
	switch {
	case now.Sub(at) > ProofMaxAge:
		return netip.Addr{}, OutcomeStale
	case at.Sub(now) > ProofMaxAhead:
		return netip.Addr{}, OutcomeFuture
	}
	return addr, OutcomeValid
}

func digitsOnly(text string) bool {
	if text == "" || len(text) > 12 {
		return false
	}
	for index := 0; index < len(text); index++ {
		if text[index] < '0' || text[index] > '9' {
			return false
		}
	}
	return true
}

// wellKnownException reports whether r is one of the 4 requests of the well-known CIDR exception: GET or HEAD of the
// JWKS or the discovery document, exact path, no escaped form.
func wellKnownException(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	if r.URL == nil || r.URL.RawPath != "" {
		return false
	}
	return r.URL.Path == WellKnownJWKSPath || r.URL.Path == WellKnownDiscoveryPath
}

// Policy is auth-service's whole client-address rule: the mode, the trusted-proxy list and the proof keys.
type Policy struct {
	Mode     Mode
	Trust    Trust
	Verifier *Verifier
	// Log reports a failed proof check (nil: no line), at most one line per outcome per minute. It gets the
	// outcome and the route class only: never an address, a key id or a MAC.
	Log func(ctx context.Context, outcome, route string)
}

// decide is the client address of r and whether r came through a believed forwarder: a valid proof (modes both and
// signed), or a trusted-proxy peer where the mode reads the list for r. observe counts the proof outcome.
func (p Policy) decide(r *http.Request, observe bool) (netip.Addr, bool) {
	wellKnown := wellKnownException(r)
	proved, outcome := netip.Addr{}, OutcomeAbsent
	if p.Verifier != nil {
		proved, outcome = p.Verifier.Check(r)
		if observe {
			route := "other"
			if wellKnown {
				route = "well_known"
			}
			p.observe(r.Context(), outcome, route)
		}
	}
	switch p.Mode {
	case ModeBoth:
		if outcome == OutcomeValid {
			return proved, true
		}
	case ModeSigned:
		if outcome == OutcomeValid {
			return proved, true
		}
		if !wellKnown {
			return socketPeer(r), false
		}
	}
	return p.Trust.Resolve(r), p.Trust.TrustsPeer(r)
}

// Resolve is the client address of r under p (the zero Addr when the peer address does not parse).
func (p Policy) Resolve(r *http.Request) netip.Addr {
	addr, _ := p.decide(r, false)
	return addr
}

// Forwarded reports whether r came through a believed forwarder under p.
func (p Policy) Forwarded(r *http.Request) bool {
	_, forwarded := p.decide(r, false)
	return forwarded
}

func socketPeer(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}
	return peer.WithZone("").Unmap()
}

// PolicyMiddleware binds the client address of p and the user agent to every request.
func PolicyMiddleware(p Policy) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			if addr, _ := p.decide(r, true); addr.IsValid() {
				ctx = With(ctx, addr)
			}
			if values := r.Header.Values("User-Agent"); len(values) > 0 {
				ctx = WithUserAgent(ctx, latin1(values[0]))
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

var proofCounter = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/clientaddr").Int64Counter(
		"auth.client_addr.proofs", metric.WithDescription("Client-address proof checks, by closed outcome and route class."))
	if err != nil {
		counter, _ = noop.NewMeterProvider().Meter("noop").Int64Counter("auth.client_addr.proofs")
	}
	return counter
}()

var proofWarn = struct {
	sync.Mutex
	last map[Outcome]time.Time
}{last: map[Outcome]time.Time{}}

func (p Policy) observe(ctx context.Context, outcome Outcome, route string) {
	proofCounter.Add(context.WithoutCancel(ctx), 1,
		metric.WithAttributes(attribute.String("outcome", string(outcome)), attribute.String("route", route)))
	if outcome == OutcomeValid || outcome == OutcomeAbsent || p.Log == nil {
		return
	}
	proofWarn.Lock()
	now := time.Now()
	due := now.Sub(proofWarn.last[outcome]) >= time.Minute
	if due {
		proofWarn.last[outcome] = now
	}
	proofWarn.Unlock()
	if due {
		p.Log(ctx, string(outcome), route)
	}
}
