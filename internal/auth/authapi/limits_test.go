package authapi

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
)

// The go-api proxy is the one trusted peer; every other peer is a client.
const (
	proxyPeer  = "10.0.0.1:4000"
	clientPeer = "203.0.113.9:5000"
)

func trustedProxy(t *testing.T, next http.Handler) http.Handler {
	t.Helper()
	trust, err := clientaddr.ParseTrust("10.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	return clientaddr.Middleware(trust)(next)
}

func send(h http.Handler, peer, forwarded, method, path, body string, header map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.RemoteAddr = peer
	if forwarded != "" {
		request.Header.Set("X-Forwarded-For", forwarded)
	}
	for k, v := range header {
		request.Header.Set(k, v)
	}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	return response
}

const testRateLimitSecret = "rate-limit-test-signing-secret-0123456789"

func mustLimits(t *testing.T, store httpapi.CounterStore) *Limits {
	t.Helper()
	limits, err := NewLimits(store, testRateLimitSecret)
	if err != nil {
		t.Fatal(err)
	}
	return limits
}

// failingStore answers every count with a store error.
type failingStore struct{}

func (failingStore) Increment(context.Context, httpapi.Hit) (int64, error) {
	return 0, errors.New("valkey down for sentinel-person@example.com")
}
func (failingStore) Peek(context.Context, httpapi.Hit) (int64, error) {
	return 0, errors.New("valkey down")
}
func (failingStore) Backend() string { return "redis" }

// recordingStore counts in memory and keeps every hit it saw.
type recordingStore struct {
	httpapi.CounterStore
	mu   sync.Mutex
	hits []httpapi.Hit
}

func (r *recordingStore) Increment(ctx context.Context, hit httpapi.Hit) (int64, error) {
	r.mu.Lock()
	r.hits = append(r.hits, hit)
	r.mu.Unlock()
	return r.CounterStore.Increment(ctx, hit)
}

func newRecording() *recordingStore {
	return &recordingStore{CounterStore: httpapi.NewMemoryCounters(time.Now)}
}

func assertRateLimited(t *testing.T, name string, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("%s = %d, want 429: %s", name, response.Code, response.Body)
	}
	body := response.Body.String()
	if !strings.Contains(body, `"rate_limited"`) {
		t.Fatalf("%s 429 is not the rate_limited envelope: %s", name, body)
	}
	for _, leak := range []string{"auth_svc", "per hour", "per 15", "sentinel", "@", "203.0.113", "10.0.0"} {
		if strings.Contains(body, leak) {
			t.Fatalf("%s 429 names %q: %s", name, leak, body)
		}
	}
}

func TestTheLimitTableIsPinned(t *testing.T) {
	want := map[string]httpapi.Limit{
		"login ip":      {ID: "auth_svc_login_ip", Count: 20, Window: 15 * time.Minute},
		"login account": {ID: "auth_svc_login_ip_account", Count: 10, Window: 15 * time.Minute},
		"verify":        {ID: "auth_svc_verify", Count: 10, Window: time.Hour},
		"resend":        {ID: "auth_svc_resend_verification", Count: 3, Window: time.Hour},
		"forgot":        {ID: "auth_svc_forgot_password", Count: 3, Window: time.Hour},
		"resend ip":     {ID: "auth_svc_resend_verification_ip", Count: 20, Window: time.Hour},
		"forgot ip":     {ID: "auth_svc_forgot_password_ip", Count: 20, Window: time.Hour},
		"reset ip":      {ID: "auth_svc_reset_ip", Count: 20, Window: time.Hour},
		"reset token":   {ID: "auth_svc_reset_ip_token", Count: 5, Window: time.Hour},
		"change":        {ID: "auth_svc_change_password", Count: 5, Window: time.Hour},
		"social link":   {ID: "auth_svc_social_link_ip_principal", Count: 10, Window: time.Hour},
	}
	got := map[string]httpapi.Limit{
		"login ip": loginIPLimit, "login account": loginAccountLimit, "verify": verifyLimit, "resend": resendLimit,
		"forgot": forgotLimit, "resend ip": resendIPLimit, "forgot ip": forgotIPLimit, "reset ip": resetIPLimit, "reset token": resetTokenLimit,
		"change": changePasswordLimit, "social link": socialLinkLimit,
	}
	for name, limit := range want {
		if got[name] != limit {
			t.Errorf("%s limit = %+v, want %+v", name, got[name], limit)
		}
		if !strings.HasPrefix(limit.ID, "auth_svc_") {
			t.Errorf("%s id %q is outside the auth_svc_ ACL prefix", name, limit.ID)
		}
	}
}

func TestLimitedRoutesRefuseToBuildWithoutACounterStore(t *testing.T) {
	f := newCompatFixture(t)
	deps := CompatDeps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer, Projector: f.projector,
		AppendAudit: func(context.Context, audit.AuditEvent) error { return nil },
		Login:       &LoginConfig{Users: f.loginUsers, Attempts: f.attempts, LockoutKey: contracts.NewSecretString(testLockoutKey)}}
	if _, err := CompatRoutes(deps); !errors.Is(err, errNoLimits) {
		t.Fatalf("login without counters: %v", err)
	}
	deps.Login = nil
	if _, err := CompatRoutes(deps); err != nil {
		t.Fatalf("no login, no limit, no store needed: %v", err)
	}
	if _, err := CredentialRoutes(CredentialDeps{Flows: &local.Flows{}, Sessions: f.sessions, Users: f.users, Issuer: f.issuer}); !errors.Is(err, errNoLimits) {
		t.Fatalf("credentials without counters: %v", err)
	}
	if _, err := SocialLinkRoutes(SocialLinkDeps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer, Social: &fakeLinkDomain{}}); !errors.Is(err, errNoLimits) {
		t.Fatalf("social link without counters: %v", err)
	}
}

func TestLoginLimitsPerClientAddressAndPerAccount(t *testing.T) {
	f := newCompatFixture(t)
	h := trustedProxy(t, f.server)
	login := func(peer, forwarded, email string) *httptest.ResponseRecorder {
		return send(h, peer, forwarded, http.MethodPost, PatternCompatLogin, loginBody(email, "wrong password", ""), nil)
	}
	for i := 0; i < 10; i++ {
		if r := login(clientPeer, "", compatEmail); r.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i+1, r.Code)
		}
	}
	assertRateLimited(t, "11th attempt, one address and account", login(clientPeer, "", compatEmail))
	assertRateLimited(t, "same account, case and space variant", login(clientPeer, "", " Alice@Example.com "))
	if r := login(clientPeer, "", "other@example.com"); r.Code != http.StatusUnauthorized {
		t.Fatalf("another account from the same address = %d, want 401", r.Code)
	}
	if r := login("198.51.100.7:5000", "", compatEmail); r.Code != http.StatusUnauthorized {
		t.Fatalf("the same account from another address = %d, want 401", r.Code)
	}
	for i := 0; i < 20; i++ {
		_ = login("198.51.100.8:5000", "", "person"+string(rune('a'+i%26))+"@example.com")
	}
	assertRateLimited(t, "21st attempt from one address", login("198.51.100.8:5000", "", "fresh@example.com"))
}

func TestMalformedLoginBodiesCostNoAllowance(t *testing.T) {
	f := newCompatFixture(t)
	for i := 0; i < 30; i++ {
		if r := f.do(http.MethodPost, PatternCompatLogin, `{"email":"not an address","password":"x"}`, nil); r.Code != http.StatusUnprocessableEntity {
			t.Fatalf("malformed login = %d", r.Code)
		}
	}
	if r := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong password", ""), nil); r.Code != http.StatusUnauthorized {
		t.Fatalf("valid body after refused ones = %d, want 401", r.Code)
	}
}

func TestAForwardedAddressIsBelievedOnlyFromTheTrustedPeer(t *testing.T) {
	f := newCompatFixture(t)
	h := trustedProxy(t, f.server)
	attempt := func(peer, forwarded string, i int) *httptest.ResponseRecorder {
		email := "user" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + "@example.com"
		return send(h, peer, forwarded, http.MethodPost, PatternCompatLogin, loginBody(email, "wrong password", ""), nil)
	}
	// An untrusted peer cannot pick its bucket: a new spoofed address on each
	// request still counts against the peer's own address.
	for i := 0; i < 20; i++ {
		attempt(clientPeer, "192.0.2."+string(rune('1'+i%9)), i)
	}
	assertRateLimited(t, "spoofed forwarded address from an untrusted peer", attempt(clientPeer, "192.0.2.200", 30))
	// Behind the trusted go-api peer, the forwarded client is the key, so
	// the proxy's own address never becomes one bucket for every user.
	for i := 0; i < 20; i++ {
		attempt(proxyPeer, "198.51.100.50", i)
	}
	assertRateLimited(t, "the 21st request of one forwarded client", attempt(proxyPeer, "198.51.100.50", 31))
	if r := attempt(proxyPeer, "198.51.100.51", 32); r.Code != http.StatusUnauthorized {
		t.Fatalf("another client behind the same proxy = %d, want 401: the proxy address is not the key", r.Code)
	}
}

func TestTwoReplicasShareOneLoginBudget(t *testing.T) {
	first := newCompatFixture(t)
	second := newCompatFixture(t)
	second.counters = first.counters
	second.buildCompat()
	replicas := []*compatFixture{first, second}
	for i := 0; i < 10; i++ {
		if r := replicas[i%2].do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong password", ""), nil); r.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i+1, r.Code)
		}
	}
	for i, replica := range replicas {
		assertRateLimited(t, "replica "+string(rune('1'+i)), replica.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong password", ""), nil))
	}
}

func TestResetPasswordLimitsPerClientAddressAndPerLinkSecret(t *testing.T) {
	c := newCredFixture(t)
	h := trustedProxy(t, c.routes)
	reset := func(peer, token string) *httptest.ResponseRecorder {
		return send(h, peer, "", http.MethodPost, PatternResetPassword, `{"token":"`+token+`","new_password":"another-password-1"}`, nil)
	}
	for i := 0; i < 5; i++ {
		if r := reset(clientPeer, "same-link-secret"); r.Code != http.StatusBadRequest {
			t.Fatalf("reset %d = %d, want 400", i+1, r.Code)
		}
	}
	assertRateLimited(t, "6th reset of one link secret", reset(clientPeer, "same-link-secret"))
	if r := reset("198.51.100.7:5000", "same-link-secret"); r.Code != http.StatusBadRequest {
		t.Fatalf("the same secret from another address = %d, want 400", r.Code)
	}
	for i := 0; i < 14; i++ {
		reset(clientPeer, "link-secret-"+string(rune('a'+i)))
	}
	assertRateLimited(t, "21st reset from one address", reset(clientPeer, "link-secret-z"))
	for i := 0; i < 8; i++ {
		if r := send(h, "198.51.100.9:5000", "", http.MethodPost, PatternResetPassword, `{"token":"x","new_password":"short"}`, nil); r.Code != http.StatusUnprocessableEntity {
			t.Fatalf("malformed reset = %d", r.Code)
		}
	}
}

func TestSocialLinkAndUnlinkShareOneBudgetPerAddressAndPrincipal(t *testing.T) {
	f, _ := linkFixture(t)
	h := trustedProxy(t, f.server)
	header := f.bearer(f.grant())
	for i := 0; i < 10; i++ {
		path := PatternSocialLink
		method := http.MethodPost
		body := linkBody
		if i%2 == 1 {
			method, path, body = http.MethodDelete, "/v1/identities/social/"+uuid.NewString(), ""
		}
		if r := send(h, clientPeer, "", method, path, body, header); r.Code == http.StatusTooManyRequests {
			t.Fatalf("request %d was limited early", i+1)
		}
	}
	assertRateLimited(t, "11th link call", send(h, clientPeer, "", http.MethodPost, PatternSocialLink, linkBody, header))
	assertRateLimited(t, "11th call on a fresh unlink id", send(h, clientPeer, "", http.MethodDelete, "/v1/identities/social/"+uuid.NewString(), "", header))
	if r := send(h, "198.51.100.7:5000", "", http.MethodPost, PatternSocialLink, linkBody, header); r.Code == http.StatusTooManyRequests {
		t.Fatal("another address was limited by the first one's budget")
	}
}

func TestAStoreErrorRefusesEveryLimitedRouteAndCountsItWithoutValues(t *testing.T) {
	compat := newCompatFixture(t)
	compat.counters = failingStore{}
	compat.buildCompat()
	cred := newCredFixtureWith(t, failingStore{})
	link, _ := linkFixtureWith(t, failingStore{})
	calls := map[string]*httptest.ResponseRecorder{
		"login":  compat.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, "wrong password", ""), nil),
		"verify": cred.call(http.MethodGet, PatternVerifyEmail+"?token=sentinel-token", "", nil),
		"resend": cred.call(http.MethodPost, PatternResendVerification, `{"email":"sentinel-person@example.com"}`, nil),
		"forgot": cred.call(http.MethodPost, PatternForgotPassword, `{"email":"sentinel-person@example.com"}`, nil),
		"reset":  cred.call(http.MethodPost, PatternResetPassword, `{"token":"sentinel-token","new_password":"another-password-1"}`, nil),
		"change": cred.call(http.MethodPost, PatternChangePassword, `{"current_password":"a-password-1","new_password":"Brand-new-pass-77!"}`, cred.bearer(cred.grant())),
		"link":   link.do(http.MethodPost, PatternSocialLink, linkBody, link.bearer(link.grant())),
	}
	for name, response := range calls {
		if response.Code != http.StatusInternalServerError {
			t.Errorf("%s with the store down = %d, want a closed 500", name, response.Code)
		}
		if strings.Contains(response.Body.String(), "sentinel") {
			t.Errorf("%s 500 names the request: %s", name, response.Body)
		}
	}
	if compat.attempts.failures != 0 {
		t.Error("the login handler ran with the store down")
	}
	var metrics bytes.Buffer
	if err := httpapi.RateLimitStoreErrors.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"auth_svc_login_ip", "auth_svc_verify", "auth_svc_resend_verification", "auth_svc_forgot_password", "auth_svc_reset_ip", "auth_svc_change_password", "auth_svc_social_link_ip_principal"} {
		if !strings.Contains(metrics.String(), `limit="`+id+`"`) {
			t.Errorf("no store-error series for %s", id)
		}
	}
	for _, leak := range []string{"sentinel", "@", "203.0.113", "valkey"} {
		if strings.Contains(metrics.String(), leak) {
			t.Errorf("the store-error metric carries %q", leak)
		}
	}
}

func TestNoCounterKeyHoldsAnEmailALinkSecretOrAPrincipal(t *testing.T) {
	store := newRecording()
	compat := newCompatFixture(t)
	compat.counters = store
	compat.buildCompat()
	cred := newCredFixtureWith(t, store)
	link, _ := linkFixtureWith(t, store)
	const email, secret = "Sentinel.Person@Example.com", "sentinel-link-secret-4711"
	compat.do(http.MethodPost, PatternCompatLogin, loginBody(email, "wrong password", ""), nil)
	cred.call(http.MethodGet, PatternVerifyEmail+"?token="+secret, "", nil)
	cred.call(http.MethodPost, PatternResendVerification, `{"email":"`+email+`"}`, nil)
	cred.call(http.MethodPost, PatternForgotPassword, `{"email":"`+email+`"}`, nil)
	cred.call(http.MethodPost, PatternResetPassword, `{"token":"`+secret+`","new_password":"another-password-1"}`, nil)
	cred.call(http.MethodPost, PatternChangePassword, `{"current_password":"a-password-1","new_password":"Brand-new-pass-77!"}`, cred.bearer(cred.grant()))
	link.do(http.MethodPost, PatternSocialLink, linkBody, link.bearer(link.grant()))
	if len(store.hits) < 9 {
		t.Fatalf("only %d hits recorded: a route did not count", len(store.hits))
	}
	principals := []string{cred.principal.String(), link.principal.String(), compat.principal.String()}
	for _, hit := range store.hits {
		for _, text := range []string{hit.Key, hit.Path, hit.Limit.ID} {
			lower := strings.ToLower(text)
			for _, sentinel := range append([]string{"sentinel", "example.com", "@", secret}, principals...) {
				if strings.Contains(lower, strings.ToLower(sentinel)) {
					t.Errorf("limit %s: counter scope %q holds %q", hit.Limit.ID, text, sentinel)
				}
			}
		}
	}
}

// The derivation, pinned by vectors computed outside this package: HKDF-SHA256
// (empty salt, info dev-health/auth-service/ratelimit-key/v1) of the signing
// secret, then HMAC-SHA256 of domain, a zero byte and the value, cut to 128
// bits. A change to the info string, the salt, the domain framing or the cut
// moves every bucket and fails here.
func TestTheDigestDerivationIsPinned(t *testing.T) {
	limits := mustLimits(t, httpapi.NewMemoryCounters(nil))
	if got := hex.EncodeToString(limits.key); got != "700764053683047808a60fe857857ef78898fddd8e781c25f281363770d34ad7" {
		t.Fatalf("derived key = %s", got)
	}
	for _, tc := range []struct{ domain, value, want string }{
		{"email", "alice@example.com", "941c792325217e1be5602103d8d5cbd8"},
		{"link-secret", "alice@example.com", "e7a9c0b98c9fc070539a55c7207b27ca"},
		{"principal", "3f2b8c1e-0000-4000-8000-000000000001", "83a689039a013268ef5d3eade3494f7f"},
	} {
		if got := limits.digest(tc.domain, tc.value); got != tc.want {
			t.Errorf("digest(%s, %s) = %s, want %s", tc.domain, tc.value, got, tc.want)
		}
	}
}

func TestTheDigestOfOneIdentifierDiffersPerSecretAndPerDomain(t *testing.T) {
	store := httpapi.NewMemoryCounters(nil)
	first, err := NewLimits(store, testRateLimitSecret)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewLimits(store, "another-signing-secret-0123456789-abcdef")
	if err != nil {
		t.Fatal(err)
	}
	const email = "alice@example.com"
	if first.digest("email", email) == second.digest("email", email) {
		t.Fatal("one e-mail digests alike under two secrets: the key is not the secret's")
	}
	if first.digest("email", email) == first.digest("link-secret", email) {
		t.Fatal("an e-mail and a link secret of one value share a digest")
	}
	if got := first.digest("email", email); len(got) != 32 {
		t.Fatalf("digest %q is %d hex characters, want 32 (128 bits)", got, len(got))
	}
	if strings.Contains(hex.EncodeToString(first.key), testRateLimitSecret) || string(first.key) == testRateLimitSecret {
		t.Fatal("the derived key is the secret")
	}
}

func TestLimitsRefuseABadStoreOrSecret(t *testing.T) {
	if _, err := NewLimits(nil, testRateLimitSecret); err == nil {
		t.Error("limits without a store")
	}
	if _, err := NewLimits(httpapi.NewMemoryCounters(nil), "too-short"); err == nil {
		t.Error("limits under a short secret")
	}
}

// The verify route reads the link secret only; a caller-chosen e-mail in the
// query must not open a fresh budget (round 1, P1).
func TestVerifyIsOneBudgetPerAddressWhateverTheQueryEmail(t *testing.T) {
	c := newCredFixture(t)
	h := trustedProxy(t, c.routes)
	for i := 0; i < 10; i++ {
		email := "variant" + string(rune('a'+i)) + "@example.com"
		if r := send(h, clientPeer, "", http.MethodGet, PatternVerifyEmail+"?token=x&email="+email, "", nil); r.Code != http.StatusBadRequest {
			t.Fatalf("verify %d = %d, want 400", i+1, r.Code)
		}
	}
	assertRateLimited(t, "11th verify with a new query e-mail", send(h, clientPeer, "", http.MethodGet, PatternVerifyEmail+"?token=x&email=fresh@example.com", "", nil))
	assertRateLimited(t, "11th verify with none", send(h, clientPeer, "", http.MethodGet, PatternVerifyEmail+"?token=y", "", nil))
	if r := send(h, "198.51.100.7:5000", "", http.MethodGet, PatternVerifyEmail+"?token=x", "", nil); r.Code != http.StatusBadRequest {
		t.Fatalf("another address = %d, want 400", r.Code)
	}
}

// A link or unlink that fails validation costs nothing (round 1, P1).
func TestRefusedSocialLinkRequestsCostNoAllowance(t *testing.T) {
	f, _ := linkFixture(t)
	h := trustedProxy(t, f.server)
	header := f.bearer(f.grant())
	for i := 0; i < 15; i++ {
		if r := send(h, clientPeer, "", http.MethodPost, PatternSocialLink, `{}`, header); r.Code != http.StatusUnprocessableEntity {
			t.Fatalf("empty link body = %d, want 422", r.Code)
		}
		if r := send(h, clientPeer, "", http.MethodDelete, "/v1/identities/social/not-a-uuid", "", header); r.Code != http.StatusNotFound {
			t.Fatalf("unlink of a non-id = %d, want 404", r.Code)
		}
	}
	if r := send(h, clientPeer, "", http.MethodPost, PatternSocialLink, linkBody, header); r.Code == http.StatusTooManyRequests {
		t.Fatalf("a valid link after 30 refused requests was limited: %s", r.Body)
	}
}

// One client address cannot start mail to more mailboxes than the cap, however
// many different e-mails it names (acp, after round 1).
func TestMailRoutesCapTheMailOneAddressCanStart(t *testing.T) {
	for _, route := range []struct{ name, path string }{{"resend", PatternResendVerification}, {"forgot", PatternForgotPassword}} {
		c := newCredFixture(t)
		h := trustedProxy(t, c.routes)
		spray := func(peer string, i int) *httptest.ResponseRecorder {
			return send(h, peer, "", http.MethodPost, route.path, `{"email":"person`+strings.Repeat("x", i)+`@example.com"}`, nil)
		}
		for i := 1; i <= 20; i++ {
			if r := spray(clientPeer, i); r.Code != http.StatusOK {
				t.Fatalf("%s: e-mail %d = %d, want 200", route.name, i, r.Code)
			}
		}
		for i := 21; i <= 25; i++ {
			assertRateLimited(t, route.name+" e-mail "+string(rune('0'+i%10)), spray(clientPeer, i))
		}
		if r := spray("198.51.100.7:5000", 1); r.Code != http.StatusOK {
			t.Fatalf("%s: another address = %d, want 200", route.name, r.Code)
		}
	}
}

// The cap cannot be dodged by rotating inside one IPv6 /64, or by naming a
// forwarded address from a peer that is not the trusted proxy.
func TestTheAddressKeyHoldsAgainstRotationAndSpoofing(t *testing.T) {
	c := newCredFixture(t)
	h := trustedProxy(t, c.routes)
	forgot := func(peer, forwarded string, i int) *httptest.ResponseRecorder {
		return send(h, peer, forwarded, http.MethodPost, PatternForgotPassword, `{"email":"rot`+strings.Repeat("y", i)+`@example.com"}`, nil)
	}
	for i := 1; i <= 20; i++ {
		forgot("[2001:db8:1:2::"+string(rune('a'+i%6))+strings.Repeat("1", i%3)+"]:5000", "", i)
	}
	assertRateLimited(t, "21st request, rotating inside one /64", forgot("[2001:db8:1:2:ffff:ffff:ffff:1]:5000", "", 21))
	if r := forgot("[2001:db8:1:3::1]:5000", "", 22); r.Code != http.StatusOK {
		t.Fatalf("another /64 = %d, want 200", r.Code)
	}
	for i := 1; i <= 20; i++ {
		forgot(clientPeer, "192.0.2."+strings.Repeat("1", i%3+1), i+100)
	}
	assertRateLimited(t, "21st request with a new spoofed forwarded address each time", forgot(clientPeer, "192.0.2.250", 130))
}

// Every spelling of one mailbox lands in one bucket: NFC against NFD local part,
// case, display-name form, surrounding space.
func TestEverySpellingOfOneMailboxSharesOneBucket(t *testing.T) {
	c := newCredFixture(t)
	h := trustedProxy(t, c.routes)
	spellings := []string{"josé@example.com", "jose\u0301@example.com", "JOSÉ@EXAMPLE.COM", "José <josé@Example.com>", " josé@example.com "}
	for i, email := range spellings[:3] {
		if r := send(h, clientPeer, "", http.MethodPost, PatternForgotPassword, `{"email":"`+email+`"}`, nil); r.Code != http.StatusOK {
			t.Fatalf("spelling %d = %d, want 200: %s", i+1, r.Code, r.Body)
		}
	}
	for i, email := range spellings[3:] {
		assertRateLimited(t, "spelling "+string(rune('4'+i)), send(h, clientPeer, "", http.MethodPost, PatternForgotPassword, `{"email":"`+email+`"}`, nil))
	}
}
