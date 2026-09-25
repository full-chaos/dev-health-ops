//go:build integration

package authflowvenue_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/http"
	"net/http/httptest"
	"net/mail"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	mailpkg "github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/smtpcapture"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// Fixed ids of what this venue seeds beside sessionscenario.Seed, so audit
// rows and invites name the same subjects on both planes.
const (
	orgA  = "a0000000-0000-4000-8000-00000000000a"
	orgD  = "d0000000-0000-4000-8000-00000000000d"
	alice = "10000000-0000-4000-8000-000000000001"
	judy  = "10000000-0000-4000-8000-000000000009"
	ivan  = "20000000-0000-4000-8000-000000000001"
	olive = "20000000-0000-4000-8000-000000000002"
	oscar = "20000000-0000-4000-8000-000000000003"
	uma   = "20000000-0000-4000-8000-000000000004"
	pia   = "20000000-0000-4000-8000-000000000005"

	inviteValid    = "30000000-0000-4000-8000-000000000001"
	inviteExpired  = "30000000-0000-4000-8000-000000000002"
	inviteAccepted = "30000000-0000-4000-8000-000000000003"
	inviteOrgD     = "30000000-0000-4000-8000-000000000004"
	inviteMember   = "30000000-0000-4000-8000-000000000005"

	expiredVerification = "40000000-0000-4000-8000-000000000001"
	expiredReset        = "40000000-0000-4000-8000-000000000002"
	unknownLink         = "40000000-0000-4000-8000-000000000003"
)

const (
	fromAddress = "Dev Health <auth@example.test>"
	appBaseURL  = "https://app.example.test/"
)

// linkToken is the link token for id under the venue's JWT key: both planes
// sign with it, so a seeded row's token is the same text on both.
func linkToken(id string) (token, hash string) {
	return signedtoken.Build(uuid.MustParse(id), sessionscenario.Key)
}

var linkPattern = regexp.MustCompile(`token=([0-9a-f]{32}\.[0-9a-f]{64})`)

// sentMail is one captured message as its receiver reads it: envelope and
// headers exactly, the HTML part decoded, the link token lifted out so each
// plane's own random token can be compared by shape and used in later
// requests.
type sentMail struct {
	mailFrom, rcptTo, from, to, subject, partType string
	html                                          string // the token replaced by TOKEN
	token                                         string
}

func decodeMail(t *testing.T, captured smtpcapture.Mail) sentMail {
	t.Helper()
	captured = smtpcapture.Normalize(captured)
	message, err := mail.ReadMessage(strings.NewReader(captured.Data))
	if err != nil {
		t.Fatalf("captured message does not parse: %v\n%s", err, captured.Data)
	}
	out := sentMail{mailFrom: captured.MailFrom, rcptTo: strings.Join(captured.RcptTo, ","),
		from: message.Header.Get("From"), to: message.Header.Get("To"), subject: message.Header.Get("Subject")}
	mediaType, params, err := mime.ParseMediaType(message.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/alternative" {
		t.Fatalf("captured message is not multipart/alternative: %q (%v)", message.Header.Get("Content-Type"), err)
	}
	part, err := multipart.NewReader(message.Body, params["boundary"]).NextRawPart()
	if err != nil {
		t.Fatalf("captured message has no part: %v", err)
	}
	out.partType = part.Header.Get("Content-Type") + "; " + part.Header.Get("Content-Transfer-Encoding")
	raw, err := io.ReadAll(part)
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	switch strings.ToLower(part.Header.Get("Content-Transfer-Encoding")) {
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(strings.NewReplacer("\r", "", "\n", "").Replace(body))
		if err != nil {
			t.Fatalf("part is not base64: %v", err)
		}
		body = string(decoded)
	case "quoted-printable":
		decoded, err := io.ReadAll(quotedprintable.NewReader(strings.NewReader(body)))
		if err != nil {
			t.Fatalf("part is not quoted-printable: %v", err)
		}
		body = string(decoded)
	}
	match := linkPattern.FindStringSubmatch(body)
	if match == nil {
		t.Fatalf("no link token in the captured HTML:\n%s", body)
	}
	out.token = match[1]
	out.html = strings.ReplaceAll(body, out.token, "TOKEN")
	return out
}

// step is one request, per plane: a later step carries the token each plane
// e-mailed or issued itself.
type step struct {
	name   string
	python venueoracle.Request
	goSide venueoracle.Request
}

func request(name, method, path, bearer, body string) venueoracle.Request {
	// The audit rows record the client address and agent: both planes see
	// the same ones (the forwarded address, as extract_request_metadata
	// reads it).
	headers := map[string]string{"User-Agent": "venue-oracle/1", "X-Forwarded-For": "203.0.113.7"}
	if bearer != "" {
		headers["Authorization"] = "Bearer " + bearer
	}
	var encoded *string
	if body != "" {
		headers["Content-Type"] = "application/json"
		encoded = venueoracle.B64(body)
	}
	if path == "/api/v1/auth/register" {
		// Register is origin-checked; both planes run with the default
		// CORS_ALLOWED_ORIGINS.
		headers["Origin"] = "http://localhost:3000"
	}
	return venueoracle.Request{Name: name, Method: method, Path: path, Headers: headers, Body: encoded}
}

// withHeaders is s with its Origin dropped and headers set on both planes.
func withHeaders(s step, headers map[string]string) step {
	for _, request := range []*venueoracle.Request{&s.python, &s.goSide} {
		copied := map[string]string{}
		for key, value := range request.Headers {
			if key != "Origin" {
				copied[key] = value
			}
		}
		for key, value := range headers {
			copied[key] = value
		}
		request.Headers = copied
	}
	return s
}

func same(name, method, path, bearer, body string) step {
	r := request(name, method, path, bearer, body)
	return step{name: name, python: r, goSide: r}
}

func split(name, method, path string, bearer, body func(plane string) string) step {
	return step{name: name, python: request(name, method, path, bearer("python"), body("python")),
		goSide: request(name, method, path, bearer("go"), body("go"))}
}

// visit is a GET whose query carries each plane's own link token.
func visit(name, path string, token func(plane string) string) step {
	return step{name: name, python: request(name, http.MethodGet, path+"?token="+token("python"), "", ""),
		goSide: request(name, http.MethodGet, path+"?token="+token("go"), "", "")}
}

// hourRetryPattern is the limited answer's remaining window: both planes
// count a one-hour window, started a moment apart.
var hourRetryPattern = regexp.MustCompile(`"retry_after_seconds":(\d+)`)

func normalize(body string) string {
	body = sessionscenario.Normalize(body)
	return hourRetryPattern.ReplaceAllStringFunc(body, func(match string) string {
		seconds, _ := strconv.Atoi(hourRetryPattern.FindStringSubmatch(match)[1])
		if seconds >= 3590 && seconds <= 3600 {
			return `"retry_after_seconds":"<about 3600>"`
		}
		return match
	})
}

type runner struct {
	t              *testing.T
	ctx            context.Context
	venue          *venueoracle.Venue
	goBase         string
	pySink, goSink *smtpcapture.Server
	receipt        strings.Builder
	// mails are each plane's messages so far, by "recipient|subject", the
	// latest last.
	mails map[string]map[string][]sentMail
}

// batch opens fresh rate-limit windows, answers steps on both planes, and
// compares every answer and then every message each plane sent.
func (r *runner) batch(label string, steps []step) map[string][2]venueoracle.Response {
	r.t.Helper()
	flushValkey(r.t, r.ctx, r.venue.ValkeyURI, r.venue.PythonValkeyURI)
	pyRequests := make([]venueoracle.Request, len(steps))
	for index, s := range steps {
		pyRequests[index] = s.python
	}
	pyResponses := r.venue.ServePython(r.t, pyRequests)
	out := map[string][2]venueoracle.Response{}
	fmt.Fprintf(&r.receipt, "-- %s\n", label)
	for index, s := range steps {
		goResponse := venueoracle.Do(r.t, r.goBase, s.goSide)
		same, compared, pyShown, goShown := venueoracle.Compare(s.python, pyResponses[index], goResponse,
			venueoracle.DiffOptions{Normalize: func(_ venueoracle.Request, body string) string { return normalize(body) }})
		fmt.Fprintf(&r.receipt, "%-70s python=%d go=%d %s\n", s.name, pyResponses[index].Status, goResponse.Status, venueoracle.Mark(same))
		if !same {
			r.t.Errorf("%s:\n python %d %s %v\n go     %d %s %v", s.name, pyShown.Status, pyShown.Body, pick(pyShown.Headers, compared),
				goShown.Status, goShown.Body, pick(goShown.Headers, compared))
		}
		out[s.name] = [2]venueoracle.Response{pyResponses[index], goResponse}
	}
	r.compareMail(label)
	return out
}

func pick(headers map[string]string, names []string) map[string]string {
	out := map[string]string{}
	for _, name := range names {
		if value, ok := headers[name]; ok {
			out[name] = value
		}
	}
	return out
}

// compareMail drains both sinks and compares the messages in the order each
// plane sent them.
func (r *runner) compareMail(label string) {
	r.t.Helper()
	planes := map[string][]sentMail{}
	for plane, sink := range map[string]*smtpcapture.Server{"python": r.pySink, "go": r.goSink} {
		for _, captured := range sink.Drain(r.t) {
			decoded := decodeMail(r.t, captured)
			planes[plane] = append(planes[plane], decoded)
			key := decoded.rcptTo + "|" + decoded.subject
			r.mails[plane][key] = append(r.mails[plane][key], decoded)
		}
	}
	py, gv := planes["python"], planes["go"]
	same := len(py) == len(gv)
	for index := 0; same && index < len(py); index++ {
		a, b := py[index], gv[index]
		a.token, b.token = "", ""
		same = a == b
	}
	fmt.Fprintf(&r.receipt, "%-70s python=%d go=%d %s\n", label+": e-mails", len(py), len(gv), venueoracle.Mark(same))
	if !same {
		r.t.Errorf("%s: the planes sent different e-mail:\n python %+v\n go     %+v", label, py, gv)
	}
}

// token is the link token plane last mailed to recipient under subject.
func (r *runner) token(plane, recipient, subject string) string {
	r.t.Helper()
	sent := r.mails[plane]["<"+recipient+">|"+subject]
	if len(sent) == 0 {
		sent = r.mails[plane][recipient+"|"+subject]
	}
	if len(sent) == 0 {
		r.t.Fatalf("%s sent no %q e-mail to %s (have %v)", plane, subject, recipient, keys(r.mails[plane]))
	}
	return sent[len(sent)-1].token
}

func keys(m map[string][]sentMail) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func (r *runner) rows(name, query string) {
	r.t.Helper()
	pyRows := venueoracle.TableRows(r.t, r.ctx, r.venue.AdminURI(r.t, r.venue.SourceDB), query)
	goRows := venueoracle.TableRows(r.t, r.ctx, r.venue.AdminURI(r.t, r.venue.GoDB), query)
	same := pyRows == goRows && pyRows != ""
	fmt.Fprintf(&r.receipt, "%s rows after the scenario: %s\n", name, venueoracle.Mark(same))
	if !same {
		r.t.Errorf("%s rows differ (or are empty):\n python %s\n go     %s", name, pyRows, goRows)
	}
}

func seed(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
	t.Helper()
	raw := venue.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.api.services.users:_hash_password",
		Args: []any{sessionscenario.Password}})
	value, err := pyjson.Decode(raw[0])
	hash, ok := value.(string)
	if err != nil || !ok {
		t.Fatalf("password hash: %v %s", err, raw[0])
	}
	tokens := sessionscenario.Seed(t, ctx, admin, hash)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed %q: %v", sql, err)
		}
	}
	for _, u := range []struct {
		id, email string
		verified  bool
	}{{ivan, "ivan@example.com", true}, {olive, "olive@example.com", true}, {oscar, "oscar@example.com", true},
		{uma, "uma@example.com", false}, {pia, "pia@example.com", true}} {
		exec(`INSERT INTO users (id, email, password_hash, full_name, auth_provider, is_active, is_verified, is_superuser,
	token_version, created_at, updated_at)
VALUES ($1, $2, $3, NULL, 'local', true, $4, false, 0, now() - interval '1 day', now() - interval '1 day')`, u.id, u.email, hash, u.verified)
	}
	for _, invite := range []struct {
		id, org, email, role, status string
		expiresIn                    string
	}{
		{inviteValid, orgA, "ivan@example.com", "admin", "pending", "3 days"},
		{inviteExpired, orgA, "ivan@example.com", "member", "pending", "-1 hour"},
		{inviteAccepted, orgA, "ivan@example.com", "member", "accepted", "3 days"},
		{inviteOrgD, orgD, "oscar@example.com", "member", "pending", "3 days"},
		{inviteMember, orgA, "someone@example.com", "viewer", "pending", "3 days"},
	} {
		_, tokenHash := linkToken(invite.id)
		exec(`INSERT INTO org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, now() + $8::interval, now() - interval '1 hour', now() - interval '1 hour')`,
			invite.id, invite.org, invite.email, invite.role, tokenHash, alice, invite.status, invite.expiresIn)
	}
	_, verificationHash := linkToken(expiredVerification)
	exec(`INSERT INTO email_verification_tokens (id, user_id, token_hash, expires_at, created_at)
VALUES ($1, $2, $3, now() - interval '1 hour', now() - interval '25 hours')`, expiredVerification, uma, verificationHash)
	_, resetHash := linkToken(expiredReset)
	exec(`INSERT INTO password_reset_tokens (id, user_id, token_hash, expires_at, created_at)
VALUES ($1, $2, $3, now() - interval '1 hour', now() - interval '2 hours')`, expiredReset, judy, resetHash)

	orgless := func(id, email string) map[string]any { return map[string]any{"user_id": id, "email": email} }
	tokens["ivan"] = orgless(ivan, "ivan@example.com")
	tokens["olive"] = orgless(olive, "olive@example.com")
	tokens["oscar"] = orgless(oscar, "oscar@example.com")
	tokens["uma"] = orgless(uma, "uma@example.com")
	tokens["pia"] = orgless(pia, "pia@example.com")
	tokens["alice-noorg"] = map[string]any{"user_id": alice, "email": "alice@example.com", "role": "owner"}
	tokens["alice-orgD"] = map[string]any{"user_id": alice, "email": "alice@example.com", "org_id": orgD, "role": "owner"}
	tokens["alice-admin"] = map[string]any{"user_id": alice, "email": "alice@example.com", "org_id": orgA, "role": "admin"}
	tokens["alice-badorg"] = map[string]any{"user_id": alice, "email": "alice@example.com", "org_id": "not-a-uuid", "role": "owner"}
	tokens["judy"] = map[string]any{"user_id": judy, "email": "judy@example.com", "org_id": orgA, "role": "member"}
	return tokens
}

// TestAuthFlowVenueOracle is the venue differential of register, verify,
// resend-verification, forgot-password, reset-password, accept-invite,
// onboard, onboarding/state and onboarding/skip-integration: the REAL
// Python api (TestClient over dev_health_ops.api.main:app) and the REAL dho
// api route set answer the same batches against two copies of one seeded
// database. Answers are compared byte for byte (tokens by their claims,
// random ids blanked), the e-mail each plane sends is captured off the wire
// by its own SMTP sink and compared message for message, the link each plane
// mailed is followed in a later batch, and the users, organizations,
// memberships, link-token, org_invites, refresh_tokens and audit_logs rows
// are compared at the end.
func TestAuthFlowVenueOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	pySink, goSink := smtpcapture.Start(t), smtpcapture.Start(t)
	pyHost, pyPort := pySink.HostPort(t)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: sessionscenario.Key, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		PythonEnv: []string{"EMAIL_PROVIDER=smtp", "EMAIL_FROM_ADDRESS=" + fromAddress,
			"SMTP_HOST=" + pyHost, "SMTP_PORT=" + strconv.Itoa(pyPort), "APP_BASE_URL=" + appBaseURL},
		Seed: seed,
	})
	for _, database := range []string{venue.PythonClickHouseDB, venue.GoClickHouseDB} {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range sessionscenario.MetricsStatements {
			if err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("seed metrics: %v", err)
			}
		}
		_ = conn.Close()
	}
	goHost, goPort := goSink.HostPort(t)
	t.Setenv("EMAIL_PROVIDER", "smtp")
	t.Setenv("EMAIL_FROM_ADDRESS", fromAddress)
	t.Setenv("SMTP_HOST", goHost)
	t.Setenv("SMTP_PORT", strconv.Itoa(goPort))
	for _, name := range []string{"EMAIL_API_KEY", "RESEND_API_KEY", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_USE_TLS",
		"SMTP_TLS_CA_FILE", "SMTP_TLS_SERVER_NAME", "AUTH_AUTO_CREATE_ORG_ON_REGISTER", "AUTH_REGISTER_LIMIT"} {
		t.Setenv(name, "")
		_ = os.Unsetenv(name)
	}
	goSender, err := mailpkg.NewSenderFromEnv(nil)
	if err != nil {
		t.Fatalf("go mail sender: %v", err)
	}
	r := &runner{t: t, ctx: ctx, venue: venue, pySink: pySink, goSink: goSink,
		mails:  map[string]map[string][]sentMail{"python": {}, "go": {}},
		goBase: startGoAPI(t, ctx, venue, admin.InviteConfig{Mail: goSender, TokenSecret: sessionscenario.Key, AppBaseURL: appBaseURL, AppBaseURLSet: true})}
	tok := venue.Tokens
	const (
		register = "/api/v1/auth/register"
		verify   = "/api/v1/auth/verify"
		resend   = "/api/v1/auth/resend-verification"
		forgot   = "/api/v1/auth/forgot-password"
		reset    = "/api/v1/auth/reset-password"
		accept   = "/api/v1/auth/accept-invite"
		onboard  = "/api/v1/auth/onboard"
		state    = "/api/v1/auth/onboarding/state"
		skip     = "/api/v1/auth/onboarding/skip-integration"
		login    = "/api/v1/auth/login"
		refresh  = "/api/v1/auth/refresh"
	)
	post, get := http.MethodPost, http.MethodGet
	validInvite, _ := linkToken(inviteValid)
	expiredInvite, _ := linkToken(inviteExpired)
	acceptedInvite, _ := linkToken(inviteAccepted)
	orgDInvite, _ := linkToken(inviteOrgD)
	memberInvite, _ := linkToken(inviteMember)
	expiredVerify, _ := linkToken(expiredVerification)
	expiredResetToken, _ := linkToken(expiredReset)
	unknown, _ := linkToken(unknownLink)
	nonASCII := strings.Split(unknown, ".")[0] + ".\u00e9"

	// Batch 1: everything that needs no earlier answer. Three register
	// bodies pass validation (the 3/hour limit counts only those).
	first := r.batch("independent", []step{
		same("register: new user with an org name", post, register, "", `{"email": "Nina.New@Example.com", "password": "Nina-pass-2026", "full_name": "Nina New", "org_name": "Nina's Team  Ünï"}`),
		same("register: taken, other case", post, register, "", `{"email": "ALICE@example.com", "password": "Another-pass-9"}`),
		same("register: policy, too short", post, register, "", `{"email": "shorty@example.com", "password": "abcdefg1"}`),
		same("register: password 7 chars", post, register, "", `{"email": "x@example.com", "password": "abcdef1"}`),
		same("register: password 129 chars", post, register, "", `{"email": "x@example.com", "password": "`+strings.Repeat("a1", 64)+`a"}`),
		same("register: password lone surrogate", post, register, "", `{"email": "x@example.com", "password": "abcdefgh1\ud800"}`),
		same("register: invalid email", post, register, "", `{"email": "not-an-email", "password": "Valid-pass-2026"}`),
		same("register: missing fields", post, register, "", `{}`),
		same("register: full_name int", post, register, "", `{"email": "x@example.com", "password": "Valid-pass-2026", "full_name": 5}`),
		same("register: body is a list", post, register, "", `[]`),
		same("register: malformed json", post, register, "", `{"email":`),
		withHeaders(same("register: no origin", post, register, "", `{"email": "o1@example.com", "password": "Valid-pass-2026"}`), nil),
		withHeaders(same("register: origin not allowed", post, register, "", `{}`), map[string]string{"Origin": "https://evil.example"}),
		withHeaders(same("register: origin null", post, register, "", `{}`), map[string]string{"Origin": "null"}),
		withHeaders(same("register: origin upper case, padded, with a path", post, register, "", `{}`),
			map[string]string{"Origin": "  HTTP://LOCALHOST:3000/x "}),
		withHeaders(same("register: bad origin, allowed referer", post, register, "", `{}`),
			map[string]string{"Origin": "https://evil.example", "Referer": "http://localhost:3000/auth/signup?x=1"}),
		withHeaders(same("register: referer not allowed", post, register, "", `{}`), map[string]string{"Referer": "https://evil.example/"}),
		withHeaders(same("register: origin urlparse error", post, register, "", `{}`), map[string]string{"Origin": "http://[::1"}),
		withHeaders(same("register: referer urlparse error", post, register, "", `{}`), map[string]string{"Referer": "http://[::1"}),
		withHeaders(same("register: origin other port", post, register, "", `{}`), map[string]string{"Origin": "http://localhost:3001"}),
		withHeaders(step{name: "register: GET is not checked", python: request("register: GET is not checked", get, register, "", ""),
			goSide: request("register: GET is not checked", get, register, "", "")}, nil),
		same("login: alice (a refresh token the reset revokes)", post, login, "", `{"email": "alice@example.com", "password": "`+sessionscenario.Password+`"}`),
		same("resend: unknown address", post, resend, "", `{"email": "nobody@example.com"}`),
		same("resend: already verified", post, resend, "", `{"email": "alice@example.com"}`),
		same("resend: unverified, upper case", post, resend, "", `{"email": "UMA@example.com"}`),
		same("resend: invalid email", post, resend, "", `{"email": "nope"}`),
		same("resend: missing", post, resend, "", `{}`),
		same("forgot: unknown address", post, forgot, "", `{"email": "nobody@example.com"}`),
		same("forgot: alice (member: audited)", post, forgot, "", `{"email": "alice@example.com"}`),
		same("forgot: pia (no membership: not audited)", post, forgot, "", `{"email": "pia@example.com"}`),
		same("forgot: invalid email", post, forgot, "", `{"email": 7}`),
		same("verify: missing token", get, verify, "", ""),
		same("verify: empty token", get, verify+"?token=", "", ""),
		same("verify: garbage", get, verify+"?token=garbage", "", ""),
		same("verify: expired", get, verify+"?token="+expiredVerify, "", ""),
		same("verify: signed, no such token", get, verify+"?token="+unknown, "", ""),
		same("verify: non-ascii signature", get, verify+"?token="+nonASCII, "", ""),
		same("reset: missing fields", post, reset, "", `{}`),
		same("reset: password 7 chars", post, reset, "", `{"token": "x", "new_password": "abcdef1"}`),
		same("reset: garbage", post, reset, "", `{"token": "garbage", "new_password": "Brand-new-pass-7"}`),
		same("reset: expired", post, reset, "", `{"token": "`+expiredResetToken+`", "new_password": "Brand-new-pass-7"}`),
		same("reset: non-ascii signature", post, reset, "", `{"token": "`+nonASCII+`", "new_password": "Brand-new-pass-7"}`),
		same("state: alice", get, state, tok["alice"], ""),
		same("state: alice, no org claim", get, state, tok["alice-noorg"], ""),
		same("state: alice, org she is not in", get, state, tok["alice-orgD"], ""),
		same("state: alice, admin role", get, state, tok["alice-admin"], ""),
		same("state: alice, org claim not a uuid", get, state, tok["alice-badorg"], ""),
		same("state: bob superuser, no membership", get, state, tok["bob"], ""),
		same("state: carol unverified", get, state, tok["carol"], ""),
		same("state: judy", get, state, tok["judy"], ""),
		same("state: pia orgless", get, state, tok["pia"], ""),
		same("state: uma unverified orgless", get, state, tok["uma"], ""),
		same("state: ghost", get, state, tok["ghost"], ""),
		same("state: no credential", get, state, "", ""),
		same("skip: no org claim", post, skip, tok["alice-noorg"], ""),
		same("skip: org claim not a uuid", post, skip, tok["alice-badorg"], ""),
		same("skip: not a member", post, skip, tok["alice-orgD"], ""),
		same("skip: no credential", post, skip, "", ""),
		same("skip: judy", post, skip, tok["judy"], ""),
		same("skip: judy again", post, skip, tok["judy"], ""),
		same("state: alice after the skip", get, state, tok["alice"], ""),
		same("accept: no credential", post, accept, "", `{"token": "`+validInvite+`"}`),
		same("accept: no credential, bad body", post, accept, "", `{}`),
		same("accept: missing token", post, accept, tok["ivan"], `{}`),
		same("accept: expired", post, accept, tok["ivan"], `{"token": "`+expiredInvite+`"}`),
		same("accept: already accepted", post, accept, tok["ivan"], `{"token": "`+acceptedInvite+`"}`),
		same("accept: garbage", post, accept, tok["ivan"], `{"token": "garbage"}`),
		same("accept: non-ascii signature", post, accept, tok["ivan"], `{"token": "`+nonASCII+`"}`),
		same("accept: ivan joins org A", post, accept, tok["ivan"], `{"token": "`+validInvite+`"}`),
		same("accept: alice, already a member", post, accept, tok["alice"], `{"token": "`+memberInvite+`"}`),
		same("onboard: alice already onboarded", post, onboard, tok["alice"], `{"action": "create_org", "org_name": "X"}`),
		same("onboard: invalid action", post, onboard, tok["pia"], `{"action": "dance"}`),
		same("onboard: blank name", post, onboard, tok["pia"], `{"action": "create_org", "org_name": "   "}`),
		same("onboard: no name", post, onboard, tok["pia"], `{"action": "create_org"}`),
		same("onboard: name 101 chars", post, onboard, tok["pia"], `{"action": "create_org", "org_name": "`+strings.Repeat("n", 101)+`"}`),
		same("onboard: join without a code", post, onboard, tok["pia"], `{"action": "join_org"}`),
		same("onboard: join with an empty code", post, onboard, tok["pia"], `{"action": "join_org", "invite_code": ""}`),
		same("onboard: join with garbage", post, onboard, tok["pia"], `{"action": "join_org", "invite_code": "garbage"}`),
		same("onboard: missing action", post, onboard, tok["pia"], `{}`),
		same("onboard: no credential", post, onboard, "", `{"action": "create_org", "org_name": "X"}`),
		same("onboard: olive creates an org", post, onboard, tok["olive"], `{"action": "create_org", "org_name": "  Olive's Ünicode Org!! "}`),
		same("onboard: olive again", post, onboard, tok["olive"], `{"action": "create_org", "org_name": "Again"}`),
		same("onboard: oscar joins org D", post, onboard, tok["oscar"], `{"action": "join_org", "invite_code": "`+orgDInvite+`"}`),
	})

	r.batch("register policy", []step{
		same("register: common password", post, register, "", `{"email": "common@example.com", "password": "PassWord1000"}`),
		same("register: no digit", post, register, "", `{"email": "nodigit@example.com", "password": "abcdefghijklm"}`),
		same("register: password over 72 bytes", post, register, "", `{"email": "long@example.com", "password": "Ab1`+strings.Repeat("x", 70)+`"}`),
	})
	r.batch("register names", []step{
		same("register: full name with a lone surrogate", post, register, "", `{"email": "surr@example.com", "password": "Surr-pass-2026", "full_name": "\ud800"}`),
		same("register: empty org name", post, register, "", `{"email": "emma@example.com", "password": "Emma-pass-2026", "full_name": "", "org_name": ""}`),
		same("register: no names", post, register, "", `{"email": "ned@example.com", "password": "Ned-pass-2026"}`),
	})
	t.Setenv("AUTH_AUTO_CREATE_ORG_ON_REGISTER", " OFF ")
	r.batch("register without an org", []step{
		same("register: solo, auto-org off", post, register, "", `{"email": "solo@example.com", "password": "Solo-pass-2026"}`),
	})
	t.Setenv("AUTH_AUTO_CREATE_ORG_ON_REGISTER", "maybe")
	r.batch("register, unrecognized auto-org value", []step{
		same("register: auto-org value not recognized keeps the default", post, register, "", `{"email": "maya@example.com", "password": "Maya-pass-2026"}`),
	})
	_ = os.Unsetenv("AUTH_AUTO_CREATE_ORG_ON_REGISTER")

	// Batch: follow every link each plane mailed, with that plane's token.
	link := func(recipient, subject string) func(string) string {
		return func(plane string) string { return r.token(plane, recipient, subject) }
	}
	verifyNina, verifyUma := link("nina.new@example.com", "Verify your email address"), link("uma@example.com", "Verify your email address")
	resetAlice, resetPia := link("alice@example.com", "Reset your password"), link("pia@example.com", "Reset your password")
	none := func(string) string { return "" }
	aliceRefresh := func(plane string) string {
		index := 0
		if plane == "go" {
			index = 1
		}
		return sessionscenario.Field(first["login: alice (a refresh token the reset revokes)"][index].Body, "refresh_token")
	}
	r.batch("links", []step{
		visit("verify: nina's link", verify, verifyNina),
		visit("verify: nina's link again", verify, verifyNina),
		visit("verify: uma's link", verify, verifyUma),
		same("state: uma now verified", get, state, tok["uma"], ""),
		split("reset: alice's link", post, reset, none, func(p string) string {
			return `{"token": "` + resetAlice(p) + `", "new_password": "Brand-new-pass-7"}`
		}),
		split("reset: alice's link again", post, reset, none, func(p string) string {
			return `{"token": "` + resetAlice(p) + `", "new_password": "Other-new-pass-7"}`
		}),
		split("reset: pia's link, short password", post, reset, none, func(p string) string {
			return `{"token": "` + resetPia(p) + `", "new_password": "short"}`
		}),
		split("reset: pia's link", post, reset, none, func(p string) string {
			return `{"token": "` + resetPia(p) + `", "new_password": "Pia-new-pass-77"}`
		}),
		same("login: alice, new password", post, login, "", `{"email": "alice@example.com", "password": "Brand-new-pass-7"}`),
		same("login: alice, old password", post, login, "", `{"email": "alice@example.com", "password": "`+sessionscenario.Password+`"}`),
		split("refresh: alice's token from before the reset", post, refresh, none, func(p string) string {
			return `{"refresh_token": "` + aliceRefresh(p) + `"}`
		}),
		same("login: pia, new password", post, login, "", `{"email": "pia@example.com", "password": "Pia-new-pass-77"}`),
	})

	// Batch: each limit's window, from fresh.
	var limits []step
	for i := 1; i <= 4; i++ {
		limits = append(limits, same(fmt.Sprintf("limit: register %d/4", i), post, register, "", `{"email": "alice@example.com", "password": "Another-pass-9"}`))
	}
	for i := 1; i <= 4; i++ {
		limits = append(limits, same(fmt.Sprintf("limit: resend %d/4", i), post, resend, "", `{"email": "alice@example.com"}`))
	}
	limits = append(limits, same("limit: resend, another address", post, resend, "", `{"email": "Nobody@example.com"}`))
	for i := 1; i <= 4; i++ {
		limits = append(limits, same(fmt.Sprintf("limit: forgot %d/4", i), post, forgot, "", `{"email": "nobody@example.com"}`))
	}
	for i := 1; i <= 11; i++ {
		limits = append(limits, same(fmt.Sprintf("limit: verify %d/11", i), get, verify+"?token=garbage", "", ""))
	}
	limits = append(limits, same("limit: verify, another e-mail key", get, verify+"?token=garbage&email=Someone@Example.com", "", ""))
	for i := 1; i <= 3; i++ {
		limits = append(limits, same(fmt.Sprintf("limit: reset has none %d/3", i), post, reset, "", `{"token": "garbage", "new_password": "Brand-new-pass-7"}`))
	}
	r.batch("limits", limits)

	r.rows("users", `SELECT email, coalesce(username, '<null>'), coalesce(full_name, '<null>'), coalesce(auth_provider, '<null>'),
		is_active, is_verified, is_superuser, token_version, last_login_at IS NOT NULL, updated_at >= created_at,
		coalesce(left(password_hash, 7), '<null>') FROM users WHERE auth_provider IS DISTINCT FROM 'service' ORDER BY email`)
	r.rows("organizations", `SELECT o.name, regexp_replace(o.slug, '-[0-9a-f]{8}$', '-<id8>'),
		coalesce(right(o.slug, 8) = (SELECT left(m.user_id::text, 8) FROM memberships m WHERE m.org_id = o.id AND m.role = 'owner' LIMIT 1), false),
		coalesce(o.description, '<null>'), o.settings::text, o.tier, coalesce(o.stripe_customer_id, '<null>'), o.managed_by, o.is_active,
		o.onboarding_integration_skipped_at IS NOT NULL, o.updated_at >= o.created_at FROM organizations o ORDER BY o.name, o.slug`)
	r.rows("memberships", `SELECT u.email, o.name, m.role, coalesce(i.email, '<null>'), m.joined_at IS NOT NULL, m.updated_at >= m.created_at
		FROM memberships m JOIN users u ON u.id = m.user_id JOIN organizations o ON o.id = m.org_id LEFT JOIN users i ON i.id = m.invited_by_id
		ORDER BY u.email, o.name`)
	for _, table := range []string{"email_verification_tokens", "password_reset_tokens"} {
		r.rows(table, `SELECT u.email, t.expires_at - t.created_at > interval '0', t.expires_at > now(), length(t.token_hash),
			extract(epoch FROM t.expires_at - t.created_at)::int / 60 FROM `+table+` t JOIN users u ON u.id = t.user_id ORDER BY u.email, t.expires_at`)
	}
	r.rows("org_invites", `SELECT id, status, accepted_at IS NOT NULL, updated_at > created_at FROM org_invites ORDER BY id`)
	r.rows("refresh_tokens", `SELECT u.email, coalesce(o.slug, '<null>'), rt.revoked_at IS NOT NULL, count(*)
		FROM refresh_tokens rt JOIN users u ON u.id = rt.user_id LEFT JOIN organizations o ON o.id = rt.org_id
		GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`)
	r.rows("audit_logs", `SELECT coalesce(o.name, a.org_id::text), coalesce(u.email, '<null>'), a.action, a.resource_type,
		coalesce((SELECT 'user:' || email FROM users WHERE id::text = a.resource_id),
		         (SELECT 'org:' || name FROM organizations WHERE id::text = a.resource_id),
		         (SELECT 'membership:' || mu.email || '@' || mo.name FROM memberships mm JOIN users mu ON mu.id = mm.user_id
		            JOIN organizations mo ON mo.id = mm.org_id WHERE mm.id::text = a.resource_id), a.resource_id),
		a.description,
		coalesce(regexp_replace(a.changes::text, '[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', '<uuid>', 'g'), '<null>'),
		coalesce(a.request_metadata::text, '<null>'), a.status, coalesce(a.error_message, '<null>')
		FROM audit_logs a LEFT JOIN organizations o ON o.id = a.org_id LEFT JOIN users u ON u.id = a.user_id
		ORDER BY a.created_at, a.action`)

	venueoracle.WriteProof(t)
	if violations := policy.WriterViolations(); violations > 0 {
		t.Errorf("%d body writes used the wrong writer for their route", violations)
	}
	receipt := r.receipt.String()
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt), 0o600)
	}
	t.Log("\n" + receipt)
}

func flushValkey(t *testing.T, ctx context.Context, uris ...string) {
	t.Helper()
	for _, uri := range uris {
		options, err := valkeygo.ParseURL(uri)
		if err != nil {
			t.Fatal(err)
		}
		client, err := valkeygo.NewClient(options)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Do(ctx, client.B().Flushdb().Build()).Error(); err != nil {
			t.Fatal(err)
		}
		client.Close()
	}
}

// startGoAPI builds the dho api route set against the venue's Go copy as
// the api role, sending its link e-mail with mailConfig. Its rate limits
// count in the venue's Go Valkey.
func startGoAPI(t *testing.T, ctx context.Context, venue *venueoracle.Venue, mailConfig admin.InviteConfig) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("DEV_HEALTH_VENUE_GO_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	verifier, err := edgetoken.New(sessionscenario.Key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(sessionscenario.Key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	valkeyClient, err := valkey.Open(ctx, valkey.DefaultConfig(venue.ValkeyURI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(valkeyClient.Close)
	ch, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.GoAPIClickHouseURI(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	deps := apiservice.Deps{Pool: pool, Valkey: valkeyClient, ClickHouse: ch, Auth: auth, Guard: policy.NewGuard(auth, logger),
		Verifier: verifier, Signer: signer, Invites: mailConfig}
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, apiservice.Routes(deps, logger), scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// venueRoot is the repository root, where the venue finds the Python api.
func venueRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
