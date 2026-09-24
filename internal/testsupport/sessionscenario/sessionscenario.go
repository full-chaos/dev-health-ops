// Package sessionscenario is the session routes' differential scenario:
// one seed, one sequence of request batches (each plane carrying the tokens
// IT issued from step to step), one normalization and one set of row
// queries, shared by two runners:
//
//   - the venue oracle (internal/apiservice/sessionvenue TestSessionVenueOracle), where
//     the REAL Python api answers every batch through TestClient and the
//     Go api answers the same batch; it also records the Python side as a
//     golden and fails when the committed golden differs;
//   - the golden replay (internal/api/session TestSessionRoutesMatchGolden),
//     where only the Go route set runs, against Postgres/ClickHouse/Valkey
//     containers, and every answer and row is compared with that golden.
//
// Bodies are compared with every token replaced by its decoded claims
// (iat, exp, jti and family_id blanked, the lifetime kept), random ids of
// users the scenario creates blanked, and /me's permission list compared
// as a set (Python returns list(set), in hash-seed order).
package sessionscenario

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// Key is JWT_SECRET_KEY on every plane.
const Key = "session-venue-signing-key-0123456789abcdef"

// Fixed ids, so audit rows name the same subjects on every plane.
const (
	orgA  = "a0000000-0000-4000-8000-00000000000a"
	orgB  = "b0000000-0000-4000-8000-00000000000b"
	orgC  = "c0000000-0000-4000-8000-00000000000c"
	orgD  = "d0000000-0000-4000-8000-00000000000d"
	alice = "10000000-0000-4000-8000-000000000001"
	bob   = "10000000-0000-4000-8000-000000000002"
	carol = "10000000-0000-4000-8000-000000000003"
	dave  = "10000000-0000-4000-8000-000000000004"
	erin  = "10000000-0000-4000-8000-000000000005"
	frank = "10000000-0000-4000-8000-000000000006"
	grace = "10000000-0000-4000-8000-000000000007"
	heidi = "10000000-0000-4000-8000-000000000008"
	judy  = "10000000-0000-4000-8000-000000000009"
	karl  = "10000000-0000-4000-8000-00000000000a"
	lena  = "10000000-0000-4000-8000-00000000000b"
	olga  = "10000000-0000-4000-8000-00000000000c"
	ghost = "10000000-0000-4000-8000-0000000000ff"
)

// Password is every seeded user's password.
const Password = "Correct-horse-1"

// SocialEnv is the social-login configuration every plane runs with:
// GitHub and GitLab configured, Google missing its secret.
var SocialEnv = []string{"SOCIAL_GITHUB_CLIENT_ID=gh-client", "SOCIAL_GITHUB_CLIENT_SECRET=gh-secret",
	"SOCIAL_GITLAB_CLIENT_ID=gl-client", "SOCIAL_GITLAB_CLIENT_SECRET=gl-secret", "SOCIAL_GOOGLE_CLIENT_ID=go-client"}

func strPtr(s string) *string { return &s }

// Seed writes the organizations, users and memberships on admin, the
// passwords hashed as passwordHash, every timestamp anchored at a fixed
// instant so answers that carry one (joined_at) are the same on every run,
// and returns the seed access tokens to
// mint: name -> create_access_token keyword arguments.
func Seed(t *testing.T, ctx context.Context, admin *pgxpool.Pool, passwordHash string) map[string]map[string]any {
	t.Helper()
	hash := passwordHash
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed %q: %v", sql, err)
		}
	}
	for _, org := range []struct {
		id, slug, name string
		tier           *string
		active         bool
	}{{orgA, "org-a", "Org A", nil, true}, {orgB, "org-b", "Org B", strPtr("enterprise"), true},
		{orgC, "org-c", "Org C", nil, false}, {orgD, "org-d", "Org D", nil, true}} {
		exec(`INSERT INTO organizations (id, slug, name, tier, is_active, settings, managed_by, created_at, updated_at)
VALUES ($1, $2, $3, coalesce($4, 'community'), $5, '{}', 'self', '2026-08-01 00:00:00+00'::timestamptz, '2026-08-01 00:00:00+00'::timestamptz)`, org.id, org.slug, org.name, org.tier, org.active)
	}
	type user struct {
		id, email, username, fullName, provider, providerID string
		hash                                                bool
		active, verified, superuser                         bool
	}
	for _, u := range []user{
		{alice, "alice@example.com", "alice", "Alice Ä", "local", "", true, true, true, false},
		{bob, "bob@example.com", "bob", "", "local", "", true, true, true, true},
		{carol, "carol@example.com", "carol", "Carol", "local", "", true, true, false, false},
		{dave, "dave@example.com", "dave", "Dave", "local", "", true, false, true, false},
		{erin, "erin@example.com", "erin", "Erin", "local", "", false, true, true, false},
		{frank, "frank@example.com", "frank", "Frank", "github", "100", false, true, true, false},
		{grace, "Grace@Example.COM", "grace", "Grace", "local", "", true, true, true, false},
		{heidi, "heidi@example.com", "heidi", "Heidi", "", "", true, true, false, false},
		{judy, "judy@example.com", "judy", "Judy", "local", "", true, true, true, false},
		{karl, "karl@example.com", "karl", "Karl", "local", "", true, true, false, false},
		{lena, "lena@example.com", "lena", "Lena", "local", "", true, true, true, false},
		{olga, "olga@example.com", "olga", "Olga", "github", "", false, true, true, false},
	} {
		var passwordHash, provider, providerID, fullName *string
		if u.hash {
			passwordHash = &hash
		}
		if u.provider != "" {
			provider = strPtr(u.provider)
		}
		if u.providerID != "" {
			providerID = strPtr(u.providerID)
		}
		if u.fullName != "" {
			fullName = strPtr(u.fullName)
		}
		exec(`INSERT INTO users (id, email, username, password_hash, full_name, auth_provider, auth_provider_id,
	is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 0, '2026-08-01 00:00:00+00'::timestamptz - interval '1 day', '2026-08-01 00:00:00+00'::timestamptz - interval '1 day')`,
			u.id, u.email, u.username, passwordHash, fullName, provider, providerID, u.active, u.verified, u.superuser)
	}
	for _, m := range []struct {
		user, org, role string
		joinedDaysAgo   int
	}{{alice, orgA, "owner", 30}, {alice, orgB, "member", 20}, {alice, orgC, "admin", 10},
		{carol, orgA, "viewer", 5}, {dave, orgA, "member", 5}, {erin, orgA, "member", 5},
		{frank, orgB, "admin", 5}, {grace, orgD, "member", 5}, {judy, orgA, "member", 5}, {lena, orgC, "member", 5}} {
		exec(`INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at)
VALUES (gen_random_uuid(), $1, $2, $3, '2026-08-01 00:00:00+00'::timestamptz - make_interval(days => $4), '2026-08-01 00:00:00+00'::timestamptz - make_interval(days => $4))`,
			m.user, m.org, m.role, m.joinedDaysAgo)
	}
	return map[string]map[string]any{
		"alice": {"user_id": alice, "email": "alice@example.com", "org_id": orgA, "role": "owner", "username": "alice", "full_name": "Alice Ä"},
		"bob":   {"user_id": bob, "email": "bob@example.com", "is_superuser": true, "username": "bob"},
		"carol": {"user_id": carol, "email": "carol@example.com", "org_id": orgA, "role": "viewer"},
		"stale": {"user_id": alice, "email": "alice@example.com", "org_id": orgA, "role": "owner", "token_version": 5},
		"ghost": {"user_id": ghost, "email": "ghost@example.com"},
	}
}

// MetricsStatements seed each plane's ClickHouse database: Org A has data,
// Org B newer data, Org C and D none.
var MetricsStatements = []string{
	"INSERT INTO repo_metrics_daily (org_id, computed_at) VALUES ('" + orgA + "', '2026-08-01 10:00:00')",
	"INSERT INTO user_metrics_daily (org_id, computed_at) VALUES ('" + orgB + "', '2026-08-05 08:30:00')",
	"INSERT INTO work_item_metrics_daily (org_id, computed_at) VALUES ('" + orgA + "', '2026-08-02 11:00:00')",
}

// FakeProvider answers the providers' profile endpoints by bearer token.
type FakeProvider struct {
	mu    sync.Mutex
	calls []string
}

func (f *FakeProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	f.mu.Lock()
	f.calls = append(f.calls, r.URL.Path+" "+token)
	f.mu.Unlock()
	write := func(status int, body string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.Copy(w, strings.NewReader(body))
	}
	switch r.URL.Path + " " + token {
	case "/github/user gh-new":
		write(200, `{"id": 555, "login": "newgh", "name": "New GH", "email": "newgh@example.com"}`)
	case "/github/user gh-frank":
		write(200, `{"id": 100, "login": "frank-gh", "name": "Frank GH", "email": "frank@example.com"}`)
	case "/github/user gh-frank-new-id":
		write(200, `{"id": 101, "login": "frank-gh2", "email": "frank@example.com"}`)
	case "/github/user gh-olga":
		write(200, `{"id": 300, "login": "olga-gh", "email": "olga@example.com"}`)
	case "/github/user gh-alice":
		write(200, `{"id": 777, "login": "alice-gh", "email": "ALICE@example.com"}`)
	case "/github/user gh-private":
		write(200, `{"id": 888, "login": "private-gh", "name": null, "email": null}`)
	case "/github/user/emails gh-private":
		write(200, `[{"email": "other@example.com", "primary": false, "verified": true}, {"email": "private@example.com", "primary": true, "verified": true}]`)
	case "/github/user gh-bad-emails":
		write(200, `{"id": 889, "email": ""}`)
	case "/github/user/emails gh-bad-emails":
		write(200, `["not-an-object", {"email": "x@example.com", "primary": true, "verified": true}]`)
	case "/github/user gh-no-id":
		write(200, `{"login": "noid", "email": "noid@example.com"}`)
	case "/github/user gh-not-json":
		write(200, `not json`)
	case "/gitlab/api/v4/user gl-frank":
		write(200, `{"id": 42, "username": "frank-gl", "email": "frank@example.com"}`)
	case "/gitlab/api/v4/user gl-no-email":
		write(200, `{"id": 43, "username": "noemail"}`)
	case "/gitlab/api/v4/user gl-taken-username":
		write(200, `{"id": 44, "username": "alice", "name": "Other Alice", "email": "other-alice@example.com"}`)
	default:
		write(401, `{"message": "Bad credentials"}`)
	}
}

// Calls is the number of profile requests the provider has answered.
func (f *FakeProvider) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// jwtPattern finds every token in a body.
var jwtPattern = regexp.MustCompile(`eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+`)

// describeJWT is a token's header and claims, in their encoded order, with
// the per-issue values blanked and the lifetime kept.
func describeJWT(token string) string {
	parts := strings.Split(token, ".")
	header, errHeader := base64.RawURLEncoding.DecodeString(parts[0])
	payload, errPayload := base64.RawURLEncoding.DecodeString(parts[1])
	if errHeader != nil || errPayload != nil {
		return "<undecodable jwt>"
	}
	value, err := pyjson.Decode(payload)
	claims, ok := value.(*pyjson.Object)
	if err != nil || !ok {
		return "<undecodable claims>"
	}
	iat, _ := claims.Get("iat")
	exp, _ := claims.Get("exp")
	if iatInt, ok := iat.(pyjson.Int); ok {
		if expInt, ok := exp.(pyjson.Int); ok {
			lifetime := expInt.Int64() - iatInt.Int64()
			// A grace-window replay keeps the successor's recorded expiry and
			// is issued now, so its lifetime is 7 days or a second or so less,
			// depending on when the replay lands.
			if lifetime > 604800-30 && lifetime <= 604800 {
				claims.Set("exp", "<iat+7 days, to the second or just under>")
			} else {
				claims.Set("exp", fmt.Sprintf("<iat+%d>", lifetime))
			}
		}
		claims.Set("iat", "<iat>")
	}
	for _, key := range []string{"jti", "family_id"} {
		if _, present := claims.Get(key); present {
			claims.Set(key, "<"+key+">")
		}
	}
	encoded, _ := pyjson.Marshal(claims)
	return "<jwt " + string(header) + " " + string(encoded) + ">"
}

var retryAfterPattern = regexp.MustCompile(`"retry_after_seconds":(\d+)`)

// randomUUIDPattern matches a uuid4 that is not one of the seed's fixed
// ids (theirs all read "-0000-4000-8000-"): a user the social login just
// created, whose id each plane draws at random.
var randomUUIDPattern = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`)

// permissionsPattern is /me's permissions list: Python returns list(set),
// whose order follows the process's string hash seed (named limit), so the
// list is compared as a set.
var permissionsPattern = regexp.MustCompile(`"permissions":\[([^\]]*)\]`)

// Normalize is the body normalization every comparison applies.
func Normalize(body string) string {
	body = jwtPattern.ReplaceAllStringFunc(body, describeJWT)
	body = randomUUIDPattern.ReplaceAllStringFunc(body, func(id string) string {
		if strings.Contains(id, "-0000-4000-8000-") {
			return id
		}
		return "<random-uuid>"
	})
	body = permissionsPattern.ReplaceAllStringFunc(body, func(match string) string {
		inner := permissionsPattern.FindStringSubmatch(match)[1]
		items := strings.Split(inner, ",")
		sort.Strings(items)
		return `"permissions":<set ` + strings.Join(items, ",") + `>`
	})
	return retryAfterPattern.ReplaceAllStringFunc(body, func(match string) string {
		var seconds int
		_, _ = fmt.Sscanf(match, `"retry_after_seconds":%d`, &seconds)
		if seconds >= 890 && seconds <= 900 {
			return `"retry_after_seconds":"<about 900>"`
		}
		return match
	})
}

// field reads one top-level string field of a JSON body ("" when absent).
func field(body, name string) string {
	value, err := pyjson.DecodeString(body)
	if err != nil {
		return ""
	}
	object, ok := value.(*pyjson.Object)
	if !ok {
		return ""
	}
	text, _ := object.Get(name)
	out, _ := text.(string)
	return out
}

type pair struct {
	name           string
	python, goSide venueoracle.Request
}

type answers struct{ python, goSide venueoracle.Response }

func (a answers) plane(name string) venueoracle.Response {
	if name == "python" {
		return a.python
	}
	return a.goSide
}

// RecordedAnswer is one Python answer, normalized.
type RecordedAnswer struct {
	Name   string `json:"name"`
	Status int    `json:"status"`
	Body   string `json:"body"`
}

// RecordedRows is one row query's Python rendering.
type RecordedRows struct {
	Name string `json:"name"`
	Rows string `json:"rows"`
}

// Recording is the Python side of one scenario run.
type Recording struct {
	Answers []RecordedAnswer `json:"answers"`
	Rows    []RecordedRows   `json:"rows"`
}

// GoldenPath is the committed Recording.
func GoldenPath() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "testdata", "python_golden.json")
}

// LoadGolden reads the committed Recording.
func LoadGolden(t *testing.T) *Recording {
	t.Helper()
	raw, err := os.ReadFile(GoldenPath())
	if err != nil {
		t.Fatal(err)
	}
	var golden Recording
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Answers) < 150 || len(golden.Rows) != 4 {
		t.Fatalf("golden holds %d answers and %d row sets; it was cut short", len(golden.Answers), len(golden.Rows))
	}
	return &golden
}

// Harness runs the scenario against one Go api and either the live Python
// api (Python set) or the golden (Python nil).
type Harness struct {
	T      *testing.T
	GoBase string
	// Tokens are the seed access tokens by name (Seed's specs, minted).
	Tokens map[string]string
	// Python answers one batch with the live Python api; nil replays the
	// golden instead.
	Python func([]venueoracle.Request) []venueoracle.Response
	// Exec runs sql as a superuser on every plane's database.
	Exec func(sql string)
	// Reset opens fresh rate-limit windows on every plane.
	Reset func()
	// Rows renders query on the Python database ("" in replay) and on the
	// Go database.
	Rows func(query string) (python, goRows string)

	golden    *Recording
	cursor    int
	rowCursor int
	recording Recording
	receipt   strings.Builder
}

func (h *Harness) run(pairs []pair) map[string]answers {
	h.T.Helper()
	h.Reset()
	var pyResponses []venueoracle.Response
	if h.Python != nil {
		requests := make([]venueoracle.Request, len(pairs))
		for index, p := range pairs {
			requests[index] = p.python
		}
		pyResponses = h.Python(requests)
	}
	out := map[string]answers{}
	for index, p := range pairs {
		goResponse := venueoracle.Do(h.T, h.GoBase, p.goSide)
		var same bool
		var detail string
		if h.Python != nil {
			var compared []string
			var pyShown, goShown venueoracle.Response
			same, compared, pyShown, goShown = venueoracle.Compare(p.python, pyResponses[index], goResponse,
				venueoracle.DiffOptions{Normalize: func(_ venueoracle.Request, body string) string { return Normalize(body) }})
			detail = fmt.Sprintf("python %d %s %v\n go     %d %s %v", pyResponses[index].Status, pyShown.Body,
				pickHeaders(pyShown.Headers, compared), goResponse.Status, goShown.Body, pickHeaders(goShown.Headers, compared))
			h.recording.Answers = append(h.recording.Answers,
				RecordedAnswer{Name: p.name, Status: pyResponses[index].Status, Body: Normalize(pyResponses[index].Body)})
			out[p.name] = answers{python: pyResponses[index], goSide: goResponse}
		} else {
			if h.cursor >= len(h.golden.Answers) || h.golden.Answers[h.cursor].Name != p.name {
				h.T.Fatalf("golden is out of step with the scenario at %q (cursor %d)", p.name, h.cursor)
			}
			want := h.golden.Answers[h.cursor]
			h.cursor++
			goBody := Normalize(goResponse.Body)
			same = want.Status == goResponse.Status && want.Body == goBody
			detail = fmt.Sprintf("python %d %s\n go     %d %s", want.Status, want.Body, goResponse.Status, goBody)
			// A replay has only the Go plane: later steps read its tokens
			// for both sides.
			out[p.name] = answers{python: goResponse, goSide: goResponse}
		}
		fmt.Fprintf(&h.receipt, "%-64s go=%d %s\n", p.name, goResponse.Status, venueoracle.Mark(same))
		if !same {
			h.T.Errorf("%s:\n %s", p.name, detail)
		}
	}
	return out
}

func pickHeaders(headers map[string]string, names []string) map[string]string {
	out := map[string]string{}
	for _, name := range names {
		if value, ok := headers[name]; ok {
			out[name] = value
		}
	}
	return out
}

func (h *Harness) compareRows(name, query string) {
	h.T.Helper()
	pyRows, goRows := h.Rows(query)
	if h.Python == nil {
		if h.rowCursor >= len(h.golden.Rows) || h.golden.Rows[h.rowCursor].Name != name {
			h.T.Fatalf("golden rows are out of step at %q", name)
		}
		pyRows = h.golden.Rows[h.rowCursor].Rows
		h.rowCursor++
	} else {
		h.recording.Rows = append(h.recording.Rows, RecordedRows{Name: name, Rows: pyRows})
	}
	same := pyRows == goRows && pyRows != ""
	if !same {
		h.T.Errorf("%s after the writes differ (or are empty):\n python %s\n go     %s", name, pyRows, goRows)
	}
	fmt.Fprintf(&h.receipt, "%s rows after writes: %s\n", name, venueoracle.Mark(same))
}

// Run runs the scenario and returns its receipt. Live, it also checks the
// committed golden against the Python answers (DEV_HEALTH_REGENERATE_TABLES=1
// rewrites it); replaying, it compares against that golden.
func Run(h *Harness) string {
	h.T.Helper()
	if h.Python == nil {
		h.golden = LoadGolden(h.T)
	}
	signer, err := edgetoken.NewSigner(Key, "dev-health-ops", "dev-health-api")
	if err != nil {
		h.T.Fatal(err)
	}
	past := time.Now().Add(-3 * time.Hour)
	expired, _ := signer.Access(edgetoken.AccessClaims{UserID: alice, Email: "alice@example.com", OrgID: orgA, Role: "owner"}, past, "e0000000-0000-4000-8000-000000000001")
	refreshAsAccess, _ := signer.Refresh(edgetoken.RefreshClaims{UserID: alice, OrgID: orgA, FamilyID: "f0000000-0000-4000-8000-000000000001"}, time.Now(), "e0000000-0000-4000-8000-000000000002")
	otherSigner, _ := edgetoken.NewSigner("a-different-signing-key-0123456789abcdef", "dev-health-ops", "dev-health-api")
	forgedRefresh, _ := otherSigner.Refresh(edgetoken.RefreshClaims{UserID: alice, OrgID: orgA, FamilyID: "f0000000-0000-4000-8000-000000000002"}, time.Now(), "e0000000-0000-4000-8000-000000000003")
	forgedUnknownOrg, _ := otherSigner.Refresh(edgetoken.RefreshClaims{UserID: alice, OrgID: "99999999-0000-4000-8000-000000000000", FamilyID: "f0000000-0000-4000-8000-000000000003"}, time.Now(), "e0000000-0000-4000-8000-000000000004")
	forgedIntOrg := forgeToken(`{"sub":"x","org_id":7,"type":"refresh","exp":9999999999}`)
	forgedIntSub := forgeToken(`{"sub":5,"org_id":"` + orgA + `","type":"refresh","exp":9999999999}`)
	// Signed with the real key: an access token whose org claim is not a
	// UUID.
	nonUUIDOrg, _ := signer.Access(edgetoken.AccessClaims{UserID: alice, Email: "alice@example.com", OrgID: "not-a-uuid", Role: "owner"}, time.Now(), "e0000000-0000-4000-8000-000000000005")

	// Batch 1: independent requests, identical on both planes.
	b1 := []pair{
		login("login: alice", "alice@example.com", Password, ""),
		login("login: alice org A", "alice@example.com", Password, `, "org_id": "`+orgA+`"`),
		login("login: alice inactive org C", "alice@example.com", Password, `, "org_id": "`+orgC+`"`),
		login("login: alice not a member of D", "alice@example.com", Password, `, "org_id": "`+orgD+`"`),
		login("login: alice org not a uuid", "alice@example.com", Password, `, "org_id": "not-a-uuid"`),
		login("login: alice empty org", "alice@example.com", Password, `, "org_id": ""`),
		login("login: bob superuser", "bob@example.com", Password, ""),
		login("login: bob unknown org", "bob@example.com", Password, `, "org_id": "99999999-0000-4000-8000-000000000000"`),
		login("login: carol unverified", "carol@example.com", Password, ""),
		login("login: dave inactive", "dave@example.com", Password, ""),
		login("login: erin no password", "erin@example.com", Password, ""),
		login("login: frank github no password", "frank@example.com", Password, ""),
		login("login: heidi null provider unverified", "heidi@example.com", Password, ""),
		login("login: unknown user", "nobody@example.com", Password, ""),
		login("login: alice wrong password", "alice@example.com", "wrong-password", ""),
		login("login: display-name form, upper case", "Alice <ALICE@EXAMPLE.COM>", Password, ""),
		login("login: password over 72 bytes", "alice@example.com", strings.Repeat("p", 100), ""),
		login("login: judy", "judy@example.com", Password, ""),
		same("login: password with lone surrogate", http.MethodPost, "/api/v1/auth/login", "", `{"email": "alice@example.com", "password": "\ud800"}`),
		login("login: password 129 chars", "alice@example.com", strings.Repeat("p", 129), ""),
		login("login: invalid email", "not-an-email", Password, ""),
		login("login: special-use domain", "a@localhost", Password, ""),
		same("login: missing fields", http.MethodPost, "/api/v1/auth/login", "", `{}`),
		same("login: null email", http.MethodPost, "/api/v1/auth/login", "", `{"email": null, "password": 5}`),
		same("login: org_id int", http.MethodPost, "/api/v1/auth/login", "", `{"email": "alice@example.com", "password": "x", "org_id": 5}`),
		same("login: body is a list", http.MethodPost, "/api/v1/auth/login", "", `[]`),
		same("login: malformed json", http.MethodPost, "/api/v1/auth/login", "", `{"email":`),
		same("login: no body", http.MethodPost, "/api/v1/auth/login", "", ``),
		same("validate: alice seed token", http.MethodPost, "/api/v1/auth/validate", "", `{"token": "`+h.Tokens["alice"]+`"}`),
		same("validate: stale token version", http.MethodPost, "/api/v1/auth/validate", "", `{"token": "`+h.Tokens["stale"]+`"}`),
		same("validate: user not found", http.MethodPost, "/api/v1/auth/validate", "", `{"token": "`+h.Tokens["ghost"]+`"}`),
		same("validate: refresh token", http.MethodPost, "/api/v1/auth/validate", "", `{"token": "`+refreshAsAccess+`"}`),
		same("validate: expired", http.MethodPost, "/api/v1/auth/validate", "", `{"token": "`+expired+`"}`),
		same("validate: garbage", http.MethodPost, "/api/v1/auth/validate", "", `{"token": "garbage"}`),
		same("validate: empty", http.MethodPost, "/api/v1/auth/validate", "", `{"token": ""}`),
		same("validate: missing", http.MethodPost, "/api/v1/auth/validate", "", `{}`),
		same("validate: lone surrogate", http.MethodPost, "/api/v1/auth/validate", "", `{"token": "\ud800"}`),
		same("me: alice", http.MethodGet, "/api/v1/auth/me", h.Tokens["alice"], ""),
		same("me: bob superuser", http.MethodGet, "/api/v1/auth/me", h.Tokens["bob"], ""),
		same("me: carol viewer", http.MethodGet, "/api/v1/auth/me", h.Tokens["carol"], ""),
		same("me: no credential", http.MethodGet, "/api/v1/auth/me", "", ""),
		same("me: stale", http.MethodGet, "/api/v1/auth/me", h.Tokens["stale"], ""),
		same("me: expired", http.MethodGet, "/api/v1/auth/me", expired, ""),
		same("me organizations: alice", http.MethodGet, "/api/v1/auth/me/organizations", h.Tokens["alice"], ""),
		same("me organizations: bob", http.MethodGet, "/api/v1/auth/me/organizations", h.Tokens["bob"], ""),
		same("me organizations: no credential", http.MethodGet, "/api/v1/auth/me/organizations", "", ""),
		same("switch-org: alice to B", http.MethodPost, "/api/v1/auth/switch-org", h.Tokens["alice"], `{"org_id": "`+orgB+`"}`),
		same("switch-org: alice to inactive C", http.MethodPost, "/api/v1/auth/switch-org", h.Tokens["alice"], `{"org_id": "`+orgC+`"}`),
		same("switch-org: alice to D", http.MethodPost, "/api/v1/auth/switch-org", h.Tokens["alice"], `{"org_id": "`+orgD+`"}`),
		same("switch-org: not a uuid", http.MethodPost, "/api/v1/auth/switch-org", h.Tokens["alice"], `{"org_id": "nope"}`),
		same("switch-org: missing body", http.MethodPost, "/api/v1/auth/switch-org", h.Tokens["alice"], `{}`),
		same("switch-org: no credential", http.MethodPost, "/api/v1/auth/switch-org", "", `{"org_id": "`+orgB+`"}`),
		same("switch-org: no credential, bad body", http.MethodPost, "/api/v1/auth/switch-org", "", `{}`),
		same("logout: anonymous garbage", http.MethodPost, "/api/v1/auth/logout", "", `{"refresh_token": "garbage"}`),
		same("logout: missing body", http.MethodPost, "/api/v1/auth/logout", "", `{}`),
		same("logout: alice seed token", http.MethodPost, "/api/v1/auth/logout", h.Tokens["alice"], `{"refresh_token": "garbage"}`),
		same("logout: expired bearer", http.MethodPost, "/api/v1/auth/logout", expired, `{"refresh_token": "garbage"}`),
		same("logout: signed token, org not a uuid", http.MethodPost, "/api/v1/auth/logout", nonUUIDOrg, `{"refresh_token": "garbage"}`),
		same("refresh: garbage", http.MethodPost, "/api/v1/auth/refresh", "", `{"refresh_token": "garbage"}`),
		same("refresh: access token", http.MethodPost, "/api/v1/auth/refresh", "", `{"refresh_token": "`+h.Tokens["alice"]+`"}`),
		same("refresh: forged, org exists", http.MethodPost, "/api/v1/auth/refresh", "", `{"refresh_token": "`+forgedRefresh+`"}`),
		same("refresh: forged, unknown org", http.MethodPost, "/api/v1/auth/refresh", "", `{"refresh_token": "`+forgedUnknownOrg+`"}`),
		same("refresh: forged, int org", http.MethodPost, "/api/v1/auth/refresh", "", `{"refresh_token": "`+forgedIntOrg+`"}`),
		same("refresh: forged, int sub", http.MethodPost, "/api/v1/auth/refresh", "", `{"refresh_token": "`+forgedIntSub+`"}`),
		same("refresh: missing", http.MethodPost, "/api/v1/auth/refresh", "", `{}`),
		same("social: github new user", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "gh-new"}`),
		same("social: github existing by provider id", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "gh-frank"}`),
		same("social: github email of a local user", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "gh-alice"}`),
		same("social: github private email", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "gh-private"}`),
		same("social: github emails not objects", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "gh-bad-emails"}`),
		same("social: github no id", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "gh-no-id"}`),
		same("social: github not json", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "gh-not-json"}`),
		same("social: github refused token", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "nope"}`),
		same("social: gitlab email of a github user", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "gitlab", "provider_access_token": "gl-frank"}`),
		same("social: gitlab no email", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "gitlab", "provider_access_token": "gl-no-email"}`),
		same("social: gitlab username taken", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "gitlab", "provider_access_token": "gl-taken-username"}`),
		same("social: google not configured", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "google", "provider_access_token": "x"}`),
		same("social: unknown provider", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "bitbucket", "provider_access_token": "x"}`),
		same("social: missing token", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "github"}`),
	}
	one := h.run(b1)
	// Batch 1b: more logins, in a fresh 20-per-window login budget.
	for name, answer := range h.run([]pair{
		login("login: karl unverified, no membership, an existing org", "karl@example.com", Password, `, "org_id": "`+orgD+`"`),
		login("login: lena only in an inactive org, empty org", "lena@example.com", Password, `, "org_id": ""`),
		login("login: alice for the rotation race", "alice@example.com", Password, ""),
		login("login: alice for the aged rotation", "alice@example.com", Password, ""),
		login("login: alice for the forged subject", "alice@example.com", Password, ""),
	}) {
		one[name] = answer
	}

	// Batch 2: each plane carries its own tokens.
	token := func(from string, fieldName string) func(plane string) string {
		return func(plane string) string { return field(one[from].plane(plane).Body, fieldName) }
	}
	aliceAccess, aliceRefresh := token("login: alice", "access_token"), token("login: alice", "refresh_token")
	two := h.run([]pair{
		perPlane("me: own login token", http.MethodGet, "/api/v1/auth/me", func(p string) (string, string) { return aliceAccess(p), "" }),
		perPlane("validate: own login token", http.MethodPost, "/api/v1/auth/validate", func(p string) (string, string) {
			return "", `{"token": "` + aliceAccess(p) + `"}`
		}),
		perPlane("validate: the OTHER plane's login token", http.MethodPost, "/api/v1/auth/validate", func(p string) (string, string) {
			other := map[string]string{"python": "go", "go": "python"}[p]
			return "", `{"token": "` + aliceAccess(other) + `"}`
		}),
		perPlane("refresh: alice", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + aliceRefresh(p) + `"}`
		}),
		// In the same batch, so well inside the 30 s grace window on both
		// planes whatever each batch's start-up costs.
		perPlane("refresh: original again inside the grace window", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + aliceRefresh(p) + `"}`
		}),
		perPlane("refresh: switch-org token", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + token("switch-org: alice to B", "refresh_token")(p) + `"}`
		}),
		perPlane("refresh: social frank", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + token("social: github existing by provider id", "refresh_token")(p) + `"}`
		}),
		perPlane("refresh: social new user (no membership, never stored)", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + token("social: github new user", "refresh_token")(p) + `"}`
		}),
		perPlane("refresh: bob no membership", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + token("login: bob superuser", "refresh_token")(p) + `"}`
		}),
		perPlane("refresh: rotation race, first rotation", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + token("login: alice for the rotation race", "refresh_token")(p) + `"}`
		}),
		perPlane("refresh: aged rotation", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + token("login: alice for the aged rotation", "refresh_token")(p) + `"}`
		}),
	})
	successor := func(plane string) string { return field(two["refresh: alice"].plane(plane).Body, "refresh_token") }
	for _, plane := range []string{"python", "go"} {
		replayed := field(two["refresh: original again inside the grace window"].plane(plane).Body, "refresh_token")
		if jtiOf(replayed) == "" || jtiOf(replayed) != jtiOf(successor(plane)) {
			h.T.Errorf("%s: the grace-window replay did not return the recorded successor", plane)
		}
	}

	// Batch 3: the successor.
	three := h.run([]pair{
		perPlane("refresh: successor", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + successor(p) + `"}`
		}),
		perPlane("refresh: rotation race, successor rotated", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + field(two["refresh: rotation race, first rotation"].plane(p).Body, "refresh_token") + `"}`
		}),
		// Inside the grace window, but its successor is no longer live:
		// reuse, not a replay.
		perPlane("refresh: rotation race, original inside the window", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + token("login: alice for the rotation race", "refresh_token")(p) + `"}`
		}),
		// Signed with the real key and carrying a live token's jti, but a
		// subject that is no user.
		perPlane("refresh: live jti, subject no user", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			jti := jtiOf(token("login: alice for the forged subject", "refresh_token")(p))
			forged, _ := signer.Refresh(edgetoken.RefreshClaims{UserID: ghost, OrgID: orgA, FamilyID: "f0000000-0000-4000-8000-000000000009"}, time.Now(), jti)
			return "", `{"refresh_token": "` + forged + `"}`
		}),
	})
	latest := func(plane string) string {
		return field(three["refresh: successor"].plane(plane).Body, "refresh_token")
	}

	// Out of the grace window; judy deactivated; alice's token version
	// bumped.
	h.Exec(`UPDATE refresh_tokens SET revoked_at = revoked_at - interval '5 minutes' WHERE revoked_at IS NOT NULL`)
	h.Exec(`UPDATE users SET is_active = false WHERE id = '` + judy + `'`)
	h.run([]pair{
		perPlane("refresh: original outside the grace window (reuse)", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + aliceRefresh(p) + `"}`
		}),
		// Its successor is still live, but the rotation is past the window.
		perPlane("refresh: aged rotation, original outside the window", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + token("login: alice for the aged rotation", "refresh_token")(p) + `"}`
		}),
		perPlane("refresh: latest after its family was revoked", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + latest(p) + `"}`
		}),
		perPlane("refresh: judy deactivated", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + token("login: judy", "refresh_token")(p) + `"}`
		}),
		perPlane("logout: bob with his tokens", http.MethodPost, "/api/v1/auth/logout", func(p string) (string, string) {
			return token("login: bob superuser", "access_token")(p), `{"refresh_token": "` + field(two["refresh: bob no membership"].plane(p).Body, "refresh_token") + `"}`
		}),
		perPlane("logout: alice with her org token", http.MethodPost, "/api/v1/auth/logout", func(p string) (string, string) {
			return token("login: alice org A", "access_token")(p), `{"refresh_token": "` + token("login: alice org A", "refresh_token")(p) + `"}`
		}),
	})
	h.Exec(`UPDATE users SET token_version = 1 WHERE id = '` + alice + `'`)
	h.run([]pair{
		perPlane("refresh: bob's logged-out token", http.MethodPost, "/api/v1/auth/refresh", func(p string) (string, string) {
			return "", `{"refresh_token": "` + field(two["refresh: bob no membership"].plane(p).Body, "refresh_token") + `"}`
		}),
		perPlane("me: token issued before the version bump", http.MethodGet, "/api/v1/auth/me", func(p string) (string, string) {
			return aliceAccess(p), ""
		}),
	})

	// Lockout ladder.
	var ladder []pair
	for attempt := 1; attempt <= 6; attempt++ {
		ladder = append(ladder, login(fmt.Sprintf("lockout: wrong password %d", attempt), "grace@example.com", "wrong", ""))
	}
	ladder = append(ladder, login("lockout: right password while locked", "GRACE@example.com", Password, ""))
	for attempt := 1; attempt <= 5; attempt++ {
		ladder = append(ladder, login(fmt.Sprintf("lockout: unknown user wrong %d", attempt), "mallory@example.com", "wrong", ""))
	}
	h.run(ladder)
	h.Exec(`UPDATE login_attempts SET locked_until = now() - interval '1 minute' WHERE locked_until IS NOT NULL`)
	after := []pair{
		login("lockout: right password after the lock expired", "grace@example.com", Password, ""),
		login("lockout: unknown user wrong again after the lock expired", "mallory@example.com", "wrong", ""),
	}
	for attempt := 1; attempt <= 5; attempt++ {
		after = append(after, login(fmt.Sprintf("lockout: second unknown user wrong %d", attempt), "trent@example.com", "wrong", ""))
	}
	h.run(after)

	// Rate limits.
	var limits []pair
	for attempt := 1; attempt <= 21; attempt++ {
		limits = append(limits, login(fmt.Sprintf("rate limit: login %d", attempt), fmt.Sprintf("rl%d@example.com", attempt), "x", ""))
	}
	for attempt := 1; attempt <= 11; attempt++ {
		limits = append(limits, same(fmt.Sprintf("rate limit: refresh %d", attempt), http.MethodPost, "/api/v1/auth/refresh", "", `{"refresh_token": "garbage"}`))
	}
	for attempt := 1; attempt <= 31; attempt++ {
		limits = append(limits, same(fmt.Sprintf("rate limit: validate %d", attempt), http.MethodPost, "/api/v1/auth/validate", "", `{"token": "same-token"}`))
	}
	limits = append(limits, same("rate limit: validate, another token", http.MethodPost, "/api/v1/auth/validate", "", `{"token": "another-token"}`))
	h.run(limits)

	// Invalid-then-valid bursts: a body the route refuses with 422 is
	// refused before the limiter counts it, so a window's worth of
	// refusals still leaves every slot open for the valid requests after
	// them.
	var bursts []pair
	for attempt := 1; attempt <= 25; attempt++ {
		bursts = append(bursts, same(fmt.Sprintf("burst: login invalid %d", attempt), http.MethodPost, "/api/v1/auth/login", "", `{"email": "not-an-email", "password": "x"}`))
	}
	for attempt := 1; attempt <= 21; attempt++ {
		bursts = append(bursts, login(fmt.Sprintf("burst: login valid %d", attempt), fmt.Sprintf("burst%d@example.com", attempt), "x", ""))
	}
	for attempt := 1; attempt <= 12; attempt++ {
		bursts = append(bursts, same(fmt.Sprintf("burst: refresh invalid %d", attempt), http.MethodPost, "/api/v1/auth/refresh", "", `{}`))
	}
	for attempt := 1; attempt <= 11; attempt++ {
		bursts = append(bursts, same(fmt.Sprintf("burst: refresh valid %d", attempt), http.MethodPost, "/api/v1/auth/refresh", "", `{"refresh_token": "garbage"}`))
	}
	for attempt := 1; attempt <= 32; attempt++ {
		bursts = append(bursts, same(fmt.Sprintf("burst: validate invalid %d", attempt), http.MethodPost, "/api/v1/auth/validate", "", `{"token": 5}`))
	}
	for attempt := 1; attempt <= 31; attempt++ {
		bursts = append(bursts, same(fmt.Sprintf("burst: validate valid %d", attempt), http.MethodPost, "/api/v1/auth/validate", "", `{"token": "same-token"}`))
	}
	h.run(bursts)

	// Social logins that link by email to a user of the same provider:
	// a different provider id is replaced, a missing one is set.
	h.run([]pair{
		same("social: github email of a github user with another id", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "gh-frank-new-id"}`),
		same("social: github email of a github user without an id", http.MethodPost, "/api/v1/auth/social-login", "", `{"provider": "github", "provider_access_token": "gh-olga"}`),
	})

	// The rows the routes wrote.
	h.compareRows("users", `SELECT email, coalesce(username, '<null>'), coalesce(full_name, '<null>'),
		coalesce(auth_provider, '<null>'), coalesce(auth_provider_id, '<null>'), is_active, is_verified, is_superuser, token_version,
		last_login_at IS NOT NULL, updated_at >= created_at, password_hash IS NULL FROM users
		WHERE auth_provider IS DISTINCT FROM 'service' ORDER BY email`)
	h.compareRows("login_attempts", `SELECT email, attempt_count, first_attempt_at IS NOT NULL, coalesce(first_attempt_at = created_at, false),
		locked_until IS NOT NULL, coalesce(locked_until > now(), false), updated_at >= created_at FROM login_attempts ORDER BY email`)
	h.compareRows("refresh_tokens", `SELECT u.email, coalesce(o.slug, '<null>'), rt.revoked_at IS NOT NULL,
		rt.replaced_by_hash IS NOT NULL, rt.successor_jti IS NOT NULL,
		coalesce(rt.replaced_by_hash = encode(sha256(convert_to(rt.successor_jti, 'UTF8')), 'hex'), false),
		EXISTS (SELECT 1 FROM refresh_tokens s WHERE s.token_hash = rt.replaced_by_hash),
		(SELECT count(*) FROM refresh_tokens f WHERE f.family_id = rt.family_id),
		rt.expires_at - rt.created_at BETWEEN interval '6 days 23 hours 59 minutes' AND interval '7 days',
		extract(microsecond FROM rt.expires_at)::int % 1000000 = 0, coalesce(rt.user_agent, '<null>')
		FROM refresh_tokens rt JOIN users u ON u.id = rt.user_id LEFT JOIN organizations o ON o.id = rt.org_id
		ORDER BY rt.created_at`)
	h.compareRows("audit_logs", `SELECT coalesce(o.slug, a.org_id::text), coalesce(u.email, '<null>'), a.action,
		a.resource_type, coalesce((SELECT email FROM users WHERE id::text = a.resource_id), a.resource_id), a.description,
		coalesce(a.changes::text, '<null>'), coalesce(a.request_metadata::text, '<null>'), a.status, coalesce(a.error_message, '<null>')
		FROM audit_logs a LEFT JOIN organizations o ON o.id = a.org_id LEFT JOIN users u ON u.id = a.user_id
		ORDER BY a.created_at`)
	if h.Python == nil {
		if h.cursor != len(h.golden.Answers) {
			h.T.Errorf("the scenario sent %d requests, the golden holds %d answers", h.cursor, len(h.golden.Answers))
		}
		return h.receipt.String()
	}
	rendered, err := json.MarshalIndent(h.recording, "", " ")
	if err != nil {
		h.T.Fatal(err)
	}
	rendered = append(rendered, '\n')
	if os.Getenv("DEV_HEALTH_REGENERATE_TABLES") == "1" {
		if err := os.WriteFile(GoldenPath(), rendered, 0o644); err != nil {
			h.T.Fatal(err)
		}
	} else if committed, err := os.ReadFile(GoldenPath()); err != nil || !bytes.Equal(committed, rendered) {
		h.T.Errorf("%s differs from the live Python answers; regenerate with DEV_HEALTH_REGENERATE_TABLES=1", GoldenPath())
	}
	return h.receipt.String()
}

// request builds one request; token "" sends no Authorization header.
func request(name, method, path, token, body string) venueoracle.Request {
	headers := map[string]string{"User-Agent": "venue-oracle/1", "X-Forwarded-For": "203.0.113.7"}
	var encoded *string
	if body != "" {
		headers["Content-Type"] = "application/json"
		encoded = venueoracle.B64(body)
	}
	if token != "" {
		headers["Authorization"] = "Bearer " + token
	}
	return venueoracle.Request{Name: name, Method: method, Path: path, Headers: headers, Body: encoded}
}

func same(name, method, path, token, body string) pair {
	r := request(name, method, path, token, body)
	return pair{name: name, python: r, goSide: r}
}

func login(name, email, password, extra string) pair {
	body := fmt.Sprintf(`{"email": %q, "password": %q%s}`, email, password, extra)
	return same(name, http.MethodPost, "/api/v1/auth/login", "", body)
}

// perPlane builds a pair whose body or token comes from each plane's own
// earlier answer.
func perPlane(name, method, path string, build func(plane string) (token, body string)) pair {
	pyToken, pyBody := build("python")
	goToken, goBody := build("go")
	return pair{name: name, python: request(name, method, path, pyToken, pyBody), goSide: request(name, method, path, goToken, goBody)}
}

// forgeToken is a token over payload signed with a key neither plane
// holds: validate_token refuses it, and the unverified read still sees
// its claims.
func forgeToken(payload string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." +
		base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + base64.RawURLEncoding.EncodeToString([]byte("not-a-signature"))
}

func jtiOf(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	return field(string(payload), "jti")
}

// ProviderEndpoints points the Go plane's social-login providers at the
// fake provider, on the paths the Python plane's redirect lands on.
func ProviderEndpoints(base string) oauthprovider.Endpoints {
	return oauthprovider.Endpoints{
		GitHubUser: base + "/github/user", GitHubEmails: base + "/github/user/emails",
		GitLabBase: base + "/gitlab", GoogleUser: base + "/google/oauth2/v2/userinfo",
	}
}

// Field reads one top-level string field of a JSON body ("" when absent).
func Field(body, name string) string { return field(body, name) }

// JTIOf is the jti claim of a token ("" when it has none).
func JTIOf(token string) string { return jtiOf(token) }
