package session

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

func TestSelectActiveMembership(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := t0.Add(time.Hour)
	memberships := []membershipRow{{OrgID: a, JoinedAt: &t0}, {OrgID: b, CreatedAt: later}, {OrgID: c, JoinedAt: &later}}
	newer, older := t0.Add(48*time.Hour), t0.Add(24*time.Hour)
	cases := []struct {
		name       string
		requested  *uuid.UUID
		activities map[uuid.UUID]activity
		want       *uuid.UUID
	}{
		{"no data: earliest join", nil, map[uuid.UUID]activity{}, &a},
		{"data beats no data", nil, map[uuid.UUID]activity{c: {hasData: true}}, &c},
		{"newest metrics among orgs with data", nil, map[uuid.UUID]activity{a: {hasData: true, last: &older}, b: {hasData: true, last: &newer}}, &b},
		{"metrics without data still order", nil, map[uuid.UUID]activity{c: {last: &newer}}, &c},
		{"ties keep the first", nil, map[uuid.UUID]activity{a: {hasData: true, last: &older}, c: {hasData: true, last: &older}}, &a},
		{"requested member", &b, map[uuid.UUID]activity{c: {hasData: true}}, &b},
		{"requested non-member", func() *uuid.UUID { id := uuid.New(); return &id }(), map[uuid.UUID]activity{}, nil},
	}
	for _, c := range cases {
		got := selectActiveMembership(memberships, c.requested, c.activities)
		if (got == nil) != (c.want == nil) || (got != nil && got.OrgID != *c.want) {
			t.Errorf("%s: got %v", c.name, got)
		}
	}
	if got := selectActiveMembership(nil, nil, nil); got != nil {
		t.Errorf("no memberships: got %v", got)
	}
}

func TestPasswordMatches(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("secret-1"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := passwordMatches("secret-1", string(hash)); !ok || err != nil {
		t.Fatalf("right password: %v %v", ok, err)
	}
	if ok, _ := passwordMatches("secret-2", string(hash)); ok {
		t.Fatal("wrong password matched")
	}
	long := strings.Repeat("p", 72)
	longHash, _ := bcrypt.GenerateFromPassword([]byte(long), bcrypt.MinCost)
	if ok, _ := passwordMatches(long, string(longHash)); !ok {
		t.Fatal("a 72-byte password must still be checked")
	}
	if ok, _ := passwordMatches(long+"p", string(longHash)); ok {
		t.Fatal("a 73-byte password is a non-match (bcrypt raises ValueError)")
	}
	if ok, _ := passwordMatches("x", "not-a-hash"); ok {
		t.Fatal("a malformed hash is a non-match")
	}
	if ok, _ := passwordMatches("secret-1", dummyHash()); ok {
		t.Fatal("the dummy hash matched")
	}
	if _, err := passwordMatches("\xed\xa0\x80", string(hash)); err == nil {
		t.Fatal("a lone surrogate cannot be encoded and must fail")
	}
}

func TestValidateKey(t *testing.T) {
	request := httptest.NewRequest("POST", "/api/v1/auth/validate", nil)
	request.RemoteAddr = "192.0.2.1:5000"
	if got, _ := validateKey(request, ""); got != "validate-ip:192.0.2.1" {
		t.Fatalf("empty token: %q", got)
	}
	got, _ := validateKey(request, "abc")
	// The key is sha256(token)'s first 32 hex digits, computed here from the
	// SHA-256 test vector for "abc".
	digest := sha256.Sum256([]byte("abc"))
	if want := "validate-token:" + hex.EncodeToString(digest[:])[:32]; got != want || !strings.HasPrefix(want, "validate-token:ba7816bf") {
		t.Fatalf("token digest: %q", got)
	}
	if _, err := validateKey(request, "\xed\xa0\x80"); err == nil {
		t.Fatal("a lone surrogate token must fail")
	}
}

// The timing hash costs what login.py's DUMMY_PASSWORD_HASH costs (bcrypt,
// cost 12) and is one hash for the process, so every login without a usable
// user hash spends the same bcrypt work.
func TestDummyHashCostsWhatPythonsDoes(t *testing.T) {
	first := dummyHash()
	cost, err := bcrypt.Cost([]byte(first))
	if err != nil || cost != 12 {
		t.Fatalf("dummy hash: cost %d, err %v; want a bcrypt hash of cost 12", cost, err)
	}
	if again := dummyHash(); again != first {
		t.Fatal("the dummy hash changed between calls")
	}
}

// CHAOS-13585: GET /api/v1/auth/me carries the caller's identity and no permission list: no consumer reads one (CENSUS-10872-perm-1),
// and a complete mutable permission set does not belong in an answer a client caches. Each expected body is main's answer for that caller
// (cfeabc7f4cd1) with the permissions member removed and every other byte unchanged; under a live impersonation session the permission
// list used to be the target role's, and the identity fields still come from the caller alone.
func TestMeCarriesNoPermissionsField(t *testing.T) {
	const identity = `{"id":"u","email":"u@example.com","username":null,"full_name":null,"org_id":"o","role":"%s","is_superuser":%t}`
	session := &policy.Impersonation{AdminUserID: uuid.New(), TargetUserID: uuid.New(), TargetOrgID: uuid.New(), TargetRole: "viewer"}
	cases := []struct {
		name string
		user *policy.User
		imp  *policy.Impersonation
		want string
	}{
		{"viewer", &policy.User{UserID: "u", Email: "u@example.com", OrgID: "o", Role: "viewer"}, nil, fmt.Sprintf(identity, "viewer", false)},
		{"owner", &policy.User{UserID: "u", Email: "u@example.com", OrgID: "o", Role: "owner"}, nil, fmt.Sprintf(identity, "owner", false)},
		{"platform admin", &policy.User{UserID: "u", Email: "u@example.com", OrgID: "o", Role: "member", IsSuperuser: true}, nil, fmt.Sprintf(identity, "member", true)},
		{"platform admin under a live impersonation session, target role viewer", &policy.User{UserID: "u", Email: "u@example.com", OrgID: "o", Role: "member", IsSuperuser: true}, session, fmt.Sprintf(identity, "member", true)},
		{"owner under a live impersonation session, target role viewer", &policy.User{UserID: "u", Email: "u@example.com", OrgID: "o", Role: "owner"}, session, fmt.Sprintf(identity, "owner", false)},
	}
	for _, c := range cases {
		request := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
		ctx := policy.WithUser(request.Context(), c.user)
		if c.imp != nil {
			ctx = policy.WithImpersonation(ctx, c.imp)
		}
		recorder := httptest.NewRecorder()
		handlers{}.me(recorder, request.WithContext(ctx))
		body := strings.TrimSpace(recorder.Body.String())
		if recorder.Code != http.StatusOK || body != c.want {
			t.Errorf("%s: %d %s, want the 200 identity answer %s with no permissions", c.name, recorder.Code, body, c.want)
		}
	}
}

// The recorded Python /auth/me answers of the differential scenario (python_golden.json, pinned by sha256, its recorder retired) carry the
// retired permissions member. Once that member is dropped, each recorded answer is byte-equal to what this handler answers for the same
// identity: so the scenario's replay of /auth/me cannot differ because of the field.
func TestRecordedMeAnswersEqualTheHandlerOnceThePermissionsMemberIsDropped(t *testing.T) {
	golden := sessionscenario.LoadGolden(t)
	checked := 0
	for _, answer := range golden.Answers {
		if answer.Status != http.StatusOK || !strings.Contains(answer.Body, `"is_superuser"`) || !strings.Contains(answer.Body, `"full_name"`) ||
			strings.Contains(answer.Body, `"access_token"`) || strings.Contains(answer.Body, `"user":`) || strings.Contains(answer.Body, `"memberships"`) {
			continue
		}
		var me struct {
			ID, Email, OrgID, Role string
			Username, FullName     *string
			IsSuperuser            bool
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(answer.Body), &raw); err != nil || raw["id"] == nil || raw["role"] == nil {
			continue
		}
		str := func(k string) string { v, _ := raw[k].(string); return v }
		opt := func(k string) *string {
			if v, ok := raw[k].(string); ok {
				return &v
			}
			return nil
		}
		me.ID, me.Email, me.OrgID, me.Role = str("id"), str("email"), str("org_id"), str("role")
		me.Username, me.FullName = opt("username"), opt("full_name")
		me.IsSuperuser, _ = raw["is_superuser"].(bool)
		user := &policy.User{UserID: me.ID, Email: me.Email, OrgID: me.OrgID, Role: me.Role, IsSuperuser: me.IsSuperuser, Username: me.Username, FullName: me.FullName}
		request := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
		recorder := httptest.NewRecorder()
		handlers{}.me(recorder, request.WithContext(policy.WithUser(request.Context(), user)))
		if got := strings.TrimSpace(recorder.Body.String()); got != answer.Body {
			t.Errorf("%s: handler %s, recorded (permissions dropped) %s", answer.Name, got, answer.Body)
		}
		checked++
	}
	t.Logf("checked %d recorded /auth/me answers", checked)
	if checked != 4 {
		t.Fatalf("%d recorded /auth/me answers checked, the golden holds 4", checked)
	}
}

// The drop of the retired member applies to the RECORDED side only. The pinned golden file is unedited (LoadGolden refuses a file whose
// sha256 is not the pinned one, and the raw bytes still carry the member 4 times), and a Go answer that still emits the member does
// not equal the recorded one after Normalize: it would fail the scenario's comparison.
func TestTheDropOfThePermissionsMemberIsOnTheGoldenSideOnly(t *testing.T) {
	golden := sessionscenario.LoadGolden(t)
	raw, err := os.ReadFile(sessionscenario.GoldenPath())
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(raw), `permissions`); n != 4 {
		t.Fatalf("the pinned golden file carries the member %d times, want 4: it must stay unedited", n)
	}
	checked := 0
	for _, answer := range golden.Answers {
		if answer.Status != http.StatusOK || !strings.HasPrefix(answer.Body, `{"id":`) || !strings.Contains(answer.Body, `"is_superuser"`) ||
			strings.Contains(answer.Body, `"user":`) || strings.Contains(answer.Body, `"access_token"`) || strings.Contains(answer.Body, `"memberships"`) {
			continue
		}
		if strings.Contains(answer.Body, "permissions") {
			t.Errorf("%s: the loaded golden answer still carries the member: %s", answer.Name, answer.Body)
		}
		emitting := strings.TrimSuffix(answer.Body, "}") + `,"permissions":["analytics:read"]}`
		if got := sessionscenario.Normalize(emitting); got == answer.Body {
			t.Errorf("%s: a Go answer that still emits permissions equals the recorded one after Normalize: %s", answer.Name, got)
		}
		checked++
	}
	if checked != 4 {
		t.Fatalf("%d recorded /auth/me answers checked, the golden holds 4", checked)
	}
}
