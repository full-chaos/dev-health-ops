package session

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

func TestRolePermissionsMatchTheRoleHierarchy(t *testing.T) {
	viewer := []string{"analytics:read", "metrics:read", "work_items:read", "git:read", "teams:read", "settings:read", "members:read", "org:read"}
	if got := rolePermissions("viewer"); !reflect.DeepEqual(got, viewer) {
		t.Fatalf("viewer: %v", got)
	}
	member := rolePermissions("member")
	if len(member) != len(viewer)+1 || member[1] != "analytics:export" {
		t.Fatalf("member holds viewer plus analytics:export, got %v", member)
	}
	if got := len(rolePermissions("admin")); got != 19 {
		t.Fatalf("admin: %d permissions", got)
	}
	if got := rolePermissions("owner"); len(got) != 20 || got[len(got)-1] != "org:delete" {
		t.Fatalf("owner: %v", got)
	}
	for _, unknown := range []string{"", "Owner", "superuser"} {
		if got := rolePermissions(unknown); len(got) != 0 {
			t.Fatalf("%q: %v", unknown, got)
		}
	}
	if got := userPermissions(nil, true, "viewer"); len(got) != 22 {
		t.Fatalf("superuser: %d", len(got))
	}
	target := "viewer"
	if got := userPermissions(&target, true, "owner"); len(got) != 8 {
		t.Fatalf("impersonation uses the target role, got %d", len(got))
	}
}

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

func TestClaimHelpers(t *testing.T) {
	for value, want := range map[any]string{"s": "s", float64(5): "5", float64(1.5): "1.5", true: "True", nil: "None"} {
		if got, err := pyStrClaim(value); err != nil || got != want {
			t.Errorf("pyStrClaim(%#v) = %q %v", value, got, err)
		}
	}
	if _, err := pyStrClaim(map[string]any{}); err == nil {
		t.Error("a dict claim must fail")
	}
	for _, value := range []any{nil, false, "", float64(0), []any{}, map[string]any{}} {
		if truthy(value) {
			t.Errorf("truthy(%#v)", value)
		}
	}
	for _, value := range []any{true, "x", float64(2), []any{1}, map[string]any{"a": 1}} {
		if !truthy(value) {
			t.Errorf("!truthy(%#v)", value)
		}
	}
}

func TestUnverifiedOrgAndSubject(t *testing.T) {
	org := uuid.New()
	token := encodeClaims(`{"alg":"HS256","typ":"JWT"}`) + "." + encodeClaims(`{"sub":"s","org_id":"`+org.String()+`"}`) + ".c2ln"
	got, subject, err := unverifiedOrgAndSubject(token)
	if err != nil || got == nil || *got != org || subject != "s" {
		t.Fatalf("got %v %v %v", got, subject, err)
	}
	if got, _, _ := unverifiedOrgAndSubject("garbage"); got != nil {
		t.Fatal("a malformed token has no org")
	}
	if _, _, err := unverifiedOrgAndSubject(encodeClaims(`{"alg":"HS256"}`) + "." + encodeClaims(`{"org_id":7}`) + ".c2ln"); err == nil {
		t.Fatal("a truthy non-str org claim must fail")
	}
	if got, _, err := unverifiedOrgAndSubject(encodeClaims(`{"alg":"HS256"}`) + "." + encodeClaims(`{"org_id":0}`) + ".c2ln"); got != nil || err != nil {
		t.Fatal("a falsy org claim is no org")
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
