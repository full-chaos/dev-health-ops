package serviceaccount

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

func TestKeyValueShape(t *testing.T) {
	value, err := MintKey()
	if err != nil {
		t.Fatal(err)
	}
	if len(value) != len("fcsak_")+43 || !strings.HasPrefix(value, "fcsak_") || !ValidKeyShape(value) || !CanonicalKey(value) {
		t.Fatalf("a minted value has the wrong shape: %d characters", len(value))
	}
	other, err := MintKey()
	if err != nil || other == value {
		t.Fatalf("two mints: %v, equal %v", err, other == value)
	}
	sum := sha256.Sum256([]byte(value))
	if KeyDigest(value) != sum {
		t.Fatal("the digest is not SHA-256 of the whole value")
	}
	for _, bad := range []string{"", "fcsak_", "fcsak_" + strings.Repeat("A", 42), "fcsak_" + strings.Repeat("A", 44),
		"fcwlb_" + strings.Repeat("A", 43), "fcsak_" + strings.Repeat("A", 42) + "=", "fcsak_" + strings.Repeat("A", 42) + "\n"} {
		if ValidKeyShape(bad) {
			t.Errorf("%q has the key shape", bad)
		}
	}
	if CanonicalKey("fcsak_" + strings.Repeat("A", 42) + "B") {
		t.Error("stray bits in the last character are canonical")
	}
}

func liveState(live int) keyState {
	return keyState{accountActive: true, orgActive: true, owner: okOwner, live: live}
}

func TestDecideIssue(t *testing.T) {
	cases := []struct {
		name string
		st   keyState
		want error
	}{
		{"no key", liveState(0), nil},
		{"one live", liveState(1), ErrKeyLive},
		{"two live", liveState(2), ErrKeyLive},
		{"account row disabled", keyState{orgActive: true, owner: okOwner}, ErrAccountDisabled},
		{"principal disabled", keyState{accountActive: true, principalOff: true, orgActive: true, owner: okOwner}, ErrAccountDisabled},
		{"org suspended", keyState{accountActive: true, owner: okOwner}, ErrOrgNotActive},
		{"owner disabled", keyState{accountActive: true, orgActive: true, owner: ownerState{isUser: true, disabled: true, member: true}}, ErrOwnerDisabled},
		{"owner not a member", keyState{accountActive: true, orgActive: true, owner: ownerState{isUser: true}}, ErrOwnerNotMember},
		{"owner not a user", keyState{accountActive: true, orgActive: true, owner: ownerState{member: true}}, ErrOwnerNotUser},
	}
	for _, c := range cases {
		if got := decideIssue(c.st); !errors.Is(got, c.want) || (c.want == nil && got != nil) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestDecideRotate(t *testing.T) {
	cases := []struct {
		name string
		st   keyState
		want error
	}{
		{"one live", liveState(1), nil},
		{"no key", liveState(0), ErrKeyNone},
		{"two live", liveState(2), ErrKeyRotating},
		{"three live", liveState(3), ErrKeyRotating},
		{"account row disabled", keyState{orgActive: true, owner: okOwner, live: 1}, ErrAccountDisabled},
		{"principal disabled", keyState{accountActive: true, principalOff: true, orgActive: true, owner: okOwner, live: 1}, ErrAccountDisabled},
		{"org suspended", keyState{accountActive: true, owner: okOwner, live: 1}, ErrOrgNotActive},
		{"owner disabled", keyState{accountActive: true, orgActive: true, owner: ownerState{isUser: true, disabled: true, member: true}, live: 1}, ErrOwnerDisabled},
		{"owner not a member", keyState{accountActive: true, orgActive: true, owner: ownerState{isUser: true}, live: 1}, ErrOwnerNotMember},
	}
	for _, c := range cases {
		if got := decideRotate(c.st); !errors.Is(got, c.want) || (c.want == nil && got != nil) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestDecideResolveOrder(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	live := func() keyRow {
		return keyRow{id: uuid.New(), principal: uuid.New(), org: uuid.New(), class: "service_account_key", domainOwner: "platform",
			expiresAt: now.Add(time.Second), kind: "service_account", revision: 7}
	}
	// Each row sets its own condition and every later one, so a refusal out of order answers another reason.
	cases := []struct {
		name string
		set  func(*keyRow)
		want string
	}{
		{"class", func(r *keyRow) {
			r.class, r.domainOwner, r.kind, r.revoked, r.expiresAt = "workload_bootstrap_credential", "acr", "user", true, now
			r.accountDisabled, r.principalDisabled, r.orgSuspended = true, true, true
		}, "wrong_class"},
		{"platform owner", func(r *keyRow) {
			r.domainOwner, r.kind, r.revoked, r.expiresAt = "acr", "user", true, now
			r.accountDisabled, r.principalDisabled, r.orgSuspended = true, true, true
		}, "not_platform"},
		{"kind", func(r *keyRow) {
			r.kind, r.revoked, r.expiresAt = "user", true, now
			r.accountDisabled, r.principalDisabled, r.orgSuspended = true, true, true
		}, "wrong_kind"},
		{"revoked", func(r *keyRow) {
			r.revoked, r.expiresAt = true, now
			r.accountDisabled, r.principalDisabled, r.orgSuspended = true, true, true
		}, "revoked"},
		{"expired at the instant", func(r *keyRow) {
			r.expiresAt = now
			r.accountDisabled, r.principalDisabled, r.orgSuspended = true, true, true
		}, "expired"},
		{"account row", func(r *keyRow) { r.accountDisabled, r.principalDisabled, r.orgSuspended = true, true, true }, "account_disabled"},
		{"principal", func(r *keyRow) { r.principalDisabled, r.orgSuspended = true, true }, "principal_disabled"},
		{"org", func(r *keyRow) { r.orgSuspended = true }, "org_suspended"},
	}
	for _, c := range cases {
		row := live()
		c.set(&row)
		if _, got := row.decide(now); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
	row := live()
	key, why := row.decide(now)
	if why != "" || key.CredentialID != row.id || key.Principal != row.principal || key.OrgID != row.org || !key.ExpiresAt.Equal(row.expiresAt) || key.Revision != 7 {
		t.Fatalf("a live row: %+v, %q", key, why)
	}
}

func TestKeyBoundsAtTheEdges(t *testing.T) {
	for d, want := range map[time.Duration]bool{0: false, -time.Second: false, time.Second: true,
		8760 * time.Hour: true, 8760*time.Hour + time.Second: false} {
		if got := validKeyExpiry(d); got != want {
			t.Errorf("expiry %s = %v, want %v", d, got, want)
		}
	}
	for d, want := range map[time.Duration]bool{-time.Second: false, 0: true, time.Second: true,
		24 * time.Hour: true, 24*time.Hour + time.Second: false} {
		if got := validKeyOverlap(d); got != want {
			t.Errorf("overlap %s = %v, want %v", d, got, want)
		}
	}
	if KeyExpiryDefault != 2160*time.Hour {
		t.Errorf("the default expiry is %s, want 2160h", KeyExpiryDefault)
	}
}

// Empty means no scope, never all.
func TestAnEmptyKeyHasNoScope(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	row := keyRow{class: "service_account_key", domainOwner: "platform", expiresAt: now.Add(time.Hour), kind: "service_account"}
	key, why := row.decide(now)
	if why != "" {
		t.Fatalf("a live row refused: %q", why)
	}
	if key.Audiences == nil || key.Scopes == nil || len(key.Audiences) != 0 || len(key.Scopes) != 0 {
		t.Fatalf("audiences %#v, scopes %#v, want empty and not nil", key.Audiences, key.Scopes)
	}
	if key.HasAudience("dev-health-api") || key.HasScope("api:access") || key.HasAudience("") || key.HasScope("") {
		t.Fatal("an empty key holds a scope or an audience")
	}
	raw, err := json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"Audiences":[]`)) || !bytes.Contains(raw, []byte(`"Scopes":[]`)) {
		t.Fatalf("empty sets marshal as %s", raw)
	}
	held := ResolvedKey{Audiences: []string{"dev-health-api"}, Scopes: []string{"api:access"}}
	if !held.HasAudience("dev-health-api") || !held.HasScope("api:access") || held.HasAudience("auth-service") || held.HasScope("api:read") {
		t.Fatal("HasAudience or HasScope misreads a held set")
	}
}

func TestIssuedKeyPrintsNoSecret(t *testing.T) {
	value, err := MintKey()
	if err != nil || !CanonicalKey(value) {
		t.Fatalf("mint: %v", err)
	}
	key := IssuedKey{Secret: contracts.NewSecretString(value), Key: Key{ID: uuid.New()}}
	var log bytes.Buffer
	slog.New(slog.NewTextHandler(&log, nil)).Info("x", "key", key)
	raw, _ := json.Marshal(key)
	body := value[len(KeyPrefix):]
	for form, text := range map[string]string{"%v": fmt.Sprintf("%v", key), "%+v": fmt.Sprintf("%+v", key), "%#v": fmt.Sprintf("%#v", key),
		"%s": fmt.Sprintf("%s", key), "slog": log.String(), "json": string(raw)} {
		if strings.Contains(text, body) {
			t.Errorf("%s prints the value", form)
		}
	}
	if key.Secret.Reveal() != value {
		t.Fatal("the secret does not hold the value")
	}
}

func TestKeyFaultKeepsOnlyTheSQLState(t *testing.T) {
	digest := hex.EncodeToString(make([]byte, 32))
	raw := fmt.Errorf("insert: %w", &pgconn.PgError{Code: "23505", Message: "duplicate " + digest, Detail: "Key (secret_hash)=(\\x" + digest + ") already exists."})
	f := faultOf(raw)
	if strings.Contains(f.Error(), digest) || !errors.Is(f, ErrStore) {
		t.Fatalf("fault %q keeps the digest or is no store error", f.Error())
	}
	var coded interface{ SQLState() string }
	if !errors.As(f, &coded) || coded.SQLState() != "23505" {
		t.Fatalf("fault %#v has no SQLSTATE 23505", f)
	}
	if !strings.Contains(faultOf(errors.New("no code "+digest)).Error(), "store unavailable") || strings.Contains(faultOf(errors.New("no code "+digest)).Error(), digest) {
		t.Fatal("a plain error keeps its text")
	}
	if got := outcomeLabel(f); got != "error" {
		t.Fatalf("outcome %q, want error", got)
	}
}
