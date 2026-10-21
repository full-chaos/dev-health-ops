package token

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/keystore"
)

func writeKey(t *testing.T, dir, kid string) ed25519.PublicKey {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, kid+".pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	return public
}

func writeSet(t *testing.T, dir string, pairs ...string) string {
	t.Helper()
	var keys []map[string]string
	for i := 0; i+1 < len(pairs); i += 2 {
		keys = append(keys, map[string]string{"kid": pairs[i], "state": pairs[i+1], "file": pairs[i] + ".pem"})
	}
	body, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		t.Fatal(err)
	}
	staged := filepath.Join(dir, ".staged")
	if err := os.WriteFile(staged, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(staged, 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "keyset.json")
	if err := os.Rename(staged, path); err != nil {
		t.Fatal(err)
	}
	return path
}

// verify parses compact as the platform's EdDSA verifiers do
// (queryapi/principal: methods pinned before key lookup, kid required) against
// the set's published keys.
func verify(t *testing.T, compact string, set KeySet) (*jwt.Token, error) {
	t.Helper()
	return jwt.Parse(compact, func(token *jwt.Token) (any, error) {
		kid, _ := token.Header["kid"].(string)
		for _, key := range set.Published() {
			if key.KeyID == kid {
				return key.PublicKey, nil
			}
		}
		return nil, errors.New("unknown kid")
	}, jwt.WithValidMethods([]string{"EdDSA"}))
}

func TestSignerTokensVerifyUnderThePublishedKey(t *testing.T) {
	dir := t.TempDir()
	writeKey(t, dir, "k1")
	writeKey(t, dir, "k2")
	signer := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "k1", "active", "k2", "pending")})
	ctx := context.Background()
	compact, kid, err := signer.Sign(ctx, "at+jwt", []byte(`{"sub":"u1","exp":4102444800}`))
	if err != nil || kid != "k1" {
		t.Fatalf("Sign = %q %q %v", compact, kid, err)
	}
	set, err := signer.KeySet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := verify(t, compact, set)
	if err != nil || !parsed.Valid {
		t.Fatalf("token does not verify: %v", err)
	}
	rawHeader, err := base64.RawURLEncoding.DecodeString(strings.Split(compact, ".")[0])
	if err != nil || string(rawHeader) != `{"alg":"EdDSA","kid":"k1","typ":"at+jwt"}` {
		t.Fatalf("header %s (%v)", rawHeader, err)
	}
	if claims := parsed.Claims.(jwt.MapClaims); claims["sub"] != "u1" {
		t.Fatalf("claims %v", claims)
	}
}

func TestSignerRefusesBadInput(t *testing.T) {
	dir := t.TempDir()
	writeKey(t, dir, "k1")
	signer := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "k1", "active")})
	for name, input := range map[string]struct {
		typ     string
		payload string
	}{
		"array payload":   {"at+jwt", `[1]`},
		"string payload":  {"at+jwt", `"x"`},
		"invalid json":    {"at+jwt", `{"a":`},
		"empty payload":   {"at+jwt", ``},
		"empty typ":       {"", `{}`},
		"upper typ":       {"JWT", `{}`},
		"typ with quotes": {`at"jwt`, `{}`},
	} {
		if _, _, err := signer.Sign(context.Background(), input.typ, []byte(input.payload)); ReasonOf(err) != string(ReasonInvalidInput) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestSignerFollowsALiveRotation(t *testing.T) {
	dir := t.TempDir()
	writeKey(t, dir, "k1")
	writeKey(t, dir, "k2")
	signer := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "k1", "active", "k2", "pending")})
	ctx := context.Background()
	kidOf := func() string {
		t.Helper()
		compact, kid, err := signer.Sign(ctx, "at+jwt", []byte(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		set, err := signer.KeySet(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := verify(t, compact, set); err != nil {
			t.Fatalf("token under %s does not verify under the published set: %v", kid, err)
		}
		return kid
	}
	if kid := kidOf(); kid != "k1" {
		t.Fatalf("kid %s, want k1", kid)
	}
	writeSet(t, dir, "k1", "retiring", "k2", "active")
	if kid := kidOf(); kid != "k2" {
		t.Fatalf("after rotation kid %s, want k2", kid)
	}
	set, err := signer.KeySet(ctx)
	if err != nil || len(set.Published()) != 2 {
		t.Fatalf("both keys published during the overlap: %v %v", set.Published(), err)
	}
	writeSet(t, dir, "k1", "retired", "k2", "active")
	if set, _ := signer.KeySet(ctx); len(set.Published()) != 1 || set.Published()[0].KeyID != "k2" {
		t.Fatalf("retired k1 still published: %v", set.Published())
	}
	writeSet(t, dir, "k1", "retired", "k2", "compromised")
	if err := signer.Check(ctx); ReasonOf(err) != string(ReasonNoActiveKey) {
		t.Fatalf("Check with the active key compromised = %v", err)
	}
	writeSet(t, dir, "k1", "retired", "k2", "active")
	if err := signer.Check(ctx); err == nil {
		t.Fatal("a kid seen compromised became active again")
	}
}

func TestSingleKeyFileIsAnActiveOneKeySet(t *testing.T) {
	dir := t.TempDir()
	public := writeKey(t, dir, "only")
	signer := NewFileSigner(FileConfig{KeyPath: filepath.Join(dir, "only.pem"), KeyID: "auth-signing-local"})
	ctx := context.Background()
	if err := signer.Check(ctx); err != nil {
		t.Fatalf("Check: %v", err)
	}
	set, err := signer.KeySet(ctx)
	if err != nil || set.Active().KeyID != "auth-signing-local" || !set.Active().PublicKey.Equal(public) {
		t.Fatalf("set %+v %v", set, err)
	}
	compact, kid, err := signer.Sign(ctx, "at+jwt", []byte(`{}`))
	if err != nil || kid != "auth-signing-local" {
		t.Fatalf("Sign: %v", err)
	}
	if _, err := verify(t, compact, set); err != nil {
		t.Fatal(err)
	}
}

func TestCheckFailsClosed(t *testing.T) {
	ctx := context.Background()
	t.Run("missing manifest", func(t *testing.T) {
		signer := NewFileSigner(FileConfig{KeySetPath: filepath.Join(t.TempDir(), "absent.json")})
		if err := signer.Check(ctx); ReasonOf(err) != "key_file_unreadable" {
			t.Fatalf("Check = %v", err)
		}
	})
	t.Run("two active keys", func(t *testing.T) {
		dir := t.TempDir()
		writeKey(t, dir, "a")
		writeKey(t, dir, "b")
		signer := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "a", "active", "b", "active")})
		if err := signer.Check(ctx); ReasonOf(err) != string(ReasonManyActiveKeys) {
			t.Fatalf("Check = %v", err)
		}
		if _, _, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); ReasonOf(err) != string(ReasonManyActiveKeys) {
			t.Fatalf("Sign = %v", err)
		}
	})
	t.Run("probe does not verify", func(t *testing.T) {
		dir := t.TempDir()
		writeKey(t, dir, "a")
		custody := mismatched{inner: NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "a", "active")}).custody}
		if err := (&Signer{custody: custody}).Check(ctx); ReasonOf(err) != string(ReasonProbeMismatch) {
			t.Fatalf("Check = %v", err)
		}
	})
}

// mismatched publishes a public key that is not the signing key's.
type mismatched struct {
	inner keystore.Custody
}

func (m mismatched) Keys(ctx context.Context) ([]keystore.Key, error) {
	keys, err := m.inner.Keys(ctx)
	if err != nil {
		return nil, err
	}
	other, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	for i := range keys {
		keys[i].PublicKey = other
	}
	return keys, nil
}

func (m mismatched) Sign(ctx context.Context, kid string, message []byte) ([]byte, error) {
	return m.inner.Sign(ctx, kid, message)
}

func TestKeySetInvariants(t *testing.T) {
	public, _, _ := ed25519.GenerateKey(rand.Reader)
	key := func(kid string, state State) Key {
		return Key{KeyID: kid, State: state, Algorithm: "EdDSA", PublicKey: public}
	}
	own := func(kid string, state State) Key {
		fresh, _, _ := ed25519.GenerateKey(rand.Reader)
		return Key{KeyID: kid, State: state, Algorithm: "EdDSA", PublicKey: fresh}
	}
	cases := map[string]struct {
		keys []Key
		want Reason
	}{
		"empty":          {nil, ReasonNoKeys},
		"no active":      {[]Key{own("a", StatePending), own("b", StateRetiring)}, ReasonNoActiveKey},
		"two active":     {[]Key{own("a", StateActive), own("b", StateActive)}, ReasonManyActiveKeys},
		"repeated kid":   {[]Key{key("a", StateActive), key("a", StateRetiring)}, ReasonDuplicateKeyID},
		"bad kid":        {[]Key{key("a b", StateActive)}, ReasonInvalidKey},
		"bad state":      {[]Key{key("a", "enabled")}, ReasonInvalidKey},
		"bad alg":        {[]Key{{KeyID: "a", State: StateActive, Algorithm: "RS256", PublicKey: public}}, ReasonInvalidKey},
		"same key twice": {[]Key{key("a", StateActive), key("b", StateRetiring)}, ReasonDuplicatePublicKey},
		"short key":      {[]Key{{KeyID: "a", State: StateActive, Algorithm: "EdDSA", PublicKey: public[:8]}}, ReasonInvalidKey},
	}
	for name, c := range cases {
		if _, err := NewKeySet(c.keys); ReasonOf(err) != string(c.want) {
			t.Errorf("%s: %v, want %s", name, err, c.want)
		}
	}
	set, err := NewKeySet([]Key{own("p", StatePending), own("a", StateActive), own("r", StateRetiring), own("x", StateRetired), own("c", StateCompromised)})
	if err != nil {
		t.Fatal(err)
	}
	var published []string
	for _, k := range set.Published() {
		published = append(published, k.KeyID)
	}
	if strings.Join(published, ",") != "p,a,r" || set.Active().KeyID != "a" || len(set.Keys()) != 5 {
		t.Fatalf("published %v active %s", published, set.Active().KeyID)
	}
}

// rotating refuses the first `refusals` signs as "not active" and makes the
// second key active from then on, as a rotation landing mid-sign does.
type rotating struct {
	inner    keystore.Custody
	rotate   func()
	refusals int
	calls    int
}

func (r *rotating) Keys(ctx context.Context) ([]keystore.Key, error) { return r.inner.Keys(ctx) }

func (r *rotating) Sign(ctx context.Context, kid string, message []byte) ([]byte, error) {
	r.calls++
	if r.calls <= r.refusals {
		r.rotate()
		return nil, &keystore.Error{Reason: keystore.ReasonKeyNotActive}
	}
	return r.inner.Sign(ctx, kid, message)
}

func TestSignerRetriesWhenARotationLandsMidSign(t *testing.T) {
	dir := t.TempDir()
	writeKey(t, dir, "a")
	writeKey(t, dir, "b")
	inner := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "a", "active", "b", "pending")}).custody
	custody := &rotating{inner: inner, refusals: 1, rotate: func() { writeSet(t, dir, "a", "compromised", "b", "active") }}
	signer := &Signer{custody: custody}
	ctx := context.Background()
	compact, kid, err := signer.Sign(ctx, "at+jwt", []byte(`{}`))
	if err != nil || kid != "b" {
		t.Fatalf("Sign after a mid-sign rotation = kid %q, %v; want kid b", kid, err)
	}
	set, err := signer.KeySet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verify(t, compact, set); err != nil {
		t.Fatalf("the retried token does not verify under the published set: %v", err)
	}
	always := &rotating{inner: inner, refusals: signAttempts, rotate: func() {}}
	if _, _, err := (&Signer{custody: always}).Sign(ctx, "at+jwt", []byte(`{}`)); ReasonOf(err) != "key_not_active" || always.calls != signAttempts {
		t.Fatalf("persistent refusal: %v after %d calls, want key_not_active after %d", err, always.calls, signAttempts)
	}
	checkAlways := &rotating{inner: inner, refusals: signAttempts, rotate: func() {}}
	if err := (&Signer{custody: checkAlways}).Check(ctx); ReasonOf(err) != "key_not_active" || checkAlways.calls != signAttempts {
		t.Fatalf("Check under persistent refusal: %v after %d calls", err, checkAlways.calls)
	}
}

// TestSignAndCheckRefuseASetThatTurnsInvalidAfterTheSnapshot: the manifest
// gains a second active key between the signer's read of the set and the
// signature; Sign and Check fail closed instead of signing.
func TestSignAndCheckRefuseASetThatTurnsInvalidAfterTheSnapshot(t *testing.T) {
	dir := t.TempDir()
	writeKey(t, dir, "a")
	writeKey(t, dir, "b")
	signer := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "a", "active", "b", "pending")})
	afterSnapshot = func() { writeSet(t, dir, "a", "active", "b", "active") }
	t.Cleanup(func() { afterSnapshot = nil })
	ctx := context.Background()
	if compact, _, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err == nil {
		t.Fatalf("Sign returned a token (%d bytes) while the current set has two active keys", len(compact))
	}
	writeSet(t, dir, "a", "active", "b", "pending")
	if err := signer.Check(ctx); err == nil {
		t.Fatal("Check passed while the current set has two active keys")
	}
}

// TestSignAndCheckRefuseASetThatGainsAnUncustodiedKey: after the snapshot the
// manifest names a pending key whose file is missing; Sign and Check fail
// closed.
func TestSignAndCheckRefuseASetThatGainsAnUncustodiedKey(t *testing.T) {
	dir := t.TempDir()
	writeKey(t, dir, "a")
	signer := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "a", "active")})
	afterSnapshot = func() { writeSet(t, dir, "a", "active", "missing", "pending") }
	t.Cleanup(func() { afterSnapshot = nil })
	ctx := context.Background()
	if compact, _, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err == nil {
		t.Fatalf("Sign returned a token (%d bytes) while the current set has a key with no custodied file", len(compact))
	}
	writeSet(t, dir, "a", "active")
	if err := signer.Check(ctx); err == nil {
		t.Fatal("Check passed while the current set has a key with no custodied file")
	}
}
