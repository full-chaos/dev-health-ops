package webhookintake

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/fernet"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestRefusedAndKnownDeliveriesDeriveNoKeyAndDecryptOnce(t *testing.T) {
	f := newRefusalFixture(t, "active")
	unavailable := uuid.NewString()
	inner := loadBinding
	loadBinding = func(ctx context.Context, pool *pgxpool.Pool, id string) (pagerdutyBinding, error) {
		if id == unavailable {
			return pagerdutyBinding{}, errBindingNotFound
		}
		return inner(ctx, pool, id)
	}
	t.Cleanup(func() { loadBinding = inner })
	body := []byte(`{"event":{}}`)
	wrong := pagerdutySign("another-"+f.secret, body)
	f.send(f.bindingID, f.sub, wrong, body)
	classes := []struct{ name, id, sub string }{
		{"known binding, bad signature", f.bindingID, f.sub},
		{"unknown binding", uuid.NewString(), f.sub},
		{"binding that is not receivable", unavailable, f.sub},
		{"other subscription", f.bindingID, "other-" + f.sub},
		{"missing subscription", f.bindingID, ""},
		{"id that is no uuid", "not-a-uuid", f.sub},
	}
	for _, c := range classes {
		derivedBefore, workBefore := fernet.Derivations(), decryptWork.Load()
		got, _ := f.send(c.id, c.sub, wrong, body)
		if got.Code != http.StatusUnauthorized {
			t.Errorf("%s: status %d", c.name, got.Code)
		}
		if delta := fernet.Derivations() - derivedBefore; delta != 0 {
			t.Errorf("%s: %d key derivations on the request path, want 0", c.name, delta)
		}
		if delta := decryptWork.Load() - workBefore; delta != 1 {
			t.Errorf("%s: %d decrypt steps, want 1", c.name, delta)
		}
	}
}

func TestSecretRotationIsSeenAtOnce(t *testing.T) {
	f := newRefusalFixture(t, "active")
	body := []byte(`{"event":{"id":"E1","event_type":"pagey.ping","occurred_at":"2026-01-01T00:00:00Z","data":{}}}`)
	if got, _ := f.send(f.bindingID, f.sub, pagerdutySign(f.secret, body), body); got.Code != http.StatusNoContent {
		t.Fatalf("the current secret answered %d", got.Code)
	}
	rotated := "rotated-" + uuid.NewString()
	encrypted, err := f.deps.Decryptor.Encrypt([]byte(rotated))
	if err != nil {
		t.Fatal(err)
	}
	previous := loadBinding
	loadBinding = func(ctx context.Context, pool *pgxpool.Pool, id string) (pagerdutyBinding, error) {
		found, err := previous(ctx, pool, id)
		found.SigningSecretEncrypted = encrypted.Reveal()
		return found, err
	}
	t.Cleanup(func() { loadBinding = previous })
	if got, _ := f.send(f.bindingID, f.sub, pagerdutySign(rotated, body), body); got.Code != http.StatusNoContent {
		t.Fatalf("the rotated secret answered %d", got.Code)
	}
	if got, _ := f.send(f.bindingID, f.sub, pagerdutySign(f.secret, body), body); got.Code != http.StatusUnauthorized {
		t.Fatalf("the retired secret answered %d", got.Code)
	}
}

func TestMirrorTokenIsSealedPerKeyAndResealedWhenItStopsOpening(t *testing.T) {
	first, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("mirror-key-one"), "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("mirror-key-two"), "")
	if err != nil {
		t.Fatal(err)
	}
	a, b := Deps{Decryptor: first}, Deps{Decryptor: second}
	tokenA, tokenB := a.mirrorToken(), b.mirrorToken()
	if tokenA == "" || tokenB == "" || tokenA == tokenB {
		t.Fatalf("tokens %q %q", tokenA, tokenB)
	}
	if _, err := first.Decrypt(secrets.NewValue(tokenA)); err != nil {
		t.Fatalf("a's token does not open under a's key: %v", err)
	}
	if _, err := first.Decrypt(secrets.NewValue(tokenB)); err == nil {
		t.Fatal("b's token opens under a's key")
	}
	mirrorTokens.mu.Lock()
	mirrorTokens.byKey[mirrorKey{first}] = "v1:not-a-token"
	mirrorTokens.mu.Unlock()
	a.mirrorDecrypt()
	fresh := a.mirrorToken()
	if _, err := first.Decrypt(secrets.NewValue(fresh)); err != nil {
		t.Fatalf("the token was not sealed again after it stopped opening: %v", err)
	}
	if token := (Deps{}).mirrorToken(); token != "" {
		t.Fatalf("an unconfigured decryptor produced a mirror token %q", token)
	}
	mirrorTokens.mu.Lock()
	_, held := mirrorTokens.byKey[mirrorKey{(Deps{}).Decryptor}]
	printed := fmt.Sprintf("%v %+v %#v %s", mirrorTokens.byKey, mirrorTokens.byKey, mirrorTokens.byKey, mirrorTokens.order)
	mirrorTokens.mu.Unlock()
	if held {
		t.Fatal("the zero decryptor holds a token")
	}
	for _, secret := range []string{"mirror-key-one", "mirror-key-two"} {
		if strings.Contains(printed, secret) {
			t.Fatalf("the formatted token map carries a key: %s", printed)
		}
	}
}

func TestAnOversizedStoredSecretIsRefusedLikeAnUnknownBinding(t *testing.T) {
	f := newRefusalFixture(t, "active")
	previous := loadBinding
	loadBinding = func(ctx context.Context, pool *pgxpool.Pool, id string) (pagerdutyBinding, error) {
		found, err := previous(ctx, pool, id)
		found.SigningSecretEncrypted += strings.Repeat("A", maxStoredSecretBytes)
		return found, err
	}
	t.Cleanup(func() { loadBinding = previous })
	body := []byte(`{"event":{}}`)
	work := decryptWork.Load()
	got, _ := f.send(f.bindingID, f.sub, pagerdutySign(f.secret, body), body)
	if got.Code != http.StatusUnauthorized || got.Body.String() != `{"detail":"Invalid signature"}` {
		t.Fatalf("an oversized stored secret answered %d %s", got.Code, got.Body.String())
	}
	if decryptWork.Load()-work != 1 {
		t.Fatalf("%d decrypt steps, want the one dummy step", decryptWork.Load()-work)
	}
}

// A stored secret sealed to the largest ciphertext at or under the cap is a
// usable binding; the next size up is refused like an unknown binding, with
// the same single decrypt step.
func TestStoredSecretCapBoundaryClassesAreEqualOnEitherSide(t *testing.T) {
	const capBytes = 1024
	if maxStoredSecretBytes != capBytes {
		t.Fatalf("the stored-secret cap is %d, the contract is %d", maxStoredSecretBytes, capBytes)
	}
	f := newRefusalFixture(t, "active")
	body := []byte(`{"event":{"id":"E1","event_type":"pagey.ping","occurred_at":"2026-01-01T00:00:00Z","data":{}}}`)
	sealTo := func(size int) (secret, ciphertext string) {
		secret = strings.Repeat("s", size)
		sealed, err := f.deps.Decryptor.Encrypt([]byte(secret))
		if err != nil {
			t.Fatal(err)
		}
		return secret, sealed.Reveal()
	}
	var atCap, overCap struct{ secret, ciphertext string }
	for size := 300; ; size++ {
		secret, ciphertext := sealTo(size)
		if len(ciphertext) > capBytes {
			overCap.secret, overCap.ciphertext = secret, ciphertext
			break
		}
		atCap.secret, atCap.ciphertext = secret, ciphertext
	}
	if len(atCap.ciphertext) > capBytes || len(overCap.ciphertext) <= capBytes || len(overCap.ciphertext)-len(atCap.ciphertext) > 32 {
		t.Fatalf("boundary sizes %d and %d around %d", len(atCap.ciphertext), len(overCap.ciphertext), capBytes)
	}
	previous := loadBinding
	var current string
	loadBinding = func(ctx context.Context, pool *pgxpool.Pool, id string) (pagerdutyBinding, error) {
		found, err := previous(ctx, pool, id)
		found.SigningSecretEncrypted = current
		return found, err
	}
	t.Cleanup(func() { loadBinding = previous })

	current = atCap.ciphertext
	work := decryptWork.Load()
	if got, _ := f.send(f.bindingID, f.sub, pagerdutySign(atCap.secret, body), body); got.Code != http.StatusNoContent {
		t.Fatalf("a secret at the cap answered %d", got.Code)
	}
	if decryptWork.Load()-work != 1 {
		t.Fatalf("%d decrypt steps at the cap, want 1", decryptWork.Load()-work)
	}
	current = overCap.ciphertext
	work = decryptWork.Load()
	got, _ := f.send(f.bindingID, f.sub, pagerdutySign(overCap.secret, body), body)
	if got.Code != http.StatusUnauthorized || got.Body.String() != `{"detail":"Invalid signature"}` {
		t.Fatalf("a secret over the cap answered %d %s", got.Code, got.Body.String())
	}
	if decryptWork.Load()-work != 1 {
		t.Fatalf("%d decrypt steps over the cap, want 1", decryptWork.Load()-work)
	}
}
