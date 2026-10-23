package webhookintake

import (
	"context"
	"net/http"
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
	mirrorTokens.byKey[first] = "v1:not-a-token"
	mirrorTokens.mu.Unlock()
	a.mirrorDecrypt()
	fresh := a.mirrorToken()
	if _, err := first.Decrypt(secrets.NewValue(fresh)); err != nil {
		t.Fatalf("the token was not sealed again after it stopped opening: %v", err)
	}
	if mirrorTokens.byKey[(Deps{}).Decryptor] != "" {
		t.Fatal("the zero decryptor holds a token")
	}
}
