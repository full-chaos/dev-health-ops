package providerfoundation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/fernet"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	decryptorKeySentinel  = "KEYSENTINEL-4f1c9a"
	decryptorSaltSentinel = "SALTSENTINEL-77be02"
)

func TestFernetDecryptorDerivesTheKeyAtBuildNotAtFirstDecrypt(t *testing.T) {
	before := fernet.Derivations()
	decryptor, err := NewFernetDecryptor(secrets.NewValue("warm-key-"+t.Name()), "warm-salt-"+t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if built := fernet.Derivations(); built != before+1 {
		t.Fatalf("derivations at build = %d, want %d: the key must be derived in NewFernetDecryptor", built-before, 1)
	}
	sealed, err := decryptor.Encrypt([]byte("plain"))
	if err != nil {
		t.Fatal(err)
	}
	if plain, err := decryptor.Decrypt(sealed); err != nil || string(plain) != "plain" {
		t.Fatalf("round trip = %q, %v", plain, err)
	}
	if after := fernet.Derivations(); after != before+1 {
		t.Fatalf("derivations after Encrypt and Decrypt = %d, want 1 (all at build)", after-before)
	}
}

func TestFernetDecryptorRefusesAnUnusableKeyAtBuild(t *testing.T) {
	for _, key := range []secrets.Value{{}, secrets.NewValue("")} {
		decryptor, err := NewFernetDecryptor(key, "")
		if err == nil {
			t.Fatal("NewFernetDecryptor accepted an empty key")
		}
		if decryptor.Configured() {
			t.Fatal("a refused build is configured")
		}
	}
	var zero FernetDecryptor
	if zero.Configured() || zero != (FernetDecryptor{}) {
		t.Fatal("zero decryptor must be unconfigured and comparable")
	}
	if _, err := zero.Encrypt([]byte("x")); !errors.Is(err, ErrCredentialInvalid) {
		t.Fatalf("zero Encrypt err = %v", err)
	}
	if _, err := zero.Decrypt(secrets.NewValue("v1:x")); !errors.Is(err, ErrCredentialInvalid) {
		t.Fatalf("zero Decrypt err = %v", err)
	}
}

type decryptorHolderExported struct{ Decryptor FernetDecryptor }
type decryptorHolderUnexported struct{ decryptor FernetDecryptor }

func decryptorForRedaction(t *testing.T) FernetDecryptor {
	t.Helper()
	decryptor, err := NewFernetDecryptor(secrets.NewValue(decryptorKeySentinel), decryptorSaltSentinel)
	if err != nil {
		t.Fatal(err)
	}
	return decryptor
}

func assertNoDecryptorMaterial(t *testing.T, label, printed string) {
	t.Helper()
	for _, secret := range []string{decryptorKeySentinel, decryptorSaltSentinel} {
		raw := []byte(secret)
		for _, form := range []string{secret, fmt.Sprintf("%x", raw), fmt.Sprintf("%v", raw), fmt.Sprintf("%X", raw)} {
			if strings.Contains(printed, form) {
				t.Errorf("%s printed secret material (%q form): %s", label, secret[:3], printed)
			}
		}
	}
}

func TestFernetDecryptorPrintsNoKeyOrSaltUnderAnyVerb(t *testing.T) {
	decryptor := decryptorForRedaction(t)
	values := map[string]any{
		"top level":             decryptor,
		"pointer":               &decryptor,
		"nested exported":       decryptorHolderExported{Decryptor: decryptor},
		"nested unexported":     decryptorHolderUnexported{decryptor: decryptor},
		"pointer to unexported": &decryptorHolderUnexported{decryptor: decryptor},
	}
	for name, value := range values {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%d", "%x", "%q"} {
			assertNoDecryptorMaterial(t, name+" "+verb, fmt.Sprintf(verb, value))
		}
	}
}

func TestFernetDecryptorLogValueAndJSONCarryNoKeyOrSalt(t *testing.T) {
	decryptor := decryptorForRedaction(t)
	var text, jsonOut bytes.Buffer
	slog.New(slog.NewTextHandler(&text, nil)).Info("m", "decryptor", decryptor, "holder", decryptorHolderExported{Decryptor: decryptor})
	slog.New(slog.NewJSONHandler(&jsonOut, nil)).Info("m", "decryptor", decryptor, "holder", decryptorHolderExported{Decryptor: decryptor})
	assertNoDecryptorMaterial(t, "slog text", text.String())
	assertNoDecryptorMaterial(t, "slog json", jsonOut.String())
	valuer, ok := any(decryptor).(slog.LogValuer)
	if !ok {
		t.Fatal("FernetDecryptor is not a slog.LogValuer")
	}
	for name, value := range map[string]any{"top level": decryptor, "nested": decryptorHolderExported{Decryptor: decryptor}, "unexported": decryptorHolderUnexported{decryptor: decryptor}} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		assertNoDecryptorMaterial(t, "json "+name, string(encoded))
	}
	if value := valuer.LogValue(); strings.Contains(value.String(), decryptorKeySentinel) || strings.Contains(value.String(), decryptorSaltSentinel) {
		t.Fatalf("LogValue = %s", value)
	}
}

func TestFernetDecryptorPrintsOnlyTheRedactionMarker(t *testing.T) {
	const marker = "providerfoundation.FernetDecryptor"
	decryptor := decryptorForRedaction(t)
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%d"} {
		if got := fmt.Sprintf(verb, decryptor); got != marker {
			t.Errorf("Sprintf(%s) = %q, want %q", verb, got, marker)
		}
	}
	stringer, ok := any(decryptor).(fmt.Stringer)
	if !ok {
		t.Fatal("FernetDecryptor is not a fmt.Stringer")
	}
	if got := stringer.String(); got != marker {
		t.Errorf("String() = %q", got)
	}
	encoded, err := json.Marshal(decryptor)
	if err != nil || string(encoded) != `"`+marker+`"` {
		t.Errorf("json.Marshal = %s, %v", encoded, err)
	}
	encoded, err = json.Marshal(decryptorHolderExported{Decryptor: decryptor})
	if err != nil || string(encoded) != `{"Decryptor":"`+marker+`"}` {
		t.Errorf("nested json.Marshal = %s, %v", encoded, err)
	}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("m", "decryptor", decryptor)
	if !strings.Contains(buf.String(), `"decryptor":{"encryption_key_configured":true}`) {
		t.Errorf("slog = %s", buf.String())
	}
}
