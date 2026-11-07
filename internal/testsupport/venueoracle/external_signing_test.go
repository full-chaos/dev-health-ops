package venueoracle

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/authrows"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

const testIssuer, testKid = "https://auth.corpus.test:8443", "auth-signing-corpus"

// writeSigningKey writes a fresh Ed25519 key as a PKCS#8 PEM file of mode in a temporary directory.
func writeSigningKey(t *testing.T, mode os.FileMode) (string, ed25519.PrivateKey, []byte) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	raw := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	path := filepath.Join(t.TempDir(), "signing.pem")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path, priv, raw
}

// useExternalIssuer points the external token issuer environment at a fresh key file of mode.
func useExternalIssuer(t *testing.T, mode os.FileMode) (string, ed25519.PrivateKey, []byte) {
	t.Helper()
	path, priv, raw := writeSigningKey(t, mode)
	t.Setenv(EnvExternalIssuer, testIssuer)
	t.Setenv(EnvExternalSigningKeyFile, path)
	t.Setenv(EnvExternalSigningKeyID, testKid)
	return path, priv, raw
}

// The issuer environment name is the one the corpus script exports and the session-token rows read.
func TestExternalIssuerIsTheSessionIssuerName(t *testing.T) {
	if EnvExternalIssuer != authrows.EnvSessionIssuer {
		t.Fatalf("%s != %s", EnvExternalIssuer, authrows.EnvSessionIssuer)
	}
}

// An external target's tokens are user access tokens of the target's own issuer: minted under the key file, as the
// issuer and key id the environment names, and they verify under that key.
func TestExternalTokenIssuerMintsUnderTheTargetsKey(t *testing.T) {
	_, priv, _ := useExternalIssuer(t, 0o600)
	issuer, err := externalTokenIssuer()
	if err != nil {
		t.Fatal(err)
	}
	user := uuid.New()
	token := issuer.MustMint(t, edtokens.Claims{UserID: user, OrgID: uuid.NewString()})
	header, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[0])
	if err != nil || !strings.Contains(string(header), `"kid":"`+testKid+`"`) || !strings.Contains(string(header), `"alg":"EdDSA"`) {
		t.Fatalf("header %s (%v), want EdDSA under kid %s", header, err, testKid)
	}
	claims, err := issuer.Verifier(t).Verify(token, time.Now())
	if err != nil || claims.Subject != edtokens.PrincipalID(user) {
		t.Fatalf("the token does not verify as the target's issuer: %+v %v", claims, err)
	}
	parts := strings.Split(token, ".")
	signature, _ := base64.RawURLEncoding.DecodeString(parts[2])
	if !ed25519.Verify(priv.Public().(ed25519.PublicKey), []byte(parts[0]+"."+parts[1]), signature) {
		t.Fatal("the token is not signed with the key file's key")
	}
	if !strings.Contains(string(mustPayload(t, token)), `"iss":"`+testIssuer+`"`) {
		t.Fatalf("payload %s does not name the issuer %s", mustPayload(t, token), testIssuer)
	}
}

func mustPayload(t *testing.T, token string) []byte {
	t.Helper()
	payload, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// The key file must be a regular file of mode 0600 holding one PKCS#8 Ed25519 PEM block; every other file is
// refused, and no refusal carries the file's content.
func TestExternalSigningKeyFileIsRefusedUnlessPrivateAndEd25519(t *testing.T) {
	for _, mode := range []os.FileMode{0o644, 0o640, 0o400, 0o700, 0o666} {
		t.Run(fmt.Sprintf("mode %04o", mode), func(t *testing.T) {
			_, priv, raw := useExternalIssuer(t, mode)
			_, err := externalTokenIssuer()
			if err == nil || !strings.Contains(err.Error(), "want 0600") {
				t.Fatalf("mode %04o: err %v, want the 0600 refusal", mode, err)
			}
			assertNoKeyIn(t, err.Error(), priv, raw)
		})
	}
	dir := t.TempDir()
	link := filepath.Join(dir, "link.pem")
	target, priv, raw := writeSigningKey(t, 0o600)
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readSigningKey(link); err == nil {
		t.Error("a symlink to a key file was accepted")
	} else {
		assertNoKeyIn(t, err.Error(), priv, raw)
	}
	marker := "MARKER-not-a-key-0123456789"
	for name, content := range map[string]string{
		"not pem":      marker,
		"two blocks":   string(raw) + string(raw),
		"wrong type":   strings.Replace(string(raw), "PRIVATE KEY", "EC PRIVATE KEY", 2),
		"trailing":     string(raw) + marker,
		"rsa-like der": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte(marker)})),
	} {
		path := filepath.Join(dir, strings.ReplaceAll(name, " ", "-"))
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := readSigningKey(path)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if strings.Contains(err.Error(), marker) {
			t.Errorf("%s: the refusal carries the file content: %v", name, err)
		}
		assertNoKeyIn(t, err.Error(), priv, raw)
	}
	for _, unset := range []string{EnvExternalIssuer, EnvExternalSigningKeyFile, EnvExternalSigningKeyID} {
		useExternalIssuer(t, 0o600)
		t.Setenv(unset, "")
		if _, err := externalTokenIssuer(); err == nil || !strings.Contains(err.Error(), unset) {
			t.Errorf("%s unset: err %v, want a refusal naming it", unset, err)
		}
	}
}

// A printed issuer names its issuer and key id, never its key.
func TestAPrintedIssuerNeverShowsItsKey(t *testing.T) {
	_, priv, raw := useExternalIssuer(t, 0o600)
	issuer, err := externalTokenIssuer()
	if err != nil {
		t.Fatal(err)
	}
	printed := fmt.Sprintf("%v|%+v|%#v|%s", issuer, issuer, issuer, issuer)
	if !strings.Contains(printed, testKid) {
		t.Fatalf("printed issuer %q does not name its key id", printed)
	}
	assertNoKeyIn(t, printed, priv, raw)
}

// assertNoKeyIn fails when text holds the key in any form a log could show it.
func assertNoKeyIn(t *testing.T, text string, priv ed25519.PrivateKey, raw []byte) {
	t.Helper()
	seed := priv.Seed()
	for name, form := range map[string]string{
		"pem":       string(raw),
		"pem body":  strings.Split(string(raw), "\n")[1],
		"seed hex":  hex.EncodeToString(seed),
		"seed b64":  base64.StdEncoding.EncodeToString(seed),
		"seed b64u": base64.RawURLEncoding.EncodeToString(seed),
		"raw bytes": string(seed),
		"go bytes":  fmt.Sprint([]byte(seed)),
	} {
		if strings.Contains(text, form) {
			t.Fatalf("the key appears (%s) in %q", name, text)
		}
	}
}
