package envelopemint

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test directory")
		}
		dir = parent
	}
}

func envelopeClassMarkers(t *testing.T) map[string][]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "contracts", "auth", "v1", "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		Classes []struct {
			ClassID string `json:"class_id"`
			Markers []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"markers"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(raw, &registry); err != nil {
		t.Fatal(err)
	}
	for _, class := range registry.Classes {
		if class.ClassID != "effective_principal_envelope" {
			continue
		}
		markers := map[string][]string{}
		for _, marker := range class.Markers {
			markers[marker.Kind] = append(markers[marker.Kind], marker.Name)
		}
		return markers
	}
	t.Fatal("credential-classes.json has no effective_principal_envelope row")
	return nil
}

// The registry's JWT header markers for the envelope are the header Mint
// writes: alg exactly; typ, a media type, without case (RFC 7515 section
// 4.1.9), and the contract files carry its one lowercase spelling.
func TestMintedHeaderIsTheEnvelopeClassMarkers(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	token, err := Mint(key, ProveEnvelopePrincipal("00000000-0000-4000-8000-000000000001"), Options{
		Issuer: "issuer", Audience: "audience", KeyID: "kid", TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	segment, _, _ := strings.Cut(token, ".")
	decoded, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		t.Fatal(err)
	}
	var header map[string]any
	if err := json.Unmarshal(decoded, &header); err != nil {
		t.Fatal(err)
	}
	markers := envelopeClassMarkers(t)
	alg, _ := header["alg"].(string)
	if names := markers["jwt_header_alg"]; len(names) != 1 || names[0] != alg {
		t.Errorf("envelope jwt_header_alg markers = %v, want exactly [%q], the alg Mint writes", names, alg)
	}
	typ, _ := header["typ"].(string)
	if names := markers["jwt_header_typ"]; len(names) != 1 || !strings.EqualFold(names[0], typ) || names[0] != strings.ToLower(names[0]) {
		t.Errorf("envelope jwt_header_typ markers = %v, want exactly one lowercase spelling of %q, the typ Mint writes", names, typ)
	}
}
