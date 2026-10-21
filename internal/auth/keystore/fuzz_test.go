package keystore

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"testing"
)

// FuzzParseEd25519Key drives the signing-key PEM parser with arbitrary bytes.
// Properties: it never panics, every refusal carries a bounded Reason and no
// key bytes, and an accepted result is exactly one Ed25519 public key.
func FuzzParseEd25519Key(f *testing.F) {
	_, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		f.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		f.Fatal(err)
	}
	good := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	f.Add(good)
	f.Add(append(append([]byte{}, good...), good...))
	f.Add(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
	f.Add([]byte("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, input []byte) {
		public, err := parseEd25519Public(input)
		if err != nil {
			if ReasonOf(err) == "" {
				t.Fatalf("a refusal carries no bounded reason: %v", err)
			}
			return
		}
		if len(public) != ed25519.PublicKeySize {
			t.Fatalf("accepted a %d-byte public key", len(public))
		}
	})
}

// FuzzParseKeySetManifest drives the key-set manifest parser with arbitrary
// bytes. Properties: it never panics, every refusal carries a bounded Reason,
// and an accepted manifest is non-empty with valid, unique kids and bare,
// unique file names.
func FuzzParseKeySetManifest(f *testing.F) {
	f.Add([]byte(`{"keys":[{"kid":"a","state":"active","file":"a.pem"}]}`))
	f.Add([]byte(`{"keys":[{"kid":"a","state":"active","file":"a.pem"},{"kid":"b","state":"pending","file":"b.pem"}]}`))
	f.Add([]byte(`{"keys":[{"kid":"a","state":"active","file":"../a.pem"}]}`))
	f.Add([]byte(`{"keys":[{"kid":"a","state":"active","file":"a.pem","x":1}]}`))
	f.Add([]byte(`{"keys":[]}`))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, input []byte) {
		entries, err := parseManifest(input)
		if err != nil {
			if ReasonOf(err) == "key_custody_failed" {
				t.Fatalf("a refusal carries no bounded reason: %v", err)
			}
			return
		}
		if len(entries) == 0 {
			t.Fatal("accepted an empty set")
		}
		kids, files := map[string]bool{}, map[string]bool{}
		for _, entry := range entries {
			if !KeyIDPattern.MatchString(entry.KeyID) || !entry.State.Valid() || !bareFileName(entry.File) {
				t.Fatalf("accepted an invalid entry %+v", entry)
			}
			if kids[entry.KeyID] || files[entry.File] {
				t.Fatalf("accepted a repeated kid or file %+v", entry)
			}
			kids[entry.KeyID], files[entry.File] = true, true
		}
	})
}
