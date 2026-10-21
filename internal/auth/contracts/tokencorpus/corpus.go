// Package tokencorpus is the signed golden corpus of access-token.v1: the
// vectors every verifier (Go now, TypeScript later) must decide the same way.
//
// A vector is stored as the three parts of a compact JWS in separate fields:
// the header text, the payload text and the signature in hex. The signing input
// is base64url(header_json) + "." + base64url(payload_json); no compact JWS
// string is ever committed. Header and payload are raw text so a vector can
// carry a repeated member, which no JSON encoder would write.
package tokencorpus

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Surface names the contract the corpus belongs to.
const Surface = "access-token.v1"

// Dir is the corpus directory, relative to the repo root.
var Dir = filepath.Join("contracts", "auth", "v1", "golden", "access-token")

// Outcome of a vector, and the closed vocabulary of refusal reasons.
const (
	Accept = "accept"
	Reject = "reject"

	ReasonContract    = "contract"
	ReasonIssuer      = "issuer"
	ReasonAudience    = "audience"
	ReasonUnknownKid  = "unknown_kid"
	ReasonSignature   = "signature"
	ReasonNotYetValid = "not_yet_valid"
	ReasonExpired     = "expired"
)

// Reasons is the closed set a manifest entry may name.
var Reasons = []string{
	ReasonContract, ReasonIssuer, ReasonAudience, ReasonUnknownKid,
	ReasonSignature, ReasonNotYetValid, ReasonExpired,
}

// Vector is one stored token.
type Vector struct {
	HeaderJSON   string `json:"header_json"`
	PayloadJSON  string `json:"payload_json"`
	SignatureHex string `json:"signature_hex"`
}

// Entry is one manifest row: the vector file, the verifier's fixed clock and
// audience, and the decision every verifier must reach.
type Entry struct {
	File     string `json:"file"`
	Now      int64  `json:"now"`
	VerifyAs string `json:"verify_as_audience"`
	Expect   string `json:"expect"`
	Reason   string `json:"reason,omitempty"`
	Stage    string `json:"contract_stage,omitempty"`
	Why      string `json:"why"`
}

// Manifest is the single inventory of the corpus.
type Manifest struct {
	Description   string   `json:"description"`
	Surface       string   `json:"surface"`
	Issuer        string   `json:"issuer"`
	LeewaySeconds int64    `json:"leeway_seconds"`
	RemovedKids   []string `json:"removed_kids"`
	RemovedKeys   []JwkKey `json:"removed_public_keys"`
	ReasonOrder   []string `json:"decision_order"`
	Vectors       []Entry  `json:"vectors"`
}

// Jwks is the public key set the corpus is verified against.
type Jwks struct {
	Keys []JwkKey `json:"keys"`
}

// JwkKey is one public Ed25519 key of the set.
type JwkKey struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Alg string `json:"alg"`
	Use string `json:"use,omitempty"`
	Kid string `json:"kid"`
	X   string `json:"x"`
}

// Corpus is a whole corpus in memory.
type Corpus struct {
	Manifest Manifest
	Jwks     Jwks
	Vectors  map[string]Vector
}

// SigningInput is the byte string a vector's signature covers.
func (v Vector) SigningInput() []byte {
	enc := base64.RawURLEncoding
	return []byte(enc.EncodeToString([]byte(v.HeaderJSON)) + "." + enc.EncodeToString([]byte(v.PayloadJSON)))
}

// PublicKeys decodes the set into key bytes by kid, refusing a malformed key.
func (j Jwks) PublicKeys() (map[string]ed25519.PublicKey, error) {
	out := map[string]ed25519.PublicKey{}
	for _, k := range j.Keys {
		raw, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil || len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("tokencorpus: key %q is not a 32-byte Ed25519 key", k.Kid)
		}
		if _, dup := out[k.Kid]; dup {
			return nil, fmt.Errorf("tokencorpus: repeated kid %q", k.Kid)
		}
		out[k.Kid] = ed25519.PublicKey(raw)
	}
	return out, nil
}

// Load reads a corpus directory.
func Load(dir string) (*Corpus, error) {
	var c Corpus
	if err := readJSON(filepath.Join(dir, "manifest.json"), &c.Manifest); err != nil {
		return nil, err
	}
	if err := readJSON(filepath.Join(dir, "jwks.json"), &c.Jwks); err != nil {
		return nil, err
	}
	c.Vectors = map[string]Vector{}
	for _, e := range c.Manifest.Vectors {
		var v Vector
		if err := readJSON(filepath.Join(dir, "vectors", e.File), &v); err != nil {
			return nil, err
		}
		c.Vectors[e.File] = v
	}
	return &c, nil
}

// Write writes a corpus directory: manifest, public JWKS and one file per vector.
func Write(dir string, c *Corpus) error {
	if err := os.MkdirAll(filepath.Join(dir, "vectors"), 0o755); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "manifest.json"), c.Manifest); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "jwks.json"), c.Jwks); err != nil {
		return err
	}
	names := make([]string, 0, len(c.Vectors))
	for name := range c.Vectors {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := writeJSON(filepath.Join(dir, "vectors", name), c.Vectors[name]); err != nil {
			return err
		}
	}
	return nil
}

func readJSON(path string, dest any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dest)
}

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

// DecodeSignature returns the signature bytes; hex that is not hex is a vector
// that cannot be a valid signature, reported as an error to the caller.
func DecodeSignature(v Vector) ([]byte, error) {
	if v.SignatureHex == "" {
		return nil, nil
	}
	if strings.ToLower(v.SignatureHex) != v.SignatureHex {
		return nil, errors.New("tokencorpus: signature_hex must be lower-case")
	}
	return hex.DecodeString(v.SignatureHex)
}
