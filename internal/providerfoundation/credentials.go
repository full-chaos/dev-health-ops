package providerfoundation

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"golang.org/x/crypto/pbkdf2"
)

const (
	credentialCiphertextV1 = "v1:"
	defaultEncryptionSalt  = "dev-health-ops-settings-encryption-v1"
	fernetVersion          = byte(0x80)
	fernetBlockSize        = aes.BlockSize
	fernetSignatureSize    = sha256.Size
	fernetHeaderSize       = 1 + 8 + fernetBlockSize
)

// FernetDecryptor is wire-compatible with core/encryption.py. It supports
// v1 PBKDF2 ciphertext and the legacy SHA-256 derived ciphertext so migration
// workers never need to mutate process-global credential environment values.
type FernetDecryptor struct {
	key  secrets.Value
	salt string
}

func NewFernetDecryptor(key secrets.Value, salt string) (FernetDecryptor, error) {
	if !key.Configured() {
		return FernetDecryptor{}, ErrCredentialInvalid
	}
	if salt == "" {
		salt = defaultEncryptionSalt
	}
	return FernetDecryptor{key: key, salt: salt}, nil
}

// Configured reports whether an encryption key is set: without one every
// Encrypt and Decrypt is refused, which callers must tell apart from a wrong
// key (Python raises RuntimeError for the first and ValueError for the second).
func (d FernetDecryptor) Configured() bool { return d.key.Configured() }

func (d FernetDecryptor) Decrypt(ciphertext secrets.Value) ([]byte, error) {
	if !d.key.Configured() || !ciphertext.Configured() {
		return nil, ErrCredentialInvalid
	}
	raw := ciphertext.Reveal()
	key := []byte(d.key.Reveal())
	if strings.HasPrefix(raw, credentialCiphertextV1) {
		return decryptFernet(strings.TrimPrefix(raw, credentialCiphertextV1), pbkdf2.Key(key, []byte(d.salt), 600000, 32, sha256.New))
	}
	if strings.HasPrefix(raw, "v") && strings.Contains(raw, ":") {
		return nil, ErrCredentialInvalid
	}
	legacy := sha256.Sum256(key)
	return decryptFernet(raw, legacy[:])
}

// Encrypt emits the same versioned Fernet format as core/encryption.py. It is
// used only when an OAuth refresh atomically replaces a short-lived token.
func (d FernetDecryptor) Encrypt(plaintext []byte) (secrets.Value, error) {
	if !d.key.Configured() {
		return secrets.Value{}, ErrCredentialInvalid
	}
	key := pbkdf2.Key(
		[]byte(d.key.Reveal()), []byte(d.salt), 600000, 32, sha256.New,
	)
	token, err := encryptFernet(plaintext, key, time.Now(), rand.Reader)
	if err != nil {
		return secrets.Value{}, ErrCredentialInvalid
	}
	return secrets.NewValue(credentialCiphertextV1 + token), nil
}

func encryptFernet(plaintext, key []byte, now time.Time, entropy io.Reader) (string, error) {
	if len(key) != 32 || entropy == nil {
		return "", ErrCredentialInvalid
	}
	padding := fernetBlockSize - len(plaintext)%fernetBlockSize
	padded := make([]byte, len(plaintext)+padding)
	copy(padded, plaintext)
	copy(padded[len(plaintext):], bytes.Repeat([]byte{byte(padding)}, padding))
	block, err := aes.NewCipher(key[16:])
	if err != nil {
		return "", ErrCredentialInvalid
	}
	iv := make([]byte, fernetBlockSize)
	if _, err := io.ReadFull(entropy, iv); err != nil {
		return "", ErrCredentialInvalid
	}
	payload := make([]byte, fernetHeaderSize, fernetHeaderSize+len(padded)+fernetSignatureSize)
	payload[0] = fernetVersion
	binary.BigEndian.PutUint64(payload[1:9], uint64(now.Unix()))
	copy(payload[9:fernetHeaderSize], iv)
	encrypted := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(encrypted, padded)
	payload = append(payload, encrypted...)
	mac := hmac.New(sha256.New, key[:16])
	_, _ = mac.Write(payload)
	payload = append(payload, mac.Sum(nil)...)
	return base64.URLEncoding.EncodeToString(payload), nil
}

func decryptFernet(token string, key []byte) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		decoded, err = base64.URLEncoding.DecodeString(token)
	}
	if err != nil || len(decoded) < fernetHeaderSize+fernetSignatureSize || decoded[0] != fernetVersion || len(key) != 32 {
		return nil, ErrCredentialInvalid
	}
	signed := decoded[:len(decoded)-fernetSignatureSize]
	mac := hmac.New(sha256.New, key[:16])
	_, _ = mac.Write(signed)
	if !hmac.Equal(mac.Sum(nil), decoded[len(decoded)-fernetSignatureSize:]) {
		return nil, ErrCredentialInvalid
	}
	block, err := aes.NewCipher(key[16:])
	if err != nil {
		return nil, ErrCredentialInvalid
	}
	if len(signed[fernetHeaderSize:]) == 0 || len(signed[fernetHeaderSize:])%fernetBlockSize != 0 {
		return nil, ErrCredentialInvalid
	}
	plain := make([]byte, len(signed[fernetHeaderSize:]))
	cipher.NewCBCDecrypter(block, signed[9:fernetHeaderSize]).CryptBlocks(plain, signed[fernetHeaderSize:])
	padding := int(plain[len(plain)-1])
	if padding == 0 || padding > fernetBlockSize || padding > len(plain) || !bytes.Equal(plain[len(plain)-padding:], bytes.Repeat([]byte{byte(padding)}, padding)) {
		return nil, ErrCredentialInvalid
	}
	return plain[:len(plain)-padding], nil
}

// githubFieldAliases mirrors github_credentials_from_mapping's alias dict
// (credentials/resolver.py:259-265) exactly: a GitHub App credential row a
// web client wrote can carry camelCase field names. Applied ONLY for
// provider=="github", matching that Python function's own call-site
// scoping (it is never called for another provider) -- an unrelated
// provider's literal "appId"-shaped field name is never silently renamed.
//
// r2 (round 1 finding #2): if both an alias and its canonical spelling
// are present on the SAME row, Python's own behavior is deterministic --
// `{aliases.get(k, k): v for k, v in cred_dict.items() if v is not None}`
// is a dict comprehension over `cred_dict.items()` in the JSON TEXT's own
// key order (Python dicts, and json.loads, preserve insertion order), so
// whichever spelling appears LAST in the stored JSON wins. This decode
// used to walk a plain `map[string]any` from encoding/json, whose
// iteration order Go randomizes on every call -- round 1 measured the
// alias winning 245/2000 runs on the IDENTICAL input, a real
// nondeterminism bug independent of whether it also happened to match
// Python (it did not, reliably, precisely because it was random).
// Decoding with pyjson.DecodeString instead (an *pyjson.Object, which
// preserves JSON document order the same way pyjson.Object does
// everywhere else in this codebase) and walking .Keys() in that order
// makes the "later key wins" rule deterministic and matches Python's own
// document-order precedence exactly, not merely "some fixed order".
var githubFieldAliases = map[string]string{
	"appId":          "app_id",
	"baseUrl":        "base_url",
	"installationId": "installation_id",
	"privateKey":     "private_key",
	"privateKeyPath": "private_key_path",
}

// pythonSecretText is what Python makes of a credential field's non-string
// JSON value where the resolver reads it as `str(value or "")`: a number is
// its str() (every integer digit, repr() for a float), a boolean is "True" and
// falsy forms (0, 0.0, false) are "" -- a present field that is not configured.
// A null is absent (Python drops None). A non-empty container has no
// faithful text here and reads as absent: the shape check then refuses the
// credential, where Python would carry str(list) through. An empty container
// is falsy in Python (""), which is also absent here.
func pythonSecretText(value pyjson.Value) (secrets.Value, bool) {
	switch number := value.(type) {
	case bool:
		if !number {
			return secrets.NewValue(""), true
		}
		return secrets.NewValue("True"), true
	case pyjson.Int:
		if number.Int == nil {
			return secrets.Value{}, false
		}
		if number.Sign() == 0 {
			return secrets.NewValue(""), true
		}
		return secrets.NewValue(number.String()), true
	case pyjson.Float:
		if number == 0 {
			return secrets.NewValue(""), true
		}
		return secrets.NewValue(pythonparity.Repr(float64(number))), true
	}
	return secrets.Value{}, false
}

func decodeCredential(record EncryptedCredential, plaintext []byte) (Credential, error) {
	decoded, err := pyjson.DecodeString(string(plaintext))
	if err != nil {
		return Credential{}, ErrCredentialInvalid
	}
	object, isObject := decoded.(*pyjson.Object)
	if !isObject {
		return Credential{}, ErrCredentialInvalid
	}
	fields := make(map[string]secrets.Value, object.Len())
	deferred := map[string]pyjson.Value{}
	for _, key := range object.Keys() {
		value, _ := object.Get(key)
		if strings.TrimSpace(key) == "" {
			return Credential{}, ErrCredentialInvalid
		}
		if record.Provider == "github" {
			if canonical, aliased := githubFieldAliases[key]; aliased {
				key = canonical
			}
		}
		// A key stored twice (an alias and its canonical spelling) keeps the
		// later one, whichever kind of value each held. A later string
		// replaces an earlier non-string because Secret reads fields first; a
		// later non-string must remove an earlier string here.
		delete(fields, key)
		if text, isString := value.(string); isString {
			fields[key] = secrets.NewValue(text)
			continue
		}
		// Python reads the fields it wants and ignores the rest
		// (`str(cred_dict.get("token") or "")`, an allow-list of kwargs), so a
		// value that is not a string is kept as decoded and judged only when a
		// caller asks for that field (Credential.Secret), never up front
		// (CHAOS-6770).
		deferred[key] = value
	}
	config := make(map[string]string, len(record.Config))
	for key, value := range record.Config {
		config[key] = value
	}
	credential := Credential{Provider: record.Provider, ID: record.ID, Name: record.Name, Config: config, fields: fields, deferred: deferred}
	// CHAOS-6782: linear_credentials_from_mapping reads
	// `str(api_key or apiKey or "")`: the canonical spelling wins when it is
	// truthy (not by document order, unlike GitHub's aliases), otherwise the
	// camelCase spelling the web wrote.
	if record.Provider == "linear" {
		if canonical, ok := credential.Secret("api_key"); !ok || !canonical.Configured() {
			if alias, ok := credential.Secret("apiKey"); ok && alias.Configured() {
				fields["api_key"] = alias
			}
		}
	}
	return credential, nil
}

// jiraAPITokenAliases lists the spellings a stored Jira credential may use for
// its API token, most canonical first. The web wrote `token` from Admin >
// Providers > JIRA > "Create New" long before anything read it, so the aliases
// are not a convenience -- without them those rows authenticate nothing
// (CHAOS-4224). Kept in step with `jira_credentials_from_mapping`, which this
// function's own contract below promises to mirror.
var jiraAPITokenAliases = []string{"api_token", "apiToken", "token"}

// jiraBaseURLAliases does the same for the instance URL: Admin > Syncs > JIRA >
// "+Add New" wrote `server_url`.
var jiraBaseURLAliases = []string{"base_url", "baseUrl", "url", "server_url"}

func hasAny(credential Credential, names []string) bool {
	for _, name := range names {
		if value, ok := credential.Secret(name); ok && value.Configured() {
			return true
		}
	}
	return false
}

// ValidateCredentialShape keeps auth construction explicit. It accepts only
// the auth fields that the current Python resolver accepts for this provider.
func ValidateCredentialShape(credential Credential) error {
	has := func(name string) bool { value, ok := credential.Secret(name); return ok && value.Configured() }
	switch credential.Provider {
	case "github":
		token := has("token")
		// r2 (round 1 finding #1): github_credentials_from_mapping accepts
		// EITHER private_key (inline PEM content) OR private_key_path (a
		// file path it reads at resolve time, resolver.py:269-276) as
		// satisfying the App-auth triple -- this is a pure SHAPE check
		// (no file I/O here; NewGitHubAppAuth does the actual read), so a
		// private_key_path-only row must pass it the same way Python's
		// own shape check (GitHubCredentials's validation) does.
		app := has("app_id") && (has("private_key") || has("private_key_path")) && has("installation_id")
		if token == app {
			return ErrCredentialInvalid
		}
		// CHAOS-6781: GitHubCredentials.__post_init__ raises when a token comes
		// with ANY App field, not only a complete triple, and the builder then
		// returns None. private_key_path counts as the key only when there is
		// no private_key entry (the builder reads the file into it only then).
		if token {
			key := has("private_key_path")
			if _, present := credential.Secret("private_key"); present {
				key = has("private_key")
			}
			if has("app_id") || has("installation_id") || key {
				return ErrCredentialInvalid
			}
		}
	case "gitlab":
		if !has("token") {
			return ErrCredentialInvalid
		}
	case "jira":
		if !hasAny(credential, jiraAPITokenAliases) || !has("email") {
			return ErrCredentialInvalid
		}
	case "linear":
		if !has("api_key") {
			return ErrCredentialInvalid
		}
	case "launchdarkly":
		if !has("api_key") {
			return ErrCredentialInvalid
		}
	case "pagerduty":
		mode := credentialValue(credential, "auth_mode")
		if mode == "" {
			switch {
			case has("api_token"):
				mode = "api_token"
			case has("access_token"):
				mode = "oauth"
			case has("client_id") && has("client_secret"):
				mode = "client_credentials"
			}
		}
		switch mode {
		case "api_token":
			if !has("api_token") || has("access_token") {
				return ErrCredentialInvalid
			}
		case "oauth":
			if !has("access_token") || has("api_token") {
				return ErrCredentialInvalid
			}
		case "client_credentials":
			if !has("client_id") || !has("client_secret") || !has("subdomain") || has("api_token") {
				return ErrCredentialInvalid
			}
		default:
			return ErrCredentialInvalid
		}
		if region := credentialValue(credential, "region"); region != "" && region != "us" && region != "eu" {
			return ErrCredentialInvalid
		}
	default:
		return fmt.Errorf("%w: unsupported provider", ErrCredentialInvalid)
	}
	return nil
}
