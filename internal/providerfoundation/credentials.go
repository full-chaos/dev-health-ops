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
// JSON value where the resolver reads it as `str(value or "")`. A null never
// gets here (the decode drops None before anything else, as Python does); a falsy value (0, 0.0, false, an empty list or
// object) is present but empty; any other value is its str() -- the integer's
// digits, repr() of a float, "True", and repr() of a list or object as Python
// writes it.
func pythonSecretText(value pyjson.Value) (secrets.Value, bool) {
	if !pyjson.Truthy(value) {
		return secrets.NewValue(""), true
	}
	return secrets.NewValue(pyjson.Str(value)), true
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
		// A blank key is a field nobody reads, like any other: Python's builders
		// ignore it and so does this decode.
		//
		// A null is dropped BEFORE alias resolution (github_credentials_from_mapping
		// filters `if v is not None` first), so it never replaces an earlier
		// value stored under the same canonical name.
		if value == nil {
			continue
		}
		if record.Provider == "github" {
			if canonical, aliased := githubFieldAliases[key]; aliased {
				key = canonical
			}
		}
		// A key stored twice (an alias and its canonical spelling) keeps the
		// later one, whichever kind of value each held.
		delete(fields, key)
		delete(deferred, key)
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

// githubResolvedKeyConfigured is whether github_credentials_from_mapping ends
// up with a non-empty private key: the private_key entry when there is one,
// else the content of private_key_path. An error is a path the builder cannot
// read (it returns None, or raises on invalid UTF-8).
func githubResolvedKeyConfigured(credential Credential) (bool, error) {
	if value, present := credential.Secret("private_key"); present {
		return value.Configured(), nil
	}
	if path, ok := credential.Secret("private_key_path"); ok && path.Configured() {
		content, err := readGitHubAppPrivateKeyFile(path.Reveal())
		if err != nil {
			return false, err
		}
		return content.Configured(), nil
	}
	return false, nil
}

// ValidateCredentialShape keeps auth construction explicit. It accepts only
// the auth fields that the current Python resolver accepts for this provider.
func ValidateCredentialShape(credential Credential) error {
	has := func(name string) bool { value, ok := credential.Secret(name); return ok && value.Configured() }
	switch credential.Provider {
	case "github":
		token := has("token")
		// github_credentials_from_mapping resolves the private key first: the
		// private_key entry when there is one, else the CONTENT of
		// private_key_path (resolver.py:269-276), and an unreadable file makes
		// the builder return None. Both the App-auth triple and the
		// token-beside-an-App-field conflict are decided on that resolved key,
		// so an empty, missing, unreadable or non-UTF-8 file is no key.
		key, err := githubResolvedKeyConfigured(credential)
		if err != nil {
			return ErrCredentialInvalid
		}
		app := has("app_id") && key && has("installation_id")
		if token == app {
			return ErrCredentialInvalid
		}
		// CHAOS-6781: GitHubCredentials.__post_init__ raises when a token comes
		// with ANY App field, not only a complete triple.
		if token && (has("app_id") || has("installation_id") || key) {
			return ErrCredentialInvalid
		}
	case "gitlab":
		if !has("token") {
			return ErrCredentialInvalid
		}
	case "jira":
		// JiraCredentials requires api_token, email AND base_url.
		if !hasAny(credential, jiraAPITokenAliases) || !has("email") || jiraCredentialBaseURL(credential) == "" {
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
