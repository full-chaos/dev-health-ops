package providerfoundation

import (
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// StoredGitHubCredential is what the worker's own decode (decodeCredential: the
// camelCase aliases applied, the later key of the document winning, a null
// dropped, a value that is not a string read the way Python's str() reads it)
// and its shape rule (ValidateCredentialShape) make of a decrypted GitHub
// credential payload, in the shape a caller outside the worker needs: the
// operator `dho sync` verb (CHAOS-9678). The worker and the verb read a stored
// row through this one decode, so a credential the web saves (a token and an `org`
// key, camelCase app keys) is usable by both.
type StoredGitHubCredential struct {
	Token, AppID, PrivateKey, InstallationID, BaseURL secrets.Value
	// Ignored names the payload keys the credential does not use, sorted: names
	// only, never values.
	Ignored []string
}

// Reasons of a StoredCredentialFailure.
const (
	StoredCredentialPayloadNotJSON       = "payload_not_json"
	StoredCredentialPayloadNotObject     = "payload_not_object"
	StoredCredentialPrivateKeyPathUnread = "private_key_path_unreadable"
	StoredCredentialShapeInvalid         = "shape_invalid"
)

// StoredCredentialFailure is a payload the worker's decode or shape rule
// refuses. It wraps ErrCredentialInvalid (errors.Is) and carries a fixed reason
// and, for a shape failure, whether a token and whether any app field were
// present: names and flags, never a value.
type StoredCredentialFailure struct {
	Reason           string
	HasToken, HasApp bool
}

func (failure *StoredCredentialFailure) Error() string {
	return "stored credential refused: " + failure.Reason
}

func (failure *StoredCredentialFailure) Unwrap() error { return ErrCredentialInvalid }

// githubUsedFields are the payload keys a GitHub credential reads
// (github_credentials_from_mapping: token, app_id, private_key, installation_id,
// base_url; private_key_path is read into private_key). Every other key of a
// stored payload is ignored.
var githubUsedFields = map[string]bool{
	"token": true, "app_id": true, "private_key": true, "private_key_path": true, "installation_id": true, "base_url": true,
}

// DecodeStoredGitHubCredential decodes a decrypted GitHub credential payload
// with the worker's decode and shape rule. A refusal is a *StoredCredentialFailure.
func DecodeStoredGitHubCredential(plaintext []byte) (StoredGitHubCredential, error) {
	decoded, err := pyjson.DecodeString(string(plaintext))
	if err != nil {
		return StoredGitHubCredential{}, &StoredCredentialFailure{Reason: StoredCredentialPayloadNotJSON}
	}
	if _, isObject := decoded.(*pyjson.Object); !isObject {
		return StoredGitHubCredential{}, &StoredCredentialFailure{Reason: StoredCredentialPayloadNotObject}
	}
	credential, err := decodeCredential(EncryptedCredential{Provider: "github"}, plaintext)
	if err != nil {
		return StoredGitHubCredential{}, &StoredCredentialFailure{Reason: StoredCredentialPayloadNotObject}
	}
	stored := StoredGitHubCredential{Ignored: ignoredFieldNames(credential, githubUsedFields)}
	value := func(name string) secrets.Value {
		field, _ := credential.Secret(name)
		return field
	}
	// The private key: the private_key entry when there is one, else the content
	// of private_key_path (github_credentials_from_mapping, resolver.py:269-276).
	if key, present := credential.Secret("private_key"); present {
		stored.PrivateKey = key
	} else if path, ok := credential.Secret("private_key_path"); ok && path.Configured() {
		content, readErr := readGitHubAppPrivateKeyFile(path.Reveal())
		if readErr != nil {
			return StoredGitHubCredential{}, &StoredCredentialFailure{Reason: StoredCredentialPrivateKeyPathUnread}
		}
		stored.PrivateKey = content
	}
	stored.Token, stored.AppID, stored.InstallationID, stored.BaseURL = value("token"), value("app_id"), value("installation_id"), value("base_url")
	if shapeErr := ValidateCredentialShape(credential); shapeErr != nil {
		hasApp := stored.AppID.Configured() || stored.PrivateKey.Configured() || stored.InstallationID.Configured()
		return StoredGitHubCredential{}, &StoredCredentialFailure{Reason: StoredCredentialShapeInvalid, HasToken: stored.Token.Configured(), HasApp: hasApp}
	}
	return stored, nil
}

// ignoredFieldNames is the sorted names of the credential's fields that are not
// in used. A blank name is a field nobody reads and is not reported.
func ignoredFieldNames(credential Credential, used map[string]bool) []string {
	var ignored []string
	seen := map[string]bool{}
	collect := func(name string) {
		if strings.TrimSpace(name) == "" || used[name] || seen[name] {
			return
		}
		seen[name] = true
		ignored = append(ignored, name)
	}
	for name := range credential.fields {
		collect(name)
	}
	for name := range credential.deferred {
		collect(name)
	}
	sort.Strings(ignored)
	return ignored
}
