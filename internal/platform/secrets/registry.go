package secrets

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// The process-wide registry of the secrets this process has resolved.
//
// This is defense in depth for the process logger. It redacts REGISTERED values,
// in the encodings listed below, from every text the process logger formats
// (logging.RedactText consults it at log time, so a secret resolved after the
// logger was installed is covered). A secret that never passed this package, or
// that appears in an encoding not listed here, is NOT covered; the verbs'
// Boundary still redacts what a verb prints with fmt.
//
// SOURCES that register (the closed list the guards enforce):
//   - the environment: Resolve* helpers, GetenvSecret/GetenvDSN/GetenvLogin/
//     GetenvNamed, and ProcessLookup, the lookup every binary hands to its
//     configuration and verbs; a direct os.Getenv/os.LookupEnv of a secret-named
//     variable, os.LookupEnv or os.Getenv handed on as a function value, and
//     os.Environ()/os.ExpandEnv()/os.Expand()/syscall.Getenv()/syscall.Environ()
//     are refused by TestNoSecretIsReadFromTheEnvironmentWithoutRegistration
//     unless classified there;
//   - DSN resolution: config.ResolveDSN and RegisterDSN (password by value, login
//     by shape);
//   - decryption: FernetDecryptor.Decrypt registers every plaintext it returns
//     (RegisterDecrypted: the secret-named fields of a JSON object, or the whole
//     text);
//   - files: Resolve reads key_FILE; the value it returns is registered by the
//     helper that asked for it (ResolveSecret/ResolveDSNSetting).
//
// ENCODINGS covered, for every registered value of at least MinRegisteredLength
// that is not a common word: raw; percent-encoded (query and path forms);
// percent-decoded (RedactText redacts again after decoding); JSON-escaped (with
// and without HTML escaping) and JSON-in-JSON; double-quoted with Go escapes;
// base64 (standard and URL alphabets, padded and not); hex (lower and upper).
// NOT covered: HTML entities, a value split across fields or lines, reversed or
// otherwise transformed text, and base64 of a composite such as "user:password"
// (a Basic header is redacted by its header name by logging.RedactText).
//
// A value shorter than MinRegisteredLength, or a common word, would mangle
// ordinary text if redacted wherever it appears: it is redacted only in
// credential SHAPES (password=X, pwd=X, passwd=X, pass=X, a "password" JSON
// field, and ://user:X@), never as a bare word, and it is reported once, by the
// setting's NAME only. A login is redacted only in the shapes a driver or server
// echoes it (the userinfo of a URL, "LOGIN: Authentication failed",
// `user "LOGIN"`, user=LOGIN).

// MinRegisteredLength is the shortest secret redacted by value.
const MinRegisteredLength = 8

// warnRegisteredAt is the size at which the registry reports, once, that it
// holds many entries. It never drops one: a dropped secret is a leak, and the
// entries a worker adds (a few per integration) are small.
const warnRegisteredAt = 10000

var registry = struct {
	sync.RWMutex
	secrets   []string
	known     map[string]struct{}
	logins    []loginRule
	shaped    []shapeRule
	knownLogs map[string]struct{}
	warned    map[string]struct{}
	dropped   bool
}{
	known:     map[string]struct{}{},
	knownLogs: map[string]struct{}{},
	warned:    map[string]struct{}{},
}

// shapeRule redacts one short value or common word, only in credential shapes.
type shapeRule struct {
	value    string
	patterns []*regexp.Regexp
}

type loginRule struct {
	login   string
	pattern *regexp.Regexp
	name    *regexp.Regexp
}

// Register puts a secret into the registry. name is the setting's name (never
// its value). A value shorter than MinRegisteredLength is not registered and is
// reported once.
func Register(name, value string) {
	if value == "" {
		return
	}
	if len(value) < MinRegisteredLength {
		warnShort(name)
		registerShaped(value)
		return
	}
	if commonWords[strings.ToLower(value)] {
		warnCommonWord(name)
		registerShaped(value)
		return
	}
	registry.Lock()
	defer registry.Unlock()
	for _, form := range encodedForms(value) {
		insertSecretLocked(form)
	}
	if len(registry.secrets) >= warnRegisteredAt && !registry.dropped {
		registry.dropped = true
		go warn("secret_registry_large", "entries", warnRegisteredAt)
	}
}

func insertSecretLocked(form string) {
	if _, ok := registry.known[form]; ok {
		return
	}
	registry.known[form] = struct{}{}
	// Longest first, so a secret that contains another is replaced whole.
	at := sort.Search(len(registry.secrets), func(i int) bool { return len(registry.secrets[i]) < len(form) })
	registry.secrets = append(registry.secrets, "")
	copy(registry.secrets[at+1:], registry.secrets[at:])
	registry.secrets[at] = form
}

// encodedForms is value and the forms an echo of it takes in a log line.
func encodedForms(value string) []string {
	forms := []string{value, url.QueryEscape(value), url.PathEscape(value)}
	for _, escapeHTML := range []bool{false, true} {
		inner := jsonInner(value, escapeHTML)
		forms = append(forms, inner, jsonInner(inner, escapeHTML))
	}
	if quoted := strconv.Quote(value); len(quoted) >= 2 {
		forms = append(forms, quoted[1:len(quoted)-1])
	}
	raw := []byte(value)
	forms = append(forms,
		base64.StdEncoding.EncodeToString(raw), base64.RawStdEncoding.EncodeToString(raw),
		base64.URLEncoding.EncodeToString(raw), base64.RawURLEncoding.EncodeToString(raw),
		hex.EncodeToString(raw), strings.ToUpper(hex.EncodeToString(raw)))
	kept := forms[:0]
	seen := map[string]bool{}
	for _, form := range forms {
		if len(form) >= MinRegisteredLength && !seen[form] {
			seen[form] = true
			kept = append(kept, form)
		}
	}
	return kept
}

// jsonInner is value as it appears inside a JSON string (without the quotes).
func jsonInner(value string, escapeHTML bool) string {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(escapeHTML)
	if err := encoder.Encode(value); err != nil {
		return value
	}
	encoded := strings.TrimSuffix(buffer.String(), "\n")
	if len(encoded) < 2 {
		return value
	}
	return encoded[1 : len(encoded)-1]
}

// registerShaped registers a short value or a common word to be redacted only in
// credential shapes.
func registerShaped(value string) {
	quoted := regexp.QuoteMeta(value)
	rule := shapeRule{value: value, patterns: []*regexp.Regexp{
		regexp.MustCompile(`(?i)(\b(?:password|passwd|pwd|pass)\b["']?\s*[=:]\s*["']?)` + quoted + `(["'&\s,;)}]|$)`),
		regexp.MustCompile(`(://[^:/@\s"]*:)` + quoted + `(@)`),
	}}
	registry.Lock()
	defer registry.Unlock()
	for _, existing := range registry.shaped {
		if existing.value == value {
			return
		}
	}
	registry.shaped = append(registry.shaped, rule)
}

var (
	warnerMu sync.RWMutex
	warner   func(msg string, args ...any)
)

// SetWarner sets where the registry reports (by setting name only, never a
// value) and returns the previous warner. logging.InstallDefault sets the
// process logger here; until then, a warning is one line on stderr, so a
// binary that never installs a logger still reports a secret it cannot
// redact.
func SetWarner(next func(msg string, args ...any)) (previous func(msg string, args ...any)) {
	warnerMu.Lock()
	defer warnerMu.Unlock()
	previous, warner = warner, next
	return previous
}

func warn(msg string, args ...any) {
	warnerMu.RLock()
	sink := warner
	warnerMu.RUnlock()
	if sink != nil {
		sink(msg, args...)
		return
	}
	var line strings.Builder
	line.WriteString("level=WARN msg=" + msg)
	for index := 0; index+1 < len(args); index += 2 {
		line.WriteString(fmt.Sprintf(" %v=%q", args[index], fmt.Sprint(args[index+1])))
	}
	fmt.Fprintln(os.Stderr, line.String())
}

// commonWords are values that are ordinary words of a deployment (an environment
// name, a default host or role), not secrets: redacting them by value would erase
// them from every log line that holds the word as plain text. A value on this
// list is redacted only in credential shapes, never as a bare word, and is
// reported once, by the setting's name only.
var commonWords = map[string]bool{
	"development": true, "production": true, "staging": true, "localhost": true, "default": true,
	"postgres": true, "postgresql": true, "clickhouse": true, "password": true, "internal": true,
	"database": true, "disabled": true, "enabled": true, "standard": true, "external": true,
}

func warnCommonWord(name string) {
	if name == "" {
		return
	}
	registry.Lock()
	_, seen := registry.warned["common:"+name]
	registry.warned["common:"+name] = struct{}{}
	registry.Unlock()
	if !seen {
		warn("secret_is_a_common_word_redacted_only_in_credential_shapes", "setting", name)
	}
}

func warnShort(name string) {
	if name == "" {
		return
	}
	registry.Lock()
	_, seen := registry.warned[name]
	registry.warned[name] = struct{}{}
	registry.Unlock()
	if !seen {
		warn("secret_shorter_than_minimum_not_redactable_by_value",
			"setting", name, "minimum_length", MinRegisteredLength,
			"detail", "password for "+name+" is shorter than the minimum: redacted only in credential shapes (password=X, ://user:X@), not as a bare word")
	}
}

// RegisterLogin puts a login name into the registry: it is redacted only in the
// shapes a driver or server echoes it.
func RegisterLogin(login string) {
	if login == "" {
		return
	}
	registry.Lock()
	defer registry.Unlock()
	if _, ok := registry.knownLogs[login]; ok {
		return
	}
	registry.knownLogs[login] = struct{}{}
	quoted := regexp.QuoteMeta(login)
	registry.logins = append(registry.logins, loginRule{
		login: login,
		name:  regexp.MustCompile(`(?i)` + quoted),
		pattern: regexp.MustCompile(`(?i)(//|\buser(?:name)?(?:\s*[=:]\s*|\s+)["']?|\blogin\s+["']?)` + quoted + `(?:["'\s:@,;)]|$)` +
			`|\b` + quoted + `(?::\s(?:Authentication failed|Not enough privileges|Access denied))`),
	})
}

// RegisterDSN puts the credential components of a DSN into the registry: its
// passwords by value (the userinfo password, a query password, a keyword
// password) and its logins by shape. name is the setting's name.
func RegisterDSN(name, dsn string) {
	if dsn == "" {
		return
	}
	var passwords, logins []string
	if u, err := url.Parse(dsn); err == nil {
		if u.User != nil {
			if pw, ok := u.User.Password(); ok {
				passwords = append(passwords, pw)
			}
			logins = append(logins, u.User.Username())
		}
		for key, values := range u.Query() {
			switch key {
			case "password":
				passwords = append(passwords, values...)
			case "user", "username":
				logins = append(logins, values...)
			}
		}
	}
	for _, m := range keywordPasswordPattern.FindAllStringSubmatch(dsn, -1) {
		passwords = appendKeywordValue(passwords, m[1])
	}
	for _, m := range keywordUserPattern.FindAllStringSubmatch(dsn, -1) {
		logins = appendKeywordValue(logins, m[1])
	}
	for _, password := range passwords {
		Register(name, password)
	}
	for _, login := range logins {
		RegisterLogin(login)
	}
}

// NewSecret wraps a sensitive value like NewValue and registers it by value
// under the setting's name.
func NewSecret(name, value string) Value {
	Register(name, value)
	return NewValue(value)
}

// RedactRegistered replaces every registered secret in text by RedactedMarker,
// and every registered login in an echo shape.
func RedactRegistered(text string) string {
	registry.RLock()
	defer registry.RUnlock()
	if len(registry.secrets) == 0 && len(registry.logins) == 0 && len(registry.shaped) == 0 {
		return text
	}
	for _, secret := range registry.secrets {
		if strings.Contains(text, secret) {
			text = strings.ReplaceAll(text, secret, RedactedMarker)
		}
	}
	for _, rule := range registry.shaped {
		if strings.Contains(text, rule.value) {
			for _, pattern := range rule.patterns {
				text = pattern.ReplaceAllString(text, "${1}"+RedactedMarker+"${2}")
			}
		}
	}
	for _, rule := range registry.logins {
		if strings.Contains(strings.ToLower(text), strings.ToLower(rule.login)) {
			text = rule.pattern.ReplaceAllStringFunc(text, func(match string) string {
				// A server may change the case of the login it echoes.
				return rule.name.ReplaceAllLiteralString(match, RedactedMarker)
			})
		}
	}
	return text
}

// ResetRegistered empties the registry. It is for tests.
func ResetRegistered() {
	registry.Lock()
	defer registry.Unlock()
	registry.secrets, registry.logins, registry.shaped = nil, nil, nil
	registry.known, registry.knownLogs = map[string]struct{}{}, map[string]struct{}{}
	registry.warned, registry.dropped = map[string]struct{}{}, false
}

// ResolveSecret is Resolve for a setting that holds a secret by itself (a key,
// a token, a password): the resolved value enters the registry under the
// setting's name.
func ResolveSecret(key string, lookup LookupEnv) (Value, bool, error) {
	value, set, err := Resolve(key, lookup)
	if err == nil && set {
		Register(key, value.Reveal())
	}
	return value, set, err
}

// ResolveDSNSetting is Resolve for a setting that holds a DSN or URI: the
// resolved URI's password enters the registry by value and its login by shape.
func ResolveDSNSetting(key string, lookup LookupEnv) (Value, bool, error) {
	value, set, err := Resolve(key, lookup)
	if err == nil && set {
		RegisterDSN(key, value.Reveal())
	}
	return value, set, err
}

// secretFieldParts are the parts of a normalised field name (lower case, no "_" or
// "-", so api_token, apiToken and API-TOKEN are one) that say a decrypted
// credential field holds a secret, or the identity a gateway echoes beside one.
// A field is registered when its name CONTAINS one: private_token, clientSecret and
// webhook_signing_key are all covered without being listed.
var secretFieldParts = []string{
	"token", "secret", "key", "password", "passwd", "credential", "bearer", "email", "authorization",
}

func isSecretField(name string) bool {
	normalized := normalizedField(name)
	for _, part := range secretFieldParts {
		if strings.Contains(normalized, part) {
			return true
		}
	}
	return false
}

func normalizedField(name string) string {
	return strings.NewReplacer("_", "", "-", "").Replace(strings.ToLower(name))
}

// RegisterCredentialJSON registers the secret fields of a decrypted integration
// credential (a JSON object, nested objects included) by value, under
// "integration credential <field>". It reports whether the text was a JSON object.
func RegisterCredentialJSON(plain []byte) bool {
	var fields map[string]any
	if err := json.Unmarshal(plain, &fields); err != nil {
		return false
	}
	registerCredentialFields(fields)
	return true
}

func registerCredentialFields(fields map[string]any) {
	for name, value := range fields {
		switch typed := value.(type) {
		case string:
			if isSecretField(name) {
				Register("integration credential "+name, typed)
			}
		case map[string]any:
			registerCredentialFields(typed)
		}
	}
}

// RegisterDecrypted registers what a decryptor returned: the secret fields of a
// JSON object, or the whole text of anything else (a bare client secret or key).
func RegisterDecrypted(plain []byte) {
	if RegisterCredentialJSON(plain) {
		return
	}
	Register("decrypted value", strings.TrimSpace(string(plain)))
}

// GetenvSecret is os.Getenv for a setting that holds a secret read directly from
// the environment: a non-empty value enters the registry under the setting's name.
func GetenvSecret(name string) string {
	value := os.Getenv(name)
	Register(name, value)
	return value
}

// LookupSecretEnv is os.LookupEnv for a secret read directly from the
// environment, registering a non-empty value under the setting's name.
func LookupSecretEnv(name string) (string, bool) {
	value, ok := os.LookupEnv(name)
	Register(name, value)
	return value, ok
}

// GetenvLogin is os.Getenv for a login name read directly from the environment: it
// is redacted only in the shapes a server echoes it.
func GetenvLogin(name string) string {
	value := os.Getenv(name)
	RegisterLogin(strings.TrimSpace(value))
	return value
}

// GetenvDSN is os.Getenv for a DSN or URI read directly from the environment:
// its password enters the registry by value and its login by shape.
func GetenvDSN(name string) string {
	value := os.Getenv(name)
	RegisterDSN(name, value)
	return value
}

var (
	secretNamePattern = regexp.MustCompile(`(?i)(TOKEN|PASSWORD|PASSWD|SECRET|PRIVATE_KEY|API_KEY|BEARER|CREDENTIAL|PASSPHRASE|_KEY$|_PASS$)`)
	loginNamePattern  = regexp.MustCompile(`(?i)(_USER|_USERNAME|_LOGIN|^USER|^USERNAME|^LOGIN)$`)
	dsnNamePattern    = regexp.MustCompile(`(?i)(_URI|_DSN|^DSN)$`)
)

// LooksSecret reports whether an environment variable's name says it holds a
// secret (a token, password, key) rather than a plain setting.
func LooksSecret(name string) bool { return secretNamePattern.MatchString(name) }

// GetenvNamed is os.Getenv for a helper that reads a variable chosen by name: the
// value is registered when the name says it is a secret (by value) or a DSN
// (password by value, login by shape), and left alone otherwise (a model or a
// base URL is not redacted).
func GetenvNamed(name string) string {
	switch {
	case dsnNamePattern.MatchString(name):
		return GetenvDSN(name)
	case LooksSecret(name):
		return GetenvSecret(name)
	case loginNamePattern.MatchString(name):
		return GetenvLogin(name)
	}
	return os.Getenv(name)
}

// ProcessLookup is os.LookupEnv for the lookup a binary hands to its
// configuration and its verbs: a value whose variable name says it holds a secret
// (a token, password, key) or a DSN enters the registry as it is read, so
// whichever reader asks for it the process logger redacts it.
func ProcessLookup(name string) (string, bool) {
	value, ok := os.LookupEnv(name)
	if ok && value != "" {
		switch {
		case dsnNamePattern.MatchString(name):
			RegisterDSN(name, value)
		case LooksSecret(name):
			Register(name, value)
		case loginNamePattern.MatchString(name):
			RegisterLogin(strings.TrimSpace(value))
		}
	}
	return value, ok
}

// ProcessGetenv is os.Getenv with ProcessLookup's registration, for a reader that
// takes a Getenv function.
func ProcessGetenv(name string) string {
	value, _ := ProcessLookup(name)
	return value
}
