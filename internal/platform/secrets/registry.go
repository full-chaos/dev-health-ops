package secrets

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// The process-wide registry of the secrets this process has resolved.
//
// A secret enters it where it is resolved (a setting loaded by Resolve, a DSN
// resolved from its components, the login and password a driver settles on, a
// decrypted integration credential), and the process logger consults it every
// time it formats a text (logging.RedactText), so a call site does not have to
// remember which of its errors can carry a store password: a secret resolved
// after the logger was installed is covered too.
//
// Passwords and tokens are redacted by VALUE wherever they appear. A value
// shorter than MinRegisteredLength is not: it would mangle ordinary text, and
// it is reported once, by the setting's NAME only. A login is a word an
// operator reads in an error, so it is redacted only in the shapes a driver or
// server echoes it (the userinfo of a URL, "LOGIN: Authentication failed",
// `user "LOGIN"`, user=LOGIN); the Boundary of a verb keeps redacting a login
// wherever it appears in the error that verb prints.

// MinRegisteredLength is the shortest secret redacted by value.
const MinRegisteredLength = 8

// maxRegistered bounds the registry (a long-running worker resolves many
// rotating tokens). Past it the oldest entry is dropped and the drop is
// reported once.
const maxRegistered = 10000

var registry = struct {
	sync.RWMutex
	secrets   []string
	known     map[string]struct{}
	logins    []loginRule
	knownLogs map[string]struct{}
	warned    map[string]struct{}
	dropped   bool
}{
	known:     map[string]struct{}{},
	knownLogs: map[string]struct{}{},
	warned:    map[string]struct{}{},
}

type loginRule struct {
	login   string
	pattern *regexp.Regexp
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
		return
	}
	registry.Lock()
	defer registry.Unlock()
	if _, ok := registry.known[value]; ok {
		return
	}
	if len(registry.secrets) >= maxRegistered {
		delete(registry.known, registry.secrets[0])
		registry.secrets = registry.secrets[1:]
		if !registry.dropped {
			registry.dropped = true
			go warn("secret_registry_full_oldest_dropped", "limit", maxRegistered)
		}
	}
	registry.known[value] = struct{}{}
	registry.secrets = append(registry.secrets, value)
	sort.SliceStable(registry.secrets, func(i, j int) bool { return len(registry.secrets[i]) > len(registry.secrets[j]) })
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
			"detail", "password for "+name+" is shorter than the minimum: not redactable by value")
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
	if len(registry.secrets) == 0 && len(registry.logins) == 0 {
		return text
	}
	for _, secret := range registry.secrets {
		if strings.Contains(text, secret) {
			text = strings.ReplaceAll(text, secret, RedactedMarker)
		}
	}
	for _, rule := range registry.logins {
		if strings.Contains(strings.ToLower(text), strings.ToLower(rule.login)) {
			text = rule.pattern.ReplaceAllStringFunc(text, func(match string) string {
				return strings.Replace(match, rule.login, RedactedMarker, 1)
			})
		}
	}
	return text
}

// ResetRegistered empties the registry. It is for tests.
func ResetRegistered() {
	registry.Lock()
	defer registry.Unlock()
	registry.secrets, registry.logins = nil, nil
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

// credentialFields are the keys of a decrypted integration credential whose
// values are secrets (or the identity a gateway echoes beside one).
var credentialFields = map[string]bool{
	"token": true, "api_token": true, "access_token": true, "refresh_token": true,
	"password": true, "secret": true, "client_secret": true, "api_key": true,
	"key": true, "private_key": true, "webhook_secret": true, "email": true,
}

// RegisterCredentialJSON registers the secret fields of a decrypted integration
// credential (a JSON object) by value, under "integration credential <field>".
// Text that is not a JSON object registers nothing.
func RegisterCredentialJSON(plain []byte) {
	var fields map[string]any
	if err := json.Unmarshal(plain, &fields); err != nil {
		return
	}
	for name, value := range fields {
		if text, ok := value.(string); ok && credentialFields[strings.ToLower(name)] {
			Register("integration credential "+name, text)
		}
	}
}
