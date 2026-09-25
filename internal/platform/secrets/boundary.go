package secrets

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

// RedactedMarker replaces every value a Boundary redacts.
const RedactedMarker = "[REDACTED]"

// keywordPasswordPattern matches a PostgreSQL keyword/value connection
// string's password parameter: password=VALUE or password = 'VALUE',
// the value either a bare run of non-space characters or single-quoted
// (libpq's own conninfo quoting: backslash-escaped quotes and
// backslashes inside the quotes).
var keywordPasswordPattern = regexp.MustCompile(`(?i)\bpassword\s*=\s*('(?:\\.|[^'\\])*'|[^'\s]+)`)

// keywordUserPattern matches every user (or username) parameter of a
// conninfo string (the drivers' keywords are lower case), the value quoted or bare exactly as for the password. The
// boundary keeps it from matching inside another parameter's name ("superuser="
// or "db_user="). Every occurrence is used, not the first: pgx keeps the last
// duplicate, so the effective login may be any of them.
var keywordUserPattern = regexp.MustCompile(`(?:^|\s)user(?:name)?\s*=\s*('(?:\\.|[^'\\])*'|[^'\s]+)`)

// CredentialComponents returns every substring of dsn that is, on its
// own, as sensitive as dsn itself: dsn's own bytes, and, separately, its
// password and login-name components -- extracted from a URL-form DSN
// (postgres://USER:PASSWORD@host/db) or a keyword-form one (host=...
// user=USER password=PASSWORD ...), whichever shape dsn has. Each is
// listed on its own, independent of dsn's literal text, because a
// downstream error can reformat, re-quote, or otherwise fail to echo dsn
// byte-for-byte while still carrying the password or login-name bytes
// unchanged (ClickHouse's authentication-failure text names the login
// name). A login name is redacted wherever it appears, so a very common
// one ("postgres", "default") also replaces the same word where it is not
// the login name (a database of that name): a redaction that cannot leak
// is chosen over a diagnostic that reads better.
// Returns nil for an empty dsn (a legal, no-op input -- -dry-run runs
// with no DSN at all).
func CredentialComponents(dsn string) []string {
	if dsn == "" {
		return nil
	}
	out := []string{dsn}
	if u, err := url.Parse(dsn); err == nil {
		if u.User != nil {
			if pw, ok := u.User.Password(); ok && pw != "" {
				out = append(out, pw)
			}
			if name := u.User.Username(); name != "" {
				out = append(out, name)
			}
		}
		// The drivers also take the login (clickhouse-go "username=", pgx
		// "user=") and the password from the query, where they override the
		// userinfo. The keys are the drivers' own, lower case: a differently
		// cased key is ignored by the driver, so its value is not a credential.
		// The values are the decoded ones the driver uses; the encoded spelling
		// is covered by the DSN's own bytes.
		for key, values := range u.Query() {
			if key == "user" || key == "username" || key == "password" {
				for _, value := range values {
					if value != "" {
						out = append(out, value)
					}
				}
			}
		}
	}
	for _, m := range keywordPasswordPattern.FindAllStringSubmatch(dsn, -1) {
		out = appendKeywordValue(out, m[1])
	}
	for _, m := range keywordUserPattern.FindAllStringSubmatch(dsn, -1) {
		out = appendKeywordValue(out, m[1])
	}
	return out
}

// RedactValues returns s with every occurrence of each non-empty value
// replaced by RedactedMarker.
func RedactValues(s string, values ...string) string {
	for _, v := range values {
		if v == "" {
			continue
		}
		s = strings.ReplaceAll(s, v, RedactedMarker)
	}
	return s
}

// Boundary is the LAST place a resolved DSN's bytes can still reach
// output. Applied once, at a binary's own top-level error print (and any
// other place a wrapped driver error might otherwise be printed or
// logged), it does not depend on every call site downstream remembering
// which of ITS errors can carry the value -- construct it once the DSN is
// resolved and apply it to whatever error eventually surfaces, no matter
// how many layers wrapped it or which one actually dialled.
type Boundary struct {
	values []string
}

// NewBoundary derives the substrings the returned Boundary redacts, from
// the DSN this run actually resolved.
func NewBoundary(dsn string) Boundary {
	return Boundary{values: CredentialComponents(dsn)}
}

// NewBoundaryWith is NewBoundary for a driver that resolves credentials from
// more than the DSN (a PostgreSQL client reads PGUSER, PGPASSWORD, a password
// file and service files): the caller passes the login and password the driver
// settled on, and they are redacted with the DSN's own components. A DSN alone
// cannot name a credential the DSN does not carry.
func NewBoundaryWith(dsn string, resolved ...string) Boundary {
	values := CredentialComponents(dsn)
	for _, value := range resolved {
		if value != "" {
			values = append(values, value)
		}
	}
	return Boundary{values: values}
}

// Redact returns err with every occurrence of the boundary's values
// replaced by RedactedMarker, or err unchanged if it is nil or contains
// none of them.
func (b Boundary) Redact(err error) error {
	if err == nil || len(b.values) == 0 {
		return err
	}
	msg := err.Error()
	redacted := RedactValues(msg, b.values...)
	if redacted == msg {
		return err
	}
	return errors.New(redacted)
}

// RedactText returns text with every occurrence of the boundary's values replaced
// by RedactedMarker: Redact for a message that is already a string.
func (b Boundary) RedactText(text string) string {
	if len(b.values) == 0 {
		return text
	}
	return RedactValues(text, b.values...)
}

// redactedCauseError is a stable sentinel plus a driver cause whose text has
// had every credential component of a DSN removed. It unwraps to the sentinel
// only: the raw cause is not kept, so no later %+v or errors.Unwrap can reach
// the unredacted text.
type redactedCauseError struct {
	sentinel error
	cause    string
}

func (e redactedCauseError) Error() string { return e.sentinel.Error() + ": " + e.cause }
func (e redactedCauseError) Unwrap() error { return e.sentinel }

// WithRedactedCause returns an error that errors.Is sentinel and whose text
// adds cause's text with every credential component of dsn redacted, so an
// operator can tell an authentication failure from a refused dial without the
// log ever holding the password or the DSN. A nil cause returns sentinel.
func WithRedactedCause(sentinel error, dsn string, cause error) error {
	return WithRedactedCauseAlso(sentinel, dsn, cause)
}

// WithRedactedCauseAlso is WithRedactedCause for a driver that resolves
// credentials from more than the DSN (a PostgreSQL client reads PGUSER,
// PGPASSWORD and service files): the caller passes the login and password the
// driver settled on, and they are redacted with the DSN's own components.
func WithRedactedCauseAlso(sentinel error, dsn string, cause error, resolved ...string) error {
	if cause == nil {
		return sentinel
	}
	values := append(CredentialComponents(dsn), resolved...)
	return redactedCauseError{sentinel: sentinel, cause: RedactValues(cause.Error(), values...)}
}

// appendKeywordValue adds a conninfo value, unquoted (libpq's quoting:
// backslash-escaped quotes and backslashes inside single quotes).
func appendKeywordValue(out []string, value string) []string {
	if len(value) >= 2 && strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		value = strings.NewReplacer(`\'`, `'`, `\\`, `\`).Replace(value[1 : len(value)-1])
	}
	if value == "" {
		return out
	}
	return append(out, value)
}
