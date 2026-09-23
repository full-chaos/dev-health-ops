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

// CredentialComponents returns every substring of dsn that is, on its
// own, as sensitive as dsn itself: dsn's own bytes, and, separately, its
// password component -- extracted from a URL-form DSN
// (postgres://user:PASSWORD@host/db) or a keyword-form one (host=...
// password=PASSWORD ...), whichever shape dsn has. The password is
// listed on its own, independent of dsn's literal text, because a
// downstream error can reformat, re-quote, or otherwise fail to echo dsn
// byte-for-byte while still carrying the password bytes unchanged.
// Returns nil for an empty dsn (a legal, no-op input -- -dry-run runs
// with no DSN at all).
func CredentialComponents(dsn string) []string {
	if dsn == "" {
		return nil
	}
	out := []string{dsn}
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		if pw, ok := u.User.Password(); ok && pw != "" {
			out = append(out, pw)
		}
	}
	if m := keywordPasswordPattern.FindStringSubmatch(dsn); m != nil {
		pw := m[1]
		if len(pw) >= 2 && strings.HasPrefix(pw, "'") && strings.HasSuffix(pw, "'") {
			pw = strings.NewReplacer(`\'`, `'`, `\\`, `\`).Replace(pw[1 : len(pw)-1])
		}
		if pw != "" {
			out = append(out, pw)
		}
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
	if cause == nil {
		return sentinel
	}
	return redactedCauseError{sentinel: sentinel, cause: RedactValues(cause.Error(), CredentialComponents(dsn)...)}
}
