package secrets

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// RedactedMarker replaces every value a Boundary redacts.
const RedactedMarker = "[REDACTED]"

// UnparsableMarker is all an unparsable-DSN Boundary lets through.
const UnparsableMarker = "[redacted: connection string cannot be parsed]"

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
	decoded, literal := credentialSplit(dsn)
	return append(append([]string{dsn}, decoded...), literal...)
}

// credentialSplit separates the credential values of dsn the driver itself
// decodes (userinfo, query, and a keyword-form DSN's unquoted values) from the
// literal text fragments the keyword patterns cut out of a URL-form DSN: in a
// URL, "password=a%40" in the query is the escaped spelling of "a@", not a
// 4-character credential of its own.
func credentialSplit(dsn string) (decoded, literal []string) {
	urlForm := false
	if u, err := url.Parse(dsn); err == nil {
		urlForm = u.Scheme != ""
		if u.User != nil {
			if pw, ok := u.User.Password(); ok && pw != "" {
				decoded = append(decoded, pw)
			}
			if name := u.User.Username(); name != "" {
				decoded = append(decoded, name)
			}
		}
		// The drivers also take the login (clickhouse-go "username=", pgx
		// "user=") and the password from the query, where they override the
		// userinfo. The keys are the drivers' own, lower case: a differently
		// cased key is ignored by the driver, so its value is not a credential.
		// The values are the decoded ones the driver uses. The DSN's own bytes
		// cover only the spelling the DSN had; a server that re-serializes the
		// credential spells it again, which boundaryValues covers.
		for key, values := range u.Query() {
			if key == "user" || key == "username" || key == "password" {
				for _, value := range values {
					if value != "" {
						decoded = append(decoded, value)
					}
				}
			}
		}
	}
	var keyword []string
	for _, m := range keywordPasswordPattern.FindAllStringSubmatch(dsn, -1) {
		keyword = appendKeywordValue(keyword, m[1])
	}
	for _, m := range keywordUserPattern.FindAllStringSubmatch(dsn, -1) {
		keyword = appendKeywordValue(keyword, m[1])
	}
	if urlForm {
		return decoded, keyword
	}
	return append(decoded, keyword...), nil
}

// minDerivedLen is the shortest credential value, in characters, that also gets
// its escaped spellings redacted. A 1-2 character value ("a@") would turn its
// escaped form ("a%40") into a pattern that matches ordinary text; the raw value
// is redacted at any length, as before.
const minDerivedLen = 3

// credentialSpellings returns value and the other ways an error text can spell
// it. plain are matched byte for byte: the value, strconv.Quote and JSON string
// escapes (HTML escaping on and off, of the value and of its quoted form).
// escaped are the URL query, path and userinfo escapes, matched with the hex
// digits of every %XX read in either case. A spelling equal to one already
// listed is dropped. A value shorter than minDerivedLen characters has only
// itself.
func credentialSpellings(value string) (plain, escaped []string) {
	if value == "" {
		return nil, nil
	}
	plain = []string{value}
	if utf8.RuneCountInString(value) < minDerivedLen {
		return plain, nil
	}
	quoted := strconv.Quote(value)
	quoted = quoted[1 : len(quoted)-1]
	userinfo := (&url.URL{User: url.UserPassword("", value)}).String()
	userinfo = strings.TrimSuffix(strings.TrimPrefix(userinfo, "//:"), "@")
	add := func(list *[]string, spelling string) {
		if spelling != "" && !hasSpelling(plain, spelling) && !hasSpelling(escaped, spelling) {
			*list = append(*list, spelling)
		}
	}
	for _, spelling := range []string{
		quoted,
		jsonInner(value, true),
		jsonInner(value, false),
		jsonInner(quoted, true),
		jsonInner(quoted, false),
	} {
		add(&plain, spelling)
	}
	for _, spelling := range []string{url.QueryEscape(value), url.PathEscape(value), userinfo} {
		add(&escaped, spelling)
	}
	return plain, escaped
}

func hasSpelling(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// boundaryValues is every string a Boundary redacts: the DSN's own bytes and
// the literal keyword fragments of a URL DSN as they are, and each decoded
// credential component and resolved value in all its spellings.
func boundaryValues(dsn string, resolved ...string) (plain, escaped []string) {
	var decoded, literal []string
	if dsn != "" {
		plain = append(plain, dsn)
		decoded, literal = credentialSplit(dsn)
	}
	plain = append(plain, literal...)
	for _, value := range append(decoded, resolved...) {
		p, e := credentialSpellings(value)
		plain = append(plain, p...)
		escaped = append(escaped, e...)
	}
	return plain, escaped
}

// RedactValues returns s with every occurrence of each non-empty value
// replaced by RedactedMarker.
func RedactValues(s string, values ...string) string {
	return redactSpans(s, values, nil)
}

// redactSpans replaces, by RedactedMarker, the union of every match of every
// value, all found in the original s: a match that overlaps another (two values
// that share bytes) becomes one marker, so no replacement can hide part of a
// value and leave the rest. plain values match byte for byte; escaped values
// match s with the hex digits of each %XX read as upper case.
func redactSpans(s string, plain, escaped []string) string {
	spans := matchSpans(nil, s, plain)
	if len(escaped) > 0 {
		spans = matchSpans(spans, foldPercentHex(s), escaped)
	}
	if len(spans) == 0 {
		return s
	}
	sort.Slice(spans, func(i, j int) bool {
		if spans[i][0] != spans[j][0] {
			return spans[i][0] < spans[j][0]
		}
		return spans[i][1] > spans[j][1]
	})
	var out strings.Builder
	pos, start, end := 0, spans[0][0], spans[0][1]
	flush := func() {
		out.WriteString(s[pos:start])
		out.WriteString(RedactedMarker)
		pos = end
	}
	for _, span := range spans[1:] {
		if span[0] < end {
			if span[1] > end {
				end = span[1]
			}
			continue
		}
		flush()
		start, end = span[0], span[1]
	}
	flush()
	out.WriteString(s[pos:])
	return out.String()
}

func matchSpans(spans [][2]int, s string, values []string) [][2]int {
	for _, value := range values {
		if value == "" {
			continue
		}
		for from := 0; from < len(s); {
			at := strings.Index(s[from:], value)
			if at < 0 {
				break
			}
			spans = append(spans, [2]int{from + at, from + at + len(value)})
			from += at + 1
		}
	}
	return spans
}

func foldPercentHex(s string) string {
	b := []byte(s)
	isHex := func(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }
	for i := 0; i+2 < len(b); i++ {
		if b[i] == '%' && isHex(b[i+1]) && isHex(b[i+2]) {
			for _, j := range []int{i + 1, i + 2} {
				if b[j] >= 'a' && b[j] <= 'f' {
					b[j] -= 'a' - 'A'
				}
			}
		}
	}
	return string(b)
}

// Boundary is the LAST place a resolved DSN's bytes can still reach
// output. Applied once, at a binary's own top-level error print (and any
// other place a wrapped driver error might otherwise be printed or
// logged), it does not depend on every call site downstream remembering
// which of ITS errors can carry the value -- construct it once the DSN is
// resolved and apply it to whatever error eventually surfaces, no matter
// how many layers wrapped it or which one actually dialled.
type Boundary struct {
	values     []string
	escaped    []string
	unparsable bool
}

// NewUnparsableBoundary is the Boundary of a DSN the driver cannot parse: the
// driver's parse error echoes the DSN with only its password masked, and the
// login cannot be found in a string that does not parse, so this Boundary fails
// closed. Redact returns the fixed UnparsableMarker (no driver text, no DSN
// fragment); a context cancellation or deadline keeps its identity. RedactText
// keeps the DSN-only redaction: it filters whole log streams, which must stay
// readable.
func NewUnparsableBoundary(dsn string) Boundary {
	plain, escaped := boundaryValues(dsn)
	return Boundary{values: plain, escaped: escaped, unparsable: true}
}

// NewBoundary derives the substrings the returned Boundary redacts, from
// the DSN this run actually resolved.
func NewBoundary(dsn string) Boundary {
	plain, escaped := boundaryValues(dsn)
	return Boundary{values: plain, escaped: escaped}
}

// NewBoundaryWith is NewBoundary for a driver that resolves credentials from
// more than the DSN (a PostgreSQL client reads PGUSER, PGPASSWORD, a password
// file and service files): the caller passes the login and password the driver
// settled on, and they are redacted with the DSN's own components. A DSN alone
// cannot name a credential the DSN does not carry.
func NewBoundaryWith(dsn string, resolved ...string) Boundary {
	plain, escaped := boundaryValues(dsn, resolved...)
	return Boundary{values: plain, escaped: escaped}
}

// Redact returns err with every occurrence of the boundary's values
// replaced by RedactedMarker, or err unchanged if it is nil or contains
// none of them.
func (b Boundary) Redact(err error) error {
	if b.unparsable && err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			return context.Canceled
		case errors.Is(err, context.DeadlineExceeded):
			return context.DeadlineExceeded
		}
		return errors.New(UnparsableMarker)
	}
	if err == nil || len(b.values)+len(b.escaped) == 0 {
		return err
	}
	msg := err.Error()
	redacted := redactSpans(msg, b.values, b.escaped)
	if redacted == msg {
		return err
	}
	return errors.New(redacted)
}

// RedactText returns text with every occurrence of the boundary's values replaced
// by RedactedMarker: Redact for a message that is already a string.
func (b Boundary) RedactText(text string) string {
	return redactSpans(text, b.values, b.escaped)
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
	plain, escaped := boundaryValues(dsn, resolved...)
	return redactedCauseError{sentinel: sentinel, cause: redactSpans(cause.Error(), plain, escaped)}
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
