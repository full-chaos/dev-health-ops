package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
)

// Error attributes for a log line (CHAOS-7933): a fixed class, the Go type name, and for a PostgreSQL error its SQLSTATE and
// the names of the constraint, table and column it names. err.Error() of a database, provider or transport error can hold a
// URL, an id, a constraint's key value, a response body or a credential-shaped string; passing it to slog as it is puts that
// text in the log backend. Everything here comes from the error's identity or from schema names, never from its message text:
// there is NO free-text detail on purpose. RedactText cannot make a message safe: it does not recognise every secret shape
// (a hyphenated `sk-...` marker passes it unchanged: measured when this file was written), and an allow-list of words keeps a letters-only
// password. A site that needs more says it in a fixed message it chose itself.

// The closed set of ErrorClass values.
const (
	ErrorClassCanceled = "canceled"
	ErrorClassDeadline = "deadline"
	ErrorClassPostgres = "postgres"
	ErrorClassDecode   = "decode"
	ErrorClassOther    = "other"
	// A transport class is TransportClass's: timeout, dns, refused, reset, tls, eof.
)

var sqlStatePattern = regexp.MustCompile(`^[0-9A-Z]{5}$`)

// sqlStater is what a PostgreSQL error (pgconn.PgError) carries.
type sqlStater interface{ SQLState() string }

// ErrorClass names the class of err from its type and identity, never from its text. A nil error is "none".
func ErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	chain := boundedChain(err)
	var state sqlStater
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	switch {
	case chainIs(chain, context.Canceled):
		return ErrorClassCanceled
	case chainIs(chain, context.DeadlineExceeded):
		return ErrorClassDeadline
	case chainAs(chain, &state):
		return ErrorClassPostgres
	case chainAs(chain, &syntaxErr), chainAs(chain, &typeErr):
		return ErrorClassDecode
	}
	if class := TransportClass(err); class != "protocol" {
		return class
	}
	return ErrorClassOther
}

// ErrorType is the Go type name of the innermost error of the chain (%T after unwrapping): a name from the program text, never
// a message. A nil error is "none".
func ErrorType(err error) string {
	if err == nil {
		return "none"
	}
	return fmt.Sprintf("%T", innermost(err))
}

// ErrorAttrs is the attributes of a failed operation for a log line: error_class, error_type and, for a PostgreSQL error,
// error_code (a valid SQLSTATE). The message, detail, hint, table, column and constraint of a PostgreSQL error have NO constructor:
// only the SQLSTATE is taken from it (CHAOS-7933, D4243).
func ErrorAttrs(err error) []slog.Attr {
	attrs := []slog.Attr{slog.String("error_class", ErrorClass(err)), slog.String("error_type", ErrorType(err))}
	if err == nil {
		return attrs
	}
	var state sqlStater
	if chainAs(boundedChain(err), &state) {
		if code := safeSQLState(state); sqlStatePattern.MatchString(code) {
			attrs = append(attrs, slog.String("error_code", code))
		}
	}
	return attrs
}

// ErrorAttr is ErrorAttrs as ONE attribute: a group named "error" holding class, type and the rest, so a call site that wrote
// slog.String("error", err.Error()) writes logging.ErrorAttr(err) in the same place and the JSON line carries
// "error":{"class":...,"type":...}.
func ErrorAttr(err error) slog.Attr {
	attrs := ErrorAttrs(err)
	args := make([]any, 0, len(attrs))
	for _, attr := range attrs {
		args = append(args, slog.Attr{Key: strings.TrimPrefix(attr.Key, "error_"), Value: attr.Value})
	}
	return slog.Group("error", args...)
}

// ErrorArgs is ErrorAttrs in the ...any form the leveled logger methods take.
func ErrorArgs(err error) []any {
	attrs := ErrorAttrs(err)
	args := make([]any, 0, len(attrs))
	for _, attr := range attrs {
		args = append(args, attr)
	}
	return args
}
