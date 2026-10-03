// Package synclog is the logger of syncdispatchruntime (CHAOS-7933, D4270). It is the only code of the package family that imports
// log/slog. The *slog.Logger is an unexported field with no accessor; there is no alias of slog.Logger.
//
// Nothing that can carry the text of an error crosses this boundary:
//   - a message (Msg), a key (Key) and a fixed label (Label) are OPAQUE values: structs with an unexported field whose only
//     values are the package-level vars of vocab.go; no string converts to one outside this package;
//   - a string that is not a constant of the code enters only through a parser that validates a closed alphabet and fails
//     closed: ParseID accepts a UUID, ParseLabel a short lower-case token of [a-z0-9_.:]; anything else (a sentence, a URL, a
//     token with a hyphen or upper case) becomes the fixed marker "invalid";
//   - an error enters only through Failure(err): a class from a closed list, the Go type name and a validated SQLSTATE, never
//     its text.
package synclog

import (
	"context"
	"log/slog"
	"regexp"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// Msg, Key, Label and ID are opaque: their fields are unexported.
type (
	Msg   struct{ text string }
	Key   struct{ text string }
	Label struct{ text string }
	ID    struct{ text string }
	// Attr is one attribute; only the constructors below make a non-zero one.
	Attr struct{ attr slog.Attr }
)

// Logger is the package's logger. A nil *Logger logs through slog.Default().
type Logger struct{ inner *slog.Logger }

// New wraps a *slog.Logger (the process's logger, handed over by the wiring code).
func New(inner *slog.Logger) *Logger { return &Logger{inner: inner} }

// Default logs through slog.Default().
func Default() *Logger { return &Logger{} }

func (logger *Logger) log() *slog.Logger {
	if logger == nil || logger.inner == nil {
		return slog.Default()
	}
	return logger.inner
}

func plain(attrs []Attr) []any {
	out := make([]any, len(attrs))
	for index, attr := range attrs {
		out[index] = attr.attr
	}
	return out
}

func (logger *Logger) Debug(ctx context.Context, message Msg, attrs ...Attr) {
	logger.log().DebugContext(ctx, message.text, plain(attrs)...)
}

func (logger *Logger) Info(ctx context.Context, message Msg, attrs ...Attr) {
	logger.log().InfoContext(ctx, message.text, plain(attrs)...)
}

func (logger *Logger) Warn(ctx context.Context, message Msg, attrs ...Attr) {
	logger.log().WarnContext(ctx, message.text, plain(attrs)...)
}

func (logger *Logger) Error(ctx context.Context, message Msg, attrs ...Attr) {
	logger.log().ErrorContext(ctx, message.text, plain(attrs)...)
}

var (
	uuidPattern  = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	labelPattern = regexp.MustCompile(`^[a-z0-9_.:]{1,48}$`)
)

// ParseID validates an identifier: a UUID, or the fixed marker "invalid".
func ParseID(value string) ID {
	if uuidPattern.MatchString(value) {
		return ID{value}
	}
	return ID{"invalid"}
}

// ParseIDs parses a list of identifiers.
func ParseIDs(values []string) []ID {
	out := make([]ID, len(values))
	for index, value := range values {
		out[index] = ParseID(value)
	}
	return out
}

// ParseLabel validates a short token the code derived (a provider, a dataset key, a cost class, a category): lower-case letters,
// digits and "_.:" up to 48 bytes, or the fixed marker "invalid".
func ParseLabel(value string) Label {
	if labelPattern.MatchString(value) {
		return Label{value}
	}
	return Label{"invalid"}
}

func Run(id ID) Attr         { return Attr{slog.String("sync_run_id", id.text)} }
func Unit(id ID) Attr        { return Attr{slog.String("unit_id", id.text)} }
func Org(id ID) Attr         { return Attr{slog.String("org_id", id.text)} }
func Integration(id ID) Attr { return Attr{slog.String("integration_id", id.text)} }
func Source(id ID) Attr      { return Attr{slog.String("source_id", id.text)} }
func Dispatch(id ID) Attr    { return Attr{slog.String("dispatch_id", id.text)} }

// Provider is a provider name as a label.
func Provider(value Label) Attr { return Attr{slog.String("provider", value.text)} }

// Text is a labelled value.
func Text(key Key, value Label) Attr { return Attr{slog.String(key.text, value.text)} }

func Count[N ~int | ~int64](key Key, value N) Attr { return Attr{slog.Int64(key.text, int64(value))} }
func Flag(key Key, value bool) Attr                { return Attr{slog.Bool(key.text, value)} }
func Elapsed(key Key, value time.Duration) Attr    { return Attr{slog.Duration(key.text, value)} }
func Instant(key Key, value time.Time) Attr        { return Attr{slog.Time(key.text, value)} }

// IDs is a bounded list of identifiers.
func IDs(key Key, ids []ID) Attr {
	out := make([]string, len(ids))
	for index, id := range ids {
		out[index] = id.text
	}
	return Attr{slog.Any(key.text, out)}
}

// Group is a named group of attributes.
func Group(key Key, attrs ...Attr) Attr { return Attr{slog.Group(key.text, plain(attrs)...)} }

// Failure is an error as its class, its Go type name and, for PostgreSQL, its SQLSTATE: never its text. It is the only function of
// this package that takes an error.
func Failure(err error) Attr { return Attr{logging.ErrorAttr(err)} }
