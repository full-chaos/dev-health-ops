package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"reflect"
	"slices"
)

// WithValueRedaction wraps a handler so that every text it is given passes
// through redact first: the message, every string attribute, every error and
// every fmt.Stringer attribute, at any depth of a group, including the
// attributes bound by With. The value of a protected key (the leaf key or an
// enclosing group name) is hidden whole, as NewJSON's ReplaceAttr hides it, so
// the rule holds over any inner handler, a bare TextHandler too. It is for a
// run that knows values no pattern can find (a database login or password it
// was handed): the process logger's own redactor (NewJSON) recognises
// credential shapes, not a login name.
//
// slog's built-in default handler is replaced by a text handler on the
// standard log package's current writer. The built-in handler writes through
// the log package, and slog.SetDefault points the log package at the new
// default: a default that kept it would take the log package's mutex again on
// its first record and block forever.
func WithValueRedaction(inner slog.Handler, redact func(string) string) slog.Handler {
	if isSlogBuiltinDefault(inner) {
		inner = slog.NewTextHandler(log.Writer(), nil)
	}
	return valueRedactor{inner: keyScrubber{inner: inner}, redact: redact}
}

// isSlogBuiltinDefault reports whether handler is slog's own default handler
// (log/slog.defaultHandler, unexported), the one a process has before any
// slog.SetDefault.
func isSlogBuiltinDefault(handler slog.Handler) bool {
	kind := reflect.TypeOf(handler)
	return kind != nil && kind.Kind() == reflect.Pointer &&
		kind.Elem().PkgPath() == "log/slog" && kind.Elem().Name() == "defaultHandler"
}

type valueRedactor struct {
	inner  slog.Handler
	redact func(string) string
	groups []string
}

func (h valueRedactor) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h valueRedactor) Handle(ctx context.Context, record slog.Record) error {
	redacted := slog.NewRecord(record.Time, record.Level, h.redact(record.Message), record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		redacted.AddAttrs(h.attr(h.groups, attr))
		return true
	})
	return h.inner.Handle(ctx, redacted)
}

func (h valueRedactor) WithAttrs(attrs []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attrs))
	for index, attr := range attrs {
		redacted[index] = h.attr(h.groups, attr)
	}
	return valueRedactor{inner: h.inner.WithAttrs(redacted), redact: h.redact, groups: h.groups}
}

func (h valueRedactor) WithGroup(name string) slog.Handler {
	return valueRedactor{inner: h.inner.WithGroup(name), redact: h.redact, groups: append(slices.Clip(h.groups), name)}
}

func (h valueRedactor) attr(groups []string, attr slog.Attr) slog.Attr {
	value := attr.Value.Resolve()
	if value.Kind() != slog.KindGroup && protectedLeaf(groups, attr) {
		return slog.String(attr.Key, redacted)
	}
	switch value.Kind() {
	case slog.KindString:
		return slog.String(attr.Key, h.redact(value.String()))
	case slog.KindGroup:
		members := value.Group()
		inner := groups
		if attr.Key != "" {
			inner = append(slices.Clip(groups), attr.Key)
		}
		redacted := make([]any, len(members))
		for index, member := range members {
			redacted[index] = h.attr(inner, member)
		}
		return slog.Group(attr.Key, redacted...)
	case slog.KindAny:
		switch typed := value.Any().(type) {
		case error:
			return slog.String(attr.Key, h.redactGuarded(typed.Error))
		case json.RawMessage, []byte, json.Marshaler:
			return slog.Attr{Key: attr.Key, Value: redactStructured(typed)}
		case fmt.Stringer:
			return slog.String(attr.Key, h.redactGuarded(typed.String))
		}
		return slog.Attr{Key: attr.Key, Value: redactStructured(value.Any())}
	}
	return slog.Attr{Key: attr.Key, Value: value}
}

// protectedLeaf is attrRedactor.replace's protected-key rule for a leaf under groups.
func protectedLeaf(groups []string, attr slog.Attr) bool {
	if protectedKeyCached(attr.Key) && !booleanFlagAttr(attr) {
		return true
	}
	return slices.ContainsFunc(groups, protectedKeyCached)
}

// redactStructured is attrRedactor.redactAny with the same panic guard attrRedactor.replace has: a value whose marshaler
// panics is logged as a redaction failure.
func redactStructured(value any) (result slog.Value) {
	defer func() {
		if recover() != nil {
			result = slog.StringValue(redactionFailed)
		}
	}()
	return attrRedactor{}.redactAny(value)
}

// redactGuarded redacts the text produced by text, and logs a redaction failure when text or the redactor panics (a panicking
// Error or String method, or a nil pointer receiver).
func (h valueRedactor) redactGuarded(text func() string) (result string) {
	defer func() {
		if recover() != nil {
			result = redactionFailed
		}
	}()
	return h.redact(text())
}
