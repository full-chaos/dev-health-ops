package logging

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"reflect"
)

// WithValueRedaction wraps a handler so that every text it is given passes
// through redact first: the message, every string attribute, every error and
// every fmt.Stringer attribute, at any depth of a group, including the
// attributes bound by With. It is for a run that knows values no pattern can
// find (a database login or password it was handed): the process logger's own
// redactor (NewJSON) recognises credential shapes, not a login name.
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
	return valueRedactor{inner: inner, redact: redact}
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
}

func (h valueRedactor) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h valueRedactor) Handle(ctx context.Context, record slog.Record) error {
	redacted := slog.NewRecord(record.Time, record.Level, h.redact(record.Message), record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		redacted.AddAttrs(h.attr(attr))
		return true
	})
	return h.inner.Handle(ctx, redacted)
}

func (h valueRedactor) WithAttrs(attrs []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attrs))
	for index, attr := range attrs {
		redacted[index] = h.attr(attr)
	}
	return valueRedactor{inner: h.inner.WithAttrs(redacted), redact: h.redact}
}

func (h valueRedactor) WithGroup(name string) slog.Handler {
	return valueRedactor{inner: h.inner.WithGroup(name), redact: h.redact}
}

func (h valueRedactor) attr(attr slog.Attr) slog.Attr {
	value := attr.Value.Resolve()
	switch value.Kind() {
	case slog.KindString:
		return slog.String(attr.Key, h.redact(value.String()))
	case slog.KindGroup:
		members := value.Group()
		redacted := make([]any, len(members))
		for index, member := range members {
			redacted[index] = h.attr(member)
		}
		return slog.Group(attr.Key, redacted...)
	case slog.KindAny:
		switch typed := value.Any().(type) {
		case error:
			return slog.String(attr.Key, h.redact(typed.Error()))
		case fmt.Stringer:
			return slog.String(attr.Key, h.redact(typed.String()))
		}
	}
	return slog.Attr{Key: attr.Key, Value: value}
}
