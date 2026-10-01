package logging

import (
	"context"
	"fmt"
	"log/slog"
)

// WithValueRedaction wraps a handler so that every text it is given passes
// through redact first: the message, every string attribute, every error and
// every fmt.Stringer attribute, at any depth of a group, including the
// attributes bound by With. It is for a run that knows values no pattern can
// find (a database login or password it was handed): the process logger's own
// redactor (NewJSON) recognises credential shapes, not a login name.
func WithValueRedaction(inner slog.Handler, redact func(string) string) slog.Handler {
	return valueRedactor{inner: inner, redact: redact}
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
