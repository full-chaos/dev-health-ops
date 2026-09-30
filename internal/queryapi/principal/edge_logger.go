package principal

import (
	"context"
	"log/slog"
)

// edgeSubjectLogKey is the attribute policy.Authenticator logs the token's subject under.
const edgeSubjectLogKey = "user_id"

// edgeLogger is the logger the edge verifier hands policy.Authenticator: the process's
// default logger with the subject attribute dropped. The Go api logs the subject on
// its own refusals; the edge telemetry never logs `sub` (edge_telemetry.go:
// logEdgeRejection), and a users-row refusal must not be the way it leaks in. The
// message and the other attributes -- which refusal it was -- are kept.
//
// It resolves slog.Default() on every record (never captured at construction), so a
// logger installed after the verifier was built, or by a test, still receives them.
func edgeLogger() *slog.Logger {
	return slog.New(subjectlessHandler{})
}

type subjectlessHandler struct {
	groups []string
	attrs  []slog.Attr
}

func (h subjectlessHandler) inner() slog.Handler {
	inner := slog.Default().Handler()
	for _, group := range h.groups {
		inner = inner.WithGroup(group)
	}
	if len(h.attrs) > 0 {
		inner = inner.WithAttrs(h.attrs)
	}
	return inner
}

func (h subjectlessHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return slog.Default().Handler().Enabled(ctx, level)
}

func (h subjectlessHandler) Handle(ctx context.Context, record slog.Record) error {
	kept := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key != edgeSubjectLogKey {
			kept.AddAttrs(attr)
		}
		return true
	})
	return h.inner().Handle(ctx, kept)
}

func (h subjectlessHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := subjectlessHandler{groups: h.groups, attrs: append([]slog.Attr(nil), h.attrs...)}
	for _, attr := range attrs {
		if attr.Key != edgeSubjectLogKey {
			next.attrs = append(next.attrs, attr)
		}
	}
	return next
}

func (h subjectlessHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return subjectlessHandler{groups: append(append([]string(nil), h.groups...), name), attrs: h.attrs}
}
