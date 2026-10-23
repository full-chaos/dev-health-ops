package logging

import (
	"context"
	"log/slog"
)

// keyScrubber wraps a handler so that every attribute key and group name passes the shared key rule (KeyClean) before the
// inner handler writes it. ReplaceAttr cannot do this: it never sees a group name as a key, so a wrapper is the one place
// that covers leaf keys, group names, With attributes and WithGroup.
type keyScrubber struct{ inner slog.Handler }

func (h keyScrubber) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h keyScrubber) Handle(ctx context.Context, record slog.Record) error {
	dirty := false
	record.Attrs(func(attr slog.Attr) bool {
		dirty = dirty || attrNeedsKeyScrub(attr)
		return !dirty
	})
	if !dirty {
		return h.inner.Handle(ctx, record)
	}
	attrs := make([]slog.Attr, 0, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, attr)
		return true
	})
	scrubbed := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	scrubbed.AddAttrs(scrubAttrKeys(attrs)...)
	return h.inner.Handle(ctx, scrubbed)
}

func (h keyScrubber) WithAttrs(attrs []slog.Attr) slog.Handler {
	return keyScrubber{inner: h.inner.WithAttrs(scrubAttrKeys(attrs))}
}

func (h keyScrubber) WithGroup(name string) slog.Handler {
	if !KeyClean(name) {
		name = RedactedKeyName
	}
	return keyScrubber{inner: h.inner.WithGroup(name)}
}

func attrNeedsKeyScrub(attr slog.Attr) bool {
	if !KeyClean(attr.Key) {
		return true
	}
	value := attr.Value.Resolve()
	if value.Kind() != slog.KindGroup {
		return false
	}
	for _, member := range value.Group() {
		if attrNeedsKeyScrub(member) {
			return true
		}
	}
	return false
}

// scrubAttrKeys returns attrs with every unclean key (leaf or group name, at any depth) replaced by RedactedKeyName. A
// leaf under a replaced key has its value hidden. Repeated placeholders at one level collapse to the first.
func scrubAttrKeys(attrs []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, 0, len(attrs))
	placeholder := false
	for _, attr := range attrs {
		next := scrubAttrKey(attr)
		if next.Key == RedactedKeyName {
			if placeholder {
				continue
			}
			placeholder = true
		}
		out = append(out, next)
	}
	return out
}

func scrubAttrKey(attr slog.Attr) slog.Attr {
	clean := KeyClean(attr.Key)
	value := attr.Value.Resolve()
	if value.Kind() == slog.KindGroup {
		members := scrubAttrKeys(value.Group())
		key := attr.Key
		if !clean {
			key = RedactedKeyName
		}
		return slog.Attr{Key: key, Value: slog.GroupValue(members...)}
	}
	if clean {
		return attr
	}
	return slog.String(RedactedKeyName, redacted)
}
