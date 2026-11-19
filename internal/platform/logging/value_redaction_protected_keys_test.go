package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// WithValueRedaction hides the value of a protected key (a leaf key, or any
// enclosing group name, bound by With, WithGroup or slog.Group) on every chain
// it wraps, a bare TextHandler too, as NewJSON's ReplaceAttr does.
func TestWithValueRedactionHidesProtectedKeyValuesOverABareTextHandler(t *testing.T) {
	identity := func(text string) string { return text }
	rows := []struct {
		name   string
		log    func(*slog.Logger)
		values []string
		want   []string
	}{
		{
			name:   "leaf protected key",
			log:    func(l *slog.Logger) { l.Info("m", "password", "pk11053leaf") },
			values: []string{"pk11053leaf"},
			want:   []string{"password=[REDACTED]"},
		},
		{
			name: "protected key under a group named redacted.key",
			log: func(l *slog.Logger) {
				l.Info("m", slog.Group(RedactedKeyName, "postgres://u:pk11053ui@h/db", "x", "password", "pk11053DV2"))
			},
			values: []string{"pk11053DV2", "pk11053ui"},
		},
		{
			name:   "nested group leaf",
			log:    func(l *slog.Logger) { l.Info("m", slog.Group("db", slog.Group("conn", "api_key", "pk11053nested"))) },
			values: []string{"pk11053nested"},
			want:   []string{"db.conn.api_key=[REDACTED]"},
		},
		{
			name:   "leaf under a protected group name",
			log:    func(l *slog.Logger) { l.Info("m", slog.Group("token", "note", "pk11053grp")) },
			values: []string{"pk11053grp"},
		},
		{
			name:   "leaf under a protected WithGroup",
			log:    func(l *slog.Logger) { l.WithGroup("secret").Info("m", "note", "pk11053withgroup") },
			values: []string{"pk11053withgroup"},
		},
		{
			name:   "bound by With",
			log:    func(l *slog.Logger) { l.With("token", "pk11053bound").Info("m") },
			values: []string{"pk11053bound"},
			want:   []string{"token=[REDACTED]"},
		},
		{
			name:   "bound by With under a protected WithGroup",
			log:    func(l *slog.Logger) { l.WithGroup("credentials").With("note", "pk11053boundgrp").Info("m") },
			values: []string{"pk11053boundgrp"},
		},
		{
			name:   "non-string value",
			log:    func(l *slog.Logger) { l.Info("m", "password", 1105342424) },
			values: []string{"1105342424"},
			want:   []string{"password=[REDACTED]"},
		},
		{
			name:   "LogValuer value",
			log:    func(l *slog.Logger) { l.Info("m", "password", stringValuer{"pk11053valuer"}) },
			values: []string{"pk11053valuer"},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			var buf bytes.Buffer
			row.log(slog.New(WithValueRedaction(slog.NewTextHandler(&buf, nil), identity)))
			out := buf.String()
			for _, value := range row.values {
				if strings.Contains(out, value) {
					t.Errorf("protected value %q is in the line: %s", value, out)
				}
			}
			for _, want := range row.want {
				if !strings.Contains(out, want) {
					t.Errorf("%q is missing from: %s", want, out)
				}
			}
		})
	}
}

// The rule hides only protected keys: other keys, the boolean flag exemption
// and the message keep their text.
func TestWithValueRedactionKeepsBenignKeysOverABareTextHandler(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(WithValueRedaction(slog.NewTextHandler(&buf, nil), func(text string) string { return text }))
	logger.WithGroup("g").Info("visible message", "note", "kept11053", "sessions_stub", true, slog.Group("inner", "count", 7))
	out := buf.String()
	for _, want := range []string{"visible message", "g.note=kept11053", "g.sessions_stub=true", "g.inner.count=7"} {
		if !strings.Contains(out, want) {
			t.Errorf("%q is missing from: %s", want, out)
		}
	}
}

// Over NewJSON's handler the rule gives the same line as NewJSON alone.
func TestWithValueRedactionProtectedKeysMatchNewJSON(t *testing.T) {
	var plain, wrapped bytes.Buffer
	emit := func(l *slog.Logger) {
		l.WithGroup("g").With("token", "pk11053j1").Info("m", "password", "pk11053j2", "note", "kept", "sessions_stub", true)
	}
	emit(NewJSON(&plain, slog.LevelInfo))
	emit(slog.New(WithValueRedaction(NewJSON(&wrapped, slog.LevelInfo).Handler(), func(text string) string { return text })))
	strip := func(line string) string {
		_, rest, _ := strings.Cut(line, `"level"`)
		return rest
	}
	if strip(plain.String()) != strip(wrapped.String()) {
		t.Fatalf("lines differ:\nNewJSON: %s\nwrapped: %s", plain.String(), wrapped.String())
	}
	if strings.Contains(wrapped.String(), "pk11053j") {
		t.Fatalf("protected value in the line: %s", wrapped.String())
	}
}

type stringValuer struct{ text string }

func (v stringValuer) LogValue() slog.Value { return slog.StringValue(v.text) }
