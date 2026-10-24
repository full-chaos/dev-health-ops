package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func syntheticKeys() (secret, userinfo string) {
	core := strings.Repeat("A1b2C3d4", 5)
	return "gh" + "p_" + core, "https://" + "svc:" + "hunter" + "2x@host.test/path"
}

func emitKeyed(logger *slog.Logger) {
	secret, userinfo := syntheticKeys()
	logger.Info("event", secret, "v1", userinfo, "v2", "repo", "octo", "count", 3)
}

func keyedLoggers(buf *bytes.Buffer) map[string]*slog.Logger {
	redact := func(s string) string { return s }
	text := slog.New(WithValueRedaction(slog.NewTextHandler(buf, nil), redact))
	return map[string]*slog.Logger{
		"json":       NewJSON(buf, slog.LevelInfo),
		"sync-cli":   text,
		"json+bound": NewJSON(buf, slog.LevelInfo).With(slog.String("repo", "octo")),
	}
}

func assertNoKeyLeak(t *testing.T, name, out string) {
	t.Helper()
	secret, userinfo := syntheticKeys()
	for _, leak := range []string{secret, strings.Repeat("A1b2C3d4", 5), "hunter" + "2x", userinfo} {
		if strings.Contains(out, leak) {
			t.Errorf("%s: key text reached the record: %q", name, out)
		}
	}
}

func TestSlogKeysAreRedactedInEveryHandler(t *testing.T) {
	for name := range keyedLoggers(&bytes.Buffer{}) {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			emitKeyed(keyedLoggers(&buf)[name])
			out := buf.String()
			assertNoKeyLeak(t, name, out)
			if n := strings.Count(out, "redacted.key"); n != 1 {
				t.Errorf("%s: want one collapsed placeholder key, got %d: %q", name, n, out)
			}
			for _, benign := range []string{"repo", "count"} {
				if !strings.Contains(out, benign) {
					t.Errorf("%s: benign key %q changed: %q", name, benign, out)
				}
			}
		})
	}
}

func TestSlogBenignKeysAreUnchangedByteForByte(t *testing.T) {
	var got, want bytes.Buffer
	logger := NewJSON(&got, slog.LevelInfo)
	plain := slog.New(slog.NewJSONHandler(&want, &slog.HandlerOptions{Level: slog.LevelInfo, ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return a
	}}))
	for _, l := range []*slog.Logger{logger, plain} {
		l.With("repo", "octo").WithGroup("grp").Info("event", "count", 3, slog.Group("inner", "outcome", "ok"))
	}
	strip := func(s string) string {
		lines := strings.Split(strings.TrimSpace(s), "\n")
		line := lines[len(lines)-1]
		if i := strings.Index(line, `"level"`); i >= 0 {
			line = "{" + line[i:]
		}
		return line
	}
	if strip(got.String()) != strip(want.String()) {
		t.Errorf("benign record changed:\n got %s\nwant %s", strip(got.String()), strip(want.String()))
	}
}

func TestSlogGroupWithAndNestedKeysAreRedacted(t *testing.T) {
	secret, _ := syntheticKeys()
	var buf bytes.Buffer
	logger := NewJSON(&buf, slog.LevelInfo)
	logger.WithGroup(secret).Info("a", "k", "v")
	logger.With(secret, "v").Info("b")
	logger.Info("c", slog.Group(secret, "k", "v"))
	logger.Info("d", "obj", map[string]any{secret: "v", "ok": 1})
	assertNoKeyLeak(t, "group/with/nested", buf.String())
	if !strings.Contains(buf.String(), `"ok"`) {
		t.Errorf("benign nested key lost: %q", buf.String())
	}
}

func TestSlogKeyRedactionIsIdempotentAcrossStackedHandlers(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(WithValueRedaction(NewJSON(&buf, slog.LevelInfo).Handler(), func(s string) string { return s }))
	emitKeyed(logger)
	assertNoKeyLeak(t, "stacked", buf.String())
	if n := strings.Count(buf.String(), "redacted.key"); n != 1 {
		t.Errorf("stacked: want one placeholder, got %d: %q", n, buf.String())
	}
}

type flipValuer struct{ calls *int }

func (v flipValuer) LogValue() slog.Value {
	*v.calls++
	if *v.calls == 1 {
		return slog.StringValue("plain")
	}
	secret, _ := syntheticKeys()
	return slog.GroupValue(slog.String(secret, "value"))
}

func TestSlogChangingLogValuerCannotBypassKeyScrub(t *testing.T) {
	for name := range keyedLoggers(&bytes.Buffer{}) {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			calls := 0
			keyedLoggers(&buf)[name].Info("event", "outer", flipValuer{calls: &calls})
			assertNoKeyLeak(t, name, buf.String())
		})
	}
}

func TestSlogOnePlaceholderPerLevelAcrossWithAndRecord(t *testing.T) {
	secret, _ := syntheticKeys()
	for name, build := range map[string]func(*bytes.Buffer) *slog.Logger{
		"json": func(b *bytes.Buffer) *slog.Logger { return NewJSON(b, slog.LevelInfo) },
		"sync-cli": func(b *bytes.Buffer) *slog.Logger {
			return slog.New(WithValueRedaction(slog.NewTextHandler(b, nil), func(s string) string { return s }))
		},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			build(&buf).With(secret, "a").With(secret+"x", "b").Info("event", secret+"y", "c")
			assertNoKeyLeak(t, name, buf.String())
			if n := strings.Count(buf.String(), "redacted.key"); n != 1 {
				t.Errorf("%s: want one placeholder across With and record, got %d: %q", name, n, buf.String())
			}
		})
	}
}

func TestSlogWithGroupStartsANewPlaceholderLevel(t *testing.T) {
	secret, _ := syntheticKeys()
	var buf bytes.Buffer
	NewJSON(&buf, slog.LevelInfo).With(secret, "a").WithGroup("grp").With(secret, "b").Info("event", secret, "c")
	assertNoKeyLeak(t, "group-level", buf.String())
	if n := strings.Count(buf.String(), "redacted.key"); n != 2 {
		t.Errorf("want one placeholder per level (2: outer, inside grp), got %d: %q", n, buf.String())
	}
}

func TestSlogDebugRecordsAreKeyScrubbedToo(t *testing.T) {
	secret, _ := syntheticKeys()
	var buf bytes.Buffer
	logger := NewJSON(&buf, slog.LevelDebug)
	logger.Debug("event", secret, "v")
	logger.With(secret, "v").Debug("event2")
	if buf.Len() == 0 {
		t.Fatal("debug records were dropped")
	}
	assertNoKeyLeak(t, "debug", buf.String())
}

func TestSlogStructuredValueKeysAreRedactedByTheSyncCLIWrapper(t *testing.T) {
	secret, _ := syntheticKeys()
	var buf bytes.Buffer
	logger := slog.New(WithValueRedaction(slog.NewTextHandler(&buf, nil), func(s string) string { return s }))
	logger.Info("event", "object", map[string]string{secret: "v", "ok": "1"}, "list", []any{map[string]any{secret + "x": 1}})
	logger.With("bound", map[string]string{secret: "v"}).Info("bound")
	assertNoKeyLeak(t, "sync-cli structured", buf.String())
	if !strings.Contains(buf.String(), "ok") {
		t.Errorf("benign nested key lost: %q", buf.String())
	}
}

func TestSlogEmptyNameGroupsShareTheParentPlaceholderLevel(t *testing.T) {
	secret, _ := syntheticKeys()
	for name, build := range map[string]func(*bytes.Buffer) *slog.Logger{
		"json": func(b *bytes.Buffer) *slog.Logger { return NewJSON(b, slog.LevelInfo) },
		"sync-cli": func(b *bytes.Buffer) *slog.Logger {
			return slog.New(WithValueRedaction(slog.NewTextHandler(b, nil), func(s string) string { return s }))
		},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			build(&buf).Info("event", secret, "a", slog.Group("", secret+"x", "b", slog.Group("", secret+"y", "c")), "kept", 1)
			assertNoKeyLeak(t, name, buf.String())
			if n := strings.Count(buf.String(), "redacted.key"); n != 1 {
				t.Errorf("%s: want one placeholder at the flattened level, got %d: %q", name, n, buf.String())
			}
			if !strings.Contains(buf.String(), "kept") {
				t.Errorf("%s: benign key lost: %q", name, buf.String())
			}
		})
	}
	var buf bytes.Buffer
	NewJSON(&buf, slog.LevelInfo).With(secret, "a").Info("event", slog.Group("", secret+"x", "b"))
	if n := strings.Count(buf.String(), "redacted.key"); n != 1 {
		t.Errorf("With-bound plus empty group: want one placeholder, got %d: %q", n, buf.String())
	}
}
