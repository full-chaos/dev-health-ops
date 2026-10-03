package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// CHAOS-8260: a `user:secret@` userinfo with no scheme in front reached the log
// because credentialURLPattern needs `scheme://user:secret@host` whole. Each
// value below is ONE whole literal, never built from parts.

var userinfoCases = []struct {
	name, text, secret, kept string
}{
	{"schemeless host", "cache at svc_user:Zq9plainsecret77@cache.internal:6379 refused", "Zq9plainsecret77", "cache.internal"},
	{"scheme cut off by a comma", "settings (password=X, ://dbuser:Qv8docshapesecret@host.internal) end", "Qv8docshapesecret", "host.internal"},
	{"empty host", "tried (login: svc2:Rt4emptyhostsecret@) end", "Rt4emptyhostsecret", "end"},
	{"password with a colon", "dial ops_user:Wm5colon:inside:secret@db.internal:5432 failed", "inside:secret", "db.internal"},
}

// Every position a worker log call can carry a text in, at every depth the
// positions list reaches (groups, nested groups, errors, slices, maps).
func TestUserinfoWithoutASchemeIsRedactedInEveryPosition(t *testing.T) {
	t.Parallel()
	for _, tc := range userinfoCases {
		for _, position := range positions {
			var output bytes.Buffer
			position.log(NewJSON(&output, slog.LevelInfo), tc.text)
			line := output.String()
			if strings.Contains(line, tc.secret) {
				t.Errorf("%s/%s leaked: %s", tc.name, position.name, line)
			}
			if !strings.Contains(line, tc.kept) {
				t.Errorf("%s/%s lost the readable part %q: %s", tc.name, position.name, tc.kept, line)
			}
			if !json.Valid(bytes.TrimSpace(output.Bytes())) {
				t.Errorf("%s/%s is not valid JSON: %s", tc.name, position.name, line)
			}
		}
	}
}

// The redaction runs BEFORE the bound on one attribute: a userinfo that
// straddles the cut must not leave its secret's first bytes behind.
func TestUserinfoStraddlingTheValueBoundIsRedactedBeforeTheCut(t *testing.T) {
	t.Parallel()
	for pad := maxLoggedValueBytes - 40; pad < maxLoggedValueBytes; pad++ {
		text := strings.Repeat("a", pad) + " svc_user:Zq9plainsecret77@cache.internal"
		var output bytes.Buffer
		NewJSON(&output, slog.LevelInfo).Warn("m", "cause", text)
		line := output.String()
		if strings.Contains(line, "Zq9") || strings.Contains(line, "plainsecret") {
			t.Fatalf("pad %d: a part of the secret survived the cut: %.80s...", pad, line[len(line)-200:])
		}
	}
}

// The persisted error columns use RedactCredentialShapes, not RedactText.
func TestUserinfoIsRedactedForThePersistedErrorColumns(t *testing.T) {
	t.Parallel()
	got := RedactCredentialShapes("upstream said svc_user:Zq9plainsecret77@cache.internal refused")
	if strings.Contains(got, "Zq9plainsecret77") || !strings.Contains(got, "cache.internal") {
		t.Fatalf("RedactCredentialShapes: %q", got)
	}
}

// A text that holds an `@` without a userinfo is left alone: an address, a
// JSON pair, a key=value pair and a mention.
func TestAnAtSignWithoutAUserinfoIsNotRedacted(t *testing.T) {
	t.Parallel()
	for _, text := range []string{
		"mail ops@example.test refused",
		`{"cause":"a@b.example","note":"x"}`,
		"owner=ops@example.test route=/a/b",
		"thanks @alice for the report",
	} {
		if got := RedactText(text); got != text {
			t.Errorf("RedactText(%q) = %q, want it unchanged", text, got)
		}
	}
}

// The call site named by CHAOS-8261: the secret registry's own warning for a
// registered secret shorter than the minimum. Its detail is a constant text; it
// must carry no credential shape and still name the setting.
func TestShortSecretWarningCarriesNoCredentialShape(t *testing.T) {
	// The raw text the registry hands its warner (before any redactor) holds no
	// userinfo shape: the reword, not the redactor, is what keeps the exported
	// detail free of a credential shape.
	var raw []string
	previous := secrets.SetWarner(func(msg string, args ...any) {
		for _, arg := range args {
			if text, ok := arg.(string); ok {
				raw = append(raw, text)
			}
		}
	})
	secrets.Register("REVIEW_SHORT_SETTING_8260_RAW", "abc")
	secrets.SetWarner(previous)
	if len(raw) == 0 {
		t.Fatal("the registry did not warn for a short secret")
	}
	for _, text := range raw {
		if userinfoPattern.MatchString(text) {
			t.Fatalf("the raw warning text carries a userinfo shape: %q", text)
		}
	}

	var output bytes.Buffer
	logger := NewJSON(&output, slog.LevelInfo)
	restore := InstallDefault(logger)
	defer restore()
	secrets.Register("REVIEW_SHORT_SETTING", "abc")
	line := output.String()
	if !strings.Contains(line, "secret_shorter_than_minimum_not_redactable_by_value") || !strings.Contains(line, "REVIEW_SHORT_SETTING") {
		t.Fatalf("the warning or the setting name is missing: %s", line)
	}
	if userinfoPattern.MatchString(line) {
		t.Fatalf("the warning carries a userinfo shape: %s", line)
	}
	// The exported text stays readable: the redactor has nothing to rewrite in it.
	if strings.Contains(line, "[REDACTED]") {
		t.Fatalf("the warning text is rewritten by the redactor: %s", line)
	}
}

// The marker replaces only the `user:secret` part: a quote around the shape stays,
// so a quoted value keeps its quotes and a JSON document stays valid.
func TestUserinfoRedactionKeepsTheQuotesAroundTheShape(t *testing.T) {
	t.Parallel()
	got := RedactText(`dial "svc_user:Zq9plainsecret77@cache.internal" and 'svc_user:Zq9plainsecret77@cache.internal' now`)
	want := `dial "[REDACTED]@cache.internal" and '[REDACTED]@cache.internal' now`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
