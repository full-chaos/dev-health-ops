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
	name, text string
	hidden     []string // parts of the credential that must be absent
	kept       string   // a readable part that must survive
}{
	{"host after user and password", "cache at svc_user:Zq9Lm4Nv77@cache.internal:6379 refused", []string{"Zq9Lm4Nv77"}, "cache.internal"},
	{"empty user", "cache at :Hx7Tk2Pw55@cache.internal:6379 refused", []string{"Hx7Tk2Pw55"}, "cache.internal"},
	{"empty user at the start", ":Hx7Tk2Pw56@cache.internal refused", []string{"Hx7Tk2Pw56"}, "cache.internal"},
	{"empty user in a url (the DSN pattern drops the host)", "dial redis://:Bn3Vc8Rd21@cache.internal:6379/0 now", []string{"Bn3Vc8Rd21"}, "now"},
	{"amqp url with an empty user", "dial amqp://:Bn3Vc8Rd22@mq.internal now", []string{"Bn3Vc8Rd22"}, "mq.internal"},
	{"slash in the password", "dial svc_user:Aa1Zz/Bb2Yy@db.internal:5432 failed", []string{"Aa1Zz", "Bb2Yy"}, "db.internal"},
	{"percent-encoded slash", "dial svc_user:Gg7Tt%2FHh8Ss@db.internal failed", []string{"Gg7Tt", "Hh8Ss"}, "db.internal"},
	{"at sign in the password", "dial svc_user:Cc3Xx@Dd4Ww@db.internal:5432 failed", []string{"Cc3Xx", "Dd4Ww"}, "db.internal"},
	{"percent-encoded at sign", "dial svc_user:Ee5Vv%40Ff6Uu@db.internal failed", []string{"Ee5Vv", "Ff6Uu"}, "db.internal"},
	{"colon in the password", "dial ops_user:Ll5Oo:Mm6Nn@db.internal failed", []string{"Ll5Oo", "Mm6Nn"}, "db.internal"},
	{"percent-encoded colon", "dial svc_user%3AIi9Rr0@db.internal failed", []string{"Ii9Rr0"}, "db.internal"},
	{"scheme cut off by a comma", "settings (key X, ://dbuser:Jj1Qq2@host.internal) end", []string{"Jj1Qq2"}, "host.internal"},
	{"empty host", "tried (login: svc2:Rt4Ee6@) end", []string{"Rt4Ee6"}, "end"},
	{"long text before the cap", strings.Repeat("a", 3990) + " svc_user:Kk3Pp4Qq5@cache.internal", []string{"Kk3Pp4Qq5"}, "cache.internal"},
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
			for _, hidden := range tc.hidden {
				if strings.Contains(line, hidden) {
					t.Errorf("%s/%s leaked %q: %.300s", tc.name, position.name, hidden, line)
				}
			}
			if !strings.Contains(line, tc.kept) {
				t.Errorf("%s/%s lost the readable part %q: %.300s", tc.name, position.name, tc.kept, line)
			}
			if !json.Valid(bytes.TrimSpace(output.Bytes())) {
				t.Errorf("%s/%s is not valid JSON: %.300s", tc.name, position.name, line)
			}
		}
	}
}

// The persisted error columns use RedactCredentialShapes (no percent-decoding
// first), and the text redactor is the same shape for a bare call.
func TestUserinfoFormsAreRedactedForThePersistedErrorColumnsAndTheBareRedactor(t *testing.T) {
	t.Parallel()
	for _, tc := range userinfoCases {
		for name, redact := range map[string]func(string) string{"RedactCredentialShapes": RedactCredentialShapes, "RedactText": RedactText} {
			got := redact(tc.text)
			for _, hidden := range tc.hidden {
				if strings.Contains(got, hidden) {
					t.Errorf("%s/%s leaked %q: %.300s", name, tc.name, hidden, got)
				}
			}
			if !strings.Contains(got, tc.kept) {
				t.Errorf("%s/%s lost %q: %.300s", name, tc.name, tc.kept, got)
			}
		}
	}
}

// The redaction runs BEFORE the bound on one attribute: a userinfo that
// straddles the cut must not leave its secret's first bytes behind.
func TestUserinfoStraddlingTheValueBoundIsRedactedBeforeTheCut(t *testing.T) {
	t.Parallel()
	for pad := maxLoggedValueBytes - 40; pad < maxLoggedValueBytes; pad++ {
		text := strings.Repeat("a", pad) + " svc_user:Zq9Lm4Nv77@cache.internal"
		var output bytes.Buffer
		NewJSON(&output, slog.LevelInfo).Warn("m", "cause", text)
		line := output.String()
		if strings.Contains(line, "Zq9") || strings.Contains(line, "Nv77") {
			t.Fatalf("pad %d: a part of the secret survived the cut: %.80s...", pad, line[len(line)-200:])
		}
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
		"image registry.example:5000/repo@sha256:0123456789abcdef pulled",
		"clone git@github.com:org/repo.git now",
		"module example.test/x/y@v1.2.3 loaded",
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
	got := RedactText(`dial "svc_user:Zq9Lm4Nv77@cache.internal" and 'svc_user:Zq9Lm4Nv77@cache.internal' now`)
	want := `dial "[REDACTED]@cache.internal" and '[REDACTED]@cache.internal' now`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// A DSN keeps losing its host together with its password: the bare-userinfo match
// must not run first and take `postgres:` for the user (CHAOS-8277 found this in
// the shell package's runtime-failure test).
func TestADsnStillLosesItsHostWithItsPassword(t *testing.T) {
	t.Parallel()
	got := RedactText("dial postgres://svc_user:Zq9Lm4Nv77@ch.internal/db now")
	for _, forbidden := range []string{"postgres://", "Zq9Lm4Nv77", "ch.internal"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("the DSN leaked %q: %q", forbidden, got)
		}
	}
}
