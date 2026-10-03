package logging

import (
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/errortext"
)

// CHAOS-8277, the shape of record: in every layer the function is userinfo_post(main_chain(x)),
// so the outer output hides at least everything the inner chain hides. This test asserts it per
// layer and per text over a generated corpus: 16 keys x 10 key forms x 20 value forms. A marker
// (a word of a value) that is unreadable in the inner output must be unreadable in the outer one.
func TestEveryLayerHidesAtLeastWhatItsInnerChainHides(t *testing.T) {
	values := []string{
		"Zq9vT4mW2xLp8Rn7", "Zq9vT4mW:2xLp8Rn7@Hk3sD6fJ", ":Zq9vT4mW@2xLp8Rn7", "svc_user:Zq9vT4mW@host.example.test",
		"amqp://svc_user:Zq9vT4mW@host.example.test/2xLp8Rn7", "https%3A%2F%2Fsvc_user%3AZq9vT4mW%40host.example.test%2F2xLp8Rn7",
		"Zq9vT4mW/2xLp8Rn7@Hk3sD6fJ", "Zq9vT4mW%402xLp8Rn7", "Zq9vT4mW@2xLp8Rn7.example.test", "svc_user:Zq9vT4mW%222xLp8Rn7@host.example.test",
		"postgres://svc_user:Zq9vT4mW@db.internal:5432/2xLp8Rn7?sslmode=require", "redis://:Zq9vT4mW@cache.internal:6379/0",
		"svc_user:Zq9vT4mW@registry.internal/repo@sha256:0123456789abcdef", "Zq9vT4mW:2xLp8Rn7", "svc_user%3AZq9vT4mW%40host.example.test",
		"https://svc_user:Zq9vT4mW@host.example.test/2xLp8Rn7@Hk3sD6fJ", "Zq9vT4mW 2xLp8Rn7", "Zq9vT4mW%2F2xLp8Rn7@Hk3sD6fJ",
		"svc_user:Zq9vT4mW@host.example.test:5432/2xLp8Rn7%40Hk3sD6fJ", "x-access-token:Zq9vT4mW@db.internal/org/2xLp8Rn7.git",
	}
	keys := []string{"password", "passwd", "secret", "token", "access_token", "api_key", "X-Api-Key", "authorization", "client_secret", "dsn", "database_url", "url", "note", "cause", "Token", "Password"}
	forms := []string{
		"%s=%s", "%s: %s", "%s:%s", `%s="%s"`, `{"%s":"%s","other":"x"}`, `{\"%s\":\"%s\"}`, "GET /cb?%s=%s&page=2", "{%s:%s Other:1}", "%s%%3D%s", "--%s %s",
	}
	markers := []string{"Zq9vT4mW", "2xLp8Rn7", "Hk3sD6fJ"}
	type pair struct {
		name         string
		outer, inner func(string) string
	}
	pairs := []pair{
		{"RedactText", RedactText, redactTextNoUserinfo},
		{"RedactCredentialShapes", RedactCredentialShapes, RedactCredentialShapesNoUserinfo},
		{"sanitizer chain (cap after the shape pass)", func(text string) string {
			return RedactUserinfoLast(errortext.Sanitize(RedactCredentialShapesNoUserinfo(text)))
		}, func(text string) string {
			return errortext.Sanitize(RedactCredentialShapesNoUserinfo(text))
		}},
	}
	checked := 0
	for _, value := range values {
		for _, key := range keys {
			for _, form := range forms {
				text := "op failed: " + fmt.Sprintf(form, key, value) + " (attempt 2)"
				for _, p := range pairs {
					outer, inner := p.outer(text), p.inner(text)
					for _, marker := range markers {
						if strings.Contains(outer, marker) && !strings.Contains(inner, marker) {
							t.Errorf("%s: %q shows %q that the inner chain hides: inner %.200s outer %.200s", p.name, text, marker, inner, outer)
						}
					}
					checked++
				}
			}
		}
	}
	if want := len(values) * len(keys) * len(forms) * len(pairs); checked != want {
		t.Fatalf("checked %d, want %d", checked, want)
	}
}
