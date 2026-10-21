package mail

import (
	"strings"
	"testing"
)

func TestFormatTemplateFollowsPythonStrFormat(t *testing.T) {
	context := map[string]string{"a": "1", "b_2": "<b>&</b>", "_c": ""}
	for _, test := range []struct {
		name, template, want string
	}{
		{"plain text", "hello", "hello"},
		{"a field", "x{a}y", "x1y"},
		{"the same field twice", "{a}{a}", "11"},
		{"identifier with underscore and digit", "{b_2}|{_c}|", "<b>&</b>||"}, // no HTML escaping, like str.format
		{"escaped braces", "{{a}} {{ }}", "{a} { }"},
		{"escaped braces around a field", "{{{a}}}", "{1}"},
		{"a value containing braces is not re-expanded", "{b_2}", "<b>&</b>"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := formatTemplate(test.template, context)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
	for _, test := range []struct {
		name, template, wantErr string
	}{
		{"missing field", "{nope}", "no value for field"},
		{"single closing brace", "a } b", "single '}'"},
		{"unterminated field", "a { b", "expected '}'"},
		{"empty field (positional)", "{}", "unsupported replacement field"},
		{"numeric field (positional)", "{0}", "unsupported replacement field"},
		{"conversion", "{a!r}", "unsupported replacement field"},
		{"format spec", "{a:>4}", "unsupported replacement field"},
		{"attribute access", "{a.b}", "unsupported replacement field"},
		{"index access", "{a[0]}", "unsupported replacement field"},
	} {
		t.Run("refuses "+test.name, func(t *testing.T) {
			_, err := formatTemplate(test.template, context)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want one containing %q", err, test.wantErr)
			}
		})
	}
}

func TestRenderTemplateRefusesAnUnknownTemplate(t *testing.T) {
	for _, name := range []string{"nope", "../invite", "invite/../invite", ""} {
		if _, err := RenderTemplate(name, nil); err == nil {
			t.Errorf("RenderTemplate(%q) = nil error, want a not-found refusal", name)
		}
	}
}

func TestRenderInviteMatchesTheKnownShape(t *testing.T) {
	got, err := RenderTemplate("invite", map[string]string{
		"org_name": "Acme", "inviter_name": "Ada", "accept_url": "http://x/accept?token=t",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"<h1>You're invited to join Acme</h1>",
		"<p>Ada invited you to join Acme on Dev Health.</p>",
		`<a href="http://x/accept?token=t">Accept invitation</a>`,
		"This link expires in 72 hours.",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered invite lacks %q:\n%s", want, got)
		}
	}
}
