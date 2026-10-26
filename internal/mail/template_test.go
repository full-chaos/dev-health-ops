package mail

import (
	"html"
	"regexp"
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

var hostileValues = map[string]string{
	"script":          `<script>alert(1)</script>`,
	"ampersand":       `a & b`,
	"double quote":    `say "hi"`,
	"single quote":    `it's`,
	"address":         `Tom <t@example.test>`,
	"already escaped": `&lt;b&gt;`,
	"attribute break": `"><img src=x onerror=alert(1)>`,
	"non-ascii":       "Zoë 山田",
	"empty":           ``,
}

// Deliberate: Go escapes every substituted value, frozen Python did not.
func TestRenderTemplateEscapesEveryField(t *testing.T) {
	fields := map[string]map[string]string{
		"verification":   {"full_name": "", "verification_url": "https://app.example.test/auth/verify?token=t"},
		"password_reset": {"full_name": "", "reset_url": "https://app.example.test/auth/reset-password?token=t"},
		"account_exists": {"full_name": "", "signin_url": "https://app.example.test/auth/signin", "reset_url": "https://app.example.test/auth/forgot-password"},
		"invite":         {"org_name": "", "inviter_name": "", "accept_url": "https://app.example.test/accept-invite?token=t"},
	}
	for name, base := range fields {
		for field := range base {
			for label, hostile := range hostileValues {
				context := map[string]string{}
				for k, v := range base {
					context[k] = v
				}
				context[field] = hostile
				got, err := RenderTemplate(name, context)
				if err != nil {
					t.Fatalf("%s/%s/%s: %v", name, field, label, err)
				}
				if hostile != "" && strings.Contains(got, hostile) && html.EscapeString(hostile) != hostile {
					t.Errorf("%s/%s/%s: the raw value reached the body:\n%s", name, field, label, got)
				}
				if want := html.EscapeString(hostile); !strings.Contains(got, want) {
					t.Errorf("%s/%s/%s: body lacks the escaped value %q", name, field, label, want)
				}
				if strings.Contains(got, "<script") || strings.Contains(got, "<img src=x") {
					t.Errorf("%s/%s/%s: markup from the value landed in the body", name, field, label)
				}
			}
		}
	}
}

// A URL field is escaped too: & in a query string becomes &amp;, which an href decodes back.
func TestRenderTemplateEscapesUrlFieldsToo(t *testing.T) {
	got, err := RenderTemplate("verification", map[string]string{"full_name": "x", "verification_url": "https://app.example.test/v?a=1&b=2"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `href="https://app.example.test/v?a=1&amp;b=2"`) || strings.Contains(got, "a=1&b=2") {
		t.Fatalf("url not escaped:\n%s", got)
	}
}

// Census: every replacement field of every shipped template is escaped, and a *_url field sits only inside an href or src attribute.
func TestEveryTemplateFieldIsEscapedAndUrlFieldsSitInAttributes(t *testing.T) {
	entries, err := templateFiles.ReadDir("templates")
	if err != nil || len(entries) == 0 {
		t.Fatalf("templates: %v (%d)", err, len(entries))
	}
	fieldPattern := regexp.MustCompile(`\{([A-Za-z_][A-Za-z0-9_]*)\}`)
	for _, entry := range entries {
		name := strings.TrimSuffix(entry.Name(), ".html")
		raw, err := templateFiles.ReadFile("templates/" + entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		context := map[string]string{}
		for _, match := range fieldPattern.FindAllStringSubmatch(string(raw), -1) {
			context[match[1]] = `<i>&"'` + match[1]
		}
		if len(context) == 0 {
			t.Errorf("%s: no replacement field found: the census measured nothing", name)
			continue
		}
		got, err := RenderTemplate(name, context)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if strings.Contains(got, "<i>") {
			t.Errorf("%s: a field reached the body unescaped:\n%s", name, got)
		}
		for field := range context {
			if !strings.HasSuffix(field, "_url") {
				continue
			}
			for _, at := range regexp.MustCompile(regexp.QuoteMeta("{"+field+"}")).FindAllStringIndex(string(raw), -1) {
				prefix := string(raw[:at[0]])
				if !strings.HasSuffix(prefix, `href="`) && !strings.HasSuffix(prefix, `src="`) {
					t.Errorf("%s: url field %s is not inside an href or src attribute", name, field)
				}
			}
		}
	}
}
