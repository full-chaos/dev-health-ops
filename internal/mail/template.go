package mail

import (
	"embed"
	"fmt"
	"html"
	"strings"
)

// templateFiles holds the HTML templates this package can render. Each is a
// byte-for-byte copy of src/dev_health_ops/templates/email/<name>.html --
// because `//go:embed` cannot reach outside the package directory. account_exists
// has no Python source.
//
//go:embed templates/*.html
var templateFiles embed.FS

// RenderTemplate is EmailService.render_template (email.py): it substitutes
// context into the named template with the same `str.format` rules Python
// applies -- `{field}` is replaced by the field's value, `{{` and `}}` are
// literal braces -- with ONE deliberate difference: every substituted value is
// HTML-escaped (EscapeHTML), the URL fields included (& in a query string
// becomes &amp;, which an href decodes back). Frozen Python wrote values raw;
// that let a user-controlled name (an org's name, an inviter's name sent to a
// third party) put markup into a mail, so the Go plane no longer does, on
// security grounds. Both planes render through this function, so the bytes
// are the same on both.
//
// Only plain `{identifier}` replacement fields are supported. Python's format
// mini-language (`{x!r}`, `{x:>10}`, attribute and index access) is not: no
// shipped template uses it, and an unsupported field is refused rather than
// silently rendered differently. A field with no value in context is an
// error, as `str.format` raises KeyError for it.
func RenderTemplate(name string, context map[string]string) (string, error) {
	return renderTemplate(name, context, true)
}

// renderTemplate is RenderTemplate with the escape switchable: the frozen
// Python oracle tests render with escape false, because the Python answers
// they replay wrote values raw; the escape itself is pinned in template_test.go.
func renderTemplate(name string, context map[string]string, escape bool) (string, error) {
	raw, err := templateFiles.ReadFile("templates/" + name + ".html")
	if err != nil {
		return "", fmt.Errorf("email template %q not found", name)
	}
	values := context
	if escape {
		values = make(map[string]string, len(context))
		for field, value := range context {
			values[field] = EscapeHTML(value)
		}
	}
	return formatTemplate(string(raw), values)
}

// EscapeHTML is the one escape every mail value goes through: & < > ' and "
// become &amp; &lt; &gt; &#39; and &#34;.
func EscapeHTML(value string) string { return html.EscapeString(value) }

func formatTemplate(template string, context map[string]string) (string, error) {
	var out strings.Builder
	for i := 0; i < len(template); i++ {
		switch template[i] {
		case '{':
			if i+1 < len(template) && template[i+1] == '{' {
				out.WriteByte('{')
				i++
				continue
			}
			end := strings.IndexByte(template[i+1:], '}')
			if end < 0 {
				return "", fmt.Errorf("email template: expected '}' before end of string")
			}
			field := template[i+1 : i+1+end]
			if !isPlainField(field) {
				return "", fmt.Errorf("email template: unsupported replacement field %q", field)
			}
			value, ok := context[field]
			if !ok {
				return "", fmt.Errorf("email template: no value for field %q", field)
			}
			out.WriteString(value)
			i += end + 1
		case '}':
			if i+1 < len(template) && template[i+1] == '}' {
				out.WriteByte('}')
				i++
				continue
			}
			return "", fmt.Errorf("email template: single '}' encountered")
		default:
			out.WriteByte(template[i])
		}
	}
	return out.String(), nil
}

// isPlainField reports whether field is a bare Python identifier -- the only
// replacement-field form RenderTemplate supports.
func isPlainField(field string) bool {
	if field == "" {
		return false
	}
	for i := 0; i < len(field); i++ {
		c := field[i]
		letter := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !letter && !(i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}
