package mail

import (
	"embed"
	"fmt"
	"strings"
)

// templateFiles holds the HTML templates this package can render. Each is a
// byte-for-byte copy of src/dev_health_ops/templates/email/<name>.html --
// TestEmbeddedTemplatesMatchThePythonOnes fails when either side drifts --
// because `//go:embed` cannot reach outside the package directory.
//
//go:embed templates/*.html
var templateFiles embed.FS

// RenderTemplate is EmailService.render_template (email.py): it substitutes
// context into the named template with the same `str.format` rules Python
// applies -- `{field}` is replaced by the field's value, `{{` and `}}` are
// literal braces -- and, exactly like Python, performs NO HTML escaping: a
// value containing markup lands in the message as markup. That is the source
// system's behavior, ported as it is; escaping here would make the two
// planes send different bytes for the same input.
//
// Only plain `{identifier}` replacement fields are supported. Python's format
// mini-language (`{x!r}`, `{x:>10}`, attribute and index access) is not: no
// shipped template uses it, and an unsupported field is refused rather than
// silently rendered differently. A field with no value in context is an
// error, as `str.format` raises KeyError for it.
func RenderTemplate(name string, context map[string]string) (string, error) {
	raw, err := templateFiles.ReadFile("templates/" + name + ".html")
	if err != nil {
		return "", fmt.Errorf("email template %q not found", name)
	}
	return formatTemplate(string(raw), context)
}

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
