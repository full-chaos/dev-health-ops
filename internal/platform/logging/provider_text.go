package logging

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// maxProviderTextRunes bounds free text a provider supplies (a repository
// full_name, a group path, an issue key) when it reaches a log line.
const maxProviderTextRunes = 128

const providerTextCutMarker = "...[cut]"

// ProviderText returns raw made safe for a log field: control, format,
// line/paragraph-separator and invalid-UTF-8 content is escaped as \uXXXX (or
// \xNN for an invalid byte), and the result holds at most 128 runes of the
// input followed by "...[cut]" when the input was longer. It is the one way
// listing text (not a shape-checked id, see ProviderIDAttr) reaches a WARN or
// ERROR line.
func ProviderText(raw string) string {
	var out strings.Builder
	kept := 0
	for index := 0; index < len(raw); {
		if kept == maxProviderTextRunes {
			out.WriteString(providerTextCutMarker)
			break
		}
		r, size := utf8.DecodeRuneInString(raw[index:])
		switch {
		case r == utf8.RuneError && size == 1:
			fmt.Fprintf(&out, `\x%02x`, raw[index])
		case r == '\\':
			out.WriteString(`\\`)
		case unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r):
			fmt.Fprintf(&out, `\u%04x`, r)
		default:
			out.WriteRune(r)
		}
		index += size
		kept++
	}
	return out.String()
}
