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
// \xNN for an invalid byte), a backslash is doubled, and the escaped text is
// cut at an escape boundary once it holds 128 runes, followed by "...[cut]"
// when anything was left out. The limit holds on the output, not the input. It
// is the one way listing text (not a shape-checked id, see ProviderIDAttr)
// reaches a WARN or ERROR line.
func ProviderText(raw string) string {
	var out strings.Builder
	written := 0
	for index := 0; index < len(raw); {
		r, size := utf8.DecodeRuneInString(raw[index:])
		var piece string
		switch {
		case r == utf8.RuneError && size == 1:
			piece = fmt.Sprintf(`\x%02x`, raw[index])
		case r == '\\':
			piece = `\\`
		case unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r):
			piece = fmt.Sprintf(`\u%04x`, r)
		default:
			piece = string(r)
		}
		pieceRunes := utf8.RuneCountInString(piece)
		if written+pieceRunes > maxProviderTextRunes {
			out.WriteString(providerTextCutMarker)
			break
		}
		out.WriteString(piece)
		written += pieceRunes
		index += size
	}
	return out.String()
}
