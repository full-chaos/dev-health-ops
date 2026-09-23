package pythonparity

import (
	"strings"
)

const quoteAlwaysSafe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-~"

// Quote is urllib.parse.quote(text, safe=safe) for a str: every UTF-8 byte
// that is not an ASCII letter, digit, "_.-~" or in safe becomes %XX
// (upper-case hex).
func Quote(text, safe string) string {
	var out strings.Builder
	for index := 0; index < len(text); index++ {
		c := text[index]
		if c < 0x80 && (strings.IndexByte(quoteAlwaysSafe, c) >= 0 || strings.IndexByte(safe, c) >= 0) {
			out.WriteByte(c)
			continue
		}
		const hexDigits = "0123456789ABCDEF"
		out.WriteByte('%')
		out.WriteByte(hexDigits[c>>4])
		out.WriteByte(hexDigits[c&0x0f])
	}
	return out.String()
}
