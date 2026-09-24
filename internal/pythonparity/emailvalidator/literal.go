package emailvalidator

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

func itoa(n int) string { return strconv.Itoa(n) }

func repr(text []rune) string { return pythonparity.StrReprRunes(text) }

// rstripSpace is str.rstrip(): Python whitespace (str.isspace).
func rstripSpace(text []rune) []rune {
	end := len(text)
	for end > 0 && pythonparity.IsSpace(text[end-1]) {
		end--
	}
	return text[:end]
}

// ipv4Pattern is re.match(r"^[0-9\.]+$", text): digits and periods, where
// "$" also matches before one final newline.
func ipv4Pattern(text []rune) bool {
	body := text
	if len(body) > 0 && body[len(body)-1] == '\n' {
		body = body[:len(body)-1]
	}
	if len(body) == 0 {
		return false
	}
	for _, r := range body {
		if !(r >= '0' && r <= '9') && r != '.' {
			return false
		}
	}
	return true
}

func isASCIIDigits(text []rune) bool {
	if len(text) == 0 {
		return false
	}
	for _, r := range text {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ipv4Error is the str() of the AddressValueError ipaddress.IPv4Address
// raises for text, or "" when text is a valid address.
func ipv4Error(text []rune) string {
	if len(text) == 0 {
		return "Address cannot be empty"
	}
	octets := splitRunes(text, '.')
	if len(octets) != 4 {
		return "Expected 4 octets in " + repr(text)
	}
	for _, octet := range octets {
		if message := ipv4OctetError(octet); message != "" {
			return message + " in " + repr(text)
		}
	}
	return ""
}

// ipv4OctetError is _parse_octet's ValueError text, or "".
func ipv4OctetError(octet []rune) string {
	switch {
	case len(octet) == 0:
		return "Empty octet not permitted"
	case !isASCIIDigits(octet):
		// isascii() and isdigit(): for ASCII text, isdigit is 0-9.
		return "Only decimal digits permitted in " + repr(octet)
	case len(octet) > 3:
		return "At most 3 characters permitted in " + repr(octet)
	case string(octet) != "0" && octet[0] == '0':
		return "Leading zeros are not permitted in " + repr(octet)
	}
	value, _ := strconv.Atoi(string(octet))
	if value > 255 {
		return fmt.Sprintf("Octet %d (> 255) not permitted", value)
	}
	return ""
}

func isHexDigits(text []rune) bool {
	for _, r := range text {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}

// hextetError is _parse_hextet's ValueError text (or int()'s, for an empty
// hextet), or "".
func hextetError(hextet []rune) string {
	switch {
	case !isHexDigits(hextet):
		return "Only hex digits permitted in " + repr(hextet)
	case len(hextet) > 4:
		return "At most 4 characters permitted in " + repr(hextet)
	case len(hextet) == 0:
		return "invalid literal for int() with base 16: ''"
	}
	return ""
}

// splitMax is str.split(sep, maxsplit).
func splitMax(text []rune, sep rune, maxSplit int) [][]rune {
	var out [][]rune
	start := 0
	for i, r := range text {
		if r == sep && len(out) < maxSplit {
			out = append(out, text[start:i])
			start = i + 1
		}
	}
	return append(out, text[start:])
}

// ipv6Error is the str() of the AddressValueError ipaddress.IPv6Address
// raises for text, or "" when text is a valid address.
func ipv6Error(text []rune) string {
	if containsRune(text, '/') {
		return "Unexpected '/' in " + repr(text)
	}
	address := text
	if index := indexRune(text, '%'); index >= 0 {
		scope := text[index+1:]
		if len(scope) == 0 || containsRune(scope, '%') {
			return `Invalid IPv6 address: "` + repr(text) + `"`
		}
		address = text[:index]
	}
	return ipv6IntError(address)
}

func indexRune(text []rune, want rune) int {
	for i, r := range text {
		if r == want {
			return i
		}
	}
	return -1
}

const hextetCount = 8

func ipv6IntError(text []rune) string {
	if len(text) == 0 {
		return "Address cannot be empty"
	}
	if len(text) > 45 {
		shorten := repr(text)
		if len(text) > 100 {
			elided := append(append([]rune{}, text[:45]...), []rune(fmt.Sprintf("(%d chars elided)", len(text)-90))...)
			shorten = repr(append(elided, text[len(text)-45:]...))
		}
		return "At most 45 characters expected in " + shorten
	}
	maxParts := hextetCount + 1
	parts := splitMax(text, ':', maxParts)
	if len(parts) < 3 {
		return "At least 3 parts expected in " + repr(text)
	}
	if containsRune(parts[len(parts)-1], '.') {
		last := parts[len(parts)-1]
		if message := ipv4Error(last); message != "" {
			return message + " in " + repr(text)
		}
		// A valid IPv4 tail becomes two hextets; their values do not
		// matter here, only that both parse.
		parts = append(parts[:len(parts)-1], []rune("0"), []rune("0"))
	}
	if len(parts) > maxParts {
		return fmt.Sprintf("At most %d colons permitted in %s", maxParts-1, repr(text))
	}
	skipIndex := -1
	for i := 1; i < len(parts)-1; i++ {
		if len(parts[i]) == 0 {
			if skipIndex >= 0 {
				return "At most one '::' permitted in " + repr(text)
			}
			skipIndex = i
		}
	}
	var partsHi, partsLo int
	if skipIndex >= 0 {
		partsHi = skipIndex
		partsLo = len(parts) - skipIndex - 1
		if len(parts[0]) == 0 {
			partsHi--
			if partsHi != 0 {
				return "Leading ':' only permitted as part of '::' in " + repr(text)
			}
		}
		if len(parts[len(parts)-1]) == 0 {
			partsLo--
			if partsLo != 0 {
				return "Trailing ':' only permitted as part of '::' in " + repr(text)
			}
		}
		if hextetCount-(partsHi+partsLo) < 1 {
			return fmt.Sprintf("Expected at most %d other parts with '::' in %s", hextetCount-1, repr(text))
		}
	} else {
		if len(parts) != hextetCount {
			return fmt.Sprintf("Exactly %d parts expected without '::' in %s", hextetCount, repr(text))
		}
		if len(parts[0]) == 0 {
			return "Leading ':' only permitted as part of '::' in " + repr(text)
		}
		if len(parts[len(parts)-1]) == 0 {
			return "Trailing ':' only permitted as part of '::' in " + repr(text)
		}
		partsHi, partsLo = len(parts), 0
	}
	for i := 0; i < partsHi; i++ {
		if message := hextetError(parts[i]); message != "" {
			return message + " in " + repr(text)
		}
	}
	for i := len(parts) - partsLo; i < len(parts); i++ {
		if message := hextetError(parts[i]); message != "" {
			return message + " in " + repr(text)
		}
	}
	return ""
}

// validateDomainLiteral is validate_email_domain_literal's refusals. A
// literal it accepts is refused by the caller anyway (literals are not
// allowed), so the accepted address itself is never needed.
func validateDomainLiteral(literal []rune) *SyntaxError {
	if ipv4Pattern(literal) {
		if message := ipv4Error(literal); message != "" {
			return syntaxError("The address in brackets after the @-sign is not valid: It is not an IPv4 address (%s) or is missing an address literal tag.", message)
		}
		return nil
	}
	if strings.HasPrefix(string(literal), "IPv6:") {
		if message := ipv6Error(literal[5:]); message != "" {
			return syntaxError("The IPv6 address in brackets after the @-sign is not valid (%s).", message)
		}
		return nil
	}
	if !containsRune(literal, ':') {
		return syntaxError("The part after the @-sign in brackets is not an IPv4 address and has no address literal tag.")
	}
	if bad := displaySet(literal, func(c rune) bool { return !domainLiteralChar(c) }); bad != "" {
		return syntaxError("The part after the @-sign contains invalid characters in brackets: %s.", bad)
	}
	return syntaxError("The part after the @-sign contains an invalid address literal tag in brackets.")
}

// domainLiteralChar is DOMAIN_LITERAL_CHARS: [!-ú^-~].
func domainLiteralChar(r rune) bool {
	return (r >= 0x21 && r <= 0xfa) || (r >= 0x5e && r <= 0x7e)
}
