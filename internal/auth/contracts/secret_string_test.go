package contracts

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
)

// fmtVerbs is every fmt verb, plain and with flags and width. A verb a type's
// String method does not serve (%d, %b, %x on a struct, ...) prints the
// struct's fields, and with them the secret, unless the type is a Formatter.
var fmtVerbs = []string{
	"%v", "%+v", "%#v", "%T", "%%", "%t", "%b", "%c", "%d", "%o", "%O", "%q", "%x", "%X", "%U",
	"%e", "%E", "%f", "%F", "%g", "%G", "%s", "%p", "%10d", "%-12x", "%#x", "% x", "%+q", "%08b",
}

func TestSecretStringIsRedactedUnderEveryVerb(t *testing.T) {
	secret := "Zq" + strings.Repeat("k7", 24)
	value := NewSecretString(secret)
	request := RefreshRequest{SchemaVersion: RefreshRequestSurface, RefreshCredential: value}
	for _, verb := range fmtVerbs {
		for name, arg := range map[string]any{"value": value, "pointer": &value, "struct": request, "struct pointer": &request} {
			out := fmt.Sprintf(verb, arg)
			for _, leak := range []string{secret, hex.EncodeToString([]byte(secret)), strings.ToUpper(hex.EncodeToString([]byte(secret)))} {
				if strings.Contains(out, leak) {
					t.Errorf("%s of a %s printed the secret: %s", verb, name, out)
				}
			}
		}
	}
	if NewSecretString(secret).Reveal() != secret {
		t.Fatal("Reveal lost the value")
	}
}
