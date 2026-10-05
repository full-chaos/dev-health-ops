package secrets

import (
	"encoding/json"
	"fmt"
	"log/slog"
)

// Hidden holds a credential behind a pointer. fmt prints a pointer reached
// through an unexported field, or through a bad verb such as %s on a struct, as
// an address, so the credential cannot appear there: methods cannot guard those
// paths, because fmt never calls them. Where fmt does call methods, Hidden
// prints the redaction marker, or nothing when it is empty. Call Reveal only at
// the one place that needs the credential.
type Hidden struct {
	p *string
}

// NewHidden wraps a credential.
func NewHidden(value string) Hidden { return Hidden{p: &value} }

// Reveal returns the credential. It must not be logged or put in an error.
func (h Hidden) Reveal() string {
	if h.p == nil {
		return ""
	}
	return *h.p
}

// Configured reports whether the credential is non-empty without exposing it.
func (h Hidden) Configured() bool { return h.Reveal() != "" }

func (h Hidden) String() string   { return RedactString(h.Reveal()) }
func (h Hidden) GoString() string { return h.String() }

// Format redacts for every verb, so %d, %x and %q cannot print the credential.
func (h Hidden) Format(state fmt.State, _ rune) { _, _ = fmt.Fprint(state, h.String()) }

func (h Hidden) LogValue() slog.Value { return slog.StringValue(h.String()) }

func (h Hidden) MarshalJSON() ([]byte, error) { return json.Marshal(h.String()) }
