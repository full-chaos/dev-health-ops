package adminops

import (
	"fmt"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// secretOptString is an optString for a secret: the flag package and fmt see
// no value, the value sits in a secrets.Hidden, and every print and log form
// shows the redaction marker.
type secretOptString struct {
	value secrets.Hidden
	set   bool
}

func (o *secretOptString) String() string { return "" }
func (o *secretOptString) Set(v string) error {
	o.value, o.set = secrets.NewHidden(v), true
	return nil
}
func (o *secretOptString) wasSet() bool { return o.set }
func (o *secretOptString) ptr() *string {
	if !o.set {
		return nil
	}
	v := o.value.Reveal()
	return &v
}

func (o secretOptString) redacted() any {
	return struct {
		Value secrets.Hidden
		Set   bool
	}{o.value, o.set}
}

func (o secretOptString) Format(state fmt.State, verb rune) {
	secrets.FormatRedacted(state, verb, o.redacted())
}
func (o secretOptString) LogValue() slog.Value { return secrets.LogRedacted(o.redacted()) }
