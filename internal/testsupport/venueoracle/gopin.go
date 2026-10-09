package venueoracle

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// GoPinUpdateEnv, set to 1, makes a GoPin record Go's answers instead of
// comparing them: Finish writes the file and fails the test with its digest,
// so an update run never reads as a pass.
const GoPinUpdateEnv = "DHO_VENUE_GO_PIN_UPDATE"

// GoPinSpec names a file of Go's own answers for requests whose Python
// reference a ruling retired (Golden.Retired). The file pins what Go answers
// so a change shows up as a diff; the ruling and its rule tests, not the file,
// state the intended behavior.
type GoPinSpec struct {
	// Path is the file, relative to the test's package directory.
	Path string
	// SHA256 is the digest of the file the test pins.
	SHA256 string
	// Ruling names the ruling that retired the Python reference.
	Ruling string
}

// GoPin is an opened GoPinSpec.
type GoPin struct {
	spec     GoPinSpec
	updating bool
	loaded   map[string]string
	recorded map[string]string
	used     map[string]bool
}

// errGoPinRecorded is finish's answer to a recording run: never a pass.
var errGoPinRecorded = errors.New("go pin recorded")

// OpenGoPin opens spec: frozen, the file must exist and match its digest.
func OpenGoPin(t *testing.T, spec GoPinSpec) *GoPin {
	t.Helper()
	pin, err := openGoPin(spec, os.Getenv(GoPinUpdateEnv) == "1")
	if err != nil {
		t.Fatal(err)
	}
	return pin
}

func openGoPin(spec GoPinSpec, updating bool) (*GoPin, error) {
	if strings.TrimSpace(spec.Path) == "" || strings.TrimSpace(spec.Ruling) == "" || strings.ContainsAny(spec.Ruling, "\n\r") {
		return nil, errors.New("a Go pin needs a path and a one-line ruling")
	}
	pin := &GoPin{spec: spec, updating: updating, recorded: map[string]string{}, used: map[string]bool{}}
	if updating {
		return pin, nil
	}
	raw, err := os.ReadFile(spec.Path)
	if err != nil {
		return nil, fmt.Errorf("go pin %s: %w; record it with %s=1", spec.Path, err, GoPinUpdateEnv)
	}
	if digest := sha256Hex(raw); digest != spec.SHA256 {
		return nil, fmt.Errorf("go pin %s has sha256 %s, the test pins %s; a file edited by hand is refused: record it with %s=1", spec.Path, digest, spec.SHA256, GoPinUpdateEnv)
	}
	if err := json.Unmarshal(raw, &pin.loaded); err != nil {
		return nil, fmt.Errorf("go pin %s: %w", spec.Path, err)
	}
	return pin, nil
}

// Check compares Go's text for name with the pinned text (recording, it keeps
// it) and reports whether they are equal. Each name is checked once.
func (p *GoPin) Check(t *testing.T, name, text string) bool {
	t.Helper()
	if err := p.check(name, text); err != nil {
		t.Error(err)
		return false
	}
	return true
}

func (p *GoPin) check(name, text string) error {
	if p.used[name] {
		return fmt.Errorf("go pin %s: %q is checked twice; use a distinct name per check", p.spec.Path, name)
	}
	p.used[name] = true
	if p.updating {
		p.recorded[name] = text
		return nil
	}
	want, ok := p.loaded[name]
	if !ok {
		return fmt.Errorf("go pin %s holds no answer for %q; record it with %s=1", p.spec.Path, name, GoPinUpdateEnv)
	}
	if want != text {
		return fmt.Errorf("%s: Go's answer differs from the pinned one (%s):\n pinned: %s\n go:     %s", name, p.spec.Ruling, want, text)
	}
	return nil
}

// Finish requires every pinned answer to have been checked; recording, it
// writes the file and fails the test with the digest to pin.
func (p *GoPin) Finish(t *testing.T) {
	t.Helper()
	if err := p.finish(); err != nil {
		t.Fatal(err)
	}
}

func (p *GoPin) finish() error {
	if p.updating {
		var buffer bytes.Buffer
		encoder := json.NewEncoder(&buffer)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(p.recorded); err != nil {
			return err
		}
		raw := buffer.Bytes()
		if err := os.WriteFile(p.spec.Path, raw, 0o644); err != nil {
			return err
		}
		return fmt.Errorf("%w: %s (%d answers): pin sha256 %s and run again without %s", errGoPinRecorded, p.spec.Path, len(p.recorded), sha256Hex(raw), GoPinUpdateEnv)
	}
	var unused []string
	for name := range p.loaded {
		if !p.used[name] {
			unused = append(unused, name)
		}
	}
	if len(unused) > 0 {
		sort.Strings(unused)
		return fmt.Errorf("go pin %s holds answers the test never checked: %s; record it with %s=1", p.spec.Path, strings.Join(unused, ", "), GoPinUpdateEnv)
	}
	return nil
}

// PinText is the text a GoPin stores for one response: the status and the
// body, as Diff compares them.
func PinText(response Response) string {
	return fmt.Sprintf("%d %s", response.Status, response.Body)
}

func sha256Hex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
