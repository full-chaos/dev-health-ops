package identity

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// goAPILoginKey is the key the go-api login and register routes compute: the
// request body's EmailStr field (pybody), then strip(lower(..)) (login.go,
// register.go). NormalizeEmail must equal it on every input.
func goAPILoginKey(raw string) (string, bool) {
	object := pyjson.NewObject()
	object.Set("email", raw)
	var errs pybody.Errors
	email, ok := errs.RequiredEmailStr(object, "email")
	if !ok || len(errs) > 0 {
		return "", false
	}
	return pythonparity.Strip(pythonparity.Lower(email)), true
}

const emailGolden = "../../pythonparity/emailvalidator/testdata/validate_email_golden.jsonl"

// goldenInputs are the frozen pydantic EmailStr corpus inputs that are valid
// UTF-8 (the corpus also holds lone surrogates, which a Go string cannot).
func goldenInputs(t testing.TB) []string {
	t.Helper()
	f, err := os.Open(emailGolden)
	if err != nil {
		t.Fatalf("open the email corpus: %v", err)
	}
	defer f.Close()
	var out []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for scanner.Scan() {
		var row struct {
			Input []rune `json:"input"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatalf("corpus row: %v", err)
		}
		if s := string(row.Input); utf8.ValidString(s) && !strings.ContainsRune(s, utf8.RuneError) {
			out = append(out, s)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read the email corpus: %v", err)
	}
	if len(out) < 1000 {
		t.Fatalf("email corpus has %d usable rows; expected the frozen pydantic corpus", len(out))
	}
	return out
}

func TestNormalizeEmailCases(t *testing.T) {
	for _, c := range []struct {
		in, want string
		ok       bool
	}{
		{"a@b.com", "a@b.com", true},
		{"Admin@Example.COM", "admin@example.com", true},
		{"  User@Example.com \n", "user@example.com", true},
		{"Bob <Bob@Example.com>", "bob@example.com", true},
		{"", "", false},
		{"no-at-sign", "", false},
		{"a @b.com", "", false},
		{"a@b", "", false},
		{"\xff@b.com", "", false},
	} {
		got, err := NormalizeEmail(c.in)
		if c.ok != (err == nil) {
			t.Errorf("NormalizeEmail(%q) error = %v, want ok=%v", c.in, err, c.ok)
			continue
		}
		if err != nil && !errors.Is(err, ErrInvalidEmail) {
			t.Errorf("NormalizeEmail(%q) error = %v, want ErrInvalidEmail", c.in, err)
		}
		if got != c.want {
			t.Errorf("NormalizeEmail(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestNormalizeEmailMatchesGoAPIOnTheFrozenCorpus(t *testing.T) {
	inputs := goldenInputs(t)
	accepted := 0
	for _, in := range inputs {
		checkNormalizeEmail(t, in)
		if _, err := NormalizeEmail(in); err == nil {
			accepted++
		}
	}
	if accepted == 0 {
		t.Fatal("no corpus input was accepted")
	}
}

// checkNormalizeEmail holds the properties for one input: parity with the
// go-api login key, a fixed point, and a key with nothing left to lower or
// strip.
func checkNormalizeEmail(t *testing.T, in string) {
	t.Helper()
	got, err := NormalizeEmail(in)
	want, wantOK := goAPILoginKey(in)
	if (err == nil) != wantOK {
		t.Fatalf("NormalizeEmail(%q) error = %v; go-api accepts = %v", in, err, wantOK)
	}
	if err != nil {
		if !errors.Is(err, ErrInvalidEmail) {
			t.Fatalf("NormalizeEmail(%q) error = %v, want ErrInvalidEmail", in, err)
		}
		return
	}
	if got != want {
		t.Fatalf("NormalizeEmail(%q) = %q; go-api login key %q", in, got, want)
	}
	again, err := NormalizeEmail(got)
	if err != nil || again != got {
		t.Fatalf("NormalizeEmail is not a fixed point on %q: %q -> (%q, %v)", in, got, again, err)
	}
	if pythonparity.Lower(got) != got || pythonparity.Strip(got) != got {
		t.Fatalf("NormalizeEmail(%q) = %q still has upper case or edge space", in, got)
	}
	if !strings.Contains(got, "@") {
		t.Fatalf("NormalizeEmail(%q) = %q has no @", in, got)
	}
}

func FuzzNormalizeEmail(f *testing.F) {
	for _, in := range goldenInputs(f) {
		f.Add(in)
	}
	f.Fuzz(func(t *testing.T, in string) {
		if !utf8.ValidString(in) {
			if _, err := NormalizeEmail(in); !errors.Is(err, ErrInvalidEmail) {
				t.Fatalf("NormalizeEmail accepted invalid UTF-8 %q: %v", in, err)
			}
			return
		}
		checkNormalizeEmail(t, in)
	})
}
