package passwordpolicy

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

// TestEmbeddedListMatchesThePythonOne pins the embedded denylist to the file
// password_policy.py reads, byte for byte.
func TestEmbeddedListMatchesThePythonOne(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file")
	}
	pythonList := filepath.Join(filepath.Dir(file), "..", "..", "..", "src", "dev_health_ops", "data", "common_passwords.txt")
	want, err := os.ReadFile(pythonList)
	if err != nil {
		t.Fatalf("read the Python denylist: %v", err)
	}
	if string(want) != commonPasswordsFile {
		t.Fatalf("data/common_passwords.txt differs from %s; copy the Python list here", pythonList)
	}
	if len(loadCommonPasswords()) == 0 {
		t.Fatal("the embedded denylist parsed to no entries")
	}
}

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		password string
		want     []string
	}{
		{"correct-horse-9", nil},
		// The denylist match is case-insensitive.
		{"PassWord1000", []string{"Password is too common"}},
		{"short1", []string{"Password must be at least 12 characters long"}},
		{"nodigitsatallhere", []string{"Password must include at least one number"}},
		{"123456789012", []string{"Password must include at least one letter"}},
		// str.isdigit() accepts a superscript digit; unicode.IsDigit does not.
		{"Abcdefghijk²", nil},
		{"", []string{
			"Password must be at least 12 characters long",
			"Password must include at least one letter",
			"Password must include at least one number",
		}},
	} {
		if got := Validate(tc.password); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Validate(%q) = %q, want %q", tc.password, got, tc.want)
		}
	}
	long := make([]rune, 129)
	for i := range long {
		long[i] = 'a'
	}
	long[0] = '1'
	if got := Validate(string(long)); !reflect.DeepEqual(got, []string{"Password must be no more than 128 characters long"}) {
		t.Errorf("Validate(129 runes) = %q", got)
	}
}
