package secrets

import (
	"fmt"
	"os"
	"strings"
)

// LookupEnv is compatible with os.LookupEnv and easy to replace in tests.
type LookupEnv func(string) (string, bool)

// Resolve loads key or key_FILE. Defining both sources is rejected instead of
// silently choosing one. Mounted secret files may end in one newline, which is
// removed without trimming meaningful whitespace from the secret itself.
func Resolve(key string, lookup LookupEnv) (Value, bool, error) {
	if lookup == nil {
		lookup = os.LookupEnv
	}

	direct, hasDirect := lookup(key)
	fileName, hasFile := lookup(key + "_FILE")
	if hasDirect && hasFile {
		return Value{}, false, fmt.Errorf("%s and %s_FILE are mutually exclusive", key, key)
	}
	if hasDirect {
		return NewValue(direct), direct != "", nil
	}
	if !hasFile {
		return Value{}, false, nil
	}
	if strings.TrimSpace(fileName) == "" {
		return Value{}, false, fmt.Errorf("%s_FILE must name a file", key)
	}

	contents, err := os.ReadFile(fileName)
	if err != nil {
		// This must never wrap err verbatim
		// ("read %s_FILE: %w"): Go's os.PathError.Error() embeds the exact
		// path it tried to open -- fileName, an operator-supplied value
		// with no shape requirement. Wrapping it would let a caller who
		// misconfigures KEY_FILE
		// to a raw credential string (a full DSN, say) instead of an
		// actual path have that entire string echoed back verbatim by any
		// caller that printed this error to a log or a CLI's stderr. Name
		// only the key and a fixed error class -- never fileName, never
		// err's own message.
		return Value{}, false, fmt.Errorf("%s_FILE could not be read (check the path and permissions)", key)
	}
	value := trimFinalNewline(string(contents))
	if value == "" {
		return Value{}, false, fmt.Errorf("%s_FILE contains an empty value", key)
	}
	return NewValue(value), true, nil
}

func trimFinalNewline(value string) string {
	value = strings.TrimSuffix(value, "\n")
	return strings.TrimSuffix(value, "\r")
}

// IsSourceConflict lets callers classify a config error without inspecting
// secret values. It intentionally only checks the safe variable names.
func IsSourceConflict(err error, key string) bool {
	if err == nil {
		return false
	}
	want := fmt.Sprintf("%s and %s_FILE are mutually exclusive", key, key)
	return err.Error() == want
}
