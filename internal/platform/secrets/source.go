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
		return Value{}, false, fmt.Errorf("read %s_FILE: %w", key, err)
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
