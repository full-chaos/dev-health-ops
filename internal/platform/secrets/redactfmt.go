package secrets

import (
	"fmt"
	"log/slog"
)

// RedactString hides a non-empty credential and keeps an empty one empty, so a
// printed struct still shows whether the credential is set.
func RedactString(value string) string {
	if value == "" {
		return ""
	}
	return redacted
}

// RedactPtr is RedactString for an optional credential; nil stays nil.
func RedactPtr(value *string) *string {
	if value == nil {
		return nil
	}
	hidden := RedactString(*value)
	return &hidden
}

// FormatRedacted prints plain with the caller's verb and flags. plain must be a
// method-less copy of the caller's struct whose credential fields are already
// redacted; a type with Format would recurse.
func FormatRedacted(state fmt.State, verb rune, plain any) {
	_, _ = fmt.Fprintf(state, fmt.FormatString(state, verb), plain)
}

// LogRedacted is the slog form of FormatRedacted. A string value keeps slog's
// JSON handler from reflecting over the exported credential field.
func LogRedacted(plain any) slog.Value {
	return slog.StringValue(fmt.Sprintf("%+v", plain))
}
