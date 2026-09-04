package categorize

import (
	"encoding/json"
	"strings"
)

// validateJSONOrEmpty ports llm/json_utils.py's validate_json_or_empty: a
// strict compact-or-empty gate used on the hot provider-response path.
// Whitespace-only input and invalid JSON both return "" -- no error is
// surfaced here, matching Python's own "no logging in the hot path" choice.
func validateJSONOrEmpty(text string) string {
	if strings.TrimSpace(text) == "" {
		return ""
	}
	var value any
	if err := json.Unmarshal([]byte(text), &value); err != nil {
		return ""
	}
	compact, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(compact)
}
