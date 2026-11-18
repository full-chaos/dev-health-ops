package providersync

import (
	"encoding/json"
	"unicode/utf8"
)

const workItemDescriptionMaxRunes = 1200

// boundedWorkItemDescription is the one cut every provider-sync work-item
// writer applies: nil is no statement, a pointer to "" is a description the
// provider states empty, anything longer than the bound is cut on a rune
// boundary.
func boundedWorkItemDescription(description *string) *string {
	if description == nil {
		return nil
	}
	text := *description
	if utf8.RuneCountInString(text) <= workItemDescriptionMaxRunes {
		return &text
	}
	count := 0
	for index := range text {
		if count == workItemDescriptionMaxRunes {
			cut := text[:index]
			return &cut
		}
		count++
	}
	return &text
}

// workItemDescriptionUnstated reports whether a provider row states no
// description, so the stored one is kept. A description stated empty (a
// pointer to "") is a statement: it clears the stored one.
func workItemDescriptionUnstated(description *string) bool {
	return description == nil
}

// statedWorkItemDescription is the row description of a provider payload: the
// text when it has one, a pointer to "" when the payload carries the field
// empty or null (the provider cleared it), nil when the payload does not
// carry the field at all.
func statedWorkItemDescription(text *string, stated bool) *string {
	if text != nil && *text != "" {
		return text
	}
	if text == nil && !stated {
		return nil
	}
	empty := ""
	return &empty
}

// jsonKeyPresent reports whether a JSON object carries the key, null or not.
func jsonKeyPresent(raw []byte, key string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return false
	}
	_, ok := fields[key]
	return ok
}
