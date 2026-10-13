package providersync

import "unicode/utf8"

const workItemDescriptionMaxRunes = 1200

// boundedWorkItemDescription is the one cut every provider-sync work-item
// writer applies: empty is no description, anything longer than the bound is
// cut on a rune boundary.
func boundedWorkItemDescription(description *string) *string {
	if description == nil || *description == "" {
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
// description, so the stored one is kept.
func workItemDescriptionUnstated(description *string) bool {
	return description == nil || *description == ""
}
