package providersync

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
)

// CHAOS-8790: work_item_interactions is keyed by the provider's own comment
// id. interaction_id = '' means a row written before the key carried the id
// (a legacy row) and nothing else: a comment the provider sent WITHOUT an id
// is skipped and counted, never written with ''.

// errInteractionWithoutID is the refusal of a work_item_interactions row that
// carries no interaction_id: a row stored by the previous release (a prepared
// snapshot an old pod wrote and a new pod replays). It is an
// ErrInvalidConfiguration, and says which row, so the log is not read as a
// configuration fault.
var errInteractionWithoutID = fmt.Errorf(
	"%w: work_item_interactions row without interaction_id (stored by the previous release)",
	ErrInvalidConfiguration,
)

var interactionMissingIDCounts = map[string]*atomic.Int64{
	"github": {}, "gitlab": {}, "jira": {}, "linear": {},
}

// InteractionMissingIDCount is how many comments this process skipped for
// provider because the payload carried no usable id.
func InteractionMissingIDCount(provider string) int64 {
	if counter := interactionMissingIDCounts[provider]; counter != nil {
		return counter.Load()
	}
	return 0
}

// skipInteractionWithoutID counts and logs one skipped comment. It logs a
// class label and the work item id only: never the body, never raw error text.
func skipInteractionWithoutID(provider, orgID, workItemID string) {
	if counter := interactionMissingIDCounts[provider]; counter != nil {
		counter.Add(1)
	}
	slog.Warn("providersync.interaction.comment_missing_id",
		"class", "missing_provider_comment_id", "provider", provider,
		"org_id", orgID, "work_item_id", workItemID)
}

// interactionIDFrom reads a provider comment id. Only a string or a number is
// an id; empty, whitespace-only and "0" are not.
func interactionIDFrom(value any) string {
	var id string
	switch typed := value.(type) {
	case string:
		id = typed
	case json.Number:
		id = typed.String()
	case float64:
		id = strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
	id = strings.TrimSpace(id)
	if id == "" || id == "0" {
		return ""
	}
	return id
}
