package providersync

import (
	"encoding/json"
	"errors"
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

// ErrPreviousReleaseSnapshot is a prepared snapshot, stored by the previous
// release, whose work_item_interactions rows have no interaction_id
// (CHAOS-8790). It is DETERMINISTIC: the same snapshot is replayed on every
// attempt, so the unit fails on the first one under its own class
// (providerunit.PreviousReleaseSnapshotCategory) instead of burning its
// attempts. The scope's next run is a new unit with a new snapshot.
var ErrPreviousReleaseSnapshot = errors.New(
	"provider sync prepared effect rows were stored by the previous release",
)

// errInteractionWithoutID is the refusal of such a row. It is also an
// ErrInvalidConfiguration (what it was before it had a class of its own).
var errInteractionWithoutID = fmt.Errorf(
	"%w: work_item_interactions rows without interaction_id: %w",
	ErrPreviousReleaseSnapshot, ErrInvalidConfiguration,
)

// refuseInteractionRowsWithoutID returns errInteractionWithoutID when any row
// has no interaction_id, after one WARN: a class label and counts only, never
// a row, a body or the raw error text.
func refuseInteractionRowsWithoutID(identity GitHubWorkItemEffectIdentity, rows []githubWorkItemInteractionRow) error {
	missing := 0
	for _, row := range rows {
		if row.InteractionID == "" {
			missing++
		}
	}
	if missing == 0 {
		return nil
	}
	slog.Warn("providersync.interaction.previous_release_snapshot",
		"class", "previous_release_snapshot", "provider", identity.Provider,
		"org_id", identity.OrgID, "rows", len(rows), "rows_without_interaction_id", missing)
	return errInteractionWithoutID
}

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
