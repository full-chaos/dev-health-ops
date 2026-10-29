package restorefence

import (
	"fmt"
	"sort"
	"strings"
)

// ActionKind names what an operator must do after an apply.
type ActionKind string

const (
	// ActionForcePasswordReset: these users' passwords may have changed after the
	// restore target, so the restored (older) password hash is live again. The
	// operator forces a reset for each BEFORE readiness is acked back.
	ActionForcePasswordReset ActionKind = "force_password_reset"
	// ActionRequeueProviderRevocation: a provider token revocation queued after
	// the target is gone; the operator re-disconnects or revokes at the provider.
	ActionRequeueProviderRevocation ActionKind = "requeue_provider_revocation"
)

// OperatorAction is the ids a section names for one action. Ids only: never a
// hash, a token or a secret.
type OperatorAction struct {
	Kind   ActionKind `json:"kind"`
	Source string     `json:"source"`
	IDs    []string   `json:"ids"`
}

// mergeActions merges actions of one kind and source and sorts every id list.
func mergeActions(in []OperatorAction) []OperatorAction {
	type key struct {
		kind   ActionKind
		source string
	}
	seen := map[key]map[string]bool{}
	for _, a := range in {
		k := key{a.Kind, a.Source}
		if seen[k] == nil {
			seen[k] = map[string]bool{}
		}
		for _, id := range a.IDs {
			seen[k][id] = true
		}
	}
	out := make([]OperatorAction, 0, len(seen))
	for k, ids := range seen {
		a := OperatorAction{Kind: k.kind, Source: k.source, IDs: []string{}}
		for id := range ids {
			a.IDs = append(a.IDs, id)
		}
		sort.Strings(a.IDs)
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Source < out[j].Source
	})
	return out
}

// ActionLines prints one line per action that has ids.
func ActionLines(actions []OperatorAction) []string {
	var lines []string
	for _, a := range mergeActions(actions) {
		if len(a.IDs) == 0 {
			continue
		}
		lines = append(lines, fmt.Sprintf("operator-action %s source=%s count=%d ids=%s", a.Kind, a.Source, len(a.IDs), strings.Join(a.IDs, ",")))
	}
	return lines
}
