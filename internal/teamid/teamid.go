// Package teamid holds the one rule for the id of a provider team: the id
// carries its provider's prefix, so a team id is unique in an organization
// across providers (the teams table is keyed by (org_id, id), without the
// provider). Every native catalog writer, the admin import and the external
// team.v1 writer build their ids here. See
// docs/contribute/architecture/team-attribution.md "Team ids".
package teamid

import (
	"errors"
	"fmt"
	"strings"
)

// ErrBareTeamID marks a provider team id that does not carry its provider's
// prefix, or a team id that cannot have one.
var ErrBareTeamID = errors.New("teamid: provider team id without its provider prefix")

// Prefix is the id prefix of one provider: "gh:" for github and "gl:" for
// gitlab (their established forms), "<provider>:" for every other provider,
// a custom external system included. An empty provider has no prefix.
func Prefix(provider string) string {
	switch provider = strings.TrimSpace(provider); provider {
	case "":
		return ""
	case "github":
		return "gh:"
	case "gitlab":
		return "gl:"
	case "atlassian":
		// A pushed Atlassian team is the team the native Atlassian Teams
		// sync names, so it takes the jira prefix.
		return "jira:"
	default:
		return provider + ":"
	}
}

// knownKeys are the prefixes a team id can already carry: the native
// providers' and every team.v1 system's.
var knownKeys = []string{"gh:", "gl:", "linear:", "jira:", "pagerduty:", "custom:", "ms-teams:"}

// HasKey reports whether the trimmed id already carries a known provider
// prefix with something after it.
func HasKey(id string) bool {
	id = strings.TrimSpace(id)
	for _, k := range knownKeys {
		if strings.HasPrefix(id, k) && strings.TrimSpace(id[len(k):]) != "" {
			return true
		}
	}
	return false
}

// canonical folds the "atlassian:" alias into "jira:".
func canonical(id string) string {
	if rest, ok := strings.CutPrefix(id, "atlassian:"); ok {
		return "jira:" + rest
	}
	return id
}

// Of returns the team id of a provider's native team key or id: the
// provider's prefix plus the trimmed key. It is idempotent: an id that
// already carries any known provider prefix keeps it, whatever provider
// writes it, and gets no second one. A key without a known prefix gets the
// provider's. With no provider it returns the trimmed id unchanged; Check
// refuses such an id at a provider writer.
func Of(provider, id string) string {
	id = canonical(strings.TrimSpace(id))
	if HasKey(id) {
		return id
	}
	prefix := Prefix(provider)
	return prefix + strings.TrimPrefix(id, prefix)
}

// Native returns the provider's own key of a team id: the id without the
// provider's prefix. An id without the prefix is returned unchanged.
func Native(provider, id string) string {
	return strings.TrimPrefix(id, Prefix(provider))
}

// Check refuses a team id that a provider writer must not write: an id
// without its provider's prefix, or with nothing after the prefix.
func Check(provider, id string) error {
	prefix := Prefix(provider)
	if prefix == "" {
		return fmt.Errorf("%w: no provider for team id %q", ErrBareTeamID, id)
	}
	native, ok := strings.CutPrefix(id, prefix)
	if !ok || strings.TrimSpace(native) == "" {
		return fmt.Errorf("%w: %s team id %q", ErrBareTeamID, strings.TrimSpace(provider), id)
	}
	return nil
}

// CheckPushed refuses a pushed team id (team.v1) that is empty after its
// prefix, or that is only another known provider's prefix. An id that
// carries another known provider's prefix and a key is accepted.
func CheckPushed(system, id string) error {
	if isBareKey(Native(system, strings.TrimSpace(id))) {
		return fmt.Errorf("%w: %s team id %q has a provider prefix and nothing after it", ErrBareTeamID, strings.TrimSpace(system), id)
	}
	if HasKey(id) {
		return nil
	}
	return Check(system, id)
}

// isBareKey reports whether id is exactly a known provider prefix.
func isBareKey(id string) bool {
	for _, k := range knownKeys {
		if id == k {
			return true
		}
	}
	return false
}
