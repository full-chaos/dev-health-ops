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

// ErrMalformedTeamID marks a team id that is empty or only a provider
// prefix: it is no team of any provider.
var ErrMalformedTeamID = errors.New("teamid: team id is empty or a provider prefix with nothing after it")

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

// KnownKeys returns the prefixes a team id can already carry.
func KnownKeys() []string {
	return append([]string(nil), knownKeys...)
}

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

// Malformed reports whether an id is no team of any provider: empty, only
// a provider prefix ("gh:", "linear: ", "atlassian:"), or a provider prefix
// followed by only another one ("linear:gh:", what a second prefix on "gh:"
// gives).
func Malformed(id string) bool {
	id = canonical(strings.TrimSpace(id))
	if id == "" || isBareKey(id) {
		return true
	}
	for _, k := range knownKeys {
		if rest, ok := strings.CutPrefix(id, k); ok {
			return isBareKey(canonical(strings.TrimSpace(rest)))
		}
	}
	return false
}

// PrefixOnlyForms returns the trimmed ids that are only a provider prefix:
// every known prefix and the "atlassian:" alias.
func PrefixOnlyForms() []string {
	return append(KnownKeys(), "atlassian:")
}

// Candidates returns the prefixed ids a bare id can have: every known
// prefix plus the trimmed id. A keyed, empty or prefix-only id has none.
func Candidates(id string) []string {
	id = canonical(strings.TrimSpace(id))
	if HasKey(id) || Malformed(id) {
		return nil
	}
	out := make([]string, 0, len(knownKeys))
	for _, k := range knownKeys {
		out = append(out, k+id)
	}
	return out
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

// NativeKey reports whether a team id belongs to the provider and returns
// the provider's own key of it. An id with the provider's prefix gives the
// trimmed key after the prefix. An id with no known prefix is a bare id of
// the provider, written before ids carried a prefix: its key is the id. An
// id with another provider's prefix is not the provider's, so ok is false: a
// caller must never take another provider's key as its own native key. An
// empty id or key, or a provider without a prefix, also gives false.
func NativeKey(provider, id string) (key string, ok bool) {
	prefix := Prefix(provider)
	id = canonical(strings.TrimSpace(id))
	if prefix == "" || id == "" {
		return "", false
	}
	if rest, own := strings.CutPrefix(id, prefix); own {
		rest = strings.TrimSpace(rest)
		return rest, rest != ""
	}
	if HasKey(id) || isBareKey(id) {
		return "", false
	}
	return id, true
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
	if isBareKey(strings.TrimPrefix(strings.TrimSpace(id), Prefix(system))) {
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
