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
	default:
		return provider + ":"
	}
}

// Of returns the team id of a provider's native team key or id: the
// provider's prefix plus the trimmed key. It is idempotent: an id that
// already carries the provider's prefix gets no second one. With no
// provider it returns the trimmed id unchanged; Check refuses such an id
// at a provider writer.
func Of(provider, id string) string {
	prefix := Prefix(provider)
	return prefix + strings.TrimPrefix(strings.TrimSpace(id), prefix)
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
