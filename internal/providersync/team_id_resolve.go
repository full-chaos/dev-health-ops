package providersync

import (
	"errors"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// TeamIDMode says how a caller uses the id it resolves.
type TeamIDMode int

const (
	// TeamIDOwner is the id of the team the caller writes: a provider's
	// team, an imported team, an admin team.
	TeamIDOwner TeamIDMode = iota
	// TeamIDReference is the id of another team that a row names (a parent):
	// a bare id that no team moves to stays as it is.
	TeamIDReference
)

// TeamIDRequest is one id to resolve and what the caller knows about it.
type TeamIDRequest struct {
	// Provider is the provider of the caller's row; "" for an admin.
	Provider string
	ID       string
	Mode     TeamIDMode
	// Holders is the prefixed teams that hold the bare id (or that it moves
	// to), by provider ("" for an admin team).
	Holders map[string]string
	// CustomHeld says custom:<id> is held by a team of another source (a
	// pushed team of the custom system), not by an admin team.
	CustomHeld bool
}

// ErrTeamIDForeign marks a prefixed id whose prefix is not the caller's
// provider.
var ErrTeamIDForeign = errors.New("team id carries another provider's prefix")

// ErrTeamIDCustomHeld marks an admin team id whose custom:<id> a team of
// another source already holds.
var ErrTeamIDCustomHeld = errors.New("team id custom:<id> is held by a team of another source")

// ResolveTeamID is the one rule that turns a team id into the id a writer
// writes: the carry (a team's own id, an admin edit, a parent), and the
// admin write seam. A malformed id is refused. A prefixed id keeps its
// canonical form, and is refused when it is another provider's than the
// caller's. A bare id resolves, for an owner with a provider, to that
// provider's id; else to the one holder; and for an admin owner with no
// holder to custom:<id> (chris D5631), refused when a team of another source
// holds that id. Two holders are a conflict (ErrTeamIDAmbiguous). A
// reference that does not resolve to one holder keeps its id; a caller that
// resolves a reference inside a provider passes that provider's holder only.
func ResolveTeamID(req TeamIDRequest) (string, error) {
	if teamid.Malformed(req.ID) {
		return "", teamid.ErrMalformedTeamID
	}
	id := teamid.Of("", req.ID)
	provider := strings.TrimSpace(req.Provider)
	if teamid.HasKey(id) {
		if provider != "" {
			if _, own := teamid.NativeKey(provider, id); !own {
				return "", ErrTeamIDForeign
			}
		}
		return id, nil
	}
	if provider != "" && req.Mode == TeamIDOwner {
		return teamid.Of(provider, id), nil
	}
	distinct := map[string]bool{}
	for _, held := range req.Holders {
		if held != "" {
			distinct[held] = true
		}
	}
	switch {
	case len(distinct) == 1:
		for held := range distinct {
			return held, nil
		}
	case req.Mode == TeamIDReference:
		return id, nil
	case len(distinct) > 1:
		return "", ErrTeamIDAmbiguous
	case req.CustomHeld:
		return "", ErrTeamIDCustomHeld
	}
	return teamid.Of(teamIDCarryAdminProvider, id), nil
}

// teamIDPrefixProvider is the provider whose prefix a keyed id carries.
func teamIDPrefixProvider(id string) string {
	switch {
	case strings.HasPrefix(id, "gh:"):
		return "github"
	case strings.HasPrefix(id, "gl:"):
		return "gitlab"
	}
	provider, _, _ := strings.Cut(id, ":")
	return provider
}
