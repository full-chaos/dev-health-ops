package providersync

import (
	"errors"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// TeamIDMode says how a caller uses the id it resolves.
type TeamIDMode int

const (
	// TeamIDOwner is the id of a team the caller's integration owns (a
	// provider's team, an imported team, an admin's own team the carry
	// moves): a bare id takes the integration's prefix.
	TeamIDOwner TeamIDMode = iota
	// TeamIDReference is the id of another team that a row names (a parent):
	// a bare id that no team moves to stays as it is.
	TeamIDReference
	// TeamIDAddress is an id a writer names to write a team (the web admin):
	// a bare id is the one existing team that holds it, else a new team of
	// the writer's integration.
	TeamIDAddress
)

// TeamIDRequest is one id to resolve and what the caller knows about it.
type TeamIDRequest struct {
	// Provider is the integration of the caller: a provider, a pushing
	// system, or the web admin (teamid.Custom). Never inferred.
	Provider string
	ID       string
	Mode     TeamIDMode
	// Holders is the prefixed teams that hold the bare id (or that it moves
	// to), by the provider their prefix names.
	Holders map[string]string
}

// ErrTeamIDForeign marks a prefixed id whose prefix is not the caller's
// provider.
var ErrTeamIDForeign = errors.New("team id carries another provider's prefix")

// ErrTeamIDNoOrigin marks a bare id that would be keyed for a caller that
// names no integration.
var ErrTeamIDNoOrigin = errors.New("team id has no provider prefix and the writer names no integration")

// ResolveTeamID is the one rule that turns a team id into the id a writer
// writes: the carry (a team's own id, an admin team, an admin edit, a
// parent), and the write seam. It keys every integration the same way: a
// bare id takes its integration's prefix (linear:, gl:, custom: for a custom
// team, pushed or the web admin's). A malformed id is refused. A
// prefixed id keeps its canonical form; an owner's is refused when its
// prefix is not the owner's. A bare id resolves, for an owner, to its
// integration's id; for an address or a reference, to the one holder; two
// holders are a conflict for an address (ErrTeamIDAmbiguous) and keep the id
// for a reference; with no holder an address is a new team of its
// integration and a reference keeps its id. A caller that resolves a
// reference inside a provider passes that provider's holder only.
func ResolveTeamID(req TeamIDRequest) (string, error) {
	if teamid.Malformed(req.ID) {
		return "", teamid.ErrMalformedTeamID
	}
	id := teamid.Of("", req.ID)
	provider := strings.TrimSpace(req.Provider)
	if teamid.HasKey(id) {
		if req.Mode == TeamIDOwner {
			if _, own := teamid.NativeKey(provider, id); !own {
				return "", ErrTeamIDForeign
			}
		}
		return id, nil
	}
	if req.Mode != TeamIDOwner {
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
		}
	}
	if provider == "" {
		return "", ErrTeamIDNoOrigin
	}
	return teamid.Of(provider, id), nil
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
