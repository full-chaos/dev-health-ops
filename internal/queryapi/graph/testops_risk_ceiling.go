package graph

import (
	"context"

	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopsrisk"
)

// refuseTeamsUnderCeiling applies a caller's repository ceiling (the allowlist of its decided scope) to the scope
// arguments of testopsRisk (CHAOS-13814):
//   - no ceiling: the input is served as sent;
//   - a ceiling and a team argument: refused at once, in the form of a decision point's deny (FORBIDDEN), before any
//     read, so the ownership tables are never touched for a ceilinged caller;
//   - a ceiling and repoIds: the ids that name a repository of the ceiling, never more; none left matches no
//     repository (Options.MatchNothing), it does not become the organization;
//   - a ceiling and no argument: served as sent.
//
// A blank team id is no team argument (teamscope drops it, so no ownership read follows).
func refuseTeamsUnderCeiling(ctx context.Context, input model.TestOpsRiskInput) (model.TestOpsRiskInput, testopsrisk.Options, error) {
	claims, ok := authctx.FromContext(ctx)
	if !ok || claims.Scope.Kind() != pepgate.KindAllowlist {
		return input, testopsrisk.Options{}, nil
	}
	for _, id := range input.TeamIds {
		if id != "" {
			return input, testopsrisk.Options{}, &gqlerror.Error{Message: "Not authorized", Extensions: map[string]any{"code": CodeForbidden}}
		}
	}
	if len(input.RepoIds) == 0 {
		return input, testopsrisk.Options{}, nil
	}
	allowed := map[string]bool{}
	for _, key := range claims.Scope.RepoKeys() {
		allowed[key] = true
	}
	var kept []string
	for _, id := range input.RepoIds {
		if allowed[id] {
			kept = append(kept, id)
		}
	}
	input.RepoIds = kept
	return input, testopsrisk.Options{MatchNothing: len(kept) == 0}, nil
}
