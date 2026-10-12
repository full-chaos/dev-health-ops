package providersync

import (
	"context"
	"log/slog"
	"sort"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// The retraction guard of the derived team_repo_ownership rows (CHAOS-9045).
//
// A derived row is retracted when this run no longer derives it. That is a
// proof of absence only when this run read the inputs that could derive it.
// The inputs are per provider (the open project links of that provider) and
// per fact kind (the project_id arm reads project links; the linear_team_key
// arm reads Linear work items and the Linear team catalog). One count over the
// links of every provider together is no proof for any one of them: in an org
// with two providers, provider A with no open link and provider B with one
// let a run retract every derived row of A.
//
// So each kind has its own SnapshotProof, made from two named terms: the
// kind's own input is in the run, and the run reached at least one repository
// through it (a team resolved through the kind for a pull request's repository
// or an issue's linked pull request). The second term covers the rest of the
// chain: a provider whose links are open but whose work items or links to
// pull requests are not in the run reaches nothing and proves nothing.
//
//	project_links:<provider>  an open project link of the provider; a repository reached through it
//	linear_team_key           a Linear work item whose native team key names a current Linear team; a repository reached through it
//
// A kind that is in the run and reaches no repository closes nothing, the
// same as an empty answer of a snapshot kind (EmptyClosesNothing).
//
// The kinds of an open derived row are the kinds that can have derived it:
// project_links:<P> for every provider P whose project links (open or closed:
// a close does not change who the link named) ever named the row's team, and
// linear_team_key when the team is a Linear team. A row is retracted only when
// the proof of EVERY one of its kinds holds. A team that no project link ever
// named and that is no Linear team has no kind; its row is retracted only when
// the run holds an open project link of any provider (the guard before
// CHAOS-9045, kept for this case only: no provider's input can derive the row).

const (
	teamRepoOwnershipRetractionKindProjectLinks  = "project_links"
	teamRepoOwnershipRetractionKindLinearTeamKey = "linear_team_key"
	teamRepoOwnershipRetractionKindNone          = "no_kind"

	// TeamRepoOwnershipNoOpenProjectLinkOfTheProvider is the reason of a
	// project_links kind whose provider has no open project link in the run.
	TeamRepoOwnershipNoOpenProjectLinkOfTheProvider = "no_open_project_link_of_the_provider"
	// TeamRepoOwnershipNoRepositoryReached is the reason of a kind whose input
	// reached no repository in the run.
	TeamRepoOwnershipNoRepositoryReached = "no_repository_reached_through_the_kind"
	// TeamRepoOwnershipNoKnownLinearTeamKey is the reason of the
	// linear_team_key kind when no Linear work item names a current Linear team.
	TeamRepoOwnershipNoKnownLinearTeamKey = "no_linear_work_item_with_a_known_team_key"
	// TeamRepoOwnershipNoOpenProjectLink is the reason of a row of no kind when
	// the run holds no open project link at all.
	TeamRepoOwnershipNoOpenProjectLink = "no_open_project_link"

	// TeamRepoOwnershipRetractionHeldLog is the WARN line of a kind that kept
	// open derived rows the run does not derive.
	TeamRepoOwnershipRetractionHeldLog = "team_repo_ownership_derivation.retraction_held"

	teamRepoOwnershipRetractionHeldName = "team_repo_ownership_retraction_held_total"
)

// TeamRepoOwnershipTeamLinkProvider is one (provider, team) pair that a
// team_project_ownership row of the org names, open or closed.
type TeamRepoOwnershipTeamLinkProvider struct {
	Provider string
	TeamID   string
}

// teamRepoOwnershipRetractionKind is one kind of input that can derive a row.
// Provider is set for the project_links kind only.
type teamRepoOwnershipRetractionKind struct {
	Kind, Provider string
}

func (kind teamRepoOwnershipRetractionKind) name() string {
	if kind.Provider == "" {
		return kind.Kind
	}
	return kind.Kind + ":" + kind.Provider
}

type teamRepoOwnershipRetractionGuard struct {
	proofs    map[teamRepoOwnershipRetractionKind]SnapshotProof
	teamKinds map[string][]teamRepoOwnershipRetractionKind
}

// newTeamRepoOwnershipRetractionGuard makes the proof of every kind from the
// run's inputs and what the derivation reached through each kind, and the
// kinds of every team from the org's link history.
func newTeamRepoOwnershipRetractionGuard(
	reached map[teamRepoOwnershipRetractionKind]int,
	projectLinks []TeamRepoOwnershipProjectLink,
	linkHistory []TeamRepoOwnershipTeamLinkProvider,
	workItems []TeamRepoOwnershipWorkItem,
	knownTeams []TeamRepoOwnershipKnownTeam,
) teamRepoOwnershipRetractionGuard {
	openLinks := map[string]int{}
	for _, link := range projectLinks {
		if link.ProjectID != "" && link.TeamID != "" {
			openLinks[link.Provider]++
		}
	}
	guard := teamRepoOwnershipRetractionGuard{
		proofs:    map[teamRepoOwnershipRetractionKind]SnapshotProof{},
		teamKinds: map[string][]teamRepoOwnershipRetractionKind{},
	}
	providerKind := func(provider string) teamRepoOwnershipRetractionKind {
		kind := teamRepoOwnershipRetractionKind{Kind: teamRepoOwnershipRetractionKindProjectLinks, Provider: provider}
		if _, made := guard.proofs[kind]; !made {
			guard.proofs[kind] = ProveSnapshot(
				SnapshotTerm{Holds: openLinks[provider] > 0, Reason: TeamRepoOwnershipNoOpenProjectLinkOfTheProvider},
				SnapshotTerm{Holds: reached[kind] > 0, Reason: TeamRepoOwnershipNoRepositoryReached})
		}
		return kind
	}
	for provider := range openLinks {
		providerKind(provider)
	}
	seen := map[teamRepoOwnershipRetractionKind]map[string]bool{}
	addTeamKind := func(teamID string, kind teamRepoOwnershipRetractionKind) {
		if seen[kind] == nil {
			seen[kind] = map[string]bool{}
		}
		if !seen[kind][teamID] {
			seen[kind][teamID] = true
			guard.teamKinds[teamID] = append(guard.teamKinds[teamID], kind)
		}
	}
	for _, link := range linkHistory {
		if link.TeamID != "" {
			addTeamKind(link.TeamID, providerKind(link.Provider))
		}
	}
	for _, link := range projectLinks {
		if link.ProjectID != "" && link.TeamID != "" {
			addTeamKind(link.TeamID, providerKind(link.Provider))
		}
	}
	linearKind := teamRepoOwnershipRetractionKind{Kind: teamRepoOwnershipRetractionKindLinearTeamKey}
	guard.proofs[linearKind] = ProveSnapshot(
		SnapshotTerm{Holds: hasResolvableLinearNativeTeamKey(workItems, knownTeams), Reason: TeamRepoOwnershipNoKnownLinearTeamKey},
		SnapshotTerm{Holds: reached[linearKind] > 0, Reason: TeamRepoOwnershipNoRepositoryReached})
	for _, teamID := range knownLinearTeamIDsByKey(knownTeams) {
		addTeamKind(teamID, linearKind)
	}
	noKind := teamRepoOwnershipRetractionKind{Kind: teamRepoOwnershipRetractionKindNone}
	guard.proofs[noKind] = ProveSnapshot(SnapshotTerm{Holds: len(openLinks) > 0, Reason: TeamRepoOwnershipNoOpenProjectLink})
	return guard
}

// kindsOf is the kinds that can have derived a row of the team.
func (guard teamRepoOwnershipRetractionGuard) kindsOf(teamID string) []teamRepoOwnershipRetractionKind {
	kinds := guard.teamKinds[teamID]
	if strings.HasPrefix(strings.TrimSpace(teamID), teamid.Prefix("linear")) {
		linearKind := teamRepoOwnershipRetractionKind{Kind: teamRepoOwnershipRetractionKindLinearTeamKey}
		has := false
		for _, kind := range kinds {
			has = has || kind == linearKind
		}
		if !has {
			kinds = append(append([]teamRepoOwnershipRetractionKind(nil), kinds...), linearKind)
		}
	}
	if len(kinds) == 0 {
		return []teamRepoOwnershipRetractionKind{{Kind: teamRepoOwnershipRetractionKindNone}}
	}
	return kinds
}

// teamRepoOwnershipRetractionHeld counts, per kind whose proof does not hold,
// the open rows it kept, with the reasons of the proof.
type teamRepoOwnershipRetractionHeld struct {
	Kind     string
	Provider string
	Reasons  []string
	Rows     int
}

// retain keeps the retractions whose every kind is proven. A row is held by
// each of its unproven kinds, and counted once in held.
func (guard teamRepoOwnershipRetractionGuard) retain(toRetract []teamRepoOwnershipActiveRow) ([]teamRepoOwnershipActiveRow, int, []teamRepoOwnershipRetractionHeld) {
	var kept []teamRepoOwnershipActiveRow
	heldRows := 0
	byKind := map[teamRepoOwnershipRetractionKind]*teamRepoOwnershipRetractionHeld{}
	for _, row := range toRetract {
		proven := true
		for _, kind := range guard.kindsOf(row.TeamID) {
			proof, made := guard.proofs[kind]
			if made && proof.Proven() {
				continue
			}
			proven = false
			held := byKind[kind]
			if held == nil {
				held = &teamRepoOwnershipRetractionHeld{Kind: kind.Kind, Provider: kind.Provider, Reasons: proof.Missing()}
				byKind[kind] = held
			}
			held.Rows++
		}
		if proven {
			kept = append(kept, row)
			continue
		}
		heldRows++
	}
	held := make([]teamRepoOwnershipRetractionHeld, 0, len(byKind))
	for _, entry := range byKind {
		held = append(held, *entry)
	}
	sort.Slice(held, func(i, j int) bool {
		if held[i].Kind != held[j].Kind {
			return held[i].Kind < held[j].Kind
		}
		return held[i].Provider < held[j].Provider
	})
	return kept, heldRows, held
}

// reportTeamRepoOwnershipRetractionHeld makes every held retraction loud: one
// WARN line and one count per reason for each kind that kept a row.
func reportTeamRepoOwnershipRetractionHeld(ctx context.Context, orgID string, held []teamRepoOwnershipRetractionHeld) {
	if len(held) == 0 {
		return
	}
	var counter metric.Int64Counter
	if built, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/providersync").Int64Counter(
		teamRepoOwnershipRetractionHeldName,
		metric.WithDescription("Derived team_repo_ownership rows a run did not derive and kept open, because the input of a kind that can derive them was not in the run, by kind, provider and reason.")); err == nil {
		counter = built
	}
	for _, entry := range held {
		slog.Default().WarnContext(ctx, TeamRepoOwnershipRetractionHeldLog,
			"org_id", orgID, "kind", entry.Kind, "provider", entry.Provider,
			"reasons", strings.Join(entry.Reasons, ","), "open_rows_kept", entry.Rows)
		if counter == nil {
			continue
		}
		for _, reason := range entry.Reasons {
			counter.Add(ctx, int64(entry.Rows), metric.WithAttributes(
				attribute.String("kind", entry.Kind), attribute.String("provider", entry.Provider), attribute.String("reason", reason)))
		}
	}
}
