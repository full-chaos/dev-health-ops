package pdp

import (
	"context"
	"slices"
	"strings"

	"github.com/full-chaos/dev-health-go/pep"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// CallerClass is how the caller of a GraphQL operation reached query-api. It picks the cell set that decides the
// operation's roots (D6721): a user access token follows the authzenforce.QueryAPI cells (F9a), the header carrier
// the authzenforce.QueryAPIHeaders cells (F9b).
type CallerClass uint8

const (
	// CallerUnknown names no class: a root whose cell is on in either set is decided, and refused (no principal).
	CallerUnknown CallerClass = iota
	// CallerUser is a user access token: the PDP judges its bearer.
	CallerUser
	// CallerExempt is a credential class on the exempt list (ExemptClasses): it never asks.
	CallerExempt
	// CallerHeaders is the X-DH-Internal-* header carrier (the MCP listener, /query on the internal listener). It
	// has no principal the PDP can judge, so a root whose headers cell is on is refused.
	CallerHeaders
)

// Caller is the caller of one GraphQL operation.
type Caller struct {
	Class CallerClass
	// Bearer is the user access token of a CallerUser.
	Bearer string
	// ExemptClass is the ExemptClasses entry of a CallerExempt.
	ExemptClass string
	// Unscoped is true when the ingress set no scope on the request (pepgate KindNone): every ingress sets one, so an
	// operation without it is refused while a cell of any of its roots is on.
	Unscoped bool
	// LegacyImpersonation is a go-api impersonation session carried by the admin's own token: the request runs as the
	// target while its bearer is the admin's. It is not decided: it answers as main does (known flaw F1). A delegated
	// bearer is not one: the decision point judges it as the target (D6714).
	LegacyImpersonation bool
}

// Root is one root field of an operation: its key ("Query.name", "Mutation.name") and its catalog action.
type Root struct {
	Key    string
	Action string
	// LegacyRefuses is true when the root's own legacy check (the resolver's pre-resolver guard) refuses the caller:
	// a deny of its action then answers with that refusal, not FORBIDDEN (CHAOS-13334, product rule 1).
	LegacyRefuses bool
}

// The closed reasons of a refused GraphQL operation the gate makes itself.
const (
	// ReasonNoPrincipal is an enforced root whose caller has no principal the PDP can judge.
	ReasonNoPrincipal pep.Reason = "authz_enforce_no_principal"
	// ReasonNoScope is an operation whose ingress set no scope, with a cell of one of its roots on.
	ReasonNoScope pep.Reason = "authz_enforce_no_scope"
)

// rootCutOver is the GraphQL cut-over registry: the root keys whose resolvers narrow every read to a scope's
// repository allowlist. An allowlist decision on an operation with an enforced root not listed is not carried out
// (503). Empty until the resolvers take the ceiling.
var rootCutOver = map[string]bool{}

// RootEnforced reports whether a root of action is decided for a caller of class: the cell of the action's namespace
// is on in the caller's cell set. An unknown class is decided when either set has the cell on.
func (g *Gate) RootEnforced(class CallerClass, action string) bool {
	if g == nil || !orgScoped(action) {
		return false
	}
	namespace, _, _ := strings.Cut(action, ".")
	switch class {
	case CallerUser:
		return g.cells.On(namespace)
	case CallerHeaders:
		return g.headerCells.On(namespace)
	case CallerUnknown:
		return g.cells.On(namespace) || g.headerCells.On(namespace)
	}
	return false
}

// AuthorizeOperation decides the roots of one GraphQL operation whose caller is caller, in the organization orgID
// the operation is served for, and returns each root key's scope. A root whose cell is off is pepgate.Legacy(orgID).
// The enforced roots are decided together: one ask per distinct action; any deny refuses the whole operation (a
// *DenyError), except a deny of an action in legacyRefusalRoots every root of which is listed there and
// LegacyRefuses: those roots get the refusal scope (zero, KindNone) and answer with their resolver's own legacy
// refusal, the deny still counted (CHAOS-13334); any missing verdict or obligation the gate does not carry out refuses
// it as unavailable (a local *DenyError); the allows merge into one scope (allowlist = intersection, organization with
// an allowlist = the allowlist) that every enforced root gets, and an allowlist is carried only when every enforced
// root is in the registry. Exempt callers never ask and are counted.
func (g *Gate) AuthorizeOperation(ctx context.Context, caller Caller, orgID string, roots []Root) (map[string]pepgate.Scope, error) {
	scopes := make(map[string]pepgate.Scope, len(roots))
	legacy := func() map[string]pepgate.Scope {
		for _, root := range roots {
			scopes[root.Key] = pepgate.Legacy(orgID)
		}
		return scopes
	}
	if g == nil {
		return legacy(), nil
	}
	if caller.Unscoped {
		for _, root := range roots {
			if g.RootEnforced(CallerUnknown, root.Action) {
				g.count(ctx, ModeEnforce, ResultUnavailable, root.Action)
				return nil, &DenyError{Reason: ReasonNoScope, Local: true}
			}
		}
	}
	if caller.Class == CallerExempt {
		for _, action := range distinctActions(roots) {
			g.countExemptAction(ctx, caller.ExemptClass, action)
		}
		return legacy(), nil
	}
	// Known flaw F1 (13211 lists the only accepted impersonation narrowing): a legacy impersonation session answers as
	// main does. Its bearer is the admin's, so a decision would judge the actor in the target's organization.
	if caller.Class == CallerUser && caller.LegacyImpersonation {
		return legacy(), nil
	}
	var enforced []Root
	for _, root := range roots {
		if g.RootEnforced(caller.Class, root.Action) {
			enforced = append(enforced, root)
			continue
		}
		// acp ruling (1), as on the REST rows: a mode named enforce never serves a user caller's organization root on
		// the legacy path. The header carrier is judged by its own cells only (D6721); a root whose action is not
		// organization-scoped is never decided (product rule 1).
		if caller.Class == CallerUser && g.mode == ModeEnforce && orgScoped(root.Action) {
			g.count(ctx, ModeEnforce, ResultNotConfigured, root.Action)
			return nil, &DenyError{Reason: ReasonEnforceCellOff, Local: true}
		}
	}
	if len(enforced) == 0 {
		return legacy(), nil
	}
	label := enforced[0].Action
	switch {
	case g.mode == modeInvalid:
		g.count(ctx, modeInvalid, ResultNotConfigured, label)
		return nil, &DenyError{Reason: ReasonNoDecisionPoint, Local: true}
	case caller.Class != CallerUser || caller.Bearer == "":
		g.count(ctx, ModeEnforce, ResultUnavailable, label)
		return nil, &DenyError{Reason: ReasonNoPrincipal, Local: true}
	case g.decider == nil:
		g.count(ctx, ModeEnforce, ResultNoPDP, label)
		return nil, &DenyError{Reason: ReasonNoDecisionPoint, Local: true}
	case orgID == "":
		// Only a superuser passes the verifier with no organization: its roots keep the legacy answer, as an enforced
		// REST row does (product rule 1).
		g.count(ctx, ModeEnforce, ResultNoOrg, label)
		return legacy(), nil
	}
	decisions := map[string]pep.Decision{}
	refused := map[string]bool{}
	var merged pepgate.Scope
	for _, root := range enforced {
		decision, asked := decisions[root.Action]
		if !asked {
			decision = g.decider.Decide(ctx, g.request(root.Action, caller.Bearer, orgID))
			decisions[root.Action] = decision
			switch {
			case decision.Allowed():
			case decision.Local():
				g.count(ctx, ModeEnforce, ResultUnavailable, root.Action)
				return nil, &DenyError{Reason: decision.Reason(), Local: true}
			default:
				g.count(ctx, ModeEnforce, ResultDeny, root.Action)
				if !legacyRefusesAll(enforced, root.Action) {
					return nil, &DenyError{Reason: decision.Reason()}
				}
			}
		}
		if !decision.Allowed() {
			refused[root.Key] = true
			continue
		}
		scope, notCarried, _ := g.scopeOf(decision, root.Key, root.Action, orgID)
		if notCarried != "" {
			g.count(ctx, ModeEnforce, ResultNotCarriedOut, root.Action)
			return nil, &DenyError{Reason: notCarried, Local: true}
		}
		if !asked {
			g.count(ctx, ModeEnforce, ResultAllow, root.Action)
		}
		merged = mergeScopes(merged, scope)
	}
	if merged.Kind() == pepgate.KindAllowlist {
		for _, root := range enforced {
			if !refused[root.Key] && !g.carries(root.Key) {
				g.count(ctx, ModeEnforce, ResultNotCarriedOut, root.Action)
				return nil, &DenyError{Reason: ReasonNotCarriedOut, Local: true}
			}
		}
	}
	for _, root := range roots {
		scopes[root.Key] = pepgate.Legacy(orgID)
	}
	for _, root := range enforced {
		scopes[root.Key] = merged
		if refused[root.Key] {
			scopes[root.Key] = pepgate.Scope{}
		}
	}
	return scopes, nil
}

// legacyRefusalRoots is the closed registry of the legacy-refusal path (CHAOS-13334): per catalog action, the root keys
// whose resolver runs its own pre-resolver guard (graph's fieldPrechecks) before any read or write. A deny of an action
// not listed, or of an operation naming a root of the action that is not listed, is FORBIDDEN. graph's census test
// fails when a root mapped to a listed action is not listed or has no guard.
var legacyRefusalRoots = map[string][]string{
	"integrations.read": {"Query.dataHealth"},
	"reports.write": {"Mutation.cloneSavedReport", "Mutation.createSavedReport", "Mutation.deleteSavedReport",
		"Mutation.triggerReport", "Mutation.updateSavedReport"},
}

// LegacyRefusalRoots is a copy of the legacy-refusal registry, for the census.
func LegacyRefusalRoots() map[string][]string {
	out := make(map[string][]string, len(legacyRefusalRoots))
	for action, keys := range legacyRefusalRoots {
		out[action] = slices.Clone(keys)
	}
	return out
}

// legacyRefusesAll reports whether every root of action in roots (the caller passes at least one) is listed in the
// legacy-refusal registry entry of action and has a legacy check that refuses the caller. An action not in the
// registry has no entry, so none of its roots is listed: false.
func legacyRefusesAll(roots []Root, action string) bool {
	keys := legacyRefusalRoots[action]
	for _, root := range roots {
		if root.Action == action && (!root.LegacyRefuses || !slices.Contains(keys, root.Key)) {
			return false
		}
	}
	return true
}

// orgScoped reports whether a root's action is organization-scoped: only those are decided. A platform action (the
// platform dashboard) stays on its legacy decision, RequirePlatformAdmin, whatever the cells (product rule 1). An
// action the catalog does not know counts as organization-scoped (decided, so refused when it cannot be).
func orgScoped(action string) bool {
	catalogued, err := policycatalog.Default().Action(action)
	return err != nil || catalogued.ResourceKind == policycatalog.ScopeOrganization
}

// carries reports whether profile (a REST row or a GraphQL root key) is in a cut-over registry.
func (g *Gate) carries(profile string) bool {
	return g.carriesAllowlist[profile] || g.carriesRoots[profile]
}

// mergeScopes is the scope of two allows of one operation, both for the same organization: an allowlist narrows an
// organization-wide scope, and two allowlists are their intersection.
func mergeScopes(a, b pepgate.Scope) pepgate.Scope {
	switch {
	case a.Kind() == pepgate.KindNone:
		return b
	case b.Kind() != pepgate.KindAllowlist:
		return a
	case a.Kind() != pepgate.KindAllowlist:
		return b
	}
	org, _ := a.OrgID()
	other := b.RepoKeys()
	var keys []string
	for _, key := range a.RepoKeys() {
		if slices.Contains(other, key) {
			keys = append(keys, key)
		}
	}
	return pepgate.Allowlist(org, keys)
}

func distinctActions(roots []Root) []string {
	var out []string
	for _, root := range roots {
		if !slices.Contains(out, root.Action) {
			out = append(out, root.Action)
		}
	}
	return out
}

func (g *Gate) countExemptAction(ctx context.Context, class, action string) {
	if g.exempt == nil || !slices.Contains(ExemptClasses, class) {
		return
	}
	g.exempt.Add(ctx, 1, metric.WithAttributes(attribute.String("class", class), attribute.String("action", action)))
}
