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
// *DenyError), any missing verdict or obligation the gate does not carry out refuses it as unavailable (a local
// *DenyError); the allows merge into one scope (allowlist = intersection, organization with an allowlist = the
// allowlist) that every enforced root gets, and an allowlist is carried only when every enforced root is in the
// registry. Exempt callers never ask and are counted.
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
	}
	decisions := map[string]pep.Decision{}
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
				return nil, &DenyError{Reason: decision.Reason()}
			}
		}
		scope, refused := g.scopeOf(decision, root.Key, root.Action, orgID)
		if refused != "" {
			g.count(ctx, ModeEnforce, ResultNotCarriedOut, root.Action)
			return nil, &DenyError{Reason: refused, Local: true}
		}
		if !asked {
			g.count(ctx, ModeEnforce, ResultAllow, root.Action)
		}
		merged = mergeScopes(merged, scope)
	}
	if merged.Kind() == pepgate.KindAllowlist {
		for _, root := range enforced {
			if !g.carries(root.Key) {
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
	}
	return scopes, nil
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
