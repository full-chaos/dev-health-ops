package policy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const platformDecisionsKey contextKey = 102

// platformNamespace is the enforcement cell of every platform action.
const platformNamespace = "admin"

// ErrPlatformDecisionUnavailable is HoldsPlatform's error when the decision point gave no verdict.
var ErrPlatformDecisionUnavailable = errors.New("policy: platform decision unavailable")

// platformDecisions answers the platform questions of one request from the decision point. The guard installs it
// when the admin cell is on; each action is asked at most once per request.
type platformDecisions struct {
	enforcer *Enforcer
	bearer   string
	// judged is false for a caller the decision point cannot judge (a legacy HS256 token): it keeps the local set,
	// as its route keeps the legacy answer.
	judged bool
	mu     sync.Mutex
	held   map[string]bool
}

func (d *platformDecisions) seed(action string, held bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.held == nil {
		d.held = map[string]bool{}
	}
	d.held[action] = held
}

func (d *platformDecisions) holds(ctx context.Context, action string) (bool, error) {
	if !rule.PlatformOf(action).Has(action) {
		return false, nil
	}
	d.mu.Lock()
	held, known := d.held[action]
	d.mu.Unlock()
	if known {
		return held, nil
	}
	held, ok := d.enforcer.askPlatform(ctx, d.bearer, action)
	if !ok {
		return false, ErrPlatformDecisionUnavailable
	}
	d.seed(action, held)
	return held, nil
}

// HoldsPlatform reports whether the caller holds the platform action now. With the admin enforcement cell on it is
// the decision point's answer for this request, so a cross-org read or write is decided by the platform action only;
// with the cell off, or for a caller the decision point cannot judge, it is user.Holds. On an error the handler
// refuses with WritePlatformDecisionUnavailable.
func HoldsPlatform(ctx context.Context, user *User, action string) (bool, error) {
	d, _ := ctx.Value(platformDecisionsKey).(*platformDecisions)
	if d == nil || !d.judged {
		return user.Holds(action), nil
	}
	return d.holds(ctx, action)
}

// PlatformBypassesOrg reports whether a platform action stands in for organization power (a caller who is no admin
// of the organization acts as one). With the admin enforcement cell on it never does for a caller the decision point
// judges: cross-org organization access stays refused until the open question on superuser access without a
// membership is answered. With the cell off it is user.Holds.
func PlatformBypassesOrg(ctx context.Context, user *User, action string) bool {
	if d, _ := ctx.Value(platformDecisionsKey).(*platformDecisions); d != nil && d.judged {
		return false
	}
	return user.Holds(action)
}

// WithPlatformDecider returns ctx carrying a platform decision point for bearer, as the guard installs it with the
// admin cell on. For tests of area handlers.
func WithPlatformDecider(ctx context.Context, decider pep.Decider, bearer string) context.Context {
	return context.WithValue(ctx, platformDecisionsKey, &platformDecisions{
		enforcer: &Enforcer{decider: decider, timeout: enforceDefaultTimeout, logger: slog.Default()}, bearer: bearer, judged: true})
}

// WritePlatformDecisionUnavailable answers a request whose platform decision could not be had: a 503 with a retry
// hint, as an enforced route that gets no verdict.
func WritePlatformDecisionUnavailable(w http.ResponseWriter) {
	WriteDetail(w, http.StatusServiceUnavailable, enforceUnavailableDetail, http.Header{"Retry-After": []string{enforceRetryAfter}})
}

// platformDecisionsFor is the request's platform decisions, or nil when the admin cell is off.
func (e *Enforcer) platformDecisionsFor(bearer string, user *User) *platformDecisions {
	if e == nil || !e.sw.On(platformNamespace) {
		return nil
	}
	return &platformDecisions{enforcer: e, bearer: bearer, judged: bearer != "" && user.accessToken}
}

// askPlatform asks the decision point one platform question. ok is false when it gave no verdict: a failure, a
// local decision, no decision point client, or an allow with an obligation (a platform allow carries none).
func (e *Enforcer) askPlatform(ctx context.Context, bearer, action string) (held, ok bool) {
	if e.unconfigured || e.decider == nil {
		return false, false
	}
	decision, failed := e.ask(ctx, pep.Request{PrincipalReference: bearer, Action: action, ResourceType: policycatalog.ScopePlatform, ResourceKey: shadowPlatformKey})
	if failed || decision.Local() {
		e.logger.WarnContext(ctx, "api enforcement: no verdict for a platform question, the handler refuses", slog.String("action", action))
		return false, false
	}
	if decision.Allowed() && !obligationsCarriedOut(decision.Obligations(), pep.Request{}, false) {
		e.logger.WarnContext(ctx, "api enforcement: a platform allow carries an obligation, no verdict", slog.String("action", action))
		return false, false
	}
	return decision.Allowed(), true
}
