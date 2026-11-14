package policy

import (
	"context"
	"errors"
	"fmt"
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

// ErrPlatformNoDecisionPoint is HoldsPlatform's error when go-api has no decision point client. It wraps
// ErrPlatformDecisionUnavailable.
var ErrPlatformNoDecisionPoint = fmt.Errorf("%w: no decision point client", ErrPlatformDecisionUnavailable)

// platformDecisions answers the platform questions of one request from the decision point. The guard installs it
// when the admin cell is on and the decision point can judge the caller; each action is asked at most once per request.
type platformDecisions struct {
	enforcer *Enforcer
	bearer   string
	mu       sync.Mutex
	held     map[string]bool
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
	held, err := d.enforcer.askPlatform(ctx, d.bearer, action)
	if err != nil {
		return false, err
	}
	d.seed(action, held)
	return held, nil
}

// HoldsPlatform reports whether the caller holds the platform action now. With the admin enforcement cell on it is
// the decision point's answer for this request; with the cell off, and for a caller the decision point cannot judge
// (a legacy HS256 token), it is user.Holds, main's answer. On an error the handler refuses with
// WritePlatformDecisionRefusal.
func HoldsPlatform(ctx context.Context, user *User, action string) (bool, error) {
	d, _ := ctx.Value(platformDecisionsKey).(*platformDecisions)
	if d == nil {
		return user.Holds(action), nil
	}
	return d.holds(ctx, action)
}

// WithPlatformDecider returns ctx carrying a platform decision point for bearer, as the guard installs it with the
// admin cell on. For tests of area handlers.
func WithPlatformDecider(ctx context.Context, decider pep.Decider, bearer string) context.Context {
	return context.WithValue(ctx, platformDecisionsKey, &platformDecisions{
		enforcer: &Enforcer{decider: decider, timeout: enforceDefaultTimeout, logger: slog.Default()}, bearer: bearer})
}

// WritePlatformDecisionRefusal answers a request whose platform question got no verdict as an enforced route that gets
// none: with no decision point client, the 503 with its reason code and no retry hint; else the 503 with a retry hint.
func WritePlatformDecisionRefusal(w http.ResponseWriter, err error) {
	verdict := enforceUnavailable
	// ErrPlatformNoDecisionPoint wraps ErrPlatformDecisionUnavailable, so it is tested first.
	switch {
	case errors.Is(err, ErrPlatformNoDecisionPoint):
		verdict = enforceNoDecisionPoint
	case errors.Is(err, ErrPlatformDecisionUnavailable):
		verdict = enforceUnavailable
	}
	writeEnforceRefusal(w, nil, Superuser, verdict, DenyText{})
}

// platformDecisionsFor is the request's platform decisions, or nil when the admin cell is off or the decision point
// cannot judge the caller (a legacy HS256 token keeps main's answer, as on an enforced row). A read by a caller with a
// live go-api impersonation row is nil too: the decision point would judge the admin's own bearer, not the session, so
// that read keeps the legacy answer end to end, its handler's platform questions included.
func (e *Enforcer) platformDecisionsFor(r *http.Request, bearer string, user *User) *platformDecisions {
	if e == nil || !e.sw.On(platformNamespace) || bearer == "" || !user.accessToken {
		return nil
	}
	if ImpersonationFrom(r.Context()) != nil && readMethod(r.Method) {
		return nil
	}
	return &platformDecisions{enforcer: e, bearer: bearer}
}

// askPlatform asks the decision point one platform question. The error is ErrPlatformNoDecisionPoint with no decision
// point client, and ErrPlatformDecisionUnavailable when it gave no verdict: a failure, a local decision, or an allow
// with an obligation (a platform allow carries none).
func (e *Enforcer) askPlatform(ctx context.Context, bearer, action string) (held bool, err error) {
	if e.unconfigured || e.decider == nil {
		return false, ErrPlatformNoDecisionPoint
	}
	decision, failed := e.ask(ctx, pep.Request{PrincipalReference: bearer, Action: action, ResourceType: policycatalog.ScopePlatform, ResourceKey: shadowPlatformKey})
	if failed || decision.Local() {
		e.logger.WarnContext(ctx, "api enforcement: no verdict for a platform question, the handler refuses", slog.String("action", action))
		return false, ErrPlatformDecisionUnavailable
	}
	if decision.Allowed() && !obligationsCarriedOut(decision.Obligations(), pep.Request{}, false, false) {
		e.logger.WarnContext(ctx, "api enforcement: a platform allow carries an obligation, no verdict", slog.String("action", action))
		return false, ErrPlatformDecisionUnavailable
	}
	return decision.Allowed(), nil
}
