package policy

import (
	"net/http"
	"sort"
	"sync"
)

// DenyText replaces the body text of a row's own 403 refusals, and only the text: the status, the decision point
// calls and the 503 answers are the guard's. An empty field is the guard's text for that refusal. A row sets it to keep
// the text its handler wrote before the decision point decided the row (product rule 1: the same answers as legacy).
type DenyText struct {
	// Denied is the text of a caller refused for not holding the action (the legacy platform refusal and the decision
	// point's deny).
	Denied string
	// Impersonated is the text of a caller refused for a live impersonation session.
	Impersonated string
	// Refused is called, with the request, for each of those two refusals, as the handler's own refusal counted and
	// logged them. It changes no answer.
	Refused func(*http.Request)
}

func (d DenyText) zero() bool { return d.Denied == "" && d.Impersonated == "" && d.Refused == nil }

func (d DenyText) denied(fallback string) string {
	if d.Denied != "" {
		return d.Denied
	}
	return fallback
}

func (d DenyText) impersonated() string {
	if d.Impersonated != "" {
		return d.Impersonated
	}
	return msgImpersonationDenied
}

func (d DenyText) refused(r *http.Request) {
	if d.Refused != nil && r != nil {
		d.Refused(r)
	}
}

var denyRows = struct {
	sync.Mutex
	keys map[string]bool
}{keys: map[string]bool{}}

func recordDenyRow(key string, deny DenyText) {
	if deny.zero() {
		return
	}
	denyRows.Lock()
	defer denyRows.Unlock()
	denyRows.keys[key] = true
}

// DenyTextRows are the routes ("METHOD /route") mounted with a DenyText in this process, so a census can require an
// executed cell for each.
func DenyTextRows() []string {
	denyRows.Lock()
	defer denyRows.Unlock()
	rows := make([]string, 0, len(denyRows.keys))
	for key := range denyRows.keys {
		rows = append(rows, key)
	}
	sort.Strings(rows)
	return rows
}

// WrapPlatformDeny is WrapPlatform for the route key ("METHOD /route"), whose 403 refusals carry deny's text.
func (g *Guard) WrapPlatformDeny(key, action string, deny DenyText, next http.Handler) http.Handler {
	recordDenyRow(key, deny)
	return g.wrap(Superuser, gate{action: action, kind: gatePlatform, deny: deny}, next)
}

// BodyFirstDeny is BodyFirst over Wrap(level, ...) for the route key ("METHOD /route"), whose refusals by the decision
// point carry deny's text.
func (g *Guard) BodyFirstDeny(key string, level Authz, deny DenyText, next http.Handler) http.Handler {
	recordDenyRow(key, deny)
	return g.bodyFirstOver(g.wrap(level, gate{deny: deny}, next))
}
