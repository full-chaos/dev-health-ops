package policy

import (
	"log/slog"
	"net/http"
)

// delegatedUseEvent is the event field of the line noteDelegatedUse writes.
const delegatedUseEvent = "api_delegated_use"

// noteDelegatedUse writes one line for a request the Guard is about to serve to a
// delegated bearer on a row the decision point does not decide. A plain bearer
// has no actor chain and writes none; a request the decision point decided is
// recorded by its authorize audit row. It carries ids only.
func (g *Guard) noteDelegatedUse(r *http.Request, user *User) {
	chain, ok := user.ActorChain()
	if !ok {
		return
	}
	g.logger.InfoContext(r.Context(), "api delegated use", slog.String("event", delegatedUseEvent),
		slog.String("route", r.Pattern), slog.String("actor", chain.Actor),
		slog.String("target", chain.Subject), slog.String("sid", chain.SessionID))
}
