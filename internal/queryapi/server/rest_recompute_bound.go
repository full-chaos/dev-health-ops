package server

import (
	"errors"
	"log"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
)

// restReadBoundDetail is the cause a 503 names when the read hit a bound: the stated reason of a ceiling recompute
// first, else the bound of the store client (logRESTBoundHit).
func restReadBoundDetail(r *http.Request, component, orgID string, err error) (string, bool) {
	if errors.Is(err, linkeditems.ErrRecomputeBound) {
		log.Printf("query-api: WARN %s: the ceiling recompute hit its bound: org_id=%s request_id=%s err=%v",
			component, orgID, envelopeRequestID(r), err)
		return linkeditems.ErrRecomputeBound.Error(), true
	}
	return logRESTBoundHit(r, component, orgID, err)
}
