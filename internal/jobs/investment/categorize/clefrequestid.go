package categorize

import (
	"net/http"

	"github.com/google/uuid"
)

// Sources of a clef attempt's request id.
const (
	RequestIDSourceAIG    = "aig"
	RequestIDSourceRay    = "ray"
	RequestIDSourceClient = "client"
)

// newClientRequestID is the id the client mints for one attempt: a UUID v7.
// uuid.NewV7 fails only when the system random source fails; the plain v4 id is
// the last resort so an attempt never goes without an id.
func newClientRequestID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}

// clefRequestID picks the id of a clef attempt: the AI Gateway log id (only
// when the call is routed through the gateway; no target is routed yet), else
// the Cloudflare ray id, else the client id of the attempt. A header that is
// empty or not id-shaped falls through to the next source; it never errors.
func (c *TypeSafeClient) clefRequestID(header http.Header, clientID string) (id, source string) {
	if c.target.viaGateway {
		if id, ok := c.usableRequestID(header.Get(cfAIGLogIDHeader)); ok {
			return id, RequestIDSourceAIG
		}
	}
	if id, ok := c.usableRequestID(header.Get(cfRayHeader)); ok {
		return id, RequestIDSourceRay
	}
	return clientID, RequestIDSourceClient
}

func (c *TypeSafeClient) usableRequestID(raw string) (string, bool) {
	id, rejected := c.cleanRequestID(raw)
	return id, id != "" && !rejected
}
