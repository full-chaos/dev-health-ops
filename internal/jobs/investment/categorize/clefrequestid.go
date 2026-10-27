package categorize

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Sources of a clef attempt's request id.
const (
	RequestIDSourceAIG    = "aig"
	RequestIDSourceRay    = "ray"
	RequestIDSourceClient = "client"
)

// clefCall is the state of one send: the attempts of one call share it.
type clefCall struct {
	start time.Time
	// seen holds the peer ids (aig, ray) that an earlier attempt of this call
	// already took, so a repeated peer id never gives two attempts the same id.
	seen map[string]struct{}
}

var nonRandomIDCounter atomic.Uint64

// newClientRequestID is the id the client mints for one attempt: a UUID v7.
// When the random source fails (uuid.NewV7 returns an error) it does not panic
// and does not retry a UUID: it returns a NON-random id made of the "nr" prefix,
// the call start time (UTC, nanoseconds) and a process-wide counter, which is
// unique for each attempt in one process. It logs one Warn without the id.
func (c *TypeSafeClient) newClientRequestID(ctx context.Context, call *clefCall) string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	c.logger.WarnContext(ctx, "clef client: the random source failed; the request id of this attempt is not random",
		slog.String("provider", string(c.target.provider)))
	return "nr" + strconv.FormatInt(call.start.UnixNano(), 10) + "-" + strconv.FormatUint(nonRandomIDCounter.Add(1), 10)
}

// clefRequestID picks the id of a clef attempt: the AI Gateway log id (only
// when the call is routed through the gateway; no target is routed yet), else
// the Cloudflare ray id, else the client id of the attempt. A header that is
// empty, not id-shaped, or already used by an earlier attempt of the same call
// falls through to the next source; it never errors.
func (c *TypeSafeClient) clefRequestID(header http.Header, clientID string, call *clefCall) (id, source string) {
	if c.target.viaGateway {
		if id, ok := c.usableRequestID(header.Get(cfAIGLogIDHeader), call); ok {
			return id, RequestIDSourceAIG
		}
	}
	if id, ok := c.usableRequestID(header.Get(cfRayHeader), call); ok {
		return id, RequestIDSourceRay
	}
	return clientID, RequestIDSourceClient
}

func (c *TypeSafeClient) usableRequestID(raw string, call *clefCall) (string, bool) {
	id, rejected := c.cleanRequestID(raw)
	if id == "" || rejected {
		return "", false
	}
	if _, used := call.seen[id]; used {
		return "", false
	}
	call.seen[id] = struct{}{}
	return id, true
}
