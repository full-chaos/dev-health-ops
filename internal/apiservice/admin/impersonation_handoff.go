package admin

import (
	"context"
	"errors"
	"net/http"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// handoffKeyPrefix keys one start handoff record by its id.
const handoffKeyPrefix = "impersonation:handoff:"

// handoffTTL bounds a start handoff record. The web's sealed handoff lives 120 s, so the record outlives it.
const handoffTTL = 150 * time.Second

// handoffConsumeLua deletes the record only when it names the caller: 1 admits, 0 refuses (no record, used, expired,
// another admin's).
const handoffConsumeLua = `if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`

var errHandoffStoreUnavailable = errors.New("admin: impersonation handoff store unavailable")

// valkeyHandoffs is the one-use record of each impersonation start handoff, in go-api's Valkey.
type valkeyHandoffs struct{ client valkeygo.Client }

func (s valkeyHandoffs) record(context.Context, string, string) error {
	return errHandoffStoreUnavailable
}

func (s valkeyHandoffs) consume(context.Context, string, string) (bool, error) {
	return false, errHandoffStoreUnavailable
}

// handoffRoutes is the consume route of the start handoff. It is not mounted yet: a new route lands
// first, its endpoint-profile row and the mount follow.
func (h *handlers) handoffRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: impersonationPrefix + "/impersonate/handoff/consume",
			Handler: h.bodyFirstPlatform(policy.ActionAdminImpersonate, http.HandlerFunc(h.consumeHandoff))},
	}
}

func (h *handlers) consumeHandoff(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func newHandoffID() (string, error) { return "", errHandoffStoreUnavailable }
