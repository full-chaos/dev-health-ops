package admin

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/google/uuid"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
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

var handoffConsumeScript = valkeygo.NewLuaScriptNoSha(handoffConsumeLua)

// handoffIDBytes is the random size of a handoff id; the id is its unpadded base64url text.
const handoffIDBytes = 32

const msgHandoffRefused = "Impersonation handoff not admitted"

var errHandoffStoreUnavailable = errors.New("admin: impersonation handoff store unavailable")

// valkeyHandoffs is the one-use record of each impersonation start handoff, in go-api's Valkey. It fails closed: no
// client or any store error is an error, never an admit. A record that is gone (expiry, eviction, restart) refuses.
type valkeyHandoffs struct{ client valkeygo.Client }

func (s valkeyHandoffs) record(ctx context.Context, id, adminUserID string) error {
	if s.client == nil {
		return errHandoffStoreUnavailable
	}
	return s.client.Do(ctx, s.client.B().Set().Key(handoffKeyPrefix+id).Value(adminUserID).Nx().Ex(handoffTTL).Build()).Error()
}

func (s valkeyHandoffs) consume(ctx context.Context, id, adminUserID string) (bool, error) {
	if s.client == nil {
		return false, errHandoffStoreUnavailable
	}
	deleted, err := handoffConsumeScript.Exec(ctx, s.client, []string{handoffKeyPrefix + id}, []string{adminUserID}).AsInt64()
	if err != nil {
		return false, err
	}
	return deleted == 1, nil
}

func newHandoffID() (string, error) {
	raw := make([]byte, handoffIDBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// handoffIDPattern is the text of a handoff id: 32 bytes, unpadded base64url.
var handoffIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

// handoffRoutes is the consume route of the start handoff. It is not mounted yet: a new route lands first, its
// endpoint-profile row and the mount follow.
func (h *handlers) handoffRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: impersonationPrefix + "/impersonate/handoff/consume",
			Handler: h.bodyFirstPlatform(policy.ActionAdminImpersonate, http.HandlerFunc(h.consumeHandoff))},
	}
}

// consumeHandoff admits the start handoff id of the caller's own start once: 200 for the one admit; 409 for every
// refusal (no record, used, expired, gone, another admin's, a malformed id), one text for all; 503 when the store
// cannot confirm.
func (h *handlers) consumeHandoff(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	body := bodyFromContext(r.Context())

	var errs pybody.Errors
	object, ok := errs.Object(body)
	var id string
	if ok {
		id, _ = errs.RequiredString(object, "handoff_id", 0, 0)
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	adminUserID, parseErr := uuid.Parse(user.UserID)
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid user_id", nil)
		return
	}
	ctx := r.Context()
	if !handoffIDPattern.MatchString(id) {
		h.logger.WarnContext(ctx, "admin impersonate handoff: malformed id refused")
		policy.WriteDetail(w, http.StatusConflict, msgHandoffRefused, nil)
		return
	}
	admitted, err := h.handoffs.consume(ctx, id, adminUserID.String())
	if err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate handoff: consume failed; refusing", slog.Any("error", err))
		h.writeDelegatedUnavailable(w)
		return
	}
	if !admitted {
		h.logger.WarnContext(ctx, "admin impersonate handoff: refused (used, expired, gone or another admin's)")
		policy.WriteDetail(w, http.StatusConflict, msgHandoffRefused, nil)
		return
	}
	response := pyjson.NewObject()
	response.Set("status", "consumed")
	policy.WriteModel(w, http.StatusOK, response, nil)
}
