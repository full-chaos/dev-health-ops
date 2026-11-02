package orgs

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

// Renamer renames an organization in auth-service (*orgreactivate.Client).
type Renamer interface {
	Rename(ctx context.Context, orgID, actor uuid.UUID, name string) error
}

// ownedNameMax is auth-service's organization name bound (org.NormalizeName).
const ownedNameMax = 100

// errAnswered: the step wrote its own answer; the caller writes nothing more.
var errAnswered = errors.New("orgs: the request was answered")

// orgsOwned is AUTH_ORGS_OWNED: auth-service writes organizations.name, so
// PATCH /api/v1/orgs/me sends the name there first and writes only the
// description itself.
func (h handlers) orgsOwned() bool { return usersowned.OrgsOn() }

// nameMax is the name bound the PATCH checks before anything else: 255 as
// before, or auth-service's 100 while it owns organizations. A stored name
// over 100 stays as it is until it is renamed.
func (h handlers) nameMax() int {
	if h.orgsOwned() {
		return ownedNameMax
	}
	return 255
}

// beginAfterRename renames the organization in auth-service first while
// organizations are owned (a refusal is answered here and is errAnswered),
// then opens go-api's transaction. Off, it only opens the transaction.
func (h handlers) beginAfterRename(w http.ResponseWriter, r *http.Request, orgID, actor uuid.UUID, name string, hasName bool) (pgx.Tx, error) {
	if hasName && h.orgsOwned() && !renameInAuth(w, r, h.renamer, orgID, actor, name, h.internal) {
		return nil, errAnswered
	}
	return h.pool.Begin(r.Context())
}

// failBegin answers a failed beginAfterRename unless it already answered.
func (h handlers) failBegin(w http.ResponseWriter, r *http.Request, err error) {
	if !errors.Is(err, errAnswered) {
		h.internal(w, r, "begin", err)
	}
}

// ownUpdateSQL is the PATCH's write. While organizations are owned it leaves
// name to auth-service: $2 is still bound, so the argument list is the same.
func (h handlers) ownUpdateSQL() string {
	if h.orgsOwned() {
		return `UPDATE organizations SET description = $3, updated_at = $4 WHERE id = $1 AND $2::text IS NOT NULL`
	}
	return `UPDATE organizations SET name = $2, description = $3, updated_at = $4 WHERE id = $1`
}

// renameInAuth calls the rename route and answers a refusal. It reports
// whether the PATCH may go on. No renamer while organizations are owned is a
// 503: go-api never writes the name itself then.
func renameInAuth(w http.ResponseWriter, r *http.Request, renamer Renamer, orgID, actor uuid.UUID, name string,
	internal func(http.ResponseWriter, *http.Request, string, error)) bool {
	if renamer == nil {
		internal(w, r, "rename organization", errors.New("no auth-service rename client while organizations are owned"))
		return false
	}
	err := renamer.Rename(r.Context(), orgID, actor, name)
	switch {
	case err == nil:
		return true
	case errors.Is(err, orgreactivate.ErrNameInvalid):
		policy.WriteDetail(w, http.StatusUnprocessableEntity, "Organization name must be 1 to 100 characters", nil)
	case errors.Is(err, orgreactivate.ErrSuspended):
		policy.WriteDetail(w, http.StatusConflict, "Organization is suspended", nil)
	case errors.Is(err, orgreactivate.ErrNotFound):
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
	case errors.Is(err, orgreactivate.ErrRefused):
		policy.WriteDetail(w, http.StatusForbidden, "You must be an org admin or owner to update organization settings", nil)
	default:
		policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
	}
	return false
}
