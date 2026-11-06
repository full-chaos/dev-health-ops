package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// errRevokedImpersonatorWrite is liveScope's answer on the write door for a
// caller that does not hold admin.impersonate while its go-api impersonation
// row is live (a revoked impersonator): its row is never applied, and a
// write is refused while the row lives.
var errRevokedImpersonatorWrite = errors.New("revoked impersonator write refused")

// impersonationAuditMessage is the message of the one log line /query writes
// when it applies an impersonation session.
const impersonationAuditMessage = "query-api impersonation session applied"

type writeDoorKey struct{}

// withWriteDoor marks a request of the mutation-only write door, the one
// /query route liveScope reads a non-holder's row on.
func withWriteDoor(ctx context.Context) context.Context {
	return context.WithValue(ctx, writeDoorKey{}, true)
}

func onWriteDoor(ctx context.Context) bool {
	door, _ := ctx.Value(writeDoorKey{}).(bool)
	return door
}

// logImpersonationApplied writes the one record of an impersonated request:
// query-api has no audit table, so this line names the actor (the admin user
// id) and the target (user id and org) of each request served in a session.
// Ids only: no email, name or token.
func logImpersonationApplied(r *http.Request, session *policy.Impersonation) {
	requestID := httpapi.RequestIDFrom(r.Context())
	if requestID == "" {
		requestID = envelopeRequestID(r)
	}
	slog.InfoContext(r.Context(), impersonationAuditMessage,
		slog.String("actor_user_id", session.AdminUserID.String()),
		slog.String("target_user_id", session.TargetUserID.String()),
		slog.String("target_org_id", session.TargetOrgID.String()),
		slog.String("route", r.URL.Path),
		slog.String("request_id", requestID))
}
