package authapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/publicaudit"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// CHAOS-11400: go-api forwards login (phase 3), refresh and logout (phase 2) to these compatibility routes, and its
// own handler, which wrote the route's public.audit_logs row, no longer runs. Each route writes that row here, in
// the transaction of the auth event it goes with, through the shared writer (publicaudit). The organization is the
// issued or current session's, else the user's first public membership in the login order, else there is no row
// (go-api writes none without an organization).

// compatSessionRow is go-api's success row of a session route: resource session/<user>, changes {}.
func compatSessionRow(action, description string) session.PublicRow {
	return func(ctx context.Context, tx audit.TxOps, principal uuid.UUID, organization *uuid.UUID) error {
		org, err := publicaudit.OrganizationOr(ctx, tx, organization, principal)
		if err != nil || org == nil {
			return err
		}
		user := principal
		return publicaudit.Write(ctx, tx, publicaudit.Row{OrgID: *org, UserID: &user, Action: action, ResourceType: "session",
			ResourceID: principal.String(), Description: &description})
	}
}

// go-api's rows of the three session routes (internal/api/session login.go:363, refresh.go:190, routes.go:340).
var (
	compatLoginRow   = compatSessionRow("login", "User logged in")
	compatRefreshRow = compatSessionRow("login", "Access token refreshed")
	compatLogoutRow  = compatSessionRow("logout", "User logged out")
)

// loginFailure is go-api's login_failed row of one refusal reason: its description and error message, and whether
// the user's first membership is preferred over the requested organization (the unverified branch's blockedOrg).
type loginFailure struct {
	description, message string
	membershipFirst      bool
}

// loginFailures maps a refused password sign-in's reason to go-api's row (internal/api/session/login.go). A reason
// not listed (a malformed body, answered before go-api's handler ran) has no row.
var loginFailures = map[string]loginFailure{
	string(local.ReasonInvalidCredentials): {"Login failed: invalid credentials", "Invalid credentials", false},
	string(local.ReasonPrincipalDisabled):  {"Login failed: account is disabled", "Account is disabled", false},
	string(local.ReasonAccountLocked):      {"Login failed: account locked", "Account temporarily locked due to failed login attempts", false},
	string(local.ReasonEmailUnverified):    {"Login blocked: email not verified", "Email not verified", true},
	reasonNotMember:                        {"Login failed: not a member of the selected organization", "User is not a member of the selected organization", false},
}

// unknownAccountFailure is the invalid_credentials row of an address no user holds.
var unknownAccountFailure = loginFailure{"Login failed: user not found", "Invalid credentials", false}

// loginFailedRow is go-api's login_failed row of a refused password sign-in: resource session/<user> (the normalized
// address for an unknown account and for a lock), changes {"email": <normalized>} (and the requested organization
// for a non-member), status failure. It returns nil when go-api writes no row: no attempt in ctx (a malformed body),
// a reason with no row, or no organization.
func loginFailedRow(ctx context.Context, principal uuid.UUID, requested *uuid.UUID, reason string) func(context.Context, audit.TxOps) error {
	attempt, ok := attemptOf(ctx)
	failure, mapped := loginFailures[reason]
	if !ok || !mapped || attempt.email == "" {
		return nil
	}
	if principal == uuid.Nil {
		failure = unknownAccountFailure
	}
	email := pythonparity.Strip(pythonparity.Lower(attempt.email))
	return func(ctx context.Context, tx audit.TxOps) error {
		org, err := loginFailureOrganization(ctx, tx, principal, requested, failure.membershipFirst)
		if err != nil || org == nil {
			return err
		}
		changes := pyjson.NewObject()
		changes.Set("email", email)
		if reason == reasonNotMember && requested != nil {
			changes.Set("requested_org_id", requested.String())
		}
		encoded, err := publicaudit.Changes(changes)
		if err != nil {
			return err
		}
		row := publicaudit.Row{OrgID: *org, Action: "login_failed", ResourceType: "session", ResourceID: email,
			Description: &failure.description, Changes: encoded, Status: "failure", ErrorMessage: &failure.message}
		if principal != uuid.Nil {
			user := principal
			row.UserID = &user
			if reason != string(local.ReasonAccountLocked) {
				row.ResourceID = principal.String()
			}
		}
		return publicaudit.Write(ctx, tx, row)
	}
}

// loginFailureOrganization is go-api's resolveAuditOrg: the requested organization when it exists, else the user's
// first membership; membershipFirst reverses the two (the unverified branch).
func loginFailureOrganization(ctx context.Context, tx audit.TxOps, principal uuid.UUID, requested *uuid.UUID, membershipFirst bool) (*uuid.UUID, error) {
	first := func() (*uuid.UUID, error) {
		if principal == uuid.Nil {
			return nil, nil
		}
		return publicaudit.FirstOrganization(ctx, tx, principal)
	}
	existing := func() (*uuid.UUID, error) {
		if requested == nil {
			return nil, nil
		}
		var id uuid.UUID
		err := tx.QueryRow(ctx, `SELECT id FROM public.organizations WHERE id = $1`, *requested).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return &id, nil
	}
	order := []func() (*uuid.UUID, error){existing, first}
	if membershipFirst {
		order = []func() (*uuid.UUID, error){first, existing}
	}
	for _, step := range order {
		org, err := step()
		if err != nil || org != nil {
			return org, err
		}
	}
	return nil, nil
}

// refreshRefusal is go-api's login_failed row of a refused refresh (internal/api/session/refresh.go): resource
// session/<sub>, changes {}, status failure, in the organization of the token (the session's), none without one.
// user_id is the user's except for a user that is gone (go-api has none to name).
type refreshRefusal struct {
	description, message string
	withUser             bool
}

func (f refreshRefusal) row(principal uuid.UUID, org uuid.UUID) publicaudit.Row {
	description, message := f.description, f.message
	row := publicaudit.Row{OrgID: org, Action: "login_failed", ResourceType: "session", ResourceID: principal.String(),
		Description: &description, Status: "failure", ErrorMessage: &message}
	if f.withUser {
		user := principal
		row.UserID = &user
	}
	return row
}

// revokeRefused ends the refused refresh's session (the go-api lineage for a go-api refresh token) and writes go-api's
// refusal row: in the revocation's audited transaction when it revokes, else in a transaction of its own (acp
// answers-11400-impl-1 G2). A failed write is go-api's 500. It returns false when it answered.
func (h compatHandlers) revokeRefused(w http.ResponseWriter, r *http.Request, record session.Record, legacy *session.LegacyRefresh, refusal *refreshRefusal) bool {
	ctx := r.Context()
	written := false
	var hook session.PublicRow
	if refusal != nil {
		hook = func(ctx context.Context, tx audit.TxOps, principal uuid.UUID, org *uuid.UUID) error {
			if org == nil {
				return nil
			}
			written = true
			return publicaudit.Write(ctx, tx, refusal.row(principal, *org))
		}
	}
	revokeCtx := session.WithPublicRow(ctx, hook)
	actor := session.Actor{RequestID: requestid.Audit(ctx)}
	var revoked bool
	var err error
	if legacy != nil {
		revoked, err = h.Sessions.RevokeLegacy(revokeCtx, *legacy, actor)
	} else {
		revoked, err = h.Sessions.Revoke(revokeCtx, record.ID, actor)
	}
	if err != nil {
		h.fail(w, r, opCompatRefresh, err)
		return false
	}
	if refusal == nil || (written && revoked) || record.OrganizationID == nil || h.PublicAuditTx == nil {
		return true
	}
	return h.writeRefusalAlone(w, r, record.PrincipalID, *record.OrganizationID, *refusal)
}

// writeRefusalAlone writes a refused refresh's go-api row in a transaction of its own (no auth state changes with it,
// or the change committed in the store's own transaction). A failed write is go-api's 500. It returns false when it
// answered.
func (h compatHandlers) writeRefusalAlone(w http.ResponseWriter, r *http.Request, principal, org uuid.UUID, refusal refreshRefusal) bool {
	if h.PublicAuditTx == nil {
		return true
	}
	if err := h.PublicAuditTx(r.Context(), func(ctx context.Context, tx publicaudit.Tx) error {
		return publicaudit.Write(ctx, tx, refusal.row(principal, org))
	}); err != nil {
		h.fail(w, r, opCompatRefresh, err)
		return false
	}
	return true
}

// invalidRefreshRow is go-api's refuseInvalidRefresh row (refresh.go:240) for a go-api refresh token that does not
// verify: login_failed "Refresh token validation failed", error "Invalid or expired refresh token", in the
// organization its unverified org_id claim names when that organization exists; resource the unverified sub, else
// "unknown". It changes no auth state, so it is written alone.
func (h compatHandlers) invalidRefreshRow(ctx context.Context, token string) error {
	if h.PublicAuditTx == nil {
		return nil
	}
	org, subject := unverifiedOrgAndSubject(token)
	if org == nil {
		return nil
	}
	description, message := "Refresh token validation failed", "Invalid or expired refresh token"
	row := publicaudit.Row{OrgID: *org, Action: "login_failed", ResourceType: "session", ResourceID: "unknown",
		Description: &description, Status: "failure", ErrorMessage: &message}
	if subject != "" {
		row.ResourceID = subject
		if id, ok := policy.ParsePyUUID(subject); ok {
			row.UserID = &id
		}
	}
	return h.PublicAuditTx(ctx, func(ctx context.Context, tx publicaudit.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.organizations WHERE id = $1)`, *org).Scan(&exists); err != nil || !exists {
			return err
		}
		return publicaudit.Write(ctx, tx, row)
	})
}

// unverifiedOrgAndSubject reads a JWT's org_id and sub claims without verifying it (go-api's unverifiedOrgAndSubject):
// the organization when org_id is a UUID string, and sub when it is a non-empty string.
func unverifiedOrgAndSubject(token string) (*uuid.UUID, string) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return nil, ""
	}
	var claims map[string]any
	if json.Unmarshal(payload, &claims) != nil {
		return nil, ""
	}
	var org *uuid.UUID
	if text, ok := claims["org_id"].(string); ok {
		if id, ok := policy.ParsePyUUID(text); ok {
			org = &id
		}
	}
	subject, _ := claims["sub"].(string)
	return org, subject
}
