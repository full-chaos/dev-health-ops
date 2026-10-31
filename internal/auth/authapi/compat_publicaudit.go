package authapi

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/publicaudit"
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
