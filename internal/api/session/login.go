package session

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"sync"
	"unicode/utf16"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// dummyHash stands in for login.py's DUMMY_PASSWORD_HASH: the hash bcrypt
// checks when there is no usable user hash, so a missing user costs the
// same bcrypt work (cost 12, as Python's). Its password is random and never
// known, and a login that reaches it has already failed, so only the cost
// matters, not which hash it is.
var dummyHash = sync.OnceValue(func() string {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic("session: no randomness for the timing hash: " + err.Error())
	}
	hash, err := bcrypt.GenerateFromPassword(secret, 12)
	if err != nil {
		panic("session: timing hash: " + err.Error())
	}
	return string(hash)
})

// bcryptMaxPasswordBytes is bcrypt 5's limit: a longer password makes
// bcrypt.checkpw raise ValueError, which login.py treats as no match.
const bcryptMaxPasswordBytes = 72

// errUnencodable is password.encode("utf-8") on a str holding a lone
// surrogate: UnicodeEncodeError, which nothing catches.
var errUnencodable = errors.New("session: text holds a lone surrogate and cannot be encoded as UTF-8")

// passwordMatches is login.py's bcrypt.checkpw call with ValueError
// mapped to false.
func passwordMatches(password, hash string) (bool, error) {
	runes := pyjson.Runes(password)
	for _, r := range runes {
		if utf16.IsSurrogate(r) {
			return false, errUnencodable
		}
	}
	if len(password) > bcryptMaxPasswordBytes {
		return false, nil
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil, nil
}

// auditEntry is emit_audit_log's arguments.
type auditEntry struct {
	orgID        uuid.UUID
	action       audit.Action
	resourceID   string
	userID       *uuid.UUID
	description  string
	changes      *pyjson.Object
	failure      bool
	errorMessage string
}

// emitAudit is emit_audit_log inside the route's transaction.
func (h handlers) emitAudit(ctx context.Context, tx pgx.Tx, r *http.Request, entry auditEntry) error {
	metadata, err := audit.RequestMetadata(r)
	if err != nil {
		return err
	}
	var changes []byte
	if entry.changes != nil {
		text, err := pyjson.Dumps(entry.changes)
		if err != nil {
			return err
		}
		changes = []byte(text)
	}
	description := entry.description
	row := audit.Entry{
		OrgID: entry.orgID, UserID: entry.userID, Action: entry.action, ResourceType: audit.ResourceSession,
		ResourceID: entry.resourceID, Description: &description, Changes: changes, RequestMetadata: metadata,
	}
	if entry.failure {
		// The error texts these routes pass are fixed strings with nothing
		// sanitize_error_text redacts.
		message := entry.errorMessage
		row.Status, row.ErrorMessage = "failure", &message
	}
	_, err = h.Audit.Write(ctx, tx, row)
	return err
}

// parseUUID is common._parse_uuid for a str value.
func parseUUID(value *string) *uuid.UUID {
	if value == nil {
		return nil
	}
	parsed, ok := policy.ParsePyUUID(*value)
	if !ok {
		return nil
	}
	return &parsed
}

// resolveAuditOrg is _resolve_login_audit_org_id.
func resolveAuditOrg(ctx context.Context, tx pgx.Tx, user *userRow, payloadOrgID *string) (*uuid.UUID, error) {
	if parsed := parseUUID(payloadOrgID); parsed != nil {
		exists, err := organizationExists(ctx, tx, *parsed)
		if err != nil {
			return nil, err
		}
		if exists {
			return parsed, nil
		}
	}
	if user == nil {
		return nil, nil
	}
	return firstMembershipOrg(ctx, tx, user.ID)
}

func emailChanges(email string) *pyjson.Object {
	changes := pyjson.NewObject()
	changes.Set("email", email)
	return changes
}

// login is POST /api/v1/auth/login (login.py).
func (h handlers) login(w http.ResponseWriter, r *http.Request) {
	input, _ := r.Context().Value(loginInputKey{}).(loginInput)
	email, password, orgID := input.email, input.password, input.orgID
	ctx := r.Context()
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	h.loginInTx(w, r, tx, email, password, orgID)
}

// loginInTx runs login.py's body in tx. Every return path has written the
// response.
func (h handlers) loginInTx(w http.ResponseWriter, r *http.Request, tx pgx.Tx, email, password string, payloadOrgID *string) {
	ctx := r.Context()
	normalized := pythonparity.Strip(pythonparity.Lower(email))
	user, err := userByEmail(ctx, tx, normalized)
	if err != nil {
		h.fail(w, r, "load user", err)
		return
	}

	locked, remaining, err := h.checkLockout(ctx, tx, normalized)
	if err != nil {
		h.fail(w, r, "check lockout", err)
		return
	}
	if locked {
		if remaining <= 0 {
			remaining = 1
		}
		failureOrg, err := resolveAuditOrg(ctx, tx, user, payloadOrgID)
		if err != nil {
			h.fail(w, r, "resolve audit org", err)
			return
		}
		if failureOrg != nil {
			var userID *uuid.UUID
			if user != nil {
				userID = &user.ID
			}
			if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: *failureOrg, action: audit.ActionLoginFailed,
				resourceID: normalized, userID: userID, description: "Login failed: account locked",
				changes: emailChanges(normalized), failure: true,
				errorMessage: "Account temporarily locked due to failed login attempts"}); err != nil {
				h.fail(w, r, "audit lockout", err)
				return
			}
			if err := tx.Commit(ctx); err != nil {
				h.fail(w, r, "commit lockout audit", err)
				return
			}
		}
		detail := policy.ErrorDetail("Too many failed login attempts. Please try again later.")
		detail.Set("retry_after_seconds", pyjson.IntOf(remaining))
		policy.WriteDetail(w, http.StatusTooManyRequests, detail, nil)
		return
	}

	var primaryOrg *uuid.UUID
	if user != nil {
		if primaryOrg, err = firstMembershipOrg(ctx, tx, user.ID); err != nil {
			h.fail(w, r, "primary org", err)
			return
		}
	}

	hash := dummyHash()
	if user != nil && user.IsActive && user.PasswordHash != nil {
		hash = *user.PasswordHash
	}
	matches, err := passwordMatches(password, hash)
	if err != nil {
		h.fail(w, r, "check password", err)
		return
	}

	description, message := "", ""
	resourceID := normalized
	var failureUser *uuid.UUID
	switch {
	case user == nil:
		description, message = "Login failed: user not found", "Invalid credentials"
	case !user.IsActive:
		description, message = "Login failed: account is disabled", "Account is disabled"
	case user.PasswordHash == nil:
		description, message = "Login failed: password login unavailable", "Password login not available for this account"
	case !matches:
		description, message = "Login failed: invalid credentials", "Invalid credentials"
	}
	if description != "" {
		if user != nil {
			resourceID, failureUser = user.ID.String(), &user.ID
		}
		if err := h.recordFailedAttempt(ctx, tx, normalized); err != nil {
			h.fail(w, r, "record failed attempt", err)
			return
		}
		failureOrg, err := resolveAuditOrg(ctx, tx, user, payloadOrgID)
		if err != nil {
			h.fail(w, r, "resolve audit org", err)
			return
		}
		if failureOrg != nil {
			if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: *failureOrg, action: audit.ActionLoginFailed,
				resourceID: resourceID, userID: failureUser, description: description,
				changes: emailChanges(normalized), failure: true, errorMessage: message}); err != nil {
				h.fail(w, r, "audit failure", err)
				return
			}
		}
		if user == nil {
			h.Logger.WarnContext(ctx, "login attempt for a user that does not exist")
		} else if message == "Invalid credentials" {
			h.Logger.WarnContext(ctx, "login attempt with an invalid password", "user_id", user.ID.String())
		}
		if err := tx.Commit(ctx); err != nil {
			h.fail(w, r, "commit failure", err)
			return
		}
		refuse(w, http.StatusUnauthorized, "Invalid credentials")
		return
	}

	if err := clearAttempts(ctx, tx, normalized); err != nil {
		h.fail(w, r, "clear attempts", err)
		return
	}

	// str(getattr(user, "auth_provider", "local")).lower(): a NULL
	// provider is the text "none", which is not "local".
	provider := "None"
	if user.AuthProvider != nil {
		provider = *user.AuthProvider
	}
	if pythonparity.Lower(provider) == "local" && !user.IsVerified {
		blockedOrg := primaryOrg
		if blockedOrg == nil {
			if blockedOrg, err = resolveAuditOrg(ctx, tx, user, payloadOrgID); err != nil {
				h.fail(w, r, "resolve audit org", err)
				return
			}
		}
		if blockedOrg != nil {
			if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: *blockedOrg, action: audit.ActionLoginFailed,
				resourceID: user.ID.String(), userID: &user.ID, description: "Login blocked: email not verified",
				changes: emailChanges(normalized), failure: true, errorMessage: "Email not verified"}); err != nil {
				h.fail(w, r, "audit unverified", err)
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			h.fail(w, r, "commit unverified", err)
			return
		}
		out := pyjson.NewObject()
		out.Set("status", "email_verification_required")
		out.Set("email", user.Email)
		out.Set("message", "Please verify your email address before logging in")
		policy.WriteModel(w, http.StatusOK, out, nil)
		return
	}

	memberships, err := orderedMemberships(ctx, tx, user.ID)
	if err != nil {
		h.fail(w, r, "memberships", err)
		return
	}
	requested := parseUUID(payloadOrgID)
	orgIDs := make([]uuid.UUID, len(memberships))
	for index, m := range memberships {
		orgIDs[index] = m.OrgID
	}
	membership := selectActiveMembership(memberships, requested, h.orgActivity(ctx, orgIDs))
	needsOnboarding := membership == nil && !user.IsSuperuser

	// login.py's `payload.org_id and membership is None and memberships`:
	// with memberships, selectActiveMembership answers nil only for a
	// requested org_id (one that parses) the user is not a member of.
	if membership == nil && len(memberships) > 0 {
		failureOrg, err := resolveAuditOrg(ctx, tx, user, payloadOrgID)
		if err != nil {
			h.fail(w, r, "resolve audit org", err)
			return
		}
		if failureOrg != nil {
			changes := emailChanges(normalized)
			changes.Set("requested_org_id", *payloadOrgID)
			if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: *failureOrg, action: audit.ActionLoginFailed,
				resourceID: user.ID.String(), userID: &user.ID, description: "Login failed: not a member of the selected organization",
				changes: changes, failure: true, errorMessage: "User is not a member of the selected organization"}); err != nil {
				h.fail(w, r, "audit wrong org", err)
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			h.fail(w, r, "commit wrong org", err)
			return
		}
		refuse(w, http.StatusUnauthorized, "User is not a member of the selected organization")
		return
	}

	if err := touchLastLogin(ctx, tx, user.ID, h.Now()); err != nil {
		h.fail(w, r, "last login", err)
		return
	}
	successOrg := requested
	if successOrg == nil {
		if membership != nil {
			successOrg = &membership.OrgID
		} else {
			successOrg = primaryOrg
		}
	}
	if successOrg != nil {
		if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: *successOrg, action: audit.ActionLogin,
			resourceID: user.ID.String(), userID: &user.ID, description: "User logged in"}); err != nil {
			h.fail(w, r, "audit login", err)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit login", err)
		return
	}
	// The refresh-token row is written after that commit, in a new
	// transaction the session scope commits on exit.
	next, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin tokens", err)
		return
	}
	defer func() { _ = next.Rollback(context.Background()) }()
	pair, err := h.issueMembershipTokens(ctx, next, r, user, membership)
	if err != nil {
		h.fail(w, r, "issue tokens", err)
		return
	}
	if err := next.Commit(ctx); err != nil {
		h.fail(w, r, "commit tokens", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, loginResponse(pair, needsOnboarding, userInfo(user, membership), nil), nil)
}
