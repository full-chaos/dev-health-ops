package identity

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/delegationend"
	"github.com/full-chaos/dev-health-ops/internal/auth/publicaudit"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionevent"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The admin user writes go-api's /api/v1/admin/users routes make through auth-service while auth-service owns users.
// Each one writes the auth rows and go-api's public rows in one audit.Commit, with go-api's own value forms (the
// public.users row is the one go-api's insertUser and updateUser write), so the route's read-back of public.users
// answers what go-api answers when it writes itself.

// ErrUsernameTaken reports a create or an update to a username another user holds (case-insensitive, as go-api's
// get_by_username). Nothing is written.
var ErrUsernameTaken = errors.New("identity: the username belongs to an existing user")

// ErrAdminPasswordInvalid reports a password set whose acting admin password does not match the admin's auth.users
// verifier (or the admin has none). Nothing is written.
var ErrAdminPasswordInvalid = errors.New("identity: the acting admin password does not match")

// The commit kinds an admin update picks between; tests record the pick.
var (
	commitMutation         = audit.Commit
	commitPublicProjection = audit.CommitPublicProjection
)

// AdminTx is the part of the audited transaction the admin writes run on: audit.TxOps, or a test double.
type AdminTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// AdminCreateRequest is go-api's admin create (users.go createUser) after its body checks.
type AdminCreateRequest struct {
	// Email is the address as the request sent it: it is stored lower-cased and stripped, as go-api stores it.
	Email string
	// Username nil or "" stores NULL; any other value is stored lower-cased and stripped (a blank one as "").
	Username       *string
	FullName       *string
	PasswordHash   *string
	AuthProvider   *string // nil is "local"; any sent value is stored verbatim
	AuthProviderID *string
	Verified       bool
	// OrgID, when set, adds the active membership of Role in both schemas and go-api's audit row.
	OrgID           *uuid.UUID
	Role            string
	RequestMetadata []byte
	Actor           uuid.UUID
	RequestID       *string
	Caller          audit.Caller
	// Authorize runs first in the transaction: the caller's locks (the platform role write lock before any row when it
	// grants) and its actor rule on the transaction. An error rolls everything back.
	Authorize func(ctx context.Context, tx audit.TxOps) error
	// Grant, when set, runs last in the same transaction with the new principal (the platform_admin grant).
	Grant func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error
}

// AdminUser is a created user.
type AdminUser struct {
	ID    uuid.UUID
	Email string
}

// adminCreatePlan is a create's values in their stored form.
type adminCreatePlan struct {
	ID                                               uuid.UUID
	Email, Provider, DisplayName                     string
	Username, FullName, PasswordHash, AuthProviderID *string
	Verified                                         bool
}

// AdminStoredEmail is the form go-api's admin create stores an address in: lower-cased, then stripped (Python's
// email.lower().strip()). It is not the register normalization: a service account's .invalid address is kept.
func AdminStoredEmail(raw string) string { return pythonparity.Strip(pythonparity.Lower(raw)) }

func planAdminCreate(req AdminCreateRequest) adminCreatePlan {
	p := adminCreatePlan{ID: uuid.New(), Email: AdminStoredEmail(req.Email), Provider: "local",
		FullName: req.FullName, PasswordHash: req.PasswordHash, AuthProviderID: req.AuthProviderID, Verified: req.Verified}
	if req.AuthProvider != nil {
		p.Provider = *req.AuthProvider
	}
	if req.Username != nil && *req.Username != "" {
		stored := pythonparity.Strip(pythonparity.Lower(*req.Username))
		p.Username = &stored
	}
	if req.FullName != nil {
		p.DisplayName = *req.FullName
	}
	return p
}

// applyAdminCreate is the create's statements on tx: the go-api duplicate checks, the principal, auth.users and
// public.users rows, then join (the membership, when set) and grant (the platform role, when set), in that order. It
// is the whole Apply of one audit.Commit: an error from any step, grant last, rolls every row back.
func applyAdminCreate(ctx context.Context, tx AdminTx, q string, p adminCreatePlan, join, grant func(context.Context) error) error {
	var taken bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.users WHERE lower(email) = $1)`, p.Email).Scan(&taken); err != nil {
		return err
	}
	if taken {
		return ErrEmailTaken
	}
	if p.Username != nil {
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.users WHERE lower(username) = $1)`,
			pythonparity.Lower(*p.Username)).Scan(&taken); err != nil {
			return err
		}
		if taken {
			return ErrUsernameTaken
		}
	}
	var algorithm *string
	if p.PasswordHash != nil {
		a := passwordAlgorithm
		algorithm = &a
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+q+`.principals (id, kind, display_name) VALUES ($1, 'user', $2)`, p.ID, p.DisplayName); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+q+`.users
	(principal_id, email, email_lower, email_verified_at, password_hash, password_algorithm, password_updated_at)
VALUES ($1, $2, $2, CASE WHEN $3::boolean THEN clock_timestamp() END, $4, $5, CASE WHEN $4::text IS NULL THEN NULL ELSE clock_timestamp() END)`,
		p.ID, p.Email, p.Verified, p.PasswordHash, algorithm); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO public.users
	(id, email, username, password_hash, full_name, auth_provider, auth_provider_id, is_active, is_verified, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8, 0, now(), now())`,
		p.ID, p.Email, p.Username, p.PasswordHash, p.FullName, p.Provider, p.AuthProviderID, p.Verified); err != nil {
		return err
	}
	if join != nil {
		if err := join(ctx); err != nil {
			return err
		}
	}
	if grant != nil {
		return grant(ctx)
	}
	return nil
}

// AdminCreateUser creates the user of go-api's admin create in one audit.Commit (reason principal_provisioned): the
// principal, auth.users (email verified when Verified), public.users in go-api's form, and with OrgID the membership
// of both schemas and go-api's "User created and added to organization" row; with Grant the platform role too. An
// email or username another user holds is ErrEmailTaken or ErrUsernameTaken; nothing is written then.
func (s *Store) AdminCreateUser(ctx context.Context, req AdminCreateRequest) (_ AdminUser, err error) {
	defer func() { s.countLink(ctx, "admin_create", err) }()
	if !s.ownsUsers {
		return AdminUser{}, ErrSignupNotAvailable
	}
	if req.Authorize == nil {
		return AdminUser{}, errors.New("identity: an admin create needs its actor rule")
	}
	if req.OrgID != nil && !IsOrganizationRole(req.Role) {
		return AdminUser{}, ErrBackfillRole
	}
	p := planAdminCreate(req)
	subject := p.ID.String()
	actor := req.Actor
	fields := []string{"email"}
	events := []audit.OutboxEvent{principalEvent(subject, "principal.created", true), userEvent(subject, "user.created")}
	if p.Verified {
		fields = append(fields, "email_verified_at")
		events = append(events, userEvent(subject, "user.email_verified"))
	}
	if req.OrgID != nil {
		fields = append(fields, "organization_id")
	}
	event := withChangedFields(localAudit("auth.principal.provisioned", "principal_provisioned", subject,
		Actor{PrincipalID: &actor, RequestID: req.RequestID}), fields...)
	req.Caller.Annotate(event.Attributes)
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := req.Authorize(ctx, tx); err != nil {
				return err
			}
			var join, grant func(context.Context) error
			if req.OrgID != nil {
				orgID, role := *req.OrgID, req.Role
				join = func(ctx context.Context) error {
					if err := s.insertJoinMembership(ctx, tx, membershipJoin{ID: uuid.New(), Principal: p.ID, OrgID: orgID, Role: role}); err != nil {
						return err
					}
					return adminCreateAuditRow(ctx, tx, orgID, actor, p.ID, p.Email, role, req.RequestMetadata)
				}
			}
			if req.Grant != nil {
				grant = func(ctx context.Context) error { return req.Grant(ctx, tx, p.ID) }
			}
			return applyAdminCreate(ctx, tx, s.q, p, join, grant)
		},
		Audit:    event,
		Events:   events,
		Refusals: []error{ErrEmailTaken, ErrUsernameTaken},
	})
	switch {
	case err == nil:
	case errors.Is(err, ErrEmailTaken), isEmailCollision(err), isPublicEmailCollision(err):
		return AdminUser{}, ErrEmailTaken
	case errors.Is(err, ErrUsernameTaken):
		return AdminUser{}, ErrUsernameTaken
	default:
		return AdminUser{}, err
	}
	return AdminUser{ID: p.ID, Email: p.Email}, nil
}

// adminCreateAuditRow is go-api's org-scoped create row (users.go insertUserWithMembership): action create on
// user/<id>, by the actor, changes {"email", "role"}, description "User created and added to organization".
func adminCreateAuditRow(ctx context.Context, tx audit.TxOps, orgID, actor, user uuid.UUID, email, role string, metadata []byte) error {
	object := pyjson.NewObject()
	object.Set("email", email)
	object.Set("role", role)
	changes, err := publicaudit.Changes(object)
	if err != nil {
		return err
	}
	at, err := publicaudit.TxTime(ctx, tx)
	if err != nil {
		return err
	}
	description := "User created and added to organization"
	return publicaudit.InsertInAuditTx(ctx, tx, publicaudit.Row{OrgID: orgID, UserID: &actor, Action: "create", ResourceType: "user",
		ResourceID: user.String(), Description: &description, Changes: changes, RequestMetadata: metadata, CreatedAt: at})
}

// AdminUpdateRequest is go-api's admin update (users.go updateUser) of every field auth-service takes over: the
// email, the verified flag and the profile (username, full_name, avatar_url). A nil field is unchanged. The is_active
// change stays go-api's refusal (CHAOS-13209 row 4), and is_superuser is the Superuser step.
type AdminUpdateRequest struct {
	Principal uuid.UUID
	// Email is the address as sent: an empty one, or one whose lower case is the stored address, changes nothing; any
	// other is checked for a collision and stored lower-cased and stripped (go-api's updateUser).
	Email    *string
	Verified *bool
	// Username "" stores NULL; any other value is checked for a collision (unless its lower case is the stored one)
	// and stored lower-cased and stripped. FullName and AvatarURL are stored as sent.
	Username, FullName, AvatarURL *string
	Actor                         uuid.UUID
	RequestID                     *string
	Caller                        audit.Caller
	// Authorize runs first in the transaction (locks, actor and target rules); Superuser, when set, runs last (the
	// platform_admin grant or revoke).
	Authorize func(ctx context.Context, tx audit.TxOps) error
	Superuser func(ctx context.Context, tx audit.TxOps) error
}

// AdminUpdateCurrent is the stored state an update is decided on; the route reads it before the Commit and the
// update checks it again under its locks (a change in between is ErrStateChangedUnderLock).
type AdminUpdateCurrent struct {
	Email    string
	Verified bool
	Username *string
}

// ErrStateChangedUnderLock reports an admin update whose user changed between the route's read and its locks.
// Nothing is written; a retry decides on the new state.
var ErrStateChangedUnderLock = errors.New("identity: the user changed while the update waited for its locks")

// AdminUpdateState reads the email and username (public.users, go-api's values) and the verified flag (auth.users,
// the authority while auth owns users) an update decides on.
func (s *Store) AdminUpdateState(ctx context.Context, principal uuid.UUID) (AdminUpdateCurrent, error) {
	return readAdminUpdateState(ctx, s.pool, s.q, principal)
}

func readAdminUpdateState(ctx context.Context, q AdminTx, schema string, principal uuid.UUID) (AdminUpdateCurrent, error) {
	var cur AdminUpdateCurrent
	err := q.QueryRow(ctx, `SELECT p.email, a.email_verified_at IS NOT NULL, p.username FROM public.users p
JOIN `+schema+`.users a ON a.principal_id = p.id WHERE p.id = $1`, principal).Scan(&cur.Email, &cur.Verified, &cur.Username)
	if errors.Is(err, pgx.ErrNoRows) {
		return cur, ErrUserNotFound
	}
	return cur, err
}

// AdminUpdateDecision is what an update changes, decided on the stored state.
type AdminUpdateDecision struct {
	Email    *string // the stored form of a changed address
	Verified *bool
	// UsernameSet is a sent username: Username is its stored form (nil is NULL), UsernameCheck whether it is checked
	// for a collision (go-api checks only a new non-empty name).
	UsernameSet, UsernameCheck bool
	Username                   *string
	FullName, AvatarURL        *string
}

// Profile reports a decision that writes a profile column.
func (d AdminUpdateDecision) Profile() bool {
	return d.UsernameSet || d.FullName != nil || d.AvatarURL != nil
}

// Empty reports an update that changes nothing.
func (d AdminUpdateDecision) Empty() bool { return d.Email == nil && d.Verified == nil && !d.Profile() }

// AdminUpdatePatch is the fields an update sent.
type AdminUpdatePatch struct {
	Email                         *string
	Verified                      *bool
	Username, FullName, AvatarURL *string
}

// DecideAdminUpdate is go-api's updateUser decision. The email and the verified flag change only when they differ
// from the stored values in go-api's write form; a sent profile column is written as go-api writes it.
func DecideAdminUpdate(cur AdminUpdateCurrent, p AdminUpdatePatch) AdminUpdateDecision {
	var d AdminUpdateDecision
	if p.Email != nil && *p.Email != "" && pythonparity.Lower(*p.Email) != cur.Email {
		stored := pythonparity.Strip(pythonparity.Lower(*p.Email))
		d.Email = &stored
	}
	if p.Verified != nil && *p.Verified != cur.Verified {
		v := *p.Verified
		d.Verified = &v
	}
	if p.Username != nil {
		d.UsernameSet = true
		if *p.Username != "" {
			stored := pythonparity.Strip(pythonparity.Lower(*p.Username))
			d.Username = &stored
			current := ""
			if cur.Username != nil {
				current = pythonparity.Lower(*cur.Username)
			}
			d.UsernameCheck = pythonparity.Lower(*p.Username) != current
		}
	}
	d.FullName, d.AvatarURL = p.FullName, p.AvatarURL
	return d
}

func (r AdminUpdateRequest) patch() AdminUpdatePatch {
	return AdminUpdatePatch{Email: r.Email, Verified: r.Verified, Username: r.Username, FullName: r.FullName, AvatarURL: r.AvatarURL}
}

// AdminUpdateUser applies the decision. An email or verified change is one audit.Commit with its provisioning events
// (user.email_changed, user.email_verified or user.email_unverified), the profile columns and the Superuser step in
// the same transaction. A profile change with a platform role change and nothing else is one audit.Commit off the
// stream (OffStreamPublicProfile; the role change queues its own events). A profile change alone is one
// audit.CommitPublicProjection (EventUserProfileUpdated): its columns are in no guarded table, so Commit would count
// it as no change. Under the locks the state is read again, and a different decision is ErrStateChangedUnderLock.
func (s *Store) AdminUpdateUser(ctx context.Context, req AdminUpdateRequest, cur AdminUpdateCurrent) (err error) {
	defer func() { s.countLink(ctx, "admin_update", err) }()
	if !s.ownsUsers {
		return ErrSignupNotAvailable
	}
	if req.Authorize == nil {
		return errors.New("identity: an admin update needs its actor rule")
	}
	decided := DecideAdminUpdate(cur, req.patch())
	subject := req.Principal.String()
	actor := Actor{PrincipalID: &req.Actor, RequestID: req.RequestID}
	var fields []string
	var events []audit.OutboxEvent
	if decided.Email != nil {
		fields = append(fields, "email")
		events = append(events, userEvent(subject, "user.email_changed"))
	}
	reason, eventType := "email_verified", "auth.user.email_verified"
	if decided.Verified != nil {
		fields = append(fields, "email_verified_at")
		verb := "user.email_verified"
		if !*decided.Verified {
			verb, reason, eventType = "user.email_unverified", "email_unverified", "auth.user.email_unverified"
		}
		events = append(events, userEvent(subject, verb))
	}
	if decided.Email != nil && decided.Verified == nil {
		reason, eventType = "principal_projected", "auth.user.email_changed"
	}
	fields = append(fields, decided.profileFields()...)
	var applied int64
	apply := func(ctx context.Context, tx audit.TxOps) error {
		if err := req.Authorize(ctx, tx); err != nil {
			return err
		}
		if err := s.lockUser(ctx, tx, req.Principal); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT id FROM public.users WHERE id = $1 FOR UPDATE`, req.Principal); err != nil {
			return err
		}
		now, err := readAdminUpdateState(ctx, tx, s.q, req.Principal)
		if err != nil {
			return err
		}
		if again := DecideAdminUpdate(now, req.patch()); !sameDecision(again, decided) {
			return ErrStateChangedUnderLock
		}
		if applied, err = applyAdminUpdate(ctx, tx, s.q, req.Principal, decided, req.Email); err != nil {
			return err
		}
		if req.Superuser != nil {
			return req.Superuser(ctx, tx)
		}
		return nil
	}
	refusals := []error{ErrEmailTaken, ErrUsernameTaken, ErrStateChangedUnderLock, ErrUserNotFound}
	switch {
	case len(events) > 0:
		event := withChangedFields(localAudit(eventType, reason, subject, actor), fields...)
		req.Caller.Annotate(event.Attributes)
		return commitMutation(ctx, s.pool, s.schema, audit.Mutation{Apply: apply, Audit: event, Events: events, Refusals: refusals})
	case decided.Profile() && req.Superuser != nil:
		event := withChangedFields(localAudit("auth.user.updated", "principal_projected", subject, actor), fields...)
		req.Caller.Annotate(event.Attributes)
		return commitMutation(ctx, s.pool, s.schema, audit.Mutation{Apply: apply, Audit: event, OffStream: audit.OffStreamPublicProfile,
			Refusals: refusals})
	case decided.Profile():
		event := withChangedFields(localAudit(audit.EventUserProfileUpdated, "principal_projected", subject, actor), fields...)
		req.Caller.Annotate(event.Attributes)
		return commitPublicProjection(ctx, s.pool, s.schema, event, func(ctx context.Context, tx audit.TxOps) (int64, error) {
			err := apply(ctx, tx)
			return applied, err
		})
	case req.Superuser != nil:
		return errors.New("identity: a platform role change alone is the platform role store's own commit")
	}
	return audit.ErrNoStateChange
}

func (d AdminUpdateDecision) profileFields() []string {
	var fields []string
	if d.UsernameSet {
		fields = append(fields, "username")
	}
	if d.FullName != nil {
		fields = append(fields, "full_name")
	}
	if d.AvatarURL != nil {
		fields = append(fields, "avatar_url")
	}
	return fields
}

func sameDecision(a, b AdminUpdateDecision) bool {
	sameText := func(x, y *string) bool { return (x == nil) == (y == nil) && (x == nil || *x == *y) }
	sameBool := (a.Verified == nil) == (b.Verified == nil) && (a.Verified == nil || *a.Verified == *b.Verified)
	return sameText(a.Email, b.Email) && sameBool && a.UsernameSet == b.UsernameSet && a.UsernameCheck == b.UsernameCheck &&
		sameText(a.Username, b.Username)
}

// applyAdminUpdate is the update's statements; it returns the public.users rows it changed. The email and username
// collision checks are go-api's (get_by_email and get_by_username on the value as sent, lower-cased), then the auth
// email_lower key decides an email race.
func applyAdminUpdate(ctx context.Context, tx AdminTx, q string, principal uuid.UUID, d AdminUpdateDecision, sent *string) (int64, error) {
	var changed int64
	if d.Email != nil {
		var taken bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.users WHERE lower(email) = $1)`,
			pythonparity.Lower(*sent)).Scan(&taken); err != nil {
			return 0, err
		}
		if taken {
			return 0, ErrEmailTaken
		}
		if _, err := tx.Exec(ctx, `UPDATE `+q+`.users SET email = $2, email_lower = $2, updated_at = clock_timestamp() WHERE principal_id = $1`,
			principal, *d.Email); err != nil {
			if isEmailCollision(err) {
				return 0, ErrEmailTaken
			}
			return 0, err
		}
	}
	if d.Verified != nil {
		if _, err := tx.Exec(ctx, `UPDATE `+q+`.users
SET email_verified_at = CASE WHEN $2::boolean THEN clock_timestamp() END, updated_at = clock_timestamp() WHERE principal_id = $1`,
			principal, *d.Verified); err != nil {
			return 0, err
		}
	}
	if d.UsernameCheck {
		var taken bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.users WHERE lower(username) = $1)`,
			pythonparity.Lower(*d.Username)).Scan(&taken); err != nil {
			return 0, err
		}
		if taken {
			return 0, ErrUsernameTaken
		}
	}
	if d.FullName != nil {
		if _, err := tx.Exec(ctx, `UPDATE `+q+`.principals SET display_name = $2, updated_at = clock_timestamp() WHERE id = $1`,
			principal, *d.FullName); err != nil {
			return 0, err
		}
	}
	for _, write := range []struct {
		set       bool
		statement string
		value     any
	}{
		{d.Email != nil, `UPDATE public.users SET email = $2, updated_at = now() WHERE id = $1`, d.Email},
		{d.Verified != nil, `UPDATE public.users SET is_verified = $2, updated_at = now() WHERE id = $1`, d.Verified},
		{d.UsernameSet, `UPDATE public.users SET username = $2, updated_at = now() WHERE id = $1`, d.Username},
		{d.FullName != nil, `UPDATE public.users SET full_name = $2, updated_at = now() WHERE id = $1`, d.FullName},
		{d.AvatarURL != nil, `UPDATE public.users SET avatar_url = $2, updated_at = now() WHERE id = $1`, d.AvatarURL},
	} {
		if !write.set {
			continue
		}
		if err := updatePublicUserRow(ctx, tx, write.statement, principal, write.value); err != nil {
			return 0, err
		}
		changed++
	}
	return changed, nil
}

func updatePublicUserRow(ctx context.Context, tx AdminTx, statement string, principal uuid.UUID, value any) error {
	tag, err := tx.Exec(ctx, statement, principal, value)
	if err != nil {
		if isPublicEmailCollision(err) {
			return ErrEmailTaken
		}
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLegacyProjection
	}
	return nil
}

// AdminPasswordRequest is go-api's admin set-password (users.go setUserPassword) after its body checks.
type AdminPasswordRequest struct {
	Principal uuid.UUID
	Hash      string
	Actor     uuid.UUID
	// AuditOrg is go-api's audit organization (the caller's org claim); nil falls back to the target's first
	// membership, and no membership writes no row (go-api's audit_logs.org_id is NOT NULL).
	AuditOrg        *uuid.UUID
	RequestMetadata []byte
	RequestID       *string
	Authorize       func(ctx context.Context, tx audit.TxOps) error
}

// VerifyAdminPassword reports whether password matches the actor's auth.users verifier; an actor with none is false.
func (s *Store) VerifyAdminPassword(ctx context.Context, actor uuid.UUID, password string) (bool, error) {
	var hash *string
	err := s.pool.QueryRow(ctx, `SELECT password_hash FROM `+s.q+`.users WHERE principal_id = $1`, actor).Scan(&hash)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && hash == nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return bcrypt.CompareHashAndPassword([]byte(*hash), []byte(password)) == nil, nil
}

// AdminSetPassword sets the user's password in one audit.Commit. Authorize runs first (the actor rule and the platform
// role read lock come before any user row, in the auth lock order); then the writes of SetPasswordHashFor (the auth
// verifier, legacy_token_version + 1, the session stamp event, the public mirror and token_version + 1), go-api's
// refresh token revoke, and go-api's "Admin changed user password" row (in AuditOrg, else the target's first
// membership; none writes no row). Reason password_changed, actor the admin.
func (s *Store) AdminSetPassword(ctx context.Context, req AdminPasswordRequest) (err error) {
	defer func() { s.countLink(ctx, "admin_set_password", err) }()
	if !s.ownsUsers {
		return ErrSignupNotAvailable
	}
	if req.Authorize == nil {
		return errors.New("identity: an admin password set needs its actor rule")
	}
	if !validBcrypt(req.Hash) {
		return ErrInvalidPasswordHash
	}
	org, err := AuditOrganization(ctx, s.pool, s.q, req.Principal)
	if err != nil {
		return s.storeErr(ctx, "read the membership of a password writer", err)
	}
	actor := req.Actor
	subject := req.Principal.String()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := req.Authorize(ctx, tx); err != nil {
				return err
			}
			if err := s.lockUser(ctx, tx, req.Principal); err != nil {
				return err
			}
			var stamp time.Time
			if err := tx.QueryRow(ctx, `UPDATE `+s.q+`.users
SET password_hash = $2, password_algorithm = $3, password_updated_at = clock_timestamp(),
    legacy_token_version = legacy_token_version + 1, updated_at = clock_timestamp()
WHERE principal_id = $1 RETURNING password_updated_at`, req.Principal, req.Hash, passwordAlgorithm).Scan(&stamp); err != nil {
				return err
			}
			if err := tx.AddEvent(sessionevent.Principal(subject, sessionevent.ReasonPasswordChange, stamp)); err != nil {
				return err
			}
			if _, err := updatePublicPassword(ctx, tx, req.Principal, req.Hash); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE public.refresh_tokens SET revoked_at = now() WHERE user_id = $1 AND revoked_at IS NULL`,
				req.Principal); err != nil {
				return err
			}
			auditOrg, err := publicaudit.OrganizationOr(ctx, tx, req.AuditOrg, req.Principal)
			if err != nil || auditOrg == nil {
				return err
			}
			at, err := publicaudit.TxTime(ctx, tx)
			if err != nil {
				return err
			}
			description := "Admin changed user password"
			return publicaudit.InsertInAuditTx(ctx, tx, publicaudit.Row{OrgID: *auditOrg, UserID: &actor, Action: "password_changed",
				ResourceType: "user", ResourceID: subject, Description: &description, RequestMetadata: req.RequestMetadata, CreatedAt: at})
		},
		Audit:    passwordAudit(PasswordChanged, subject, Actor{PrincipalID: &actor, RequestID: req.RequestID}, org),
		Events:   []audit.OutboxEvent{userEvent(subject, "user.credentials_changed")},
		Refusals: []error{ErrUserNotFound},
	})
	return s.localErr(ctx, "admin set password", err)
}

// AdminSoftDeleteRequest is go-api's soft delete (userdelete.go softDeleteUser).
type AdminSoftDeleteRequest struct {
	Principal uuid.UUID
	Actor     uuid.UUID
	RequestID *string
	Caller    audit.Caller
	Authorize func(ctx context.Context, tx audit.TxOps) error
}

// AdminSoftDeleteResult is what the soft delete found and did; AlreadyDone is go-api's: the user was inactive and no
// token was left to end.
type AdminSoftDeleteResult struct {
	WasActive, AlreadyDone bool
}

// AdminSoftDeleteUser turns the login off and keeps the rows, in one audit.Commit (reason principal_disabled): the
// principal disabled, its delegated sessions ended, auth.users legacy_token_version + 1, and go-api's public writes
// (is_active false, token_version + 1, refresh tokens revoked). It runs on every call, for an
// inactive user too, as go-api's does.
func (s *Store) AdminSoftDeleteUser(ctx context.Context, req AdminSoftDeleteRequest) (result AdminSoftDeleteResult, err error) {
	defer func() { s.countLink(ctx, "admin_soft_delete", err) }()
	if !s.ownsUsers {
		return result, ErrSignupNotAvailable
	}
	if req.Authorize == nil {
		return result, errors.New("identity: an admin delete needs its actor rule")
	}
	subject := req.Principal.String()
	actor := req.Actor
	event := withChangedFields(localAudit("auth.principal.disabled", "principal_disabled", subject,
		Actor{PrincipalID: &actor, RequestID: req.RequestID}), "disabled_at")
	req.Caller.Annotate(event.Attributes)
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := req.Authorize(ctx, tx); err != nil {
				return err
			}
			r, err := applyAdminSoftDelete(ctx, tx, s.q, req.Principal)
			result = r
			if err != nil {
				return err
			}
			// A disabled user's delegated sessions end with it, as the projection's disable ends them.
			_, err = delegationend.EndOfActor(ctx, tx, s.q, req.Principal)
			return err
		},
		Audit:    event,
		Events:   []audit.OutboxEvent{principalEvent(subject, "principal.disabled", false), userEvent(subject, "user.tokens_invalidated")},
		Refusals: []error{ErrUserNotFound},
	})
	return result, err
}

func applyAdminSoftDelete(ctx context.Context, tx AdminTx, q string, principal uuid.UUID) (AdminSoftDeleteResult, error) {
	var result AdminSoftDeleteResult
	var locked uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM `+q+`.principals WHERE id = $1 AND kind = 'user' FOR UPDATE`, principal).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrUserNotFound
	}
	if err != nil {
		return result, err
	}
	err = tx.QueryRow(ctx, `SELECT COALESCE(is_active, false) FROM public.users WHERE id = $1 FOR UPDATE`, principal).Scan(&result.WasActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrLegacyProjection
	}
	if err != nil {
		return result, err
	}
	revoked, err := tx.Exec(ctx, `UPDATE public.refresh_tokens SET revoked_at = now() WHERE user_id = $1 AND revoked_at IS NULL`, principal)
	if err != nil {
		return result, err
	}
	if _, err := tx.Exec(ctx, `UPDATE public.users SET is_active = false, token_version = token_version + 1, updated_at = now() WHERE id = $1`, principal); err != nil {
		return result, err
	}
	if _, err := tx.Exec(ctx, `UPDATE `+q+`.principals SET disabled_at = COALESCE(disabled_at, clock_timestamp()), updated_at = clock_timestamp() WHERE id = $1`, principal); err != nil {
		return result, err
	}
	if _, err := tx.Exec(ctx, `UPDATE `+q+`.users SET legacy_token_version = legacy_token_version + 1, updated_at = clock_timestamp() WHERE principal_id = $1`, principal); err != nil {
		return result, err
	}
	// go-api's soft delete also deletes the user's password reset and email verification tokens; while auth-service
	// owns users those tables hold no row (the users backfill purges them, CHAOS-11506), so there is none to delete or count.
	result.AlreadyDone = !result.WasActive && revoked.RowsAffected() == 0
	return result, nil
}
