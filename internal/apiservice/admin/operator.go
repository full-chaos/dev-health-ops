package admin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Operator is the operator-verb surface of the admin area: what
// `dev-hops admin users|orgs` did through UserService, OrganizationService and
// MembershipService, over the same pgStore the routes use. It adds no route and
// changes none; the CLI (internal/adminops) is its only caller. Each method runs
// one Python session as one transaction, so a refused step leaves nothing behind.
type Operator struct {
	Pool *pgxpool.Pool
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

// OperatorError is a refusal the operator reads: its Message is the text the
// Python verb printed after "Error: ".
type OperatorError struct{ Message string }

func (err *OperatorError) Error() string { return err.Message }

func refuse(format string, args ...any) error {
	return &OperatorError{Message: fmt.Sprintf(format, args...)}
}

// PasswordMinLength is PASSWORD_MIN_LENGTH.
const PasswordMinLength = 8

// bcryptTooLong is the text Python's bcrypt raises for a password over 72 bytes.
const bcryptTooLong = "password cannot be longer than 72 bytes, truncate manually if necessary (e.g. my_password[:72])"

// User is one user as the verbs print it.
type User struct {
	ID          uuid.UUID
	Email       string
	Username    *string
	IsSuperuser bool
	IsActive    bool
}

// Organization is one organization as the verbs print it.
type Organization struct {
	ID       uuid.UUID
	Slug     string
	Name     string
	Tier     string
	IsActive bool
}

func userView(user *fullUser) *User {
	return &User{ID: user.ID, Email: user.Email, Username: user.Username, IsSuperuser: user.IsSuperuser, IsActive: user.IsActive}
}

func orgView(org *organization) *Organization {
	return &Organization{ID: org.ID, Slug: org.Slug, Name: org.Name, Tier: org.Tier, IsActive: org.IsActive}
}

func (o Operator) store(db pgDB) pgStore { return pgStore{Pool: db, Now: o.Now} }

// inTransaction runs fn over one transaction: committed when it returns nil,
// rolled back otherwise.
func (o Operator) inTransaction(ctx context.Context, fn func(store pgStore) error) error {
	tx, err := o.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(o.store(tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func hashPassword(password string) (string, error) {
	hash, err := passwordhash.Hash(password)
	if err != nil {
		if strings.Contains(err.Error(), "72 bytes") {
			return "", refuse("%s", bcryptTooLong)
		}
		return "", err
	}
	return hash, nil
}

// CreateUserInput is `admin users create`.
type CreateUserInput struct {
	Email, Password string
	Username        *string
	FullName        *string
	Superuser       bool
}

// CreateUser is UserService.create with is_verified=True: the email and the
// username must be free, then the password is at least eight characters.
func (o Operator) CreateUser(ctx context.Context, in CreateUserInput) (*User, error) {
	var created *fullUser
	err := o.inTransaction(ctx, func(store pgStore) error {
		if existing, err := store.userByEmail(ctx, in.Email); err != nil {
			return err
		} else if existing != nil {
			return refuse("User with email %s already exists", in.Email)
		}
		if in.Username != nil && *in.Username != "" {
			if existing, err := store.userByUsername(ctx, *in.Username); err != nil {
				return err
			} else if existing != nil {
				return refuse("User with username %s already exists", *in.Username)
			}
		}
		if utf8.RuneCountInString(in.Password) < PasswordMinLength {
			return refuse("Password must be at least %d characters", PasswordMinLength)
		}
		hash, err := hashPassword(in.Password)
		if err != nil {
			return err
		}
		email := in.Email
		user, err := store.insertUser(ctx, userCreateInput{
			Email: &email, Username: in.Username, FullName: in.FullName, PasswordHash: &hash,
			IsVerified: true, IsSuperuser: in.Superuser,
		})
		if err != nil {
			return err
		}
		created = user
		return nil
	})
	if err != nil {
		return nil, err
	}
	return userView(created), nil
}

// ListUsers is UserService.list_all: newest first.
func (o Operator) ListUsers(ctx context.Context, limit int, includeInactive bool) ([]*User, error) {
	users, err := o.store(o.Pool).listAllUsers(ctx, listUsersFilter{Limit: limit, ActiveOnly: !includeInactive})
	if err != nil {
		return nil, err
	}
	out := make([]*User, len(users))
	for index, user := range users {
		out[index] = userView(user)
	}
	return out, nil
}

// UpdateUserInput is `admin users update`: a nil field is not given.
type UpdateUserInput struct {
	// The user: exactly one of ID, Email, Username is used, in that order.
	ID, Email, Username *string

	NewEmail, NewUsername, FullName    *string
	Password                           *string
	Active, Verified, Superuser        *bool
	MembershipOrg, Role, RemoveFromOrg *string
}

// UpdateUserResult is what an update did.
type UpdateUserResult struct {
	Email   string
	ID      uuid.UUID
	Changes []string
}

func pyBool(value bool) string {
	if value {
		return "True"
	}
	return "False"
}

// resolveOrg is the verb's _resolve_org: by slug, else by id (an id that is not
// a uuid is no organization).
func resolveOrg(ctx context.Context, store pgStore, identifier string) (*organization, error) {
	org, err := store.orgBySlug(ctx, identifier)
	if err != nil || org != nil {
		return org, err
	}
	id, err := pyUUID(identifier)
	if err != nil {
		return nil, nil
	}
	return store.orgByID(ctx, id)
}

// pyUUID is uuid.UUID(text) in Python (see internal/backfillrun for the same
// rule): "urn:" and "uuid:" dropped, braces trimmed, every hyphen dropped, 32 hex
// digits.
func pyUUID(text string) (uuid.UUID, error) {
	digits := strings.ReplaceAll(strings.ReplaceAll(text, "urn:", ""), "uuid:", "")
	digits = strings.ReplaceAll(strings.Trim(digits, "{}"), "-", "")
	if len(digits) != 32 {
		return uuid.Nil, errors.New("badly formed hexadecimal UUID string")
	}
	for _, character := range digits {
		if !strings.ContainsRune("0123456789abcdefABCDEF", character) {
			return uuid.Nil, errors.New("badly formed hexadecimal UUID string")
		}
	}
	digits = strings.ToLower(digits)
	return uuid.Parse(digits[0:8] + "-" + digits[8:12] + "-" + digits[12:16] + "-" + digits[16:20] + "-" + digits[20:])
}

// UpdateUser is the `admin users update` session: the user is found, the profile
// fields, the password and the memberships change in one transaction, and any
// refusal rolls the whole update back.
func (o Operator) UpdateUser(ctx context.Context, in UpdateUserInput) (*UpdateUserResult, error) {
	hasProfileChange := in.NewEmail != nil || in.NewUsername != nil || in.FullName != nil ||
		in.Active != nil || in.Verified != nil || in.Superuser != nil
	if !hasProfileChange && in.Password == nil && in.MembershipOrg == nil && in.RemoveFromOrg == nil {
		return nil, refuse("No updates specified. Provide at least one field to update.")
	}
	if in.Role != nil && in.MembershipOrg == nil {
		return nil, refuse("--role requires --org.")
	}
	var result *UpdateUserResult
	err := o.inTransaction(ctx, func(store pgStore) error {
		var user *fullUser
		var err error
		switch {
		case in.ID != nil && *in.ID != "":
			id, parseErr := pyUUID(*in.ID)
			if parseErr != nil {
				return refuse("%s", parseErr.Error())
			}
			user, err = store.fullUserByID(ctx, id)
		case in.Email != nil && *in.Email != "":
			user, err = store.userByEmail(ctx, *in.Email)
		case in.Username != nil && *in.Username != "":
			user, err = store.userByUsername(ctx, *in.Username)
		default:
			return refuse("Identify the user with --id, --email, or --username.")
		}
		if err != nil {
			return err
		}
		if user == nil {
			return refuse("User not found.")
		}
		changes := []string{}
		shownEmail := user.Email
		if hasProfileChange {
			patch := userUpdate{Email: in.NewEmail, Username: in.NewUsername, FullName: in.FullName,
				IsActive: in.Active, IsVerified: in.Verified, IsSuperuser: in.Superuser}
			updated, err := store.updateUser(ctx, user.ID, patch)
			if err != nil {
				switch {
				case errors.Is(err, errEmailExists):
					return refuse("Email %s already in use", deref(in.NewEmail))
				case errors.Is(err, errUsernameExists):
					return refuse("Username %s already in use", deref(in.NewUsername))
				}
				return err
			}
			if updated != nil {
				// The verb prints the user object the update changed in place.
				shownEmail = updated.Email
			}
			if in.NewEmail != nil {
				changes = append(changes, "email -> "+pythonparity.Strip(pythonparity.Lower(*in.NewEmail)))
			}
			if in.NewUsername != nil {
				shown := *in.NewUsername
				if shown == "" {
					shown = "(cleared)"
				}
				changes = append(changes, "username -> "+shown)
			}
			if in.FullName != nil {
				changes = append(changes, "full_name updated")
			}
			if in.Active != nil {
				changes = append(changes, "active -> "+pyBool(*in.Active))
			}
			if in.Verified != nil {
				changes = append(changes, "verified -> "+pyBool(*in.Verified))
			}
			if in.Superuser != nil {
				changes = append(changes, "superuser -> "+pyBool(*in.Superuser))
			}
		}
		if in.Password != nil {
			if utf8.RuneCountInString(*in.Password) < PasswordMinLength {
				return refuse("Password must be at least %d characters", PasswordMinLength)
			}
			hash, err := hashPassword(*in.Password)
			if err != nil {
				return err
			}
			tx, err := store.Pool.Begin(ctx)
			if err != nil {
				return err
			}
			if _, err := store.setUserPassword(ctx, tx, user.ID, hash); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			if err := store.revokeAllRefreshTokens(ctx, tx, user.ID); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			changes = append(changes, "password updated (existing sessions revoked)")
		}
		if in.MembershipOrg != nil {
			org, err := resolveOrg(ctx, store, *in.MembershipOrg)
			if err != nil {
				return err
			}
			if org == nil {
				return refuse("Organization '%s' not found.", *in.MembershipOrg)
			}
			existing, err := store.membershipByOrgUser(ctx, org.ID, user.ID)
			if err != nil {
				return err
			}
			if existing != nil {
				role := existing.Role
				if in.Role != nil && *in.Role != "" {
					role = *in.Role
				}
				if _, err := store.updateMembershipRole(ctx, org.ID, user.ID, role); err != nil {
					if errors.Is(err, errInvalidRole) {
						return refuse("Invalid role: %s", role)
					}
					return err
				}
				changes = append(changes, fmt.Sprintf("org '%s' role -> %s", org.Slug, role))
			} else {
				role := "member"
				if in.Role != nil && *in.Role != "" {
					role = *in.Role
				}
				if _, err := store.insertMembership(ctx, org.ID, user.ID, role, nil); err != nil {
					if errors.Is(err, errMembershipExists) {
						return refuse("User is already a member of this organization")
					}
					return err
				}
				changes = append(changes, fmt.Sprintf("added to org '%s' as %s", org.Slug, role))
			}
		}
		if in.RemoveFromOrg != nil {
			org, err := resolveOrg(ctx, store, *in.RemoveFromOrg)
			if err != nil {
				return err
			}
			if org == nil {
				return refuse("Organization '%s' not found.", *in.RemoveFromOrg)
			}
			removed, err := store.removeMembership(ctx, org.ID, user.ID)
			if err != nil {
				if errors.Is(err, errLastOwner) {
					return refuse("Cannot remove the last owner of an organization")
				}
				return err
			}
			if removed {
				changes = append(changes, fmt.Sprintf("removed from org '%s'", org.Slug))
			} else {
				changes = append(changes, fmt.Sprintf("not a member of org '%s' (no change)", org.Slug))
			}
		}
		result = &UpdateUserResult{Email: shownEmail, ID: user.ID, Changes: changes}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// CreateOrgInput is `admin orgs create`.
type CreateOrgInput struct {
	Name        string
	Slug        *string
	Description *string
	Tier        string
	OwnerEmail  *string
}

// CreateOrgResult is what a create made.
type CreateOrgResult struct {
	Org        *Organization
	OwnerEmail string
}

// validateOrganizationName is validate_organization_name.
func validateOrganizationName(name string) (string, error) {
	normalized := pythonparity.Strip(name)
	if normalized == "" {
		return "", refuse("Workspace name is required")
	}
	if utf8.RuneCountInString(normalized) > 100 {
		return "", refuse("Workspace name must be 100 characters or fewer")
	}
	return normalized, nil
}

// CreateOrg is OrganizationService.create with the owner looked up first: the
// slug is the name's slug unless given, and an existing slug gets a random
// eight-hex-digit suffix; a paid tier is managed "manual", community "stripe".
func (o Operator) CreateOrg(ctx context.Context, in CreateOrgInput) (*CreateOrgResult, error) {
	var result *CreateOrgResult
	err := o.inTransaction(ctx, func(store pgStore) error {
		var ownerID *uuid.UUID
		ownerEmail := ""
		if in.OwnerEmail != nil && *in.OwnerEmail != "" {
			owner, err := store.userByEmail(ctx, *in.OwnerEmail)
			if err != nil {
				return err
			}
			if owner == nil {
				return refuse("User with email %s not found", *in.OwnerEmail)
			}
			ownerID, ownerEmail = &owner.ID, *in.OwnerEmail
		}
		name, err := validateOrganizationName(in.Name)
		if err != nil {
			return err
		}
		slug := ""
		if in.Slug != nil {
			slug = *in.Slug
		}
		if slug == "" {
			slug = slugify(name)
		}
		if existing, err := store.orgBySlug(ctx, slug); err != nil {
			return err
		} else if existing != nil {
			suffix := make([]byte, 4)
			if _, err := rand.Read(suffix); err != nil {
				return err
			}
			slug = slug + "-" + hex.EncodeToString(suffix)
		}
		managedBy := "stripe"
		if in.Tier != "community" {
			managedBy = "manual"
		}
		org := &organization{ID: uuid.New(), Slug: slug, Name: name, Description: in.Description,
			Settings: []byte("{}"), Tier: in.Tier, ManagedBy: managedBy, IsActive: true}
		if err := store.insertOrganizationTx(ctx, store.Pool, org); err != nil {
			return err
		}
		if ownerID != nil {
			if _, err := store.insertMembershipTx(ctx, store.Pool, org.ID, *ownerID, "owner", nil); err != nil {
				return err
			}
		}
		result = &CreateOrgResult{Org: orgView(org), OwnerEmail: ownerEmail}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ListOrgs is OrganizationService.list_all: newest first.
func (o Operator) ListOrgs(ctx context.Context, limit int, includeInactive bool) ([]*Organization, error) {
	orgs, err := o.store(o.Pool).listOrganizations(ctx, limit, 0, !includeInactive)
	if err != nil {
		return nil, err
	}
	out := make([]*Organization, len(orgs))
	for index, org := range orgs {
		out[index] = orgView(org)
	}
	return out, nil
}

var _ = pgx.ErrNoRows
