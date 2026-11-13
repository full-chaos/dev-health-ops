package authprovision

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// The user-admin routes auth-service serves to go-api while it owns users (AUTH_USERS_OWNED).
const (
	PathUsersCreate   = "/internal/v1/users/create"
	PathUsersUpdate   = "/internal/v1/users/update"
	PathUsersPassword = "/internal/v1/users/password"
	PathUsersDelete   = "/internal/v1/users/delete"
)

// UserCreateRequest is go-api's admin create after its own checks. A nil field is left out of the request.
type UserCreateRequest struct {
	Actor                                                          uuid.UUID
	Email                                                          string
	PasswordHash, Username, FullName, AuthProvider, AuthProviderID *string
	IsVerified, GrantPlatformAdmin                                 bool
	// OrgID and Role go together: an org-scoped create.
	OrgID           *uuid.UUID
	Role            string
	RequestMetadata json.RawMessage
}

// CreatedUser is the principal auth-service created.
type CreatedUser struct {
	ID    uuid.UUID
	Email string
}

// UserUpdateRequest is go-api's admin update; a nil field is not sent and does not change.
type UserUpdateRequest struct {
	Actor, Principal          uuid.UUID
	OrgID                     *uuid.UUID
	Email, Username, FullName *string
	AvatarURL                 *string
	IsVerified, IsSuperuser   *bool
	// IsActive false deactivates the user (the soft delete's writes), true reactivates it.
	IsActive *bool
}

// UserPasswordRequest is go-api's admin password set: the acting admin's password, checked by auth-service, and the
// new verifier.
type UserPasswordRequest struct {
	Actor, Principal            uuid.UUID
	OrgID                       *uuid.UUID
	AdminPassword, PasswordHash string
	RequestMetadata             json.RawMessage
}

// UserDeleteRequest is go-api's admin delete; Mode is "soft".
type UserDeleteRequest struct {
	Actor, Principal uuid.UUID
	OrgID            *uuid.UUID
	Mode             string
}

// DeletedUser is a delete's answer: AlreadyDone is a user that was already inactive.
type DeletedUser struct {
	WasActive, AlreadyDone bool
}

// UsersAdmin is the user-admin routes as go-api calls them.
type UsersAdmin interface {
	CreateUser(ctx context.Context, in UserCreateRequest) (CreatedUser, error)
	UpdateUser(ctx context.Context, in UserUpdateRequest) error
	SetUserPassword(ctx context.Context, in UserPasswordRequest) error
	DeleteUser(ctx context.Context, in UserDeleteRequest) (DeletedUser, error)
}

// UsersClient calls the user-admin routes of one auth-service.
type UsersClient struct{ core *Core }

var _ UsersAdmin = (*UsersClient)(nil)

// NewUsersClient is a UsersClient over core.
func NewUsersClient(core *Core) *UsersClient { return &UsersClient{core: core} }

func setText(request map[string]any, name string, value *string) {
	if value != nil {
		request[name] = *value
	}
}

func setOrg(request map[string]any, org *uuid.UUID) {
	if org != nil {
		request["organization_id"] = org.String()
	}
}

func setMetadata(request map[string]any, metadata json.RawMessage) {
	if len(metadata) > 0 && len(metadata) <= maxRequestMetadataBytes {
		request["request_metadata"] = metadata
	}
}

// CreateUser creates the user in both schemas in one auth-service commit.
func (c *UsersClient) CreateUser(ctx context.Context, in UserCreateRequest) (CreatedUser, error) {
	request := map[string]any{"actor_principal_id": in.Actor.String(), "email": in.Email, "is_verified": in.IsVerified,
		"grant_platform_admin": in.GrantPlatformAdmin}
	setText(request, "password_hash", in.PasswordHash)
	setText(request, "username", in.Username)
	setText(request, "full_name", in.FullName)
	setText(request, "auth_provider", in.AuthProvider)
	setText(request, "auth_provider_id", in.AuthProviderID)
	if in.OrgID != nil {
		setOrg(request, in.OrgID)
		request["role"] = in.Role
	}
	setMetadata(request, in.RequestMetadata)
	var answer struct {
		PrincipalID string `json:"principal_id"`
		Email       string `json:"email"`
	}
	if err := c.core.Post(ctx, PathUsersCreate, request, &answer); err != nil {
		return CreatedUser{}, err
	}
	id, err := uuid.Parse(answer.PrincipalID)
	if err != nil || answer.Email == "" {
		return CreatedUser{}, fmt.Errorf("%w: answer is not a created user", ErrUnavailable)
	}
	return CreatedUser{ID: id, Email: answer.Email}, nil
}

type userChangeAnswer struct {
	PrincipalID string `json:"principal_id"`
}

func (c *UsersClient) postChange(ctx context.Context, path string, request map[string]any, principal uuid.UUID) error {
	var answer userChangeAnswer
	if err := c.core.Post(ctx, path, request, &answer); err != nil {
		return err
	}
	if answer.PrincipalID != principal.String() {
		return fmt.Errorf("%w: answer is not the user", ErrUnavailable)
	}
	return nil
}

// UpdateUser sends the changed fields of an admin update.
func (c *UsersClient) UpdateUser(ctx context.Context, in UserUpdateRequest) error {
	request := map[string]any{"actor_principal_id": in.Actor.String(), "principal_id": in.Principal.String()}
	setOrg(request, in.OrgID)
	setText(request, "email", in.Email)
	setText(request, "username", in.Username)
	setText(request, "full_name", in.FullName)
	setText(request, "avatar_url", in.AvatarURL)
	if in.IsVerified != nil {
		request["is_verified"] = *in.IsVerified
	}
	if in.IsSuperuser != nil {
		request["is_superuser"] = *in.IsSuperuser
	}
	if in.IsActive != nil {
		request["is_active"] = *in.IsActive
	}
	return c.postChange(ctx, PathUsersUpdate, request, in.Principal)
}

// SetUserPassword sets the user's verifier after auth-service checks the acting admin's password.
func (c *UsersClient) SetUserPassword(ctx context.Context, in UserPasswordRequest) error {
	request := map[string]any{"actor_principal_id": in.Actor.String(), "principal_id": in.Principal.String(),
		"admin_password": in.AdminPassword, "password_hash": in.PasswordHash}
	setOrg(request, in.OrgID)
	setMetadata(request, in.RequestMetadata)
	return c.postChange(ctx, PathUsersPassword, request, in.Principal)
}

// DeleteUser soft-deletes the user.
func (c *UsersClient) DeleteUser(ctx context.Context, in UserDeleteRequest) (DeletedUser, error) {
	request := map[string]any{"actor_principal_id": in.Actor.String(), "principal_id": in.Principal.String(), "mode": in.Mode}
	setOrg(request, in.OrgID)
	var answer struct {
		PrincipalID string `json:"principal_id"`
		WasActive   bool   `json:"was_active"`
		AlreadyDone bool   `json:"already_done"`
	}
	if err := c.core.Post(ctx, PathUsersDelete, request, &answer); err != nil {
		return DeletedUser{}, err
	}
	if answer.PrincipalID != in.Principal.String() {
		return DeletedUser{}, fmt.Errorf("%w: answer is not the user", ErrUnavailable)
	}
	return DeletedUser{WasActive: answer.WasActive, AlreadyDone: answer.AlreadyDone}, nil
}
