package authprovision

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// The service account lifecycle routes on auth-service's API listener.
const (
	PathServiceAccountCreate  = "/internal/v1/service-accounts/create"
	PathServiceAccountDisable = "/internal/v1/service-accounts/disable"
	PathServiceAccountEnable  = "/internal/v1/service-accounts/enable"
	PathServiceAccountOwner   = "/internal/v1/service-accounts/owner"
)

// ServiceAccounts is the lifecycle verbs of auth-service's service accounts
// as go-api calls them. Each answer is the route's own wire body, unchanged.
type ServiceAccounts interface {
	CreateAccount(ctx context.Context, org, owner, actor uuid.UUID, name string) (json.RawMessage, error)
	DisableAccount(ctx context.Context, account, actor uuid.UUID) (json.RawMessage, error)
	EnableAccount(ctx context.Context, account, actor uuid.UUID) (json.RawMessage, error)
	SetAccountOwner(ctx context.Context, account, owner, actor uuid.UUID) (json.RawMessage, error)
}

// AccountClient calls the lifecycle routes of one auth-service.
type AccountClient struct{ core *Core }

var _ ServiceAccounts = (*AccountClient)(nil)

// NewAccountClient builds an AccountClient on core.
func NewAccountClient(core *Core) *AccountClient { return &AccountClient{core: core} }

// post sends request to path and returns the answer when it names the
// service account the request named (any account for a create).
func (c *AccountClient) post(ctx context.Context, path string, request map[string]any, account string) (json.RawMessage, error) {
	var answer json.RawMessage
	if err := c.core.Post(ctx, path, request, &answer); err != nil {
		return nil, err
	}
	var head struct {
		ServiceAccountID string `json:"service_account_id"`
	}
	if json.Unmarshal(answer, &head) != nil {
		return nil, fmt.Errorf("%w: answer is not a service account", ErrUnavailable)
	}
	id, err := uuid.Parse(head.ServiceAccountID)
	if err != nil || id.String() != head.ServiceAccountID || (account != "" && head.ServiceAccountID != account) {
		return nil, fmt.Errorf("%w: answer is not the service account asked for", ErrUnavailable)
	}
	return answer, nil
}

// CreateAccount creates the service account name in org, owned by owner,
// acting as actor.
func (c *AccountClient) CreateAccount(ctx context.Context, org, owner, actor uuid.UUID, name string) (json.RawMessage, error) {
	request := map[string]any{"organization_id": org.String(), "owner_principal_id": owner.String(), "actor_principal_id": actor.String(), "name": name}
	return c.post(ctx, PathServiceAccountCreate, request, "")
}

// DisableAccount disables account, acting as actor.
func (c *AccountClient) DisableAccount(ctx context.Context, account, actor uuid.UUID) (json.RawMessage, error) {
	request := map[string]any{"service_account_id": account.String(), "actor_principal_id": actor.String()}
	return c.post(ctx, PathServiceAccountDisable, request, account.String())
}

// EnableAccount enables account, acting as actor.
func (c *AccountClient) EnableAccount(ctx context.Context, account, actor uuid.UUID) (json.RawMessage, error) {
	request := map[string]any{"service_account_id": account.String(), "actor_principal_id": actor.String()}
	return c.post(ctx, PathServiceAccountEnable, request, account.String())
}

// SetAccountOwner names owner the owner of account, acting as actor.
func (c *AccountClient) SetAccountOwner(ctx context.Context, account, owner, actor uuid.UUID) (json.RawMessage, error) {
	request := map[string]any{"service_account_id": account.String(), "owner_principal_id": owner.String(), "actor_principal_id": actor.String()}
	return c.post(ctx, PathServiceAccountOwner, request, account.String())
}
