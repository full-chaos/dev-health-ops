package authprovision

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// The service account key routes on auth-service's API listener.
const (
	PathServiceAccountKeyIssue  = "/internal/v1/service-accounts/keys/issue"
	PathServiceAccountKeyRotate = "/internal/v1/service-accounts/keys/rotate"
	PathServiceAccountKeyRevoke = "/internal/v1/service-accounts/keys/revoke"
	PathServiceAccountKeyList   = "/internal/v1/service-accounts/keys/list"
)

// The wire forms the key routes answer with.
const (
	credentialReceiptSurface = "credential-receipt.v1"
	credentialEntrySurface   = "credential-entry.v1"
	credentialListSurface    = "credential-list.v1"
)

// ServiceAccountKeys is the key verbs of auth-service's service accounts as
// go-api calls them. Each answer is the route's own wire body, unchanged: an
// issue or rotate receipt carries the key's secret, which auth-service shows
// only in that one answer.
type ServiceAccountKeys interface {
	IssueKey(ctx context.Context, account, actor uuid.UUID, expiresInSeconds *int64) (json.RawMessage, error)
	RotateKey(ctx context.Context, account, actor uuid.UUID, overlapSeconds int64, expiresInSeconds *int64) (json.RawMessage, error)
	RevokeKey(ctx context.Context, credentialID string, actor uuid.UUID, reason string) (json.RawMessage, error)
	ListKeys(ctx context.Context, account, actor uuid.UUID) (json.RawMessage, error)
}

// KeyClient calls the key routes of one auth-service.
type KeyClient struct{ core *Core }

var _ ServiceAccountKeys = (*KeyClient)(nil)

// NewKeyClient builds a KeyClient on core.
func NewKeyClient(core *Core) *KeyClient { return &KeyClient{core: core} }

// post sends request to path and returns the answer when its schema_version
// is surface.
func (c *KeyClient) post(ctx context.Context, path string, request map[string]any, surface string) (json.RawMessage, error) {
	var answer json.RawMessage
	if err := c.core.Post(ctx, path, request, &answer); err != nil {
		return nil, err
	}
	var head struct {
		SchemaVersion string `json:"schema_version"`
	}
	if json.Unmarshal(answer, &head) != nil || head.SchemaVersion != surface {
		return nil, fmt.Errorf("%w: answer is not a %s", ErrUnavailable, surface)
	}
	return answer, nil
}

// IssueKey issues the first key of account, acting as actor; nil
// expiresInSeconds leaves auth-service's default.
func (c *KeyClient) IssueKey(ctx context.Context, account, actor uuid.UUID, expiresInSeconds *int64) (json.RawMessage, error) {
	request := map[string]any{"service_account_id": account.String(), "actor_principal_id": actor.String()}
	if expiresInSeconds != nil {
		request["expires_in_seconds"] = *expiresInSeconds
	}
	return c.post(ctx, PathServiceAccountKeyIssue, request, credentialReceiptSurface)
}

// RotateKey replaces account's live key, keeping the old one valid for
// overlapSeconds, acting as actor.
func (c *KeyClient) RotateKey(ctx context.Context, account, actor uuid.UUID, overlapSeconds int64, expiresInSeconds *int64) (json.RawMessage, error) {
	request := map[string]any{"service_account_id": account.String(), "actor_principal_id": actor.String(), "overlap_seconds": overlapSeconds}
	if expiresInSeconds != nil {
		request["expires_in_seconds"] = *expiresInSeconds
	}
	return c.post(ctx, PathServiceAccountKeyRotate, request, credentialReceiptSurface)
}

// RevokeKey revokes the key credentialID (its wire form, crd_<uuid>), acting
// as actor. A repeat answers the revoked entry.
func (c *KeyClient) RevokeKey(ctx context.Context, credentialID string, actor uuid.UUID, reason string) (json.RawMessage, error) {
	request := map[string]any{"credential_id": credentialID, "actor_principal_id": actor.String(), "reason": reason}
	return c.post(ctx, PathServiceAccountKeyRevoke, request, credentialEntrySurface)
}

// ListKeys lists account's keys (no secret), acting as actor.
func (c *KeyClient) ListKeys(ctx context.Context, account, actor uuid.UUID) (json.RawMessage, error) {
	request := map[string]any{"service_account_id": account.String(), "actor_principal_id": actor.String()}
	return c.post(ctx, PathServiceAccountKeyList, request, credentialListSurface)
}
