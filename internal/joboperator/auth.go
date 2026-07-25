package joboperator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	WorkerOperatorService = "worker-operator"
	ScopeWorkerRead       = "workers:read"
	ScopeWorkerOperate    = "workers:operate"
)

var (
	ErrAuthentication = errors.New("worker operator authentication failed")
	ErrAuthorization  = errors.New("worker operator authorization failed")
	workerToken       = regexp.MustCompile(`^svc_worker_[A-Za-z0-9_-]{32,256}$`)
)

// Authenticator verifies a worker operator bearer token against the semantic
// database. Only the SHA-256 digest crosses the database boundary; plaintext
// tokens are never accepted as a principal or written to audit records.
type Authenticator struct {
	pool *pgxpool.Pool
}

func NewAuthenticator(pool *pgxpool.Pool) (*Authenticator, error) {
	if pool == nil {
		return nil, ErrAuthentication
	}
	return &Authenticator{pool: pool}, nil
}

// Authentication can only be populated by Authenticate. Its authorizer is
// bound to the exact credential ID returned for the supplied token, so a
// transport cannot substitute a caller-provided principal.
type Authentication struct {
	principal Principal
	scopes    map[string]struct{}
}

func (authentication Authentication) Principal() Principal {
	return authentication.principal
}

func (authentication Authentication) Authorizer() Authorizer {
	scopes := make(map[string]struct{}, len(authentication.scopes))
	for scope := range authentication.scopes {
		scopes[scope] = struct{}{}
	}
	return &credentialAuthorizer{principal: authentication.principal, scopes: scopes}
}

func (authenticator *Authenticator) Authenticate(ctx context.Context, token string) (Authentication, error) {
	if authenticator == nil || authenticator.pool == nil || !workerToken.MatchString(token) {
		return Authentication{}, ErrAuthentication
	}
	digest := sha256.Sum256([]byte(token))
	var credentialID, rawScopes string
	err := authenticator.pool.QueryRow(ctx, `
		WITH authenticated AS (
			SELECT id, scopes
			FROM public.internal_service_credentials
			WHERE token_hash = $1
				AND service_name = $2
				AND revoked_at IS NULL
				AND (expires_at IS NULL OR expires_at > statement_timestamp())
		), touched AS (
			UPDATE public.internal_service_credentials AS credential
			SET last_used_at = statement_timestamp()
			FROM authenticated
			WHERE credential.id = authenticated.id
			RETURNING authenticated.id::text, authenticated.scopes::text
		)
		SELECT id, scopes FROM touched`,
		hex.EncodeToString(digest[:]),
		WorkerOperatorService,
	).Scan(&credentialID, &rawScopes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Authentication{}, ErrAuthentication
		}
		return Authentication{}, ErrAuthentication
	}

	var encodedScopes []string
	if err := json.Unmarshal([]byte(rawScopes), &encodedScopes); err != nil {
		return Authentication{}, ErrAuthentication
	}
	scopes, err := validatedWorkerScopes(encodedScopes)
	if err != nil || !uuidIdentifier.MatchString(credentialID) {
		return Authentication{}, ErrAuthentication
	}
	return Authentication{
		principal: Principal{Type: "service_credential", ID: credentialID},
		scopes:    scopes,
	}, nil
}

func validatedWorkerScopes(values []string) (map[string]struct{}, error) {
	if len(values) == 0 || len(values) > 2 {
		return nil, ErrAuthentication
	}
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	result := make(map[string]struct{}, len(sorted))
	for _, scope := range sorted {
		if scope != ScopeWorkerRead && scope != ScopeWorkerOperate {
			return nil, ErrAuthentication
		}
		if _, duplicate := result[scope]; duplicate {
			return nil, ErrAuthentication
		}
		result[scope] = struct{}{}
	}
	return result, nil
}

type credentialAuthorizer struct {
	principal Principal
	scopes    map[string]struct{}
}

func (authorizer *credentialAuthorizer) Authorize(_ context.Context, request AuthorizationRequest) error {
	if authorizer == nil || request.Principal != authorizer.principal ||
		authorizer.principal.Type != "service_credential" || !uuidIdentifier.MatchString(authorizer.principal.ID) {
		return ErrAuthorization
	}
	required := ScopeWorkerOperate
	if request.Action == ActionInspect || request.Action == ActionInspectRoute {
		required = ScopeWorkerRead
	}
	if _, allowed := authorizer.scopes[required]; !allowed {
		return ErrAuthorization
	}
	return nil
}
