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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	WorkerOperatorService = "worker-operator"
	ScopeWorkerRead       = "workers:read"
	ScopeWorkerOperate    = "workers:operate"
)

// The bounded operator reason-code vocabulary for authentication outcomes,
// following the same idiom as cmd/dev-health-stream-runner/dependencies.go's
// DependencyReason: every code is a compile-time constant chosen from this
// closed set, never interpolated from a token, a role name, a catalog row or
// a driver message. That is what makes an operator-visible reason safe to
// emit and log unredacted.
const (
	// ReasonAuthenticationFailed is the credential verdict: the statement ran
	// and the supplied token did not resolve to a live, correctly scoped
	// worker-operator credential. It is also the conservative default for any
	// failure whose cause is not positively identified.
	ReasonAuthenticationFailed = "authentication_failed"
	// ReasonCredentialStoreForbidden is the deployment verdict, and is
	// deliberately NOT a credential verdict: PostgreSQL refused the
	// authentication statement with 42501 before the token was ever compared,
	// so the connected role lacks SELECT/UPDATE on
	// public.internal_service_credentials. Reporting this as
	// ReasonAuthenticationFailed (CHAOS-3100) sent operators after a rotated
	// token when the actual defect was a missing grant or a binary wired onto
	// the wrong pool.
	ReasonCredentialStoreForbidden = "credential_store_forbidden"
)

// insufficientPrivilege is PostgreSQL's SQLSTATE for a denied permission. It
// is raised during execution, after parse analysis, so it cannot be confused
// with an undefined table or column.
const insufficientPrivilege = "42501"

var (
	ErrAuthentication = errors.New("worker operator authentication failed")
	ErrAuthorization  = errors.New("worker operator authorization failed")
	workerToken       = regexp.MustCompile(`^svc_worker_[A-Za-z0-9_-]{32,256}$`)
)

// authenticationFailure carries one bounded reason code alongside the
// ErrAuthentication sentinel, so existing errors.Is(err, ErrAuthentication)
// callers keep working unchanged while a caller that wants the finer verdict
// can ask for it. Its reason field is only ever assigned a constant from the
// vocabulary above.
type authenticationFailure struct{ reason string }

func (failure authenticationFailure) Error() string {
	return ErrAuthentication.Error() + ": " + failure.reason
}

func (authenticationFailure) Unwrap() error { return ErrAuthentication }

// AuthenticationReason satisfies the operator shell's reason-code interface.
func (failure authenticationFailure) AuthenticationReason() string { return failure.reason }

func authenticationDenied(reason string) error { return authenticationFailure{reason: reason} }

// AuthenticationReason maps an Authenticate error onto the bounded reason-code
// vocabulary above. Anything that does not positively identify itself — a bare
// ErrAuthentication, a wrapped one, a nil error — reports
// ReasonAuthenticationFailed, so a new failure mode can never widen what an
// operator surface prints.
func AuthenticationReason(err error) string {
	var failure interface{ AuthenticationReason() string }
	if errors.As(err, &failure) {
		return failure.AuthenticationReason()
	}
	return ReasonAuthenticationFailed
}

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
		// A privilege denial is not a credential verdict: the CTE above never
		// compared the digest, because the connected role may not read or
		// touch the credential store at all. Only the SQLSTATE is inspected
		// and only a constant is returned -- the driver's message can quote
		// statement text, so it never reaches the operator surface.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == insufficientPrivilege {
			return Authentication{}, authenticationDenied(ReasonCredentialStoreForbidden)
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
