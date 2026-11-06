package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
)

// impersonationProxyEnv is go-api's AUTH_PROXY_IMPERSONATION: on, an
// impersonation is a delegated session whose own bearer is the target, and
// auth-service still writes go-api's impersonation row beside the admin's
// own bearer.
const impersonationProxyEnv = "AUTH_PROXY_IMPERSONATION"

// refusesLegacyImpersonation reads AUTH_PROXY_IMPERSONATION as go-api reads
// it: unset or empty is off, true or false as strconv.ParseBool reads them,
// any other value an error.
func refusesLegacyImpersonation(getenv getenvFunc) (bool, error) {
	raw := strings.TrimSpace(getenv(impersonationProxyEnv))
	if raw == "" {
		return false, nil
	}
	on, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", impersonationProxyEnv)
	}
	return on, nil
}

// newGraphQLEdgeDeps is /graphql's middleware dependencies from the settings.
func newGraphQLEdgeDeps(getenv getenvFunc, lookup func(string) (string, bool), auth *policy.Authenticator, logger *slog.Logger) (graphQLEdgeDeps, error) {
	refuse, err := refusesLegacyImpersonation(getenv)
	if err != nil {
		return graphQLEdgeDeps{}, err
	}
	return graphQLEdgeDeps{
		auth:                      auth,
		corsOrigins:               corsAllowedOrigins(lookup),
		maxBytes:                  graphQLMaxQueryBytes(getenv),
		logger:                    logger,
		refuseLegacyImpersonation: refuse,
	}, nil
}

// errLegacyImpersonationRefused is liveScope's answer for the legacy caller
// while the request refuses it (withLegacyImpersonationRefused).
var errLegacyImpersonationRefused = errors.New("legacy impersonation caller refused")

type legacyImpersonationRefusedKey struct{}

// withLegacyImpersonationRefused marks the request so liveScope refuses the
// admin's own bearer beside a live go-api row instead of serving it as the target.
func withLegacyImpersonationRefused(ctx context.Context) context.Context {
	return context.WithValue(ctx, legacyImpersonationRefusedKey{}, true)
}

func legacyImpersonationRefused(ctx context.Context) bool {
	refused, _ := ctx.Value(legacyImpersonationRefusedKey{}).(bool)
	return refused
}

// graphQLEdgeReads is /graphql's read test for the read limit of the legacy
// impersonation refusal (policy.Scope.RefuseLegacyImpersonationReading). GET,
// HEAD and OPTIONS read, by the method as go-api tells them (the dispatch
// refuses a mutation over GET). A POST reads when its document is one query
// operation, by the document's AST (digest.DocumentKind): its body is read
// within limit and handed on whole, and a body or document that cannot be
// read or parsed, or is not one query, is not a read. Other methods do not read.
func graphQLEdgeReads(limit int) func(r *http.Request) bool {
	return func(r *http.Request) bool {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			return true
		case http.MethodPost:
		default:
			return false
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, int64(limit)+1))
		r.Body = handedOnBody{Reader: io.MultiReader(bytes.NewReader(body), r.Body), Closer: r.Body}
		if err != nil || len(body) > limit {
			return false
		}
		text, _ := graphQLEdgePOSTDocument(r, body)
		kind, err := digest.DocumentKind(text)
		return err == nil && kind == digest.KindQuery
	}
}

// handedOnBody is a request body put back after a read: the bytes read, then
// the rest, closed by the original.
type handedOnBody struct {
	io.Reader
	io.Closer
}
