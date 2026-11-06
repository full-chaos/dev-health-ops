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

// refusesLegacyImpersonation reads AUTH_PROXY_IMPERSONATION: unset or empty
// is off, true or false as strconv.ParseBool reads them, any other value an
// error, which every reader treats as on (fail closed) and which never stops
// the process (warnInvalidImpersonationSwitch).
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

// warnInvalidImpersonationSwitch writes the one WARN for an
// AUTH_PROXY_IMPERSONATION that does not parse: the process starts, and the
// legacy caller is refused as with the switch on.
func warnInvalidImpersonationSwitch(getenv getenvFunc) {
	if _, err := refusesLegacyImpersonation(getenv); err != nil {
		slog.Warn("query-api: the impersonation switch does not parse; the legacy impersonation caller is refused as with it on",
			slog.String("variable", impersonationProxyEnv), slog.String("accepted", "unset, true or false (as strconv.ParseBool reads them)"))
	}
}

// newGraphQLEdgeDeps is /graphql's middleware dependencies from the settings.
// A switch value that does not parse refuses (fail closed).
func newGraphQLEdgeDeps(getenv getenvFunc, lookup func(string) (string, bool), auth *policy.Authenticator, logger *slog.Logger) graphQLEdgeDeps {
	refuse, err := refusesLegacyImpersonation(getenv)
	return graphQLEdgeDeps{
		auth:                      auth,
		corsOrigins:               corsAllowedOrigins(lookup),
		maxBytes:                  graphQLMaxQueryBytes(getenv),
		logger:                    logger,
		refuseLegacyImpersonation: refuse || err != nil,
	}
}

// errLegacyImpersonationRefused is liveScope's answer for the legacy caller
// while the request refuses it (withLegacyImpersonationRefused).
var errLegacyImpersonationRefused = errors.New("legacy impersonation caller refused")

type legacyImpersonationRefusedKey struct{}

// legacyRefusal is the request's refusal state: liveScope sets holderRowLive
// for a holder of admin.impersonate that is not a superuser (its row is never
// applied) when its row is live or cannot be read, and the dispatch then
// refuses its non-query documents.
type legacyRefusal struct{ holderRowLive bool }

// withLegacyImpersonationRefused marks the request so liveScope refuses the
// admin's own bearer beside a live go-api row instead of serving it as the target.
func withLegacyImpersonationRefused(ctx context.Context) context.Context {
	return context.WithValue(ctx, legacyImpersonationRefusedKey{}, &legacyRefusal{})
}

func legacyImpersonationRefused(ctx context.Context) bool {
	_, refused := ctx.Value(legacyImpersonationRefusedKey{}).(*legacyRefusal)
	return refused
}

func markHolderRowLive(ctx context.Context) {
	if state, ok := ctx.Value(legacyImpersonationRefusedKey{}).(*legacyRefusal); ok {
		state.holderRowLive = true
	}
}

func holderRowLive(ctx context.Context) bool {
	state, ok := ctx.Value(legacyImpersonationRefusedKey{}).(*legacyRefusal)
	return ok && state.holderRowLive
}

// legacyDocumentReads reports whether text, the document the resolver runs,
// is one query operation by its AST (digest.DocumentKind); a mutation, a
// subscription and a text that does not parse are not.
func legacyDocumentReads(text string) bool {
	kind, err := digest.DocumentKind(text)
	return err == nil && kind == digest.KindQuery
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
