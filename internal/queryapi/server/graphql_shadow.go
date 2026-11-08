package server

import (
	"context"
	"log/slog"
	"net/http"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// graphQLShadow is the one interceptor both GraphQL servers use: it reads the process's shadow from the request's gate.
// No gate, or a gate with no shadow (off, enforce, a bad knob), is off. A root the gate decides for a user caller is
// not observed: it has no legacy answer.
func graphQLShadow() graph.OperationShadow {
	return graph.OperationShadow{Sink: func(ctx context.Context) graph.ShadowSink {
		if shadow := pdp.FromContext(ctx).Shadow(); shadow != nil {
			return shadow
		}
		return nil
	}, Enforced: func(ctx context.Context, action string) bool {
		if claims, _ := authctx.FromContext(ctx); claims.ImpersonationActive && !claims.Delegated {
			return false // a legacy impersonation session is not decided, so it keeps its shadow
		}
		return pdp.FromContext(ctx).RootEnforced(pdp.CallerUser, action)
	}}
}

// useGraphQLGuards is the guard slot of every GraphQL server: the shadow first, then the org guard, then the decision
// (CHAOS-10865), which decides for the organization the guard admitted.
func useGraphQLGuards(gql *gqlhandler.Server) {
	gql.Use(graphQLShadow())
	gql.Use(graph.OperationOrgGuard{})
	gql.Use(graphQLEnforce())
}

// graphQLShadowRows are the shadow's rows of the GraphQL roots: "Query.name" and "Mutation.name", each the catalog row
// of its action. A root whose action is not a shadow row is left out, after one ERROR.
func graphQLShadowRows() map[string]policy.ShadowRow {
	rows := map[string]policy.ShadowRow{}
	for key, action := range graph.FieldActionTable() {
		row, err := policy.ShadowRowFor(action)
		if err != nil {
			slog.Error("query-api pdp: a GraphQL root has no shadow row", slog.String("root", key), slog.String("action", action))
			continue
		}
		rows[key] = row
	}
	return rows
}

// graphQLShadowSubject is the subject /graphql hands the shadow for the bearer it authenticated: the token and its class.
func graphQLShadowSubject(token string) graph.ShadowSubject {
	class := principal.AccessClassOf(token)
	return graph.ShadowSubject{Bearer: token, UserAccessToken: class == principal.AccessClassUser, SkipReason: shadowSkipReason(class)}
}

// withGate carries gate on each request's context, for a listener the plane handler does not wrap (the internal and
// MCP ones).
func withGate(next http.Handler, gate *pdp.Gate) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gated := r.WithContext(pdp.WithGate(r.Context(), gate))
		next.ServeHTTP(w, gated)
		r.Pattern = gated.Pattern
	})
}
