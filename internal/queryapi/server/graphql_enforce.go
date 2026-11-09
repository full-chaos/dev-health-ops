package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// graphQLEnforce is the one decision interceptor every GraphQL server uses (CHAOS-10865): it decides an operation's
// roots through the request's gate.
func graphQLEnforce() graph.OperationEnforce {
	return graph.OperationEnforce{Authorize: func(ctx context.Context, claims authctx.Claims, roots []graph.EnforceRoot) (map[string]pepgate.Scope, *graph.EnforceRefusal) {
		asked := make([]pdp.Root, len(roots))
		for i, root := range roots {
			asked[i] = pdp.Root{Key: root.Key, Action: root.Action}
		}
		caller := graphQLCaller(ctx)
		caller.Unscoped = claims.Scope.Kind() == pepgate.KindNone
		caller.LegacyImpersonation = claims.ImpersonationActive && !claims.Delegated
		scopes, err := pdp.FromContext(ctx).AuthorizeOperation(ctx, caller, claims.OrgID, asked)
		if err == nil {
			return scopes, nil
		}
		var denied *pdp.DenyError
		if !errors.As(err, &denied) {
			return nil, &graph.EnforceRefusal{Reason: string(pep.ReasonNoDecision)}
		}
		return nil, &graph.EnforceRefusal{Forbidden: !denied.Local, Reason: string(denied.Reason)}
	}}
}

// graphQLCaller is the class of the operation's caller, from the bearer its ingress authenticated (the subject #5781
// carries): a user access token is decided; the envelope is the exempt class (an HS256 token never authenticates: its
// class is retired); no bearer is the X-DH-Internal-* header carrier (D6721), which has its own cells.
func graphQLCaller(ctx context.Context) pdp.Caller {
	subject, _ := graph.ShadowSubjectFrom(ctx)
	switch {
	case subject.Bearer == "":
		return pdp.Caller{Class: pdp.CallerHeaders}
	case subject.UserAccessToken:
		return pdp.Caller{Class: pdp.CallerUser, Bearer: subject.Bearer}
	case subject.SkipReason == policy.ShadowReasonEnvelope:
		return pdp.Caller{Class: pdp.CallerExempt, ExemptClass: pdp.ClassEnvelope}
	}
	return pdp.Caller{Class: pdp.CallerUnknown}
}

// refusalStatusWriter answers 503 for an operation OperationEnforce refused as unavailable; every other answer is
// written as it is.
type refusalStatusWriter struct {
	http.ResponseWriter
	status *graph.RefusalStatus
	wrote  bool
}

func (w *refusalStatusWriter) WriteHeader(code int) {
	if !w.wrote && code == http.StatusOK && w.status.Unavailable() {
		code = http.StatusServiceUnavailable
	}
	w.wrote = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *refusalStatusWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *refusalStatusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// withRefusalStatus carries a RefusalStatus to the operation and the writer that reads it.
func withRefusalStatus(w http.ResponseWriter, r *http.Request) (http.ResponseWriter, *http.Request) {
	status := &graph.RefusalStatus{}
	return &refusalStatusWriter{ResponseWriter: w, status: status}, r.WithContext(graph.WithRefusalStatus(r.Context(), status))
}

// serveMCPGraphQL runs the MCP listener's operation into buffered; an operation the decision refused answers with its
// typed status (S4): 503 unavailable, 403 a deny. The body is the refusal.
func serveMCPGraphQL(gql http.Handler, buffered *mcpBufferedResponse, r *http.Request) {
	refusal := &graph.RefusalStatus{}
	gql.ServeHTTP(buffered, r.WithContext(graph.WithRefusalStatus(r.Context(), refusal)))
	switch {
	case refusal.Unavailable():
		buffered.status = http.StatusServiceUnavailable
	case refusal.Forbidden():
		buffered.status = http.StatusForbidden
	}
}
