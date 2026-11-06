package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// graphQLEnforce is the one decision interceptor every GraphQL server uses (CHAOS-10865): it decides an operation's
// roots through the request's gate. fallback is the caller class of a request whose ingress named none: the MCP
// listener serves the header carrier only; the /graphql and /query server falls back to unknown, which a cell that is
// on refuses.
func graphQLEnforce(fallback pdp.CallerClass) graph.OperationEnforce {
	return graph.OperationEnforce{Authorize: func(ctx context.Context, org string, roots []graph.EnforceRoot) (map[string]pepgate.Scope, *graph.EnforceRefusal) {
		asked := make([]pdp.Root, len(roots))
		for i, root := range roots {
			asked[i] = pdp.Root{Key: root.Key, Action: root.Action}
		}
		scopes, err := pdp.FromContext(ctx).AuthorizeOperation(ctx, graphQLCaller(ctx, fallback), org, asked)
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

// graphQLCaller is the class of the operation's caller: the header carrier when its ingress marked it, else the
// bearer /graphql authenticated (a user access token, or an exempt class), else fallback.
func graphQLCaller(ctx context.Context, fallback pdp.CallerClass) pdp.Caller {
	if graph.HeaderCarrierFrom(ctx) {
		return pdp.Caller{Class: pdp.CallerHeaders}
	}
	subject, ok := graph.ShadowSubjectFrom(ctx)
	switch {
	case ok && subject.UserAccessToken && subject.Bearer != "":
		return pdp.Caller{Class: pdp.CallerUser, Bearer: subject.Bearer}
	case ok && subject.SkipReason == policy.ShadowReasonHS256:
		return pdp.Caller{Class: pdp.CallerExempt, ExemptClass: pdp.ClassHS256}
	case ok && subject.SkipReason == policy.ShadowReasonEnvelope:
		return pdp.Caller{Class: pdp.CallerExempt, ExemptClass: pdp.ClassEnvelope}
	}
	return pdp.Caller{Class: fallback}
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
