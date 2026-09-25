package graph

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqljson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/reports"
)

// requestBodyKey carries the raw POST body of the request to the resolvers.
type requestBodyKey struct{}

// WithRequestBody attaches the raw request body so a resolver can read a JSON
// variable in the order the client wrote its keys. gqlgen decodes variables
// into Go maps before a resolver runs, which loses that order; the Python
// resolvers store the JSON columns in it (dict insertion order).
func WithRequestBody(ctx context.Context, body []byte) context.Context {
	return context.WithValue(ctx, requestBodyKey{}, body)
}

// requireMutationOrg is the organisation rule of a mutation. The Python
// schema extension (OrgIdAuthExtension) refuses an orgId that is not a
// non-empty, unpadded string, and refuses an org that is not the caller's
// unless the caller is a verified, non-impersonating superuser.
//
// Named divergence: the request identity query-api verifies carries no
// "verified" flag for a superuser, so a superuser's write to another org is
// refused here where Python allows it. The refusal is the safe direction.
// Errors carry a message only, as Python's AuthorizationError does.
func requireMutationOrg(ctx context.Context, orgID string) error {
	fail := func(message string) error {
		return &gqlerror.Error{Message: message, Path: graphql.GetPath(ctx)}
	}
	if orgID == "" || orgID != strings.TrimSpace(orgID) {
		return fail("A valid organization ID is required")
	}
	claims, ok := authctx.FromContext(ctx)
	if !ok || claims.OrgID == "" {
		return fail("Authorization required")
	}
	if claims.OrgID != orgID {
		return fail("Access denied: cannot query org '" + orgID + "'")
	}
	return nil
}

func (r *mutationResolver) writer() *reports.Writer { return r.ReportWriter }

// inputJSON is the client's JSON for field of the input argument, as bytes in
// the client's key order when the argument was sent as a variable and the raw
// body is known, else the canonical encoding gqlgen decoded.
func inputJSON(ctx context.Context, argument, field string, decoded graphqljson.JSON) []byte {
	if raw, ok := rawVariableMember(ctx, argument, field); ok {
		return raw
	}
	return []byte(decoded)
}

func rawVariableMember(ctx context.Context, argument, field string) ([]byte, bool) {
	body, ok := ctx.Value(requestBodyKey{}).([]byte)
	if !ok {
		return nil, false
	}
	fc := graphql.GetFieldContext(ctx)
	if fc == nil || fc.Field.Field == nil {
		return nil, false
	}
	arg := fc.Field.Arguments.ForName(argument)
	if arg == nil || arg.Value == nil || arg.Value.Kind != ast.Variable {
		return nil, false
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, false
	}
	var variables map[string]json.RawMessage
	if err := json.Unmarshal(top["variables"], &variables); err != nil {
		return nil, false
	}
	input, present := variables[arg.Value.Raw]
	if !present {
		return nil, false
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(input, &members); err != nil {
		return nil, false
	}
	member, present := members[field]
	return []byte(member), present
}

func (r *mutationResolver) createSavedReport(ctx context.Context, orgID string, input model.CreateSavedReportInput) (*model.SavedReportType, error) {
	if err := requireMutationOrg(ctx, orgID); err != nil {
		return nil, err
	}
	return r.writer().Create(ctx, orgID, reports.CreateInput{
		Name:             input.Name,
		Description:      input.Description,
		ReportPlan:       inputJSON(ctx, "input", "reportPlan", input.ReportPlan),
		IsTemplate:       input.IsTemplate,
		Parameters:       inputJSON(ctx, "input", "parameters", input.Parameters),
		ScheduleCron:     input.ScheduleCron,
		ScheduleTimezone: input.ScheduleTimezone,
	})
}

func (r *mutationResolver) updateSavedReport(ctx context.Context, orgID, reportID string, input model.UpdateSavedReportInput) (*model.SavedReportType, error) {
	if err := requireMutationOrg(ctx, orgID); err != nil {
		return nil, err
	}
	return r.writer().Update(ctx, orgID, reportID, reports.UpdateInput{
		Name:             input.Name,
		Description:      input.Description,
		ReportPlan:       inputJSON(ctx, "input", "reportPlan", input.ReportPlan),
		IsTemplate:       input.IsTemplate,
		Parameters:       inputJSON(ctx, "input", "parameters", input.Parameters),
		IsActive:         input.IsActive,
		ScheduleCron:     input.ScheduleCron,
		ScheduleTimezone: input.ScheduleTimezone,
	})
}

func (r *mutationResolver) deleteSavedReport(ctx context.Context, orgID, reportID string) (bool, error) {
	if err := requireMutationOrg(ctx, orgID); err != nil {
		return false, err
	}
	return r.writer().Delete(ctx, orgID, reportID)
}

func (r *mutationResolver) cloneSavedReport(ctx context.Context, orgID string, input model.CloneSavedReportInput) (*model.SavedReportType, error) {
	if err := requireMutationOrg(ctx, orgID); err != nil {
		return nil, err
	}
	return r.writer().Clone(ctx, orgID, reports.CloneInput{
		SourceReportID:     input.SourceReportID,
		NewName:            input.NewName,
		ParameterOverrides: inputJSON(ctx, "input", "parameterOverrides", input.ParameterOverrides),
	})
}

func (r *mutationResolver) triggerReport(ctx context.Context, orgID, reportID string) (*model.ReportRunType, error) {
	if err := requireMutationOrg(ctx, orgID); err != nil {
		return nil, err
	}
	return r.writer().Trigger(ctx, orgID, reportID)
}
