package graph

import (
	"context"
	"fmt"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// RefuseNullForNonNullArguments is a field middleware for the serving path.
// A nullable variable may feed a non-null argument that declares a default
// (see location_default_rule.go); the specification then makes an OMITTED
// variable take the default and an explicit null an error, and graphql-core
// answers the error. gqlgen's generated argument readers turn the explicit
// null into the type's zero value, so an explicit `"limit": null` would be
// served as a zero limit. This refuses it with graphql-core's message.
func RefuseNullForNonNullArguments(ctx context.Context, next graphql.Resolver) (any, error) {
	fc := graphql.GetFieldContext(ctx)
	oc := graphql.GetOperationContext(ctx)
	if fc == nil || oc == nil || fc.Field.Field == nil || fc.Field.Definition == nil {
		return next(ctx)
	}
	for _, argument := range fc.Field.Arguments {
		if argument.Value == nil || argument.Value.Kind != ast.Variable {
			continue
		}
		def := fc.Field.Definition.Arguments.ForName(argument.Name)
		if def == nil || !def.Type.NonNull {
			continue
		}
		if value, present := oc.Variables[argument.Value.Raw]; present && value == nil {
			return nil, &gqlerror.Error{
				Message: fmt.Sprintf("Argument '%s' of non-null type '%s' must not be null.", argument.Name, def.Type.String()),
				Path:    graphql.GetPath(ctx),
				Extensions: map[string]interface{}{
					"code": "GRAPHQL_VALIDATION_FAILED",
				},
			}
		}
	}
	return next(ctx)
}
