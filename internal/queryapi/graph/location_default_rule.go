package graph

import (
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/validator"
	_ "github.com/vektah/gqlparser/v2/validator/rules" // registers the rule this file replaces
)

// ruleName is gqlparser's own name for the rule this file replaces.
const ruleName = "VariablesInAllowedPosition"

// The GraphQL specification lets a nullable variable feed a non-null argument
// when the ARGUMENT declares a default value (its `hasLocationDefaultValue`
// clause), and graphql-core, which serves the Python plane, follows it. The
// web client relies on that: its saved-report documents declare `$limit: Int`
// for `limit: Int! = 50`. gqlparser only honours a default declared on the
// variable, so without this replacement the same document is answered by a
// validation error here and served by Python. The replacement is otherwise
// gqlparser's rule unchanged: a nullable variable feeding a non-null argument
// with no default, or any other incompatible type, is still refused.
func init() {
	validator.RemoveRule(ruleName)
	validator.AddRule(ruleName, locationDefaultVariableRule)
}

func locationDefaultVariableRule(observers *validator.Events, addError validator.AddErrFunc) {
	// A variable that fails the strict check is held until its field has
	// been seen: the walker visits an argument's value before the field
	// that owns the argument, and only the field knows the argument's
	// declared default.
	type failure struct {
		message string
		at      *ast.Position
	}
	pending := map[*ast.Value]failure{}
	var order []*ast.Value
	observers.OnValue(func(walker *validator.Walker, value *ast.Value) {
		if value.Kind != ast.Variable || value.ExpectedType == nil || value.VariableDefinition == nil || walker.CurrentOperation == nil {
			return
		}
		expected := *value.ExpectedType
		if def := value.VariableDefinition.DefaultValue; def != nil && def.Kind != ast.NullValue && expected.NonNull {
			expected.NonNull = false
		}
		if !value.VariableDefinition.Type.IsCompatible(&expected) {
			pending[value] = failure{
				message: fmt.Sprintf(`Variable "%s" of type "%s" used in position expecting type "%s".`,
					value, value.VariableDefinition.Type.String(), value.ExpectedType.String()),
				at: value.Position,
			}
			order = append(order, value)
		}
	})
	observers.OnField(func(_ *validator.Walker, field *ast.Field) {
		if field.Definition == nil {
			return
		}
		for _, argument := range field.Arguments {
			def := field.Definition.Arguments.ForName(argument.Name)
			if def == nil || def.DefaultValue == nil || def.DefaultValue.Kind == ast.NullValue {
				continue
			}
			// Only a direct variable argument is forgiven, and only when
			// the variable is nullable and otherwise compatible.
			if argument.Value.Kind == ast.Variable && argument.Value.ExpectedType != nil {
				expected := *argument.Value.ExpectedType
				expected.NonNull = false
				if argument.Value.VariableDefinition != nil && argument.Value.VariableDefinition.Type.IsCompatible(&expected) {
					delete(pending, argument.Value)
				}
			}
		}
	})
	flush := func() {
		for _, value := range order {
			if f, ok := pending[value]; ok {
				addError(validator.Message("%s", f.message), validator.At(f.at))
				delete(pending, value)
			}
		}
		order = nil
	}
	observers.OnOperation(func(_ *validator.Walker, _ *ast.OperationDefinition) { flush() })
	observers.OnFragment(func(_ *validator.Walker, _ *ast.FragmentDefinition) { flush() })
}
