package stepcause

import (
	"go/token"
	"go/types"
	"testing"

	"golang.org/x/tools/go/packages"
)

// TestExportedAPIStaysClosed pins the claim "no code outside this package can
// put text into a Step". That claim is only as strong as the exported API:
// one `func Named(label string) Step` and every outside caller can mint a
// Step from driver text, with the types still checking. So the exported
// surface is derived with go/types and must be exactly:
//   - type Step, a struct with NO exported field and NO exported method;
//   - func Failure(Step, error) error;
//   - exported variables of type Step (the closed set of steps).
//
// Anything else exported (a constructor, a setter, a string-taking helper, an
// alias, a constant, a second function) fails the test.
func TestExportedAPIStaysClosed(t *testing.T) {
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedSyntax,
		Dir:  ".",
	}, ".")
	if err != nil || packages.PrintErrors(loaded) > 0 || len(loaded) != 1 {
		t.Fatalf("load: %v", err)
	}
	scope := loaded[0].Types.Scope()
	stepType := scope.Lookup("Step")
	if stepType == nil {
		t.Fatal("Step is gone")
	}
	exported := 0
	for _, name := range scope.Names() {
		if !token.IsExported(name) {
			continue
		}
		exported++
		object := scope.Lookup(name)
		switch object := object.(type) {
		case *types.TypeName:
			if name != "Step" || object.IsAlias() {
				t.Errorf("exported type %s: only the struct type Step may be exported", name)
				continue
			}
			structure, ok := object.Type().Underlying().(*types.Struct)
			if !ok {
				t.Errorf("Step is not a struct")
				continue
			}
			for i := 0; i < structure.NumFields(); i++ {
				if structure.Field(i).Exported() {
					t.Errorf("Step has the exported field %s", structure.Field(i).Name())
				}
			}
			for _, receiver := range []types.Type{object.Type(), types.NewPointer(object.Type())} {
				methods := types.NewMethodSet(receiver)
				for i := 0; i < methods.Len(); i++ {
					if methods.At(i).Obj().Exported() {
						t.Errorf("Step has the exported method %s", methods.At(i).Obj().Name())
					}
				}
			}
		case *types.Func:
			signature := object.Type().(*types.Signature)
			if name != "Failure" || signature.Params().Len() != 2 || signature.Results().Len() != 1 ||
				!types.Identical(signature.Params().At(0).Type(), stepType.Type()) ||
				signature.Params().At(1).Type().String() != "error" ||
				signature.Results().At(0).Type().String() != "error" {
				t.Errorf("exported func %s%s: only Failure(Step, error) error may be exported", name, signature.String())
			}
		case *types.Var:
			if !types.Identical(object.Type(), stepType.Type()) {
				t.Errorf("exported var %s has type %s, want Step", name, object.Type())
			}
		default:
			t.Errorf("exported %s %s (%T): only Step, Failure and Step-typed vars may be exported", name, object.Type(), object)
		}
	}
	if exported < 30 {
		t.Fatalf("only %d exported names found; the surface check did not see the package", exported)
	}
}
