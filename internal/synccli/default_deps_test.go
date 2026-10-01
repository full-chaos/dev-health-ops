package synccli

import (
	"reflect"
	"testing"
)

// CHAOS-7132 follow-up: `dho sync teams --provider jira` failed on bigboy with "provider credential is
// invalid" because defaultDeps() left doer nil: resolveJiraStoredSettings hands it straight to
// providerfoundation.NewJiraClient, which refuses a nil doer. Every test injected a doer, so nothing ever ran
// the production wiring. The class is a dependency that production leaves nil and only tests populate.
//
// This guard walks every field of deps: defaultDeps() (what the verb really runs with) must leave none nil.
// A new field with no production default fails here instead of in an operator's terminal.
func TestDefaultDepsLeavesNoDependencyNil(t *testing.T) {
	value := reflect.ValueOf(defaultDeps())
	for index := 0; index < value.NumField(); index++ {
		field := value.Type().Field(index)
		switch field.Type.Kind() {
		case reflect.Func, reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan:
			if value.Field(index).IsNil() {
				t.Errorf("defaultDeps() leaves deps.%s (%s) nil: production runs the verb with no %s, and only tests populate it",
					field.Name, field.Type, field.Name)
			}
		}
	}
}

// The same guard for the in-process sync executor's deps: defaultInlineDeps() is what production hands it.
func TestDefaultInlineDepsLeavesNoDependencyNil(t *testing.T) {
	value := reflect.ValueOf(defaultInlineDeps())
	for index := 0; index < value.NumField(); index++ {
		field := value.Type().Field(index)
		if field.Type.Kind() == reflect.Func && value.Field(index).IsNil() {
			t.Errorf("defaultInlineDeps() leaves InlineDeps.%s nil", field.Name)
		}
	}
}

// The doer production runs with never follows a redirect: a stored credential must not be replayed to
// another host (the worker's own doer has the same CheckRedirect).
func TestProductionDoerDoesNotFollowRedirects(t *testing.T) {
	doer := productionDoer()
	if doer.CheckRedirect == nil || doer.CheckRedirect(nil, nil) == nil {
		t.Fatal("the production doer follows redirects")
	}
}
