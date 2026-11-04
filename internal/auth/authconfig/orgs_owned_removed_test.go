package authconfig

import (
	"reflect"
	"testing"
)

// A set AUTH_ORGS_OWNED, of any value, changes nothing Load returns.
func TestRemovedOrgsOwnedKeyChangesNothingInLoad(t *testing.T) {
	unset, err := Load(Spec{LookupEnv: lookupFrom(baseEnv())})
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	for _, value := range []string{"true", "false", "TRUE", "1", "maybe-not-a-switch"} {
		env := baseEnv()
		env["AUTH_ORGS_OWNED"] = value
		got, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil {
			t.Errorf("%q: Load refused: %v", value, err)
			continue
		}
		if !reflect.DeepEqual(got, unset) {
			t.Errorf("%q: Load differs from the unset result", value)
		}
	}
}
