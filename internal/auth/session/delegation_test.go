package session

import (
	"reflect"
	"strings"
	"testing"
)

// G-4: the actor comes from the base session row, never from the request.
func TestStartInputHasNoActorField(t *testing.T) {
	input := reflect.TypeFor[StartInput]()
	for i := range input.NumField() {
		if strings.Contains(strings.ToLower(input.Field(i).Name), "actor") {
			t.Fatalf("StartInput.%s names an actor", input.Field(i).Name)
		}
	}
}

func TestDelegationReasonBounds(t *testing.T) {
	for name, row := range map[string]struct {
		raw  string
		want bool
	}{
		"plain":       {"support ticket", true},
		"at_the_cap":  {strings.Repeat("é", 500), true},
		"over_cap":    {strings.Repeat("é", 501), false},
		"empty":       {"", false},
		"blank":       {" \t\n", false},
		"nul":         {"a\x00b", false},
		"invalid_utf": {"a\xffb", false},
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := delegationReason(row.raw); ok != row.want {
				t.Fatalf("delegationReason = %v, want %v", ok, row.want)
			}
		})
	}
}
