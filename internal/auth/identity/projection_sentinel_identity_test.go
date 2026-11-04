package identity

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// sameTextClone builds a separate error of the same dynamic type and the same
// text as sentinel, without naming the type, so it compiles for a value type
// and a pointer type alike.
func sameTextClone(t *testing.T, sentinel error) error {
	t.Helper()
	typ := reflect.TypeOf(sentinel)
	var clone reflect.Value
	if typ.Kind() == reflect.Pointer {
		clone = reflect.New(typ.Elem())
		clone.Elem().Set(reflect.ValueOf(sentinel).Elem())
	} else {
		clone = reflect.New(typ).Elem()
		clone.Set(reflect.ValueOf(sentinel))
	}
	err, ok := clone.Interface().(error)
	if !ok {
		t.Fatalf("clone of %T is not an error", sentinel)
	}
	return err
}

// The race sentinels are matched by identity: an error built with the same
// type and the same text is not the sentinel, so a caller that retries or
// reports success on errors.Is never does so for a look-alike.
func TestReviewSameTextProjectionErrorIsNotIdentity(t *testing.T) {
	cases := []struct {
		check    string
		sentinel error
	}{
		{"insertProjection", errPrincipalExists},
		{"insertBackfill", errPrincipalExists},
		{"insertOrganization", errOrganizationProjectedConcurrently},
		{"projectIdentity", errProjectedConcurrently},
		{"relinkIdentity", errProjectedConcurrently},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			if !errors.Is(tc.sentinel, tc.sentinel) || !errors.Is(fmt.Errorf("wrapped: %w", tc.sentinel), tc.sentinel) {
				t.Fatalf("the sentinel %q does not match itself", tc.sentinel)
			}
			clone := sameTextClone(t, tc.sentinel)
			if clone.Error() != tc.sentinel.Error() {
				t.Fatalf("clone text %q, want %q", clone.Error(), tc.sentinel.Error())
			}
			if errors.Is(clone, tc.sentinel) {
				t.Fatalf("distinct same-type error with same text matched refusal: type=%T text=%q errors.Is=true; want false", clone, clone.Error())
			}
		})
	}
}
