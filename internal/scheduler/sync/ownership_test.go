package sync

import (
	"errors"
	"reflect"
	"testing"
)

func TestOwnershipPolicyHasNoExportedConstructionFields(t *testing.T) {
	policyType := reflect.TypeOf(DefaultOwnershipPolicy())
	for index := 0; index < policyType.NumField(); index++ {
		field := policyType.Field(index)
		if field.IsExported() {
			t.Fatalf("ownership policy field %s is exported", field.Name)
		}
	}
}

func TestOwnershipPolicyOnlyPermitsExplicitOwnerModePairs(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy OwnershipPolicy
		valid  bool
	}{
		{"default", DefaultOwnershipPolicy(), true},
		{"celery shadow", OwnershipPolicy{owner: schedulerOwnerCelery, mode: schedulerModeShadow}, true},
		{"go mutation", OwnershipPolicy{owner: schedulerOwnerGo, mode: schedulerModeMutation}, true},
		{"go shadow", OwnershipPolicy{owner: schedulerOwnerGo, mode: schedulerModeShadow}, false},
		{"celery mutation", OwnershipPolicy{owner: schedulerOwnerCelery, mode: schedulerModeMutation}, false},
		{"unknown", OwnershipPolicy{owner: "other", mode: "other"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.policy.Validate()
			if test.valid && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if !test.valid && !errors.Is(err, ErrInvalidOwnershipPolicy) {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}
