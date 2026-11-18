package datahealth

import (
	"reflect"
	"testing"
)

func TestProviderIdentityGroupsKeepsTheKeysInOrder(t *testing.T) {
	got := ProviderIdentityGroups(`{"GitHub": ["Sam"], "bitbucket": ["s1", "s2"], "gitlab": []}`)
	want := []ProviderIdentityGroup{
		{Provider: "GitHub", Values: []string{"Sam"}},
		{Provider: "bitbucket", Values: []string{"s1", "s2"}},
		{Provider: "gitlab", Values: []string{}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("groups = %#v, want %#v", got, want)
	}
	for _, text := range []string{"", "  ", "[]", "not json", "null"} {
		if groups := ProviderIdentityGroups(text); len(groups) != 0 {
			t.Errorf("%q decoded to %#v, want no groups", text, groups)
		}
	}
}
