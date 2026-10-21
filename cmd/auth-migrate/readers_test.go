package main

import (
	"reflect"
	"testing"
)

func TestSessionReaderRolesFromTheEnvironment(t *testing.T) {
	for name, tc := range map[string]struct {
		value   *string
		want    []string
		wantErr bool
	}{
		"unset":      {},
		"empty":      {value: ptr("")},
		"two":        {value: ptr("devhealth_api, devhealth_query"), want: []string{"devhealth_api", "devhealth_query"}},
		"blanks":     {value: ptr(" ,devhealth_api,, "), want: []string{"devhealth_api"}},
		"unusable":   {value: ptr("devhealth_api,Bad-Name"), wantErr: true},
		"quote char": {value: ptr(`devhealth"api`), wantErr: true},
	} {
		lookup := func(key string) (string, bool) {
			if key == envSessionReaders && tc.value != nil {
				return *tc.value, true
			}
			return "", false
		}
		got, err := sessionReaderRoles(lookup)
		if (err != nil) != tc.wantErr || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %v, %v; want %v, error %v", name, got, err, tc.want, tc.wantErr)
		}
	}
}

func ptr(s string) *string { return &s }
