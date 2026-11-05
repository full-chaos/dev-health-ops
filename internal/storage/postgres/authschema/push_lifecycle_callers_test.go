package authschema

import "testing"

// CHAOS-12828: exactly two runtime logins may execute the push lifecycle
// function (go-api's and the worker domain login); the options refuse any
// other count and any blank or invalid entry.
func TestPushLifecycleCallersExactlyTwoValidLogins(t *testing.T) {
	base := Options{Schema: "auth", RuntimeRole: "devhealth_auth", PushLifecycleWriterRole: "push_writer"}
	cases := map[string]struct {
		callers []string
		ok      bool
	}{
		"two valid callers":    {[]string{"devhealth_api", "devhealth_domain"}, true},
		"one caller":           {[]string{"devhealth_api"}, false},
		"three callers":        {[]string{"devhealth_api", "devhealth_domain", "extra"}, false},
		"a blank caller":       {[]string{"devhealth_api", ""}, false},
		"a spaces-only caller": {[]string{"devhealth_api", "  "}, false},
		"an invalid caller":    {[]string{"devhealth_api", "bad;name"}, false},
		"a repeated caller":    {[]string{"devhealth_api", "devhealth_api"}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			o := base
			o.PushLifecycleCallerRoles = tc.callers
			err := o.validatePushLifecycle("")
			if tc.ok && err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("the options were accepted; they must be refused")
			}
		})
	}
}
