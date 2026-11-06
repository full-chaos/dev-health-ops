package authschema

import "testing"

// The repository identity function has exactly one caller, the worker domain login, and its writer role is its own:
// the options refuse every other count and every role shared with another writer, a caller, the runtime or the
// migration role.
func TestRepoIdentityWriterOptions(t *testing.T) {
	base := Options{Schema: "auth", RuntimeRole: "devhealth_auth",
		PushLifecycleWriterRole: "push_writer", PushLifecycleCallerRoles: []string{"devhealth_api", "devhealth_domain"}}
	cases := map[string]struct {
		writer  string
		callers []string
		ok      bool
	}{
		"unset":                            {"", nil, true},
		"the worker login":                 {"repo_writer", []string{"devhealth_domain"}, true},
		"callers without a writer":         {"", []string{"devhealth_domain"}, false},
		"a writer without a caller":        {"repo_writer", nil, false},
		"two callers":                      {"repo_writer", []string{"devhealth_domain", "devhealth_api"}, false},
		"a blank caller":                   {"repo_writer", []string{""}, false},
		"an invalid caller":                {"repo_writer", []string{"bad;name"}, false},
		"the writer is the push writer":    {"push_writer", []string{"devhealth_domain"}, false},
		"the writer is a push caller":      {"devhealth_api", []string{"devhealth_domain"}, false},
		"the writer is the runtime role":   {"devhealth_auth", []string{"devhealth_domain"}, false},
		"the writer is the migration role": {"migrator", []string{"devhealth_domain"}, false},
		"the caller is the writer":         {"repo_writer", []string{"repo_writer"}, false},
		"the caller is the push writer":    {"repo_writer", []string{"push_writer"}, false},
		"the caller is the runtime role":   {"repo_writer", []string{"devhealth_auth"}, false},
		"the caller is the migration role": {"repo_writer", []string{"migrator"}, false},
		"an invalid writer":                {"bad;writer", []string{"devhealth_domain"}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			o := base
			o.RepoIdentityWriterRole, o.RepoIdentityCallerRoles = tc.writer, tc.callers
			err := o.validateRepoIdentity("migrator")
			if tc.ok && err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("the options were accepted; they must be refused")
			}
		})
	}
}
