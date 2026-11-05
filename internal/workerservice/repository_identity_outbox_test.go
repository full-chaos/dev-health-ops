package workerservice

import "testing"

func TestRepositoryIdentityOutboxFlag(t *testing.T) {
	for _, c := range []struct {
		name   string
		env    map[string]string
		on     bool
		schema string
		err    bool
	}{
		{"unset is off", map[string]string{}, false, "", false},
		{"false is off", map[string]string{"WORKER_REPO_IDENTITY_OUTBOX": "false"}, false, "", false},
		{"on, default schema", map[string]string{"WORKER_REPO_IDENTITY_OUTBOX": "true"}, true, "auth", false},
		{"on, named schema", map[string]string{"WORKER_REPO_IDENTITY_OUTBOX": "true", "AUTH_DATABASE_SCHEMA": " auth_v2 "}, true, "auth_v2", false},
		{"on, bad schema", map[string]string{"WORKER_REPO_IDENTITY_OUTBOX": "true", "AUTH_DATABASE_SCHEMA": "auth;drop"}, false, "", true},
		{"bad flag", map[string]string{"WORKER_REPO_IDENTITY_OUTBOX": "on"}, false, "", true},
		{"off ignores a bad schema", map[string]string{"AUTH_DATABASE_SCHEMA": "auth;drop"}, false, "", false},
	} {
		outbox, err := repositoryIdentityOutbox(func(key string) (string, bool) { v, ok := c.env[key]; return v, ok })
		if (err != nil) != c.err || (outbox != nil) != c.on {
			t.Fatalf("%s: outbox=%v err=%v", c.name, outbox, err)
		}
		if outbox != nil && outbox.Schema.String() != c.schema {
			t.Fatalf("%s: schema=%q want %q", c.name, outbox.Schema.String(), c.schema)
		}
	}
}
