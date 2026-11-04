package adminops

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func lookupOf(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
}

// The CLI reads AUTH_PROXY_ORG_PROVISIONING as go-api does (a strict bool,
// default off) and hands it to the Operator, which refuses an organization
// create with it on; membership changes and deletes go through the routes
// whatever it says.
func TestOperatorForReadsTheOrgProvisioningSwitch(t *testing.T) {
	for _, tc := range []struct {
		value   string
		set     bool
		want    bool
		wantErr bool
	}{{"", false, false, false}, {"", true, false, false}, {"true", true, true, false}, {" true ", true, true, false},
		{"false", true, false, false}, {"maybe", true, false, true}} {
		values := map[string]string{"API_DATABASE_URI": "postgres://user@127.0.0.1:1/db"}
		if tc.set {
			values["AUTH_PROXY_ORG_PROVISIONING"] = tc.value
		}
		on, err := orgProvisioningFromEnv(cli.Env{Lookup: lookupOf(values)})
		if (err != nil) != tc.wantErr || on != tc.want {
			t.Errorf("%q: on=%v err=%v", tc.value, on, err)
		}
		if tc.wantErr {
			continue
		}
		op, closePool, code := accountOperator(context.Background(), cli.Env{Lookup: lookupOf(values)})
		closePool()
		if code != 0 || op.OrgProvisioning != tc.want {
			t.Errorf("%q: accountOperator code=%d OrgProvisioning=%v, want %v", tc.value, code, op.OrgProvisioning, tc.want)
		}
	}
}

// The CLI reaches auth-service's provisioning routes with go-api's own
// settings: AUTH_PROXY_URL and the AUTH_SESSION_ISSUE_TOKEN service credential.
// Either missing gives no client (the Operator then refuses by name); a bad
// URL or credential is a configuration error that never prints the credential.
func TestProvisionerFromEnv(t *testing.T) {
	const token = "svc_authsess_" + "0123456789abcdef0123456789abcdef0123456789a"
	for _, tc := range []struct {
		name    string
		values  map[string]string
		wantNil bool
		wantErr bool
	}{
		{"both", map[string]string{"AUTH_PROXY_URL": "http://auth-service:8095", "AUTH_SESSION_ISSUE_TOKEN": token}, false, false},
		{"no url", map[string]string{"AUTH_SESSION_ISSUE_TOKEN": token}, true, false},
		{"no token", map[string]string{"AUTH_PROXY_URL": "http://auth-service:8095"}, true, false},
		{"bad url", map[string]string{"AUTH_PROXY_URL": "ftp://x", "AUTH_SESSION_ISSUE_TOKEN": token}, true, true},
		{"bad token", map[string]string{"AUTH_PROXY_URL": "http://auth-service:8095", "AUTH_SESSION_ISSUE_TOKEN": "not-a-credential"}, true, true},
	} {
		var stderr bytes.Buffer
		provisioner, code := provisionerFromEnv(cli.Env{Lookup: lookupOf(tc.values), Stderr: &stderr})
		if (provisioner == nil) != tc.wantNil || (code != 0) != tc.wantErr {
			t.Errorf("%s: provisioner nil=%v code=%d", tc.name, provisioner == nil, code)
		}
		if strings.Contains(stderr.String(), "not-a-credential") || strings.Contains(stderr.String(), token) {
			t.Errorf("%s: the credential reached stderr: %q", tc.name, stderr.String())
		}
	}
}
