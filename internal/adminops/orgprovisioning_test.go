package adminops

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/workloadclient"
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
// settings: AUTH_PROXY_URL and the go-api workload token inputs. No URL or no
// subject file gives no client (the Operator then refuses by name), and the
// retired AUTH_SESSION_ISSUE_TOKEN is never read: alone it gives no client. A
// bad URL or workload input is a configuration error that never prints a
// credential or a path.
func TestProvisionerFromEnv(t *testing.T) {
	const token = "svc_authsess_" + "0123456789abcdef0123456789abcdef0123456789a"
	bootstrap := workloadclient.BootstrapFile(t)
	missing := filepath.Join(t.TempDir(), "absent")
	const url, issuer = "http://auth-service:8095", workloadclient.Issuer
	for _, tc := range []struct {
		name    string
		values  map[string]string
		wantNil bool
		wantErr bool
	}{
		{"workload", map[string]string{"AUTH_PROXY_URL": url, "AUTH_WORKLOAD_BOOTSTRAP_FILE": bootstrap, "AUTH_WORKLOAD_ISSUER": issuer}, false, false},
		{"kubernetes subject file", map[string]string{"AUTH_PROXY_URL": url, "AUTH_WORKLOAD_SUBJECT_FILE": bootstrap, "AUTH_WORKLOAD_ISSUER": issuer}, false, false},
		{"workload beside the retired credential", map[string]string{"AUTH_PROXY_URL": url, "AUTH_WORKLOAD_BOOTSTRAP_FILE": bootstrap,
			"AUTH_WORKLOAD_ISSUER": issuer, "AUTH_SESSION_ISSUE_TOKEN": token}, false, false},
		{"the retired credential alone", map[string]string{"AUTH_PROXY_URL": url, "AUTH_SESSION_ISSUE_TOKEN": token}, true, false},
		{"no url", map[string]string{"AUTH_WORKLOAD_BOOTSTRAP_FILE": bootstrap, "AUTH_WORKLOAD_ISSUER": issuer}, true, false},
		{"no subject file", map[string]string{"AUTH_PROXY_URL": url, "AUTH_WORKLOAD_ISSUER": issuer}, true, false},
		{"bad url", map[string]string{"AUTH_PROXY_URL": "ftp://x", "AUTH_WORKLOAD_BOOTSTRAP_FILE": bootstrap, "AUTH_WORKLOAD_ISSUER": issuer}, true, true},
		{"unreadable subject file", map[string]string{"AUTH_PROXY_URL": url, "AUTH_WORKLOAD_BOOTSTRAP_FILE": missing, "AUTH_WORKLOAD_ISSUER": issuer}, true, true},
		{"no issuer", map[string]string{"AUTH_PROXY_URL": url, "AUTH_WORKLOAD_BOOTSTRAP_FILE": bootstrap}, true, true},
		{"both subject files", map[string]string{"AUTH_PROXY_URL": url, "AUTH_WORKLOAD_BOOTSTRAP_FILE": bootstrap,
			"AUTH_WORKLOAD_SUBJECT_FILE": bootstrap, "AUTH_WORKLOAD_ISSUER": issuer}, true, true},
	} {
		var stderr bytes.Buffer
		provisioner, code := provisionerFromEnv(cli.Env{Lookup: lookupOf(tc.values), Stderr: &stderr})
		if (provisioner == nil) != tc.wantNil || (code != 0) != tc.wantErr {
			t.Errorf("%s: provisioner nil=%v code=%d stderr=%q", tc.name, provisioner == nil, code, stderr.String())
		}
		for _, secret := range []string{token, bootstrap, missing} {
			if strings.Contains(stderr.String(), secret) {
				t.Errorf("%s: a credential or path reached stderr: %q", tc.name, stderr.String())
			}
		}
	}
}
