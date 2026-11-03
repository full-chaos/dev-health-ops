package adminops

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func lookupOf(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
}

// The CLI reads AUTH_PROXY_ORG_PROVISIONING as go-api does (a strict bool,
// default off) and hands it to the Operator, which refuses the membership
// changes and real deletes the HTTP routes send to auth-service.
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
