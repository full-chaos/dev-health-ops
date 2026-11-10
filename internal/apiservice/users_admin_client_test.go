package apiservice

import "testing"

// The user-admin client exists exactly while users are owned and a workload client exists; with none, nothing
// refuses start-up (the owned admin users writes answer 503).
func TestUsersAdminClientIsBuiltWhenUsersOwned(t *testing.T) {
	cfg := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth.example.test", EnvAuthProxyOrgProvisioning: "true"})
	provisioning, _, err := newCredentials(cfg, issueEnv(workloadEnv(t, map[string]string{EnvAuthProvisioningCredential: "workload"})))
	if err != nil || provisioning.workload == nil {
		t.Fatalf("workload client: %v %v", provisioning.workload, err)
	}
	if client, err := usersAdminClient(cfg, true, provisioning.workload); err != nil || client == nil {
		t.Fatalf("owned: %v %v, want a client", client, err)
	}
	if client, err := usersAdminClient(cfg, false, provisioning.workload); err != nil || client != nil {
		t.Fatalf("not owned: %v %v, want none", client, err)
	}
	if client, err := usersAdminClient(cfg, true, nil); err != nil || client != nil {
		t.Fatalf("owned, no workload client: %v %v, want none and no error", client, err)
	}
}
