package prove

import (
	"context"
	"net/http"
	"reflect"
	"sync"
	"testing"
)

// The widened credential exists only when -admin-principal is given: no flag, no admin credential in the run.
func TestNoAdminCredentialExistsWithoutTheFlag(t *testing.T) {
	fs, f := registerFlags()
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if f.adminPrincipal || adminEdgeCredentialFor(*f) != nil {
		t.Fatal("an admin credential exists without -admin-principal")
	}
	fs, f = registerFlags()
	if err := fs.Parse([]string{"-admin-principal"}); err != nil {
		t.Fatal(err)
	}
	if !f.adminPrincipal {
		t.Fatal("-admin-principal did not set the flag")
	}
}

// With the flag the credential mints through the EXISTING edge-token verb as the org-admin proof principal, for the run's org.
func TestTheAdminCredentialMintsTheAdminProofEdgeToken(t *testing.T) {
	const org = "9d1e2c3b-4a5f-4b6c-8d7e-0f1a2b3c4d5e"
	var mu sync.Mutex
	var helper string
	var args []string
	withFakeMinter(t, func(_ context.Context, helperName string, a []string) (string, error) {
		mu.Lock()
		helper, args = helperName, append([]string(nil), a...)
		mu.Unlock()
		return syntheticJWT(t, map[string]string{"sub": "admin-proof", "org_id": org}), nil
	})
	_, f := registerFlags()
	f.orgID, f.adminPrincipal = org, true
	credential := adminEdgeCredentialFor(*f)
	if credential == nil {
		t.Fatal("no credential with the flag set")
	}
	credential.BindOrg(org)
	request, _ := http.NewRequest(http.MethodPost, "http://edge.invalid/graphql", nil)
	if err := credential.Apply(context.Background(), request); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if request.Header.Get("Authorization") == "" {
		t.Fatal("the credential set no Authorization header")
	}
	if helper != "mint-edge-token" || !reflect.DeepEqual(args, []string{"-org", org, "-principal", "admin-proof"}) {
		t.Fatalf("minted with helper %q args %v", helper, args)
	}
}
