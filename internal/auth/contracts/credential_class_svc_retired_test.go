package contracts

import "testing"

// svc_acr_ and svc_worker_ are accepted by no route, and the verbs that minted,
// rotated and revoked them are gone: both classes are retired, so the gate
// refuses any endpoint-profile row that would accept one again.
func TestTheSvcACRAndSvcWorkerClassesAreRetired(t *testing.T) {
	retired, err := retiredClassIDs(readContractFile(t, "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, class := range []string{"internal_svc_acr_token", "internal_svc_worker_token"} {
		if !retired[class] {
			t.Errorf("%s is not status retired in credential-classes.json", class)
		}
	}
}
