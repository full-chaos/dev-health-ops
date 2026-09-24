package goapiproof

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestDHOAPICorpusPinsItsRoutes names the routes the corpus must cover.
// The mounted-route test only proves every entry is a real route; this pins
// the other direction, so deleting an entry (or its run-order line) fails
// here instead of silently shrinking what a run measures.
func TestDHOAPICorpusPinsItsRoutes(t *testing.T) {
	want := []string{
		"REST:GET:/api/v1/external-ingest/schemas",
		"REST:GET:/api/v1/external-ingest/schemas/{schema_version}",
		"REST:GET:/api/v1/external-ingest/availability",
		"REST:GET:/api/v1/external-ingest/batches",
		"REST:GET:/api/v1/external-ingest/batches/{ingestion_id}",
		"REST:POST:/api/v1/external-ingest/validate",
		"REST:GET:/ready",
		"REST:GET:/health",
		"REST:GET:/health/workers",
		"REST:GET:/api/v1/webhooks/health",
		"REST:GET:/api/v1/orgs/me",
		"REST:GET:/api/v1/licensing/entitlements/{org_id}",
		"REST:GET:/api/v1/telemetry/status",
	}
	if got := RESTRunOrderFor(RESTServiceDHOAPI); !slices.Equal(got, want) {
		t.Fatalf("dho-api run order = %v, want %v", got, want)
	}
	for _, operation := range want {
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		if spec.EffectiveService() != RESTServiceDHOAPI {
			t.Errorf("%s targets %s", operation, spec.EffectiveService())
		}
	}
	if !restOperatorSuppliedProducers[dhoAPIOrgIDProducer] {
		t.Errorf("%s must be an operator-supplied producer: the entitlements path binds it", dhoAPIOrgIDProducer)
	}
}

// TestIngestEntriesSendThePushTokenAndTheRestDoNot pins the credential split:
// every external-ingest entry authenticates with the push token, and no other
// dho-api entry does, so a run's own bearers are never sent to the ingest API
// and the push token is never sent anywhere else.
func TestIngestEntriesSendThePushTokenAndTheRestDoNot(t *testing.T) {
	for _, operation := range RESTRunOrderFor(RESTServiceDHOAPI) {
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		isIngest := strings.Contains(spec.Path, "/api/v1/external-ingest/")
		if isIngest != (spec.Credential == RESTCredentialPushToken) {
			t.Errorf("%s: push-token credential = %v, want %v", operation, spec.Credential == RESTCredentialPushToken, isIngest)
		}
	}
	if !PlansPushTokenEntries(RESTServiceDHOAPI) || PlansPushTokenEntries(RESTServiceQueryAPI) {
		t.Fatal("only the dho-api service plans push-token entries")
	}
}

func TestPathLiteralsFillThePlaceholdersAndAreValidated(t *testing.T) {
	spec, err := SpecForREST("REST:GET:/api/v1/external-ingest/batches/{ingestion_id}")
	if err != nil {
		t.Fatal(err)
	}
	path, _, _, unresolved := ResolveRESTIDBindings(spec.Path, spec.Requests[0], nil)
	if len(unresolved) != 0 || path != "/api/v1/external-ingest/batches/"+ingestMissingBatchID {
		t.Fatalf("resolved path = %q (unresolved %v)", path, unresolved)
	}
	operation := "REST:GET:/test-only-literal"
	restEndpointSpecs[operation] = RESTEndpointSpec{
		Method: "GET", Path: "/no/placeholder", Service: RESTServiceDHOAPI,
		Requests: []RESTRequest{{Name: "r", PathLiterals: map[string]string{"absent": "x"}, WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: RESTBodyModeJSON}},
	}
	restRunOrder = append(restRunOrder, operation)
	t.Cleanup(func() {
		delete(restEndpointSpecs, operation)
		restRunOrder = restRunOrder[:len(restRunOrder)-1]
	})
	if err := ValidateRESTCorpus(); err == nil {
		t.Fatal("a PathLiterals key that is not a placeholder of the path must be refused")
	}
}

func TestValidatePushTokenShape(t *testing.T) {
	for _, ok := range []string{"fcpush_abc", "Bearer fcpush_abc", " fcpush_abc\n"} {
		if err := ValidatePushTokenShape(ok); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "fcpush_", "abc", "Bearer abc.def.ghi", "fcpush_a b"} {
		if err := ValidatePushTokenShape(bad); err == nil {
			t.Errorf("%q accepted, want refusal", bad)
		}
	}
}

func TestPushTokenFileCredentialReadsTheFileEachTimeAndNeverLeaksIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("fcpush_first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	credential := PushTokenFileCredential(path)
	apply := func() (string, error) {
		request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/x", nil)
		err := credential.Apply(context.Background(), request)
		return request.Header.Get("Authorization"), err
	}
	if got, err := apply(); err != nil || got != "Bearer fcpush_first" {
		t.Fatalf("Authorization = %q, %v; want the trimmed file value with the Bearer scheme", got, err)
	}
	// A wrong token must be refused by shape, and the error must not carry it.
	if err := os.WriteFile(path, []byte("not-a-push-token-SECRETVALUE"), 0o600); err != nil {
		t.Fatal(err)
	}
	fresh := PushTokenFileCredential(path)
	request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/x", nil)
	err := fresh.Apply(context.Background(), request)
	if err == nil || strings.Contains(err.Error(), "SECRETVALUE") {
		t.Fatalf("want a shape refusal that does not leak the file content, got %v", err)
	}
	if _, err := PushTokenFileCredential(filepath.Join(t.TempDir(), "absent")).value(context.Background()); err == nil {
		t.Fatal("a missing token file must be an error, never an empty credential")
	}
}
