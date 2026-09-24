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
		"REST:GET:/api/v1/billing/plans",
		"REST:GET:/api/v1/billing/plans/{plan_id}",
		"REST:GET:/api/v1/billing/invoices",
		"REST:GET:/api/v1/billing/invoices/{invoice_id}",
		"REST:GET:/api/v1/billing/refunds",
		"REST:GET:/api/v1/billing/refunds/{refund_id}",
		"REST:GET:/api/v1/billing/entitlements/{org_id}",
		"REST:GET:/api/v1/billing/subscriptions/list",
		"REST:GET:/api/v1/billing/subscriptions",
		"REST:GET:/api/v1/billing/subscriptions/history",
		"REST:GET:/api/v1/billing/audit",
		"REST:GET:/api/v1/billing/audit/{audit_id}",
		"REST:HEAD:/health",
		"REST:HEAD:/ready",
		"REST:HEAD:/health/workers",
	}
	// Set equality: which file's init registers an entry (and so its position
	// in the run order) is not what is pinned, only that every entry is there
	// exactly once.
	got := slices.Clone(RESTRunOrderFor(RESTServiceDHOAPI))
	slices.Sort(got)
	sortedWant := slices.Clone(want)
	slices.Sort(sortedWant)
	if !slices.Equal(got, sortedWant) {
		t.Fatalf("dho-api operations = %v, want %v", got, sortedWant)
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
	// The validate entry's request cases and the statuses both planes must give.
	validate, err := SpecForREST("REST:POST:/api/v1/external-ingest/validate")
	if err != nil {
		t.Fatal(err)
	}
	wantValidate := map[string]int{"bigint_4301_digits": 500, "float_1e1000": 200}
	if len(validate.Requests) != len(wantValidate) {
		t.Fatalf("validate has %d request cases, want %d", len(validate.Requests), len(wantValidate))
	}
	for _, request := range validate.Requests {
		status, ok := wantValidate[request.Name]
		if !ok || request.WantCandidateStatus != status || request.WantBaselineStatus != status {
			t.Errorf("validate case %q: statuses %d/%d, want %d/%d (listed=%v)", request.Name, request.WantCandidateStatus, request.WantBaselineStatus, status, status, ok)
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
	// One credential, the file rotated between two requests: the second request
	// must carry the new token (nothing is cached across uses).
	rotating := PushTokenFileCredential(path)
	if err := os.WriteFile(path, []byte("fcpush_before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	send := func() string {
		request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/x", nil)
		if err := rotating.Apply(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		return request.Header.Get("Authorization")
	}
	first := send()
	if err := os.WriteFile(path, []byte("fcpush_after\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if second := send(); first != "Bearer fcpush_before" || second != "Bearer fcpush_after" {
		t.Fatalf("a rotated token file must be picked up by the next request: first=%q second=%q", first, second)
	}
	if _, err := PushTokenFileCredential(filepath.Join(t.TempDir(), "absent")).value(context.Background()); err == nil {
		t.Fatal("a missing token file must be an error, never an empty credential")
	}
}

func TestHEADEntriesMustBeStatusOnlyAndBodyless(t *testing.T) {
	operation := "REST:HEAD:/test-only-head"
	restEndpointSpecs[operation] = RESTEndpointSpec{
		Method: "HEAD", Path: "/test-only-head", Service: RESTServiceDHOAPI, PublicNoAuth: true,
		Requests: []RESTRequest{{Name: "head", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: RESTBodyModeJSON}},
	}
	restRunOrder = append(restRunOrder, operation)
	t.Cleanup(func() {
		delete(restEndpointSpecs, operation)
		restRunOrder = restRunOrder[:len(restRunOrder)-1]
	})
	if err := ValidateRESTCorpus(); err == nil {
		t.Fatal("a HEAD entry compared as JSON must be refused: a HEAD answer has no body to decode")
	}
}

func TestBillingReadEntriesAreSharedCasesOnly(t *testing.T) {
	// Every billing entry is a GET (billing writes are real-use only, no
	// synthetic case) and none sends the push token.
	for _, operation := range RESTRunOrderFor(RESTServiceDHOAPI) {
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(spec.Path, "/api/v1/billing/") {
			if spec.Method != "GET" || spec.Credential != RESTCredentialRun {
				t.Errorf("%s: method %s credential %q, want a GET on the run's own bearers", operation, spec.Method, spec.Credential)
			}
		}
	}
}

// TestBillingReadCasesPinTheCapturedStatuses pins the statuses both planes must
// answer the proof principal (a non-superuser): they are the Python api's own
// answers from a live capture, so a change here is a change of the reference.
func TestBillingReadCasesPinTheCapturedStatuses(t *testing.T) {
	want := map[string]int{
		"REST:GET:/api/v1/billing/plans/list":                        200,
		"REST:GET:/api/v1/billing/plans/{plan_id}/missing":           404,
		"REST:GET:/api/v1/billing/invoices/list":                     200,
		"REST:GET:/api/v1/billing/invoices/{invoice_id}/missing":     404,
		"REST:GET:/api/v1/billing/refunds/not_superuser":             403,
		"REST:GET:/api/v1/billing/refunds/{refund_id}/not_superuser": 403,
		"REST:GET:/api/v1/billing/entitlements/{org_id}/own_org":     200,
		"REST:GET:/api/v1/billing/subscriptions/list/list":           200,
		"REST:GET:/api/v1/billing/subscriptions/no_subscription":     404,
		"REST:GET:/api/v1/billing/subscriptions/history/history":     200,
		"REST:GET:/api/v1/billing/audit/missing_org_id":              422,
		"REST:GET:/api/v1/billing/audit/not_superuser":               403,
		"REST:GET:/api/v1/billing/audit/{audit_id}/not_superuser":    403,
		"REST:HEAD:/health/head":                                     200,
		"REST:HEAD:/ready/head":                                      200,
		"REST:HEAD:/health/workers/head":                             200,
	}
	got := map[string]int{}
	for _, operation := range RESTRunOrderFor(RESTServiceDHOAPI) {
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(spec.Path, "/api/v1/billing/") && spec.Method != "HEAD" {
			continue
		}
		for _, request := range spec.Requests {
			if request.WantCandidateStatus != request.WantBaselineStatus {
				t.Errorf("%s/%s: the planes must be held to the same status (%d vs %d)", operation, request.Name, request.WantCandidateStatus, request.WantBaselineStatus)
			}
			got[operation+"/"+request.Name] = request.WantBaselineStatus
		}
	}
	if len(got) != len(want) {
		t.Fatalf("billing/HEAD cases = %d, want %d: %v", len(got), len(want), got)
	}
	for key, status := range want {
		if got[key] != status {
			t.Errorf("%s: status %d, want %d", key, got[key], status)
		}
	}
}
