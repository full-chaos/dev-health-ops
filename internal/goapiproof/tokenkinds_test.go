package goapiproof

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A JWT-shaped value assembled at run time, so no token-looking literal is
// committed (the secret scanner reads a whole literal as a key).
var testAccessToken = strings.Join([]string{"aGVhZGVy", "cGF5bG9hZA", "c2lnbmF0dXJl"}, ".")

func TestTokenFileKindsAreCompleteAndDistinct(t *testing.T) {
	seenKind, seenFlag := map[RESTCredentialKind]bool{}, map[string]bool{}
	for _, kind := range TokenFileKinds() {
		if kind.Kind == RESTCredentialRun || kind.Flag == "" || !strings.HasPrefix(kind.Flag, "-") || kind.What == "" || kind.Label == "" || kind.Validate == nil {
			t.Fatalf("incomplete token file kind %+v", kind)
		}
		if seenKind[kind.Kind] || seenFlag[kind.Flag] {
			t.Fatalf("duplicate token file kind or flag %+v", kind)
		}
		seenKind[kind.Kind], seenFlag[kind.Flag] = true, true
		if got, ok := TokenFileKindFor(kind.Kind); !ok || got.Flag != kind.Flag {
			t.Fatalf("TokenFileKindFor(%q) = %+v, %v", kind.Kind, got, ok)
		}
	}
	for _, want := range []RESTCredentialKind{RESTCredentialPushToken, RESTCredentialOrgAdmin, RESTCredentialPlatformSuperadmin} {
		if !seenKind[want] {
			t.Fatalf("no token file kind for %q", want)
		}
	}
	if _, ok := TokenFileKindFor(RESTCredentialRun); ok {
		t.Fatal("the run credential is minted, not file-fed")
	}
}

func TestAccessTokenKindsRefuseAPushTokenAndAPushKindRefusesAJWT(t *testing.T) {
	for _, kind := range []RESTCredentialKind{RESTCredentialOrgAdmin, RESTCredentialPlatformSuperadmin} {
		descriptor, _ := TokenFileKindFor(kind)
		if err := descriptor.Validate("fcpush_notajwt"); err == nil {
			t.Fatalf("%s must refuse a push token", kind)
		}
		if err := descriptor.Validate(testAccessToken); err != nil {
			t.Fatalf("%s must accept a JWT: %v", kind, err)
		}
	}
	push, _ := TokenFileKindFor(RESTCredentialPushToken)
	if err := push.Validate(testAccessToken); err == nil {
		t.Fatal("the push kind must refuse a JWT")
	}
}

func TestAccessTokenFileCredentialReadsTheFileEachTimeAndNeverLeaksIt(t *testing.T) {
	for _, kind := range []RESTCredentialKind{RESTCredentialOrgAdmin, RESTCredentialPlatformSuperadmin} {
		descriptor, _ := TokenFileKindFor(kind)
		path := filepath.Join(t.TempDir(), "token")
		write := func(content string) {
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		write(testAccessToken + "\n")
		credential := TokenFileCredential(descriptor, path)
		send := func() (string, error) {
			request, _ := http.NewRequest(http.MethodGet, "http://example.invalid/x", nil)
			err := credential.Apply(context.Background(), request)
			return request.Header.Get("Authorization"), err
		}
		if got, err := send(); err != nil || got != "Bearer "+testAccessToken {
			t.Fatalf("%s: Authorization = %q, %v", kind, got, err)
		}
		// Rotated between two uses on ONE credential: nothing is cached.
		rotated := strings.Join([]string{"cm90YXRlZA", "cGF5bG9hZA", "c2ln"}, ".")
		write(rotated)
		if got, err := send(); err != nil || got != "Bearer "+rotated {
			t.Fatalf("%s: a rotated file must be picked up: %q, %v", kind, got, err)
		}
		write("fcpush_SECRETVALUE")
		if _, err := send(); err == nil || strings.Contains(err.Error(), "SECRETVALUE") {
			t.Fatalf("%s: want a shape refusal that does not leak the file content, got %v", kind, err)
		}
		if _, err := TokenFileCredential(descriptor, filepath.Join(t.TempDir(), "absent")).value(context.Background()); err == nil {
			t.Fatalf("%s: a missing file must be an error, never an empty credential", kind)
		}
	}
}

func TestCheckTokenFileRefusesMissingWrongShapeAndNeverLeaks(t *testing.T) {
	for _, descriptor := range TokenFileKinds() {
		dir := t.TempDir()
		if err := CheckTokenFile(descriptor, filepath.Join(dir, "absent")); err == nil || !strings.Contains(err.Error(), descriptor.What) {
			t.Fatalf("%s: missing file must refuse naming the credential, got %v", descriptor.Kind, err)
		}
		wrong := filepath.Join(dir, "wrong")
		if err := os.WriteFile(wrong, []byte("Usage: mint-token SECRETVALUE"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := CheckTokenFile(descriptor, wrong); err == nil || strings.Contains(err.Error(), "SECRETVALUE") {
			t.Fatalf("%s: wrong-shape file must refuse without leaking, got %v", descriptor.Kind, err)
		}
		good := filepath.Join(dir, "good")
		value := testAccessToken
		if descriptor.Kind == RESTCredentialPushToken {
			value = "fcpush_ok"
		}
		if err := os.WriteFile(good, []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := CheckTokenFile(descriptor, good); err != nil {
			t.Fatalf("%s: a good file must pass: %v", descriptor.Kind, err)
		}
	}
}

func TestAdminCredentialEntriesMustBeDHOAPIReadOnlyGETs(t *testing.T) {
	for _, kind := range []RESTCredentialKind{RESTCredentialOrgAdmin, RESTCredentialPlatformSuperadmin} {
		for name, spec := range map[string]RESTEndpointSpec{
			"query-api": {Method: "GET", Path: "/test-only-admin", Credential: kind},
			"public":    {Method: "GET", Path: "/test-only-admin", Service: RESTServiceDHOAPI, PublicNoAuth: true, Credential: kind},
			"write":     {Method: "POST", Path: "/test-only-admin", Service: RESTServiceDHOAPI, Credential: kind},
		} {
			operation := "REST:" + spec.Method + ":/test-only-admin"
			spec.Requests = []RESTRequest{{Name: "case", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: RESTBodyModeStatusOnly}}
			restEndpointSpecs[operation] = spec
			restRunOrder = append(restRunOrder, operation)
			err := validateRESTCredentialKinds()
			delete(restEndpointSpecs, operation)
			restRunOrder = restRunOrder[:len(restRunOrder)-1]
			if err == nil {
				t.Fatalf("%s/%s: must be refused", kind, name)
			}
		}
	}
}
