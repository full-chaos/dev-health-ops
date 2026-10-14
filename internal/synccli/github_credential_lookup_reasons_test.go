package synccli

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-9678: every way the stored GitHub credential lookup finds nothing usable
// is its own reason (the exit text stays the one ported "Missing GitHub
// credentials" text), carried by a typed failure and written to the log as a
// reason and, where one names it, a payload key NAME (never a value).

const reasonTestKey = "unit-test-settings-key"

func envOf(values map[string]string) cli.Env {
	return cli.Env{Lookup: func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}}
}

func sealPayload(t *testing.T, key, salt, payload string) string {
	t.Helper()
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), salt)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := decryptor.Encrypt([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return sealed.Reveal()
}

func captureLookupLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buffer
}

func failureOf(t *testing.T, err error) credentialLookupFailure {
	t.Helper()
	var failure credentialLookupFailure
	if !errors.As(err, &failure) {
		t.Fatalf("error %v is not a credentialLookupFailure", err)
	}
	if !errors.Is(err, errNoDBCredential) {
		t.Fatalf("failure %v is not an errNoDBCredential: the exit text would change", err)
	}
	return failure
}

func TestEveryCredentialLookupBranchHasItsOwnReason(t *testing.T) {
	key := map[string]string{"SETTINGS_ENCRYPTION_KEY": reasonTestKey}
	sealed := func(payload string) string { return sealPayload(t, reasonTestKey, "", payload) }
	missingFile := filepath.Join(t.TempDir(), "no-such-key-file")
	cases := []struct {
		name       string
		ciphertext string
		env        map[string]string
		reason     string
		field      string
		hasToken   bool
		hasApp     bool
	}{
		{"no encryption key", sealed(`{"token":"t"}`), nil, reasonEncryptionKeyNotSet, "", false, false},
		{"encryption key given twice", sealed(`{"token":"t"}`), map[string]string{"SETTINGS_ENCRYPTION_KEY": reasonTestKey, "SETTINGS_ENCRYPTION_KEY_FILE": missingFile}, reasonEncryptionKeyUnusable, "", false, false},
		{"encryption key file unreadable", sealed(`{"token":"t"}`), map[string]string{"SETTINGS_ENCRYPTION_KEY_FILE": missingFile}, reasonEncryptionKeyUnusable, "", false, false},
		{"salt given twice", sealed(`{"token":"t"}`), map[string]string{"SETTINGS_ENCRYPTION_KEY": reasonTestKey, "SETTINGS_ENCRYPTION_SALT": "s", "SETTINGS_ENCRYPTION_SALT_FILE": missingFile}, reasonEncryptionKeyUnusable, "", false, false},
		{"garbage ciphertext", "v1:garbage", key, reasonDecryptFailed, "", false, false},
		{"wrong key", sealPayload(t, "another-key", "", `{"token":"t"}`), key, reasonDecryptFailed, "", false, false},
		{"wrong salt", sealPayload(t, reasonTestKey, "another-salt", `{"token":"t"}`), key, reasonDecryptFailed, "", false, false},
		{"payload not json", sealed(`not json`), key, reasonPayloadNotJSON, "", false, false},
		{"payload a list", sealed(`["a"]`), key, reasonPayloadNotObject, "", false, false},
		{"payload null", sealed(`null`), key, reasonPayloadNotObject, "", false, false},
		{"token not text", sealed(`{"token":5}`), key, reasonFieldNotText, "token", false, false},
		{"installation id beside a token not text", sealed(`{"token":"t","installation_id":5}`), key, reasonFieldNotText, "installation_id", false, false},
		{"alias key not text", sealed(`{"appId":5}`), key, reasonFieldNotText, "app_id", false, false},
		{"private key path unreadable", sealed(`{"app_id":"1","private_key_path":"/nonexistent/key.pem","installation_id":"2"}`), key, reasonPrivateKeyPathUnread, "", false, false},
		{"token and an app", sealed(`{"token":"t","app_id":"1","private_key":"k","installation_id":"2"}`), key, reasonCredentialShapeInvalid, "", true, true},
		{"partial app", sealed(`{"app_id":"1"}`), key, reasonCredentialShapeInvalid, "", false, true},
		{"nothing usable", sealed(`{}`), key, reasonCredentialShapeInvalid, "", false, false},
		{"empty token", sealed(`{"token":""}`), key, reasonCredentialShapeInvalid, "", false, false},
	}
	seen := map[string]bool{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := githubCredentialFromCiphertext(context.Background(), tc.ciphertext, "org-1", envOf(tc.env))
			failure := failureOf(t, err)
			if failure.Reason != tc.reason || failure.Field != tc.field || failure.HasToken != tc.hasToken || failure.HasApp != tc.hasApp {
				t.Fatalf("failure = %+v, want reason %q field %q has_token %v has_app %v", failure, tc.reason, tc.field, tc.hasToken, tc.hasApp)
			}
			if tc.reason == reasonDecryptFailed && (failure.Class == "" || failure.Type == "") {
				t.Fatalf("a decrypt failure carries no class and type: %+v", failure)
			}
			seen[tc.reason] = true
		})
	}
	// Reasons that need the database or the network have their own tests below and in the integration test.
	for _, reason := range []string{reasonEncryptionKeyNotSet, reasonEncryptionKeyUnusable, reasonDecryptFailed, reasonPayloadNotJSON, reasonPayloadNotObject, reasonFieldNotText, reasonPrivateKeyPathUnread, reasonCredentialShapeInvalid} {
		if !seen[reason] {
			t.Errorf("reason %q has no case", reason)
		}
	}
}

func TestADatabaseURLTheLookupCannotUseHasItsOwnReason(t *testing.T) {
	for _, dbURL := range []string{
		"postgresql+asyncpg://u:p@h/db?bogus=1",
		"postgresql+asyncpg://u:p@h/db?sslmode=disable",
		"postgresql+asyncpg://u:p@h/db#fragment",
	} {
		_, err := storedGitHubCredential(context.Background(), dbURL, "org-1", envOf(nil))
		if failure := failureOf(t, err); failure.Reason != reasonDatabaseURLNotUsable {
			t.Errorf("%s: reason %q, want %q", dbURL, failure.Reason, reasonDatabaseURLNotUsable)
		}
	}
}

func TestAnUnreachableDatabaseHasItsOwnReasonAndNoErrorText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := storedGitHubCredential(ctx, "postgresql+asyncpg://user:secretpw@127.0.0.1:1/db", "org-1", envOf(nil))
	failure := failureOf(t, err)
	if failure.Reason != reasonDatabaseConnectFailed || failure.Class == "" || failure.Type == "" {
		t.Fatalf("failure = %+v, want %q with an error class and type", failure, reasonDatabaseConnectFailed)
	}
	if strings.Contains(err.Error(), "secretpw") || strings.Contains(failure.Class+failure.Type, "secretpw") {
		t.Fatalf("the failure carries the password: %v %+v", err, failure)
	}
}

// githubCredentialFromDB writes the reason of a failure to the log and the exit
// text stays the Python one; a payload key the credential does not use is
// logged by name and ignored.
func TestTheLookupLogsTheReasonAndTheIgnoredKeyNamesNeverAValue(t *testing.T) {
	log := captureLookupLog(t)
	_, err := githubCredentialFromDB(context.Background(), "postgresql+asyncpg://u:p@h/db?bogus=1", "org-1", envOf(nil))
	failureOf(t, err)
	for _, want := range []string{"synccli.github_credential_lookup_failed", "org_id=org-1", "reason=database_url_not_usable"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q: %s", want, log.String())
		}
	}

	log = captureLookupLog(t)
	credential, err := githubCredentialFromCiphertext(context.Background(), sealPayload(t, reasonTestKey, "", `{"token":"ghp_value_that_must_not_be_logged","org":"acme-secret-value","zeta":"z"}`), "org-1", envOf(map[string]string{"SETTINGS_ENCRYPTION_KEY": reasonTestKey}))
	if err != nil || credential == nil || credential.Mode != CredentialPAT || credential.Token != "ghp_value_that_must_not_be_logged" {
		t.Fatalf("credential = %+v, %v, want the PAT with the unused keys ignored", credential, err)
	}
	for _, want := range []string{"synccli.github_credential_fields_ignored", "org_id=org-1", "fields=org,zeta"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q: %s", want, log.String())
		}
	}
	for _, leaked := range []string{"ghp_value_that_must_not_be_logged", "acme-secret-value"} {
		if strings.Contains(log.String(), leaked) {
			t.Fatalf("the log carries a credential value: %s", log.String())
		}
	}
}

// The exit text of every failure is the one ported text: a reason changes the
// log, never the contract.
func TestEveryLookupFailureEndsInTheMissingCredentialsText(t *testing.T) {
	for _, reason := range []string{reasonDatabaseURLNotUsable, reasonDatabaseConnectFailed, reasonDatabaseQueryFailed, reasonCredentialRowNotFound, reasonCiphertextEmpty, reasonEncryptionKeyNotSet, reasonEncryptionKeyUnusable, reasonDecryptFailed, reasonPayloadNotJSON, reasonPayloadNotObject, reasonFieldNotText, reasonPrivateKeyPathUnread, reasonCredentialShapeInvalid} {
		h := &lookupHarness{org: "org-1", orgFound: true, credErr: lookupFailure(reason)}
		code, _, stderr := runVerb(t, "security", h.executor(), githubGitArgs("--org", "org-1"), dbEnv)
		if code == cli.ExitOK || !strings.Contains(stderr, missingGitHubCredentials) {
			t.Errorf("reason %q: exit %d, stderr %q, want the Missing GitHub credentials text", reason, code, stderr)
		}
	}
}

func TestUnusedKeysAndAliasesInTheStoredPayload(t *testing.T) {
	cases := []struct {
		name, payload string
		mode          string
		ignored       string
		appID         string
	}{
		{"a key the credential does not use", `{"token":"t","org":"acme"}`, CredentialPAT, "org", ""},
		{"several unused keys, sorted", `{"zeta":1,"token":"t","alpha":{"a":1}}`, CredentialPAT, "alpha,zeta", ""},
		{"camelCase app keys are aliased", `{"appId":"1","privateKey":"k","installationId":"2"}`, CredentialApp, "", "1"},
		{"the later key of the document wins over its alias", `{"app_id":"1","appId":"9","private_key":"k","installation_id":"2"}`, CredentialApp, "", "9"},
		{"the later canonical key wins over an alias", `{"appId":"9","app_id":"1","private_key":"k","installation_id":"2"}`, CredentialApp, "", "1"},
		{"a null alias never replaces a value", `{"app_id":"1","appId":null,"private_key":"k","installation_id":"2"}`, CredentialApp, "", "1"},
		{"dataclass fields are accepted and not used", `{"token":"t","provider":"github","credential_name":"x","extra":{"a":1}}`, CredentialPAT, "", ""},
		{"an unused key of any type is ignored", `{"token":"t","org":5,"flags":[1]}`, CredentialPAT, "flags,org", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ignored, err := githubCredentialFromJSON([]byte(tc.payload))
			if err != nil || got.Mode != tc.mode || strings.Join(ignored, ",") != tc.ignored || got.AppID != tc.appID {
				t.Fatalf("got %+v ignored %v err %v, want mode %q ignored %q app id %q", got, ignored, err, tc.mode, tc.ignored, tc.appID)
			}
		})
	}
}
