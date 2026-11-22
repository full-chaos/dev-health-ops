package secrets

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type warning struct {
	msg   string
	attrs map[string]any
	args  []any
}

// captureWarnings resets the registry and records every warning until the test ends.
func captureWarnings(t *testing.T) func() []warning {
	t.Helper()
	ResetRegistered()
	var mu sync.Mutex
	var got []warning
	previous := SetWarner(func(msg string, args ...any) {
		attrs := map[string]any{}
		for index := 0; index+1 < len(args); index += 2 {
			key, _ := args[index].(string)
			if _, dup := attrs[key]; dup {
				t.Errorf("%s: attribute %q twice: %v", msg, key, args)
			}
			attrs[key] = args[index+1]
		}
		if len(args)%2 != 0 {
			t.Errorf("%s: odd attribute list: %v", msg, args)
		}
		mu.Lock()
		defer mu.Unlock()
		got = append(got, warning{msg: msg, attrs: attrs, args: append([]any(nil), args...)})
	})
	t.Cleanup(func() { SetWarner(previous); ResetRegistered() })
	return func() []warning {
		mu.Lock()
		defer mu.Unlock()
		return append([]warning(nil), got...)
	}
}

func credentialWithKey(t *testing.T, key, value string) []byte {
	t.Helper()
	plain, err := json.Marshal(map[string]string{key: value})
	if err != nil {
		t.Fatal(err)
	}
	return plain
}

func attrKeys(w warning) string {
	keys := make([]string, 0, len(w.args)/2)
	for index := 0; index+1 < len(w.args); index += 2 {
		keys = append(keys, fmt.Sprint(w.args[index]))
	}
	return strings.Join(keys, ",")
}

func argsContain(w warning, text string) bool {
	for _, arg := range w.args {
		if strings.Contains(fmt.Sprint(arg), text) {
			return true
		}
	}
	return false
}

const (
	shortWarning  = "secret_shorter_than_minimum_not_redactable_by_value"
	commonWarning = "secret_is_a_common_word_redacted_only_in_credential_shapes"
)

func TestWarnShortNamesKnownNonSecretField(t *testing.T) {
	warnings := captureWarnings(t)
	RegisterCredentialJSON([]byte(`{"api_key":"aaaaaaaaaaaaaaaaaaaa","project_key":"pk7409"}`))
	got := warnings()
	if len(got) != 1 || got[0].msg != shortWarning {
		t.Fatalf("want one short warning, got %+v", got)
	}
	w := got[0]
	if keys := attrKeys(w); keys != "setting,field,minimum_length,detail" {
		t.Errorf("attribute keys = %s, want setting,field,minimum_length,detail", keys)
	}
	if w.attrs["setting"] != "integration credential" || w.attrs["field"] != "project_key" || w.attrs["minimum_length"] != MinRegisteredLength {
		t.Errorf("want field project_key named, got %v", w.args)
	}
	if argsContain(w, "pk7409") {
		t.Errorf("the value reached the warning: %v", w.args)
	}
}

func TestWarnCommonWordNamesKnownNonSecretField(t *testing.T) {
	warnings := captureWarnings(t)
	RegisterCredentialJSON([]byte(`{"project_key":"production"}`))
	got := warnings()
	if len(got) != 1 || got[0].msg != commonWarning {
		t.Fatalf("want one common-word warning, got %+v", got)
	}
	if keys := attrKeys(got[0]); keys != "setting,field" {
		t.Errorf("attribute keys = %s, want setting,field", keys)
	}
	if got[0].attrs["field"] != "project_key" {
		t.Errorf("want field project_key named, got %v", got[0].args)
	}
}

func assertHiddenByLength(t *testing.T, sent string, got []warning) {
	t.Helper()
	if len(got) != 1 || got[0].msg != shortWarning {
		t.Errorf("%q: want one short warning, got %+v", sent, got)
		return
	}
	w := got[0]
	if w.attrs["field"] != "undeclared_name" || w.attrs["field_length"] != len(sent) {
		t.Errorf("%q: not reported as undeclared with its length: %v", sent, w.args)
	}
	if argsContain(w, sent) {
		t.Errorf("%q: caller-supplied text reached the warning: %v", sent, w.args)
	}
}

func TestWarnShortKeepsAnOffListNameHidden(t *testing.T) {
	for _, sent := range []string{
		"project_key_secret", "my_project_key", "project_keys", "project_key_token", "token_type", "key_id", "a_project_key_b",
	} {
		warnings := captureWarnings(t)
		RegisterCredentialJSON(credentialWithKey(t, sent, "pk7409"))
		got := warnings()
		assertHiddenByLength(t, sent, got)
		for _, w := range got {
			if argsContain(w, "project") {
				t.Errorf("%q: a name fragment reached the warning: %v", sent, w.args)
			}
		}
	}
}

func TestWarnShortHidesLookAlikesOfAKnownNonSecretField(t *testing.T) {
	for _, sent := range []string{
		"Project_Key", "PROJECT_KEY", "projectKey", "project-key", "projectkey",
		" project_key", "project_key ", "project_key\n", "project_key\x00",
		"project_Key", "prоject_key", "project​_key", "ｐroject_key",
	} {
		warnings := captureWarnings(t)
		RegisterCredentialJSON(credentialWithKey(t, sent, "pk7409"))
		assertHiddenByLength(t, sent, warnings())
	}
	// An escape that decodes to the exact bytes is the same key: the literal is named.
	warnings := captureWarnings(t)
	RegisterCredentialJSON([]byte(`{"project_key":"pk7409"}`))
	if got := warnings(); len(got) != 1 || got[0].attrs["field"] != "project_key" {
		t.Errorf("an escaped spelling of the exact key: got %+v", got)
	}
}

func TestWarnShortStillNamesDeclaredFieldsAsBefore(t *testing.T) {
	for sent, declared := range map[string]string{"client_secret": "client_secret", "clientSecret": "client_secret", "API-KEY": "api_key"} {
		warnings := captureWarnings(t)
		RegisterCredentialJSON(credentialWithKey(t, sent, "abc"))
		got := warnings()
		if len(got) != 1 || got[0].attrs["field"] != declared {
			t.Errorf("%q: want field %q, got %+v", sent, declared, got)
		}
	}
	warnings := captureWarnings(t)
	RegisterCredentialJSON([]byte(`{"client_secret":"abc","project_key":"pk7409"}`))
	fields := map[any]int{}
	for _, w := range warnings() {
		fields[w.attrs["field"]]++
	}
	if len(fields) != 2 || fields["client_secret"] != 1 || fields["project_key"] != 1 {
		t.Errorf("want one warning each for client_secret and project_key, got %v", fields)
	}
}

func TestWarnNeverCarriesTheValue(t *testing.T) {
	for _, tc := range []struct {
		plain, value, msg, keys string
	}{
		{`{"project_key":"pk7409"}`, "pk7409", shortWarning, "setting,field,minimum_length,detail"},
		{`{"project_key":"production"}`, "production", commonWarning, "setting,field"},
		{`{"ld":{"project_key":"pk7409"},"project_key":"pk7409"}`, "pk7409", shortWarning, "setting,field,minimum_length,detail"},
	} {
		warnings := captureWarnings(t)
		RegisterCredentialJSON([]byte(tc.plain))
		got := warnings()
		if len(got) != 1 || got[0].msg != tc.msg {
			t.Errorf("%s: want one %s, got %+v", tc.plain, tc.msg, got)
			continue
		}
		if keys := attrKeys(got[0]); keys != tc.keys {
			t.Errorf("%s: attribute keys = %s, want %s", tc.plain, keys, tc.keys)
		}
		if argsContain(got[0], tc.value) {
			t.Errorf("%s: the value reached the warning: %v", tc.plain, got[0].args)
		}
		if got[0].attrs["field"] != "project_key" {
			t.Errorf("%s: want field project_key, got %v", tc.plain, got[0].args)
		}
	}
}
