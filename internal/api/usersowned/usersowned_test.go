package usersowned

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func lookupOf(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func TestFromLookupReadsTheSwitchLikeAuthService(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  map[string]string
		want    bool
		wantErr bool
	}{
		{"unset", nil, false, false},
		{"empty", map[string]string{Env: ""}, false, false},
		{"false", map[string]string{Env: "false"}, false, false},
		{"true", map[string]string{Env: "true"}, true, false},
		{"padded and upper case", map[string]string{Env: " TRUE "}, true, false},
		{"garbage", map[string]string{Env: "maybe"}, false, true},
		{"one", map[string]string{Env: "1"}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := FromLookup(lookupOf(tc.values))
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("FromLookup = %v, %v; want %v, error %v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestWriteAnswers409WithTheMovedOperationReason(t *testing.T) {
	recorder := httptest.NewRecorder()
	Write(recorder)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status %d, want 409", recorder.Code)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `"reason":"operation_moved"`) || !strings.Contains(body, Message) {
		t.Fatalf("body %q lacks the reason or the message", body)
	}
	if strings.Contains(strings.ToLower(body), "token_version") || strings.Contains(strings.ToLower(body), "password_hash") {
		t.Fatalf("body %q names a column", body)
	}
}
