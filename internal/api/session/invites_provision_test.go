package session

import (
	"errors"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// PARITY-10988 row 6: each auth invite-accept refusal is today's answer;
// anything else is the 503 of the session proxy.
func TestAcceptRefusalsMapToTodaysAnswers(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		text   string
	}{
		{&authprovision.Refused{Status: 400, Reason: "invite_invalid"}, http.StatusBadRequest, "Invalid or expired invite"},
		{&authprovision.Refused{Status: 403, Reason: "email_unverified"}, http.StatusForbidden, reasonInviteEmailUnverified},
		{&authprovision.Refused{Status: 403, Reason: "email_mismatch"}, http.StatusForbidden, reasonInviteEmailMismatch},
		{&authprovision.Refused{Status: 409, Reason: "already_member"}, http.StatusBadRequest, "User is already a member of this organization"},
		{&authprovision.Refused{Status: 404, Reason: "organization_not_found"}, http.StatusNotFound, "Organization not found"},
		{&authprovision.Refused{Status: 403, Reason: "principal_deactivated"}, http.StatusServiceUnavailable, sessionissue.UnavailableText},
		{&authprovision.Refused{Status: 409, Reason: "users_not_owned"}, http.StatusServiceUnavailable, sessionissue.UnavailableText},
		{&authprovision.Refused{Status: 409, Reason: "organization_suspended"}, http.StatusServiceUnavailable, sessionissue.UnavailableText},
		{&authprovision.Refused{Status: 400, Reason: "malformed"}, http.StatusServiceUnavailable, sessionissue.UnavailableText},
		{&authprovision.Refused{Status: 401}, http.StatusServiceUnavailable, sessionissue.UnavailableText},
		{authprovision.ErrUnavailable, http.StatusServiceUnavailable, sessionissue.UnavailableText},
		{errors.New("other"), http.StatusServiceUnavailable, sessionissue.UnavailableText},
	} {
		status, text := acceptRefusalAnswer(tc.err)
		if status != tc.status || text != tc.text {
			t.Fatalf("%v: %d %q, want %d %q", tc.err, status, text, tc.status, tc.text)
		}
	}
}

// A token auth would refuse as malformed (empty, or over its 256-byte bound)
// is today's invalid invite, decided before any call.
func TestAnUnusableAcceptTokenIsRefusedLocally(t *testing.T) {
	for token, usable := range map[string]bool{
		"":                        false,
		string(make([]byte, 257)): false,
		"abc.def":                 true,
		string(make([]byte, 256)): true,
	} {
		if got := acceptTokenUsable(token); got != usable {
			t.Fatalf("acceptTokenUsable(len %d) = %v, want %v", len(token), got, usable)
		}
	}
}
