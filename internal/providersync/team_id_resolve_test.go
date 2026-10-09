package providersync

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

func TestResolveTeamIDDecidesEveryCase(t *testing.T) {
	two := map[string]string{"linear": "linear:ENG", "gitlab": "gl:ENG"}
	web := teamid.Custom
	for name, c := range map[string]struct {
		req  TeamIDRequest
		want string
		err  error
	}{
		"malformed":                  {TeamIDRequest{Provider: web, ID: "gh:", Mode: TeamIDAddress}, "", teamid.ErrMalformedTeamID},
		"address keyed":              {TeamIDRequest{Provider: web, ID: " linear:ENG ", Mode: TeamIDAddress}, "linear:ENG", nil},
		"address keyed custom":       {TeamIDRequest{Provider: web, ID: "custom:eng", Mode: TeamIDAddress}, "custom:eng", nil},
		"address, one holder":        {TeamIDRequest{Provider: web, ID: "ENG", Mode: TeamIDAddress, Holders: map[string]string{"linear": "linear:ENG"}}, "linear:ENG", nil},
		"address, two holders":       {TeamIDRequest{Provider: web, ID: "ENG", Mode: TeamIDAddress, Holders: two}, "", ErrTeamIDAmbiguous},
		"address, no holder":         {TeamIDRequest{Provider: web, ID: "eng", Mode: TeamIDAddress}, "custom:eng", nil},
		"address, pushed custom":     {TeamIDRequest{Provider: web, ID: "eng", Mode: TeamIDAddress, Holders: map[string]string{"custom": "custom:eng"}}, "custom:eng", nil},
		"address, no origin":         {TeamIDRequest{ID: "eng", Mode: TeamIDAddress}, "", ErrTeamIDNoOrigin},
		"owner keyed own":            {TeamIDRequest{Provider: "jira", ID: "atlassian:x"}, "jira:x", nil},
		"owner keyed foreign":        {TeamIDRequest{Provider: "jira", ID: "linear:ENG"}, "", ErrTeamIDForeign},
		"owner, no holder":           {TeamIDRequest{Provider: "gitlab", ID: "ENG"}, "gl:ENG", nil},
		"owner, holders":             {TeamIDRequest{Provider: "linear", ID: "ENG", Holders: two}, "linear:ENG", nil},
		"owner custom":               {TeamIDRequest{Provider: web, ID: "eng", Holders: two}, "custom:eng", nil},
		"owner, no origin":           {TeamIDRequest{ID: "eng"}, "", ErrTeamIDNoOrigin},
		"reference, one holder":      {TeamIDRequest{Provider: "gitlab", ID: "ENG", Mode: TeamIDReference, Holders: map[string]string{"gitlab": "gl:ENG"}}, "gl:ENG", nil},
		"reference, two holders":     {TeamIDRequest{Provider: "jira", ID: "ENG", Mode: TeamIDReference, Holders: two}, "ENG", nil},
		"reference, no holder":       {TeamIDRequest{Provider: "jira", ID: "ENG", Mode: TeamIDReference}, "ENG", nil},
		"reference keyed of another": {TeamIDRequest{Provider: "jira", ID: "linear:ENG", Mode: TeamIDReference}, "linear:ENG", nil},
	} {
		got, err := ResolveTeamID(c.req)
		if got != c.want || !errors.Is(err, c.err) || (c.err == nil && err != nil) {
			t.Errorf("%s: ResolveTeamID = %q, %v; want %q, %v", name, got, err, c.want, c.err)
		}
	}
}
