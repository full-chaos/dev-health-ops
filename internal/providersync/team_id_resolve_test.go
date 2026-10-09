package providersync

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

func TestResolveTeamIDDecidesEveryCase(t *testing.T) {
	two := map[string]string{"linear": "linear:ENG", "gitlab": "gl:ENG"}
	for name, c := range map[string]struct {
		req  TeamIDRequest
		want string
		err  error
	}{
		"malformed":                 {TeamIDRequest{ID: "gh:"}, "", teamid.ErrMalformedTeamID},
		"keyed admin":               {TeamIDRequest{ID: " linear:ENG "}, "linear:ENG", nil},
		"keyed own provider":        {TeamIDRequest{Provider: "jira", ID: "atlassian:x"}, "jira:x", nil},
		"keyed foreign provider":    {TeamIDRequest{Provider: "jira", ID: "linear:ENG"}, "", ErrTeamIDForeign},
		"owner provider, no holder": {TeamIDRequest{Provider: "gitlab", ID: "ENG"}, "gl:ENG", nil},
		"owner provider, holders":   {TeamIDRequest{Provider: "linear", ID: "ENG", Holders: two}, "linear:ENG", nil},
		"admin, one holder":         {TeamIDRequest{ID: "ENG", Holders: map[string]string{"linear": "linear:ENG"}}, "linear:ENG", nil},
		"admin, two holders":        {TeamIDRequest{ID: "ENG", Holders: two}, "", ErrTeamIDAmbiguous},
		"admin, no holder":          {TeamIDRequest{ID: "eng"}, "custom:eng", nil},
		"admin, custom held":        {TeamIDRequest{ID: "eng", CustomHeld: true}, "", ErrTeamIDCustomHeld},
		"admin, holder beats held":  {TeamIDRequest{ID: "eng", CustomHeld: true, Holders: map[string]string{"linear": "linear:eng"}}, "linear:eng", nil},
		"reference, one holder":     {TeamIDRequest{Provider: "gitlab", ID: "ENG", Mode: TeamIDReference, Holders: map[string]string{"gitlab": "gl:ENG"}}, "gl:ENG", nil},
		"reference, two holders":    {TeamIDRequest{Provider: "jira", ID: "ENG", Mode: TeamIDReference, Holders: two}, "ENG", nil},
		"reference, no holder":      {TeamIDRequest{Provider: "jira", ID: "ENG", Mode: TeamIDReference}, "ENG", nil},
	} {
		got, err := ResolveTeamID(c.req)
		if got != c.want || !errors.Is(err, c.err) || (c.err == nil && err != nil) {
			t.Errorf("%s: ResolveTeamID = %q, %v; want %q, %v", name, got, err, c.want, c.err)
		}
	}
}
