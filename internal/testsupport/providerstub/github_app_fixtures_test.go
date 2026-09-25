package providerstub

import (
	"encoding/json"
	"testing"
)

func TestGitHubAppInstallCallbackFixtures(t *testing.T) {
	fixtures, err := LoadDir("fixtures")
	if err != nil {
		t.Fatal(err)
	}
	// the OAuth code exchange goes to github.com, the installations read to api.github.com: two hosts, one provider
	if got := get(t, fixtures, "github.com", "POST", "/login/oauth/access_token", nil); got.Code != 200 {
		t.Fatalf("access_token answered %d", got.Code)
	}
	list := get(t, fixtures, "api.github.com", "GET", "/user/installations?per_page=100&page=1", nil)
	var payload struct {
		Installations []struct {
			ID int64 `json:"id"`
		} `json:"installations"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &payload); err != nil || list.Code != 200 || len(payload.Installations) != 2 {
		t.Fatalf("installations: %d %v %s", list.Code, err, list.Body.String())
	}
	if payload.Installations[0].ID != 9100001 || payload.Installations[1].ID != 9100002 {
		t.Fatalf("the two venue installation ids are the ones pass 5 uses: %+v", payload)
	}
}
