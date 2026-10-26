//go:build integration

package authflowvenue_test

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/authrows"
)

const goldenFile = "testdata/golden/auth-flow-venue-oracle.json"

// goldenMails is every mail the committed golden recorded from the Python plane, in the order of the file's keys.
func goldenMails(t *testing.T) []sentMail {
	t.Helper()
	raw, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Rows map[string]struct {
			Rows string `json:"rows"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	var names []string
	for name := range golden.Rows {
		if strings.HasPrefix(name, "python mails: ") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var mails []sentMail
	for _, name := range names {
		var recorded []recordedMail
		if err := json.Unmarshal([]byte(golden.Rows[name].Rows), &recorded); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, mail := range recorded {
			mails = append(mails, mail.sent())
		}
	}
	return mails
}

// The golden's link tokens are scrubbed to a placeholder when it is frozen, so the plane that mailed them is what says
// their format: the Python plane's is go-api's signed one.
func TestGoldenLinkMailsReadAsTheGoAPIFormat(t *testing.T) {
	rows := linkMailRows(goldenMails(t))
	if len(rows) != 8 {
		t.Fatalf("the golden holds %d link mails, want 8: %v", len(rows), rows)
	}
	for _, row := range rows {
		if !strings.HasSuffix(row, " "+authrows.LinkTokenGoAPI) {
			t.Errorf("golden link mail row %q is not of the go-api format", row)
		}
	}
}

func TestRecordedPlaceholderTokenIsNotAFormatOfItsOwn(t *testing.T) {
	recorded := recordedMail{Subject: "Verify your email address", Token: "<link-token>"}.sent()
	got := linkMailRows([]sentMail{recorded})
	if len(got) != 1 || got[0] != authrows.LinkMailRow("Verify your email address", authrows.LinkTokenGoAPI) {
		t.Fatalf("rows = %v, want the go-api format", got)
	}
}
