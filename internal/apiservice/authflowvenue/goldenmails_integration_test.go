//go:build integration

package authflowvenue_test

import (
	"encoding/json"
	"os"
	"regexp"
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
	rows := linkMailRows(goldenMails(t), authrows.LinkTokenGoAPI)
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
	got := linkMailRows([]sentMail{recorded}, authrows.LinkTokenGoAPI)
	if len(got) != 1 || got[0] != authrows.LinkMailRow("Verify your email address", authrows.LinkTokenGoAPI) {
		t.Fatalf("rows = %v, want the go-api format", got)
	}
}

// A failure message that prints mails holds no link token in any form: the bare 43-character one, go-api's signed one,
// or one after "token=".
func TestDescribeMailsHoldsNoLinkToken(t *testing.T) {
	opaque := "HBPZUFDXW596HSRDyraWzGxjt9ZH7kXADsArR5G7KzQ"
	signed := strings.Repeat("ab", 16) + "." + strings.Repeat("cd", 32)
	mails := []sentMail{
		{subject: "Verify your email address", token: opaque, html: `<a href="https://app.example.test/auth/verify?token=` + opaque + `">x</a>`},
		{subject: "Reset your password", token: signed, html: `<a href="https://app.example.test/auth/reset-password?token=` + signed + `">y</a>`},
	}
	text := describeMails(mails)
	for name, shaped := range map[string]*regexp.Regexp{
		"43-character token": regexp.MustCompile(`[A-Za-z0-9_-]{43}`),
		"signed token":       regexp.MustCompile(`[0-9a-f]{32}\.[0-9a-f]{64}`),
		"token= value":       regexp.MustCompile(`token=[^<>"\s]{20,}`),
	} {
		if found := shaped.FindString(text); found != "" {
			t.Errorf("the description holds a %s", name)
		}
	}
	if !strings.Contains(text, "Verify your email address") || !strings.Contains(text, "Reset your password") {
		t.Errorf("the description lost the subjects: %s", text)
	}
}

// The target's mails are not declared: each token is read, so a plane mailing the other format is named.
func TestTargetLinkMailsAreReadByTheirTokens(t *testing.T) {
	opaque := "HBPZUFDXW596HSRDyraWzGxjt9ZH7kXADsArR5G7KzQ"
	signed := strings.Repeat("ab", 16) + "." + strings.Repeat("cd", 32)
	got := linkMailRows([]sentMail{{subject: "A", token: opaque}, {subject: "B", token: signed}, {subject: "C"}}, "")
	want := []string{authrows.LinkMailRow("A", authrows.LinkTokenOpaque), authrows.LinkMailRow("B", authrows.LinkTokenGoAPI)}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("rows = %v, want %v", got, want)
	}
}
