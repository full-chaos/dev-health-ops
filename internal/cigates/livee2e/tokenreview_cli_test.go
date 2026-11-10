package livee2e

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange"
)

func TestReviewAudienceIsTheExchangeAudience(t *testing.T) {
	if ReviewAudience != workloadexchange.ReviewAudience {
		t.Fatalf("ReviewAudience %q != exchange %q", ReviewAudience, workloadexchange.ReviewAudience)
	}
}

func TestSubjectAndReviewerVerbsWriteANewFileOnce(t *testing.T) {
	dir := t.TempDir()
	for verb, segments := range map[string]int{"subject-jws": 3, "reviewer-token": 1} {
		path := filepath.Join(dir, verb)
		var out, errb bytes.Buffer
		if rc := Run([]string{verb, path}, func(string) string { return "" }, &out, &errb); rc != 0 {
			t.Fatalf("%s: rc %d: %s", verb, rc, errb.String())
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s: file mode %v err %v", verb, info, err)
		}
		raw, _ := os.ReadFile(path)
		if got := len(strings.Split(strings.TrimSpace(string(raw)), ".")); got != segments {
			t.Fatalf("%s: %d segments, want %d", verb, got, segments)
		}
		if out.Len() != 0 {
			t.Fatalf("%s printed a value", verb)
		}
		if rc := Run([]string{verb, path}, func(string) string { return "" }, &out, &errb); rc != 1 {
			t.Fatalf("%s: second write rc %d, want 1", verb, rc)
		}
	}
}

func TestTokenReviewServeNeedsItsServiceAccount(t *testing.T) {
	var out, errb bytes.Buffer
	if rc := Run([]string{"tokenreview-serve", "127.0.0.1:0", "c", "k", "s", "r"}, func(string) string { return "" }, &out, &errb); rc != 1 {
		t.Fatalf("rc %d, want 1", rc)
	}
}
