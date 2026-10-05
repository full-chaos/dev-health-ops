package pgmigrate_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// Revision 0146 names the MCP class document digest and the live-digest setting as literals (a migration cannot
// import Go). They must be the digest the old class switch keyed its rows by (mcpclass.DocumentDigest) and the
// setting the upgrade verb sets from its environment, in the chain file and in the Alembic mirror alike (CHAOS-8735).
func TestClassDecisionBackfillNamesTheClassDigestAndTheVerbsSetting(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var sql string
	for _, file := range chain {
		if file.Revision == "0146" {
			sql = file.SQL
		}
	}
	if sql == "" {
		t.Fatal("no chain file for revision 0146")
	}
	check := func(t *testing.T, name, text string, want int) {
		t.Helper()
		if got := strings.Count(text, "'"+mcpclass.DocumentDigest()+"'"); got != want {
			t.Errorf("%s names the class document digest %d times, want %d (the guard and the copy)", name, got, want)
		}
		if !strings.Contains(text, pgmigrate.ClassDecisionLiveDigestSetting) {
			t.Errorf("%s does not read the setting %s", name, pgmigrate.ClassDecisionLiveDigestSetting)
		}
	}
	check(t, "sql/0146", sql, 2)
	if got := strings.Count(sql, "current_setting('"+pgmigrate.ClassDecisionLiveDigestSetting+"', true)"); got != 2 {
		t.Errorf("sql/0146 reads the setting %d times, want 2 (the guard and the copy)", got)
	}

	t.Run("alembic mirror", func(t *testing.T) {
		script := filepath.Join("..", "..", "src", "dev_health_ops", "alembic", "versions", "0146_add_go_api_class_decision.py")
		body, err := os.ReadFile(script)
		if os.IsNotExist(err) {
			t.Skip("the Alembic scripts are gone: the chain is the source")
		}
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		if !regexp.MustCompile(`CLASS_DOCUMENT_DIGEST = \(?\s*"` + mcpclass.DocumentDigest() + `"`).MatchString(text) {
			t.Errorf("the Alembic mirror's CLASS_DOCUMENT_DIGEST is not mcpclass.DocumentDigest()")
		}
		if !strings.Contains(text, `LIVE_DIGEST_SETTING = "`+pgmigrate.ClassDecisionLiveDigestSetting+`"`) ||
			!strings.Contains(text, `LIVE_DIGEST_ENV = "`+pgmigrate.ClassDecisionLiveDigestEnv+`"`) {
			t.Errorf("the Alembic mirror does not name the verb's setting and environment variable")
		}
	})
}
