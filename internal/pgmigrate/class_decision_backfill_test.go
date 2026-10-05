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
		if !strings.Contains(text, `sa.text("SELECT set_config(:name, :value, true)")`) {
			t.Errorf("the Alembic mirror does not set the live digest for the migration's transaction only (set_config(..., true))")
		}
		if !strings.Contains(text, `LIVE_DIGEST_SETTING = "`+pgmigrate.ClassDecisionLiveDigestSetting+`"`) ||
			!strings.Contains(text, `LIVE_DIGEST_ENV = "`+pgmigrate.ClassDecisionLiveDigestEnv+`"`) {
			t.Errorf("the Alembic mirror does not name the verb's setting and environment variable")
		}
		// The guard is a DO block, which takes no bind parameters: the mirror runs the chain file's own text.
		guard := regexp.MustCompile(`(?s)DO \$\$\n.*?\nEND\n\$\$`)
		sqlGuard, mirrorGuard := guard.FindString(sql), guard.FindString(text)
		if sqlGuard == "" || mirrorGuard != sqlGuard {
			t.Errorf("the Alembic mirror's guard is not the chain file's DO block:\nmirror: %q\nchain:  %q", mirrorGuard, sqlGuard)
		}
		// The copy binds the setting name and the class digest (no SQL built from strings).
		for _, want := range []string{
			"AND schema_digest = current_setting(:live_digest_setting, true)",
			"AND document_digest = :class_document_digest",
			"live_digest_setting=LIVE_DIGEST_SETTING,",
			"class_document_digest=CLASS_DOCUMENT_DIGEST,",
		} {
			if !strings.Contains(text, want) {
				t.Errorf("the Alembic mirror's copy does not hold %q", want)
			}
		}
	})
}
