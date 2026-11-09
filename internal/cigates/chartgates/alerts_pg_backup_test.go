package chartgates_test

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The alerts on the nightly pg_dump CronJob of the bundled PostgreSQL (alerts/rules.yml, group postgres_backup,
// CHAOS-13373). The k8s_cluster receiver writes dotted names only, so the rules select them by __name__. The stale
// rule is built on the Job TTL in the chart: a good Job's successful_pods series lasts ttlSecondsAfterFinished, so the
// window plus the TTL must be the 25h the rule promises, and the job-name pattern must match the CronJob's Jobs.

const (
	pgBackupGroup         = "postgres_backup"
	pgBackupFailedAlert   = "PostgresBackupFailed"
	pgBackupStaleAlert    = "PostgresBackupStale"
	pgBackupJobPattern    = `".+-postgresql-backup-.+"`
	pgBackupPromise       = 25 * 3600
	pgBackupValuesRel     = "deploy/helm/dev-health/values.yaml"
	pgBackupTemplateRel   = "deploy/helm/dev-health/templates/postgres-backup.yaml"
	pgBackupRunbookRel    = "docs/operate/maintain/backup-and-restore.md"
	pgBackupWindowComment = "43200 s ttlSecondsAfterFinished + 13h window = 25h since the last good dump; change both together"
)

var (
	pgBackupFailedExpr = `max(max_over_time({__name__="k8s.job.failed_pods", "k8s.job.name"=~` + pgBackupJobPattern + `}[1h])) > 0`
	pgBackupStaleExpr  = regexp.MustCompile(`^\(max\(max_over_time\(\{__name__="k8s\.job\.successful_pods", "k8s\.job\.name"=~` +
		regexp.QuoteMeta(pgBackupJobPattern) + `\}\[([0-9]+)h\]\)\) or vector\(0\)\) < 1$`)
	pgBackupTTLValue = regexp.MustCompile(`(?m)^  backup:\n(?:    .*\n)*?    ttlSecondsAfterFinished: ([0-9]+)$`)
)

func pgBackupAlertsAreTheGroup(g *guard) {
	var names []string
	for _, r := range groupRules(g, pgBackupGroup) {
		names = append(names, str(r["alert"]))
	}
	g.expect(strings.Join(names, ",") == pgBackupFailedAlert+","+pgBackupStaleAlert, "group %s holds %v, want exactly %s and %s", pgBackupGroup, names, pgBackupFailedAlert, pgBackupStaleAlert)
}

func pgBackupFailedShape(g *guard) {
	a := alertNamed(g, pgBackupFailedAlert)
	g.expect(oneLine(str(a["expr"])) == pgBackupFailedExpr, "%s expr is %q, want %q", pgBackupFailedAlert, oneLine(str(a["expr"])), pgBackupFailedExpr)
	_, hasFor := a["for"]
	g.expect(!hasFor, "%s has a `for`: one failed pod must alert", pgBackupFailedAlert)
}

// The stale window plus the chart's Job TTL is the 25h the rule promises.
func pgBackupStaleWindowMatchesTheTTL(g *guard) {
	a := alertNamed(g, pgBackupStaleAlert)
	m := pgBackupStaleExpr.FindStringSubmatch(oneLine(str(a["expr"])))
	if m == nil {
		g.failf("%s expr is %q, want the absent-data form %s", pgBackupStaleAlert, oneLine(str(a["expr"])), pgBackupStaleExpr)
		return
	}
	window, _ := strconv.Atoi(m[1])
	ttl := pgBackupTTLValue.FindStringSubmatch(g.read(pgBackupValuesRel))
	if ttl == nil {
		g.failf("%s has no postgresql.backup.ttlSecondsAfterFinished", pgBackupValuesRel)
		return
	}
	seconds, _ := strconv.Atoi(ttl[1])
	g.expect(seconds+window*3600 == pgBackupPromise, "ttlSecondsAfterFinished %d s + window %dh = %d s, want %d s (25h)", seconds, window, seconds+window*3600, pgBackupPromise)
	g.expect(strings.Contains(g.read(rulesRel), "# "+pgBackupWindowComment+"\n      - alert: "+pgBackupStaleAlert+"\n"), "the line before `- alert: %s` is not the comment %q", pgBackupStaleAlert, pgBackupWindowComment)
}

// The Jobs of the CronJob are named <fullname>-postgresql-backup-<n>, which the pattern matches.
func pgBackupPatternMatchesTheCronJob(g *guard) {
	g.expect(strings.Contains(g.read(pgBackupTemplateRel), `name: {{ include "dev-health.fullname" . }}-postgresql-backup`+"\n"), "%s no longer names the CronJob <fullname>-postgresql-backup", pgBackupTemplateRel)
	re := regexp.MustCompile("^" + strings.Trim(pgBackupJobPattern, `"`) + "$")
	g.expect(re.MatchString("dev-health-ops-postgresql-backup-29384520") && !re.MatchString("dev-health-ops-migrate"), "pattern %s does not select the backup Jobs only", pgBackupJobPattern)
}

func pgBackupAlertsRouted(g *guard) {
	for _, name := range []string{pgBackupFailedAlert, pgBackupStaleAlert} {
		a := alertNamed(g, name)
		labels := mp(a["labels"])
		g.expect(str(labels["severity"]) == "warning" && str(labels["team"]) == "platform", "%s labels are %v, want severity warning and team platform", name, labels)
		g.expect(str(mp(a["annotations"])["summary"]) != "", "%s has no summary", name)
		g.expect(strings.Contains(str(mp(a["annotations"])["description"]), pgBackupRunbookRel), "%s description does not point at %s", name, pgBackupRunbookRel)
	}
	g.expect(strings.Contains(g.read(pgBackupRunbookRel), "## Prod Postgres from the nightly dump"), "%s has no section \"Prod Postgres from the nightly dump\"", pgBackupRunbookRel)
}

func TestPostgresBackupAlertGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"pg-backup-alerts-are-the-group", pgBackupAlertsAreTheGroup},
		check{"pg-backup-failed-shape", pgBackupFailedShape},
		check{"pg-backup-stale-window-matches-the-ttl", pgBackupStaleWindowMatchesTheTTL},
		check{"pg-backup-pattern-matches-the-cronjob", pgBackupPatternMatchesTheCronJob},
		check{"pg-backup-alerts-routed", pgBackupAlertsRouted},
	)
}

func TestPostgresBackupAlertGuardsPlantedDefects(t *testing.T) {
	rules := []string{rulesRel}
	both := []string{rulesRel, pgBackupValuesRel}
	runPlanted(t, []planted{
		{"failed-window-widened", rules, mut(rulesRel, `-postgresql-backup-.+"}[1h])) > 0`, `-postgresql-backup-.+"}[6h])) > 0`), pgBackupFailedShape, "expr is"},
		{"stale-window-moved", both, mut(rulesRel, `[13h])) or vector(0)) < 1`, `[25h])) or vector(0)) < 1`), pgBackupStaleWindowMatchesTheTTL, "want 90000 s"},
		{"ttl-moved-alone", both, mut(pgBackupValuesRel, "    ttlSecondsAfterFinished: 43200\n", "    ttlSecondsAfterFinished: 3600\n"), pgBackupStaleWindowMatchesTheTTL, "want 90000 s"},
		{"stale-loses-the-absent-form", both, mut(rulesRel, ` or vector(0)) < 1`, `) < 1`), pgBackupStaleWindowMatchesTheTTL, "absent-data form"},
		{"stale-paged-critical", rules, mut(rulesRel, "or vector(0)) < 1\n        labels:\n          severity: warning", "or vector(0)) < 1\n        labels:\n          severity: critical"), pgBackupAlertsRouted, "labels are"},
	})
}
