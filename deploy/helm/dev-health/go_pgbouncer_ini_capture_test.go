package devhealth_test

import (
	"regexp"
	"strings"
	"testing"
)

// pgbouncerRecord is one pgbouncer log record, e.g. "2026-10-30 15:09:33.995 UTC [1] LOG kernel file descriptor limit".
var pgbouncerRecord = regexp.MustCompile(`^\s*\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)? \S+ \[\d+\] [A-Z]+ `)

// generatedPgbouncerIni returns the config block the pgbouncer image prints at start, cut from the container's
// combined log text (stdout and stderr in one stream). pgbouncer's own log records that land inside the block are
// dropped and runs of blank lines are collapsed, so two captures compare equal whatever each stream interleaved.
func generatedPgbouncerIni(logs string) (string, bool) {
	i, j := strings.Index(logs, "################## Auto generated"), strings.Index(logs, "################## end file")
	if i < 0 || j <= i {
		return "", false
	}
	var kept []string
	for _, line := range strings.Split(logs[i:j], "\n") {
		if pgbouncerRecord.MatchString(line) || (strings.TrimSpace(line) == "" && len(kept) > 0 && strings.TrimSpace(kept[len(kept)-1]) == "") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n"), true
}

const cleanIni = "################## Auto generated ##################\n[databases]\ndevhealth = host=pg port=5432\n[pgbouncer]\nlisten_port = 6432\nadmin_users = devhealth_domain\n\n# Connection sanity checks, timeouts\n\n# TLS settings\n\n"

// The container log carries pgbouncer's own stderr records beside the printed ini; a record that lands inside the
// block must not reach the captured text, and two runs' captures must compare equal whatever time each record bears.
func TestGeneratedPgbouncerIniDropsInterleavedLogRecords(t *testing.T) {
	const record = "2026-10-30 15:09:33.995 UTC [1] LOG kernel file descriptor limit: 1048576 (hard: 1048576); max_client_conn: 1000, max expected fd use: 1012\n"
	clean := cleanIni + "################## end file ##################\n"
	withRecord := strings.Replace(cleanIni, "admin_users = devhealth_domain\n\n", "admin_users = devhealth_domain\n\n"+record+"\n", 1) + "################## end file ##################\n"
	other := strings.Replace(withRecord, "15:09:33.995", "15:11:02.417", 1)

	want, ok := generatedPgbouncerIni(clean)
	if !ok {
		t.Fatal("the clean stream has no ini block")
	}
	for name, stream := range map[string]string{"record inside the block": withRecord, "record with another time": other,
		"records before and after the block": record + clean + record} {
		got, ok := generatedPgbouncerIni(stream)
		if !ok {
			t.Fatalf("%s: no ini block", name)
		}
		if got != want {
			t.Errorf("%s: captured ini differs from the clean capture:\n--- want\n%s\n--- got\n%s", name, want, got)
		}
	}
	if a, _ := generatedPgbouncerIni(withRecord); a == "" {
		t.Fatal("empty capture")
	}
}

func TestGeneratedPgbouncerIniNeedsItsMarkers(t *testing.T) {
	for name, stream := range map[string]string{"empty": "", "no end marker": "################## Auto generated ###\n[x]\n",
		"end before start": "################## end file ###\n################## Auto generated ###\n"} {
		if got, ok := generatedPgbouncerIni(stream); ok {
			t.Errorf("%s: captured %q, want no block", name, got)
		}
	}
}
