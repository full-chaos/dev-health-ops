package ingressplanes

import (
	"encoding/xml"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

const (
	systemLogsFileSource = "./docker/clickhouse-config.d/system-logs.xml"
	systemLogsFileTarget = "/etc/clickhouse-server/config.d/system-logs.xml"
)

// systemLogDays is the retention, in days, each system log table of the local
// stack's ClickHouse must have: text_log and trace_log grow fastest, query_log
// stays long enough for diagnosis reads of system.query_log.
var systemLogDays = map[string]int{
	"text_log":                     3,
	"trace_log":                    3,
	"query_log":                    14,
	"query_thread_log":             7,
	"query_views_log":              7,
	"metric_log":                   7,
	"asynchronous_metric_log":      7,
	"part_log":                     7,
	"processors_profile_log":       7,
	"session_log":                  7,
	"crash_log":                    7,
	"error_log":                    7,
	"asynchronous_insert_log":      7,
	"background_schedule_pool_log": 7,
}

type systemLogConfig struct {
	Tables []struct {
		XMLName  xml.Name
		TTL      string `xml:"ttl"`
		Level    string `xml:"level"`
		Settings struct {
			Inner string `xml:",innerxml"`
		} `xml:"settings"`
	} `xml:",any"`
}

var systemLogTTL = regexp.MustCompile(`^event_date \+ INTERVAL (\d+) DAY DELETE$`)

func systemLogsFindings(text []byte) ([]string, error) {
	var config systemLogConfig
	if err := xml.Unmarshal(text, &config); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	var findings []string
	seen := map[string]bool{}
	for _, table := range config.Tables {
		name := table.XMLName.Local
		seen[name] = true
		want, listed := systemLogDays[name]
		if !listed {
			findings = append(findings, fmt.Sprintf("%s: not a table this guard knows; add it with its retention", name))
			continue
		}
		match := systemLogTTL.FindStringSubmatch(strings.TrimSpace(table.TTL))
		if match == nil || match[1] != fmt.Sprint(want) {
			findings = append(findings, fmt.Sprintf("%s: ttl %q, want event_date + INTERVAL %d DAY DELETE", name, table.TTL, want))
		}
		if !strings.Contains(table.Settings.Inner, "ttl_only_drop_parts") {
			findings = append(findings, fmt.Sprintf("%s: no ttl_only_drop_parts setting: expired rows would rewrite whole parts", name))
		}
		if name == "text_log" && table.Level != "warning" && table.Level != "information" {
			findings = append(findings, fmt.Sprintf("text_log: level %q, want warning or information", table.Level))
		}
	}
	for name := range systemLogDays {
		if !seen[name] {
			findings = append(findings, fmt.Sprintf("%s: no entry, the table has no ttl", name))
		}
	}
	return findings, nil
}

func TestComposeSystemLogsFileHoldsEveryTTL(t *testing.T) {
	text, err := os.ReadFile(repoFile(t, strings.TrimPrefix(systemLogsFileSource, "./")))
	if err != nil {
		t.Fatalf("read the system logs file compose mounts: %v", err)
	}
	findings, err := systemLogsFindings(text)
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range findings {
		t.Errorf("%s: %s", systemLogsFileSource, finding)
	}
}

func TestSystemLogsCheckSeesEachDefect(t *testing.T) {
	good := `<clickhouse><text_log><level>warning</level><ttl>event_date + INTERVAL 3 DAY DELETE</ttl><settings>ttl_only_drop_parts=1</settings></text_log></clickhouse>`
	findings, err := systemLogsFindings([]byte(good))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(findings, "\n"), "query_log: no entry") {
		t.Fatalf("a file without query_log passed: %v", findings)
	}
	for name, plant := range map[string]struct{ old, new, want string }{
		"trace level":   {"<level>warning</level>", "<level>trace</level>", `text_log: level "trace"`},
		"no ttl":        {"<ttl>event_date + INTERVAL 3 DAY DELETE</ttl>", "", `text_log: ttl ""`},
		"longer ttl":    {"INTERVAL 3 DAY", "INTERVAL 30 DAY", "text_log: ttl"},
		"no drop parts": {"<settings>ttl_only_drop_parts=1</settings>", "", "no ttl_only_drop_parts"},
		"unknown table": {"</clickhouse>", "<foo_log/></clickhouse>", "foo_log: not a table"},
	} {
		changed := strings.Replace(good, plant.old, plant.new, 1)
		if changed == good {
			t.Fatalf("%s: the plant changed nothing", name)
		}
		got, err := systemLogsFindings([]byte(changed))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(strings.Join(got, "\n"), plant.want) {
			t.Errorf("%s: findings %v lack %q", name, got, plant.want)
		}
	}
}
