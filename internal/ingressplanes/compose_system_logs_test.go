package ingressplanes

import (
	"encoding/xml"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

const (
	systemLogsFileSource = "./docker/clickhouse-config.d/system-logs.xml"
	systemLogsFileTarget = "/etc/clickhouse-server/config.d/system-logs.xml"
)

// systemLogSpec is what the local stack's ClickHouse must do with one system log table: stay off (the config-merge
// remove attribute stops the server creating and filling it) or stay on with a retention in days.
type systemLogSpec struct {
	off  bool
	days int
	// partition is the partition_by a kept table must carry: with the server default (monthly) a one-day TTL with
	// ttl_only_drop_parts=1 could only drop a part once the whole month part had expired. Empty means none (the default).
	partition string
}

// systemLogSpecs: the bigboy compose ClickHouse filled 82 GB in system log tables (processors_profile_log, text_log,
// query_log, trace_log). Every log table the image can create is listed. query_log and error_log stay, for one day: diagnosis
// and `dho audit perf` read system.query_log. crash_log is tiny and keeps its week.
var systemLogSpecs = map[string]systemLogSpec{
	"query_log":                    {days: 1, partition: dayPartition},
	"error_log":                    {days: 1, partition: dayPartition},
	"crash_log":                    {days: 7},
	"processors_profile_log":       {off: true},
	"text_log":                     {off: true},
	"trace_log":                    {off: true},
	"asynchronous_metric_log":      {off: true},
	"metric_log":                   {off: true},
	"query_metric_log":             {off: true},
	"query_thread_log":             {off: true},
	"query_views_log":              {off: true},
	"part_log":                     {off: true},
	"session_log":                  {off: true},
	"asynchronous_insert_log":      {off: true},
	"background_schedule_pool_log": {off: true},
	"opentelemetry_span_log":       {off: true},
	"zookeeper_log":                {off: true},
	"aggregated_zookeeper_log":     {off: true},
	"backup_log":                   {off: true},
	"s3queue_log":                  {off: true},
	"blob_storage_log":             {off: true},
	"transactions_info_log":        {off: true},
	"instrumentation_trace_log":    {off: true},
	"iceberg_metadata_log":         {off: true},
	"delta_lake_metadata_log":      {off: true},
	"zookeeper_connection_log":     {off: true},
}

const dayPartition = "toYYYYMMDD(event_date)"

type systemLogConfig struct {
	Tables []struct {
		XMLName  xml.Name
		Attrs    []xml.Attr `xml:",any,attr"`
		TTL      string     `xml:"ttl"`
		Level    string     `xml:"level"`
		Part     string     `xml:"partition_by"`
		Settings struct {
			Inner string `xml:",innerxml"`
		} `xml:"settings"`
	} `xml:",any"`
}

var systemLogTTL = regexp.MustCompile(`^event_date \+ INTERVAL (\d+) DAY DELETE$`)

// removeAttr returns the value of the remove attribute and whether it is present.
func removeAttr(attrs []xml.Attr) (string, bool) {
	for _, a := range attrs {
		if a.Name.Local == "remove" {
			return a.Value, true
		}
	}
	return "", false
}

func systemLogsFindings(text []byte) ([]string, error) {
	var config systemLogConfig
	if err := xml.Unmarshal(text, &config); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	var findings []string
	seen := map[string]bool{}
	for _, table := range config.Tables {
		name := table.XMLName.Local
		if seen[name] {
			findings = append(findings, fmt.Sprintf("%s: listed twice", name))
		}
		seen[name] = true
		if name == "logger" {
			if table.Level != "warning" {
				findings = append(findings, fmt.Sprintf("logger: level %q, want warning", table.Level))
			}
			continue
		}
		want, listed := systemLogSpecs[name]
		if !listed {
			findings = append(findings, fmt.Sprintf("%s: not a table this guard knows; add it, off or with its retention", name))
			continue
		}
		if want.off {
			if value, present := removeAttr(table.Attrs); !present {
				findings = append(findings, fmt.Sprintf("%s: no remove attribute, the table must stay off", name))
			} else if value != "1" {
				findings = append(findings, fmt.Sprintf("%s: remove=%q, want remove=\"1\"", name, value))
			}
			if strings.TrimSpace(table.TTL) != "" || strings.TrimSpace(table.Settings.Inner) != "" {
				findings = append(findings, fmt.Sprintf("%s: off but carries a ttl or settings", name))
			}
			continue
		}
		if _, present := removeAttr(table.Attrs); present {
			findings = append(findings, fmt.Sprintf("%s: must stay on (diagnosis reads it), it carries a remove attribute", name))
		}
		if strings.TrimSpace(table.Part) != want.partition {
			findings = append(findings, fmt.Sprintf("%s: partition_by %q, want %q", name, table.Part, want.partition))
		}
		match := systemLogTTL.FindStringSubmatch(strings.TrimSpace(table.TTL))
		if match == nil || match[1] != fmt.Sprint(want.days) {
			findings = append(findings, fmt.Sprintf("%s: ttl %q, want event_date + INTERVAL %d DAY DELETE", name, table.TTL, want.days))
		}
		if !strings.Contains(table.Settings.Inner, "ttl_only_drop_parts") {
			findings = append(findings, fmt.Sprintf("%s: no ttl_only_drop_parts setting: expired rows would rewrite whole parts", name))
		}
	}
	if !seen["logger"] {
		findings = append(findings, "logger: no entry, the server logs at its default level")
	}
	for name := range systemLogSpecs {
		if !seen[name] {
			findings = append(findings, fmt.Sprintf("%s: no entry, the table has no retention and is not off", name))
		}
	}
	return findings, nil
}

func TestComposeSystemLogsFileMatchesTheRetentionTable(t *testing.T) {
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

// goodSystemLogs renders a file that satisfies systemLogSpecs, in table-name order.
func goodSystemLogs() string {
	names := make([]string, 0, len(systemLogSpecs))
	for name := range systemLogSpecs {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString("<clickhouse><logger><level>warning</level></logger>")
	for _, name := range names {
		spec := systemLogSpecs[name]
		if spec.off {
			fmt.Fprintf(&b, `<%s remove="1"/>`, name)
			continue
		}
		part := ""
		if spec.partition != "" {
			part = "<partition_by>" + spec.partition + "</partition_by>"
		}
		fmt.Fprintf(&b, `<%s>%s<ttl>event_date + INTERVAL %d DAY DELETE</ttl><settings>ttl_only_drop_parts=1</settings></%s>`, name, part, spec.days, name)
	}
	b.WriteString("</clickhouse>")
	return b.String()
}

func TestSystemLogsCheckSeesEachDefect(t *testing.T) {
	good := goodSystemLogs()
	findings, err := systemLogsFindings([]byte(good))
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("the generated good file has findings: %v", findings)
	}
	empty, err := systemLogsFindings([]byte("<clickhouse></clickhouse>"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(empty, "\n"), "query_log: no entry") || !strings.Contains(strings.Join(empty, "\n"), "logger: no entry") {
		t.Fatalf("an empty file passed: %v", empty)
	}
	query := `<query_log><partition_by>toYYYYMMDD(event_date)</partition_by><ttl>event_date + INTERVAL 1 DAY DELETE</ttl><settings>ttl_only_drop_parts=1</settings></query_log>`
	for name, plant := range map[string]struct{ old, new, want string }{
		"an off table written on": {`<text_log remove="1"/>`, `<text_log><ttl>event_date + INTERVAL 3 DAY DELETE</ttl><settings>ttl_only_drop_parts=1</settings></text_log>`, "text_log: no remove attribute"},
		"remove missing":          {`<trace_log remove="1"/>`, `<trace_log/>`, "trace_log: no remove attribute"},
		"off with a ttl":          {`<part_log remove="1"/>`, `<part_log remove="1"><ttl>event_date + INTERVAL 1 DAY DELETE</ttl></part_log>`, "part_log: off but carries"},
		"query_log longer":        {query, strings.Replace(query, "1 DAY", "14 DAY", 1), "query_log: ttl"},
		"query_log turned off":    {query, `<query_log remove="1"/>`, "query_log: must stay on"},
		"query_log no drop parts": {query, strings.Replace(query, "<settings>ttl_only_drop_parts=1</settings>", "", 1), "query_log: no ttl_only_drop_parts"},
		"error_log missing":       {strings.ReplaceAll(query, "query_log", "error_log"), "", "error_log: no entry"},
		"monthly partition":       {"<partition_by>toYYYYMMDD(event_date)</partition_by><ttl>event_date + INTERVAL 1 DAY DELETE</ttl><settings>ttl_only_drop_parts=1</settings></query_log>", "<ttl>event_date + INTERVAL 1 DAY DELETE</ttl><settings>ttl_only_drop_parts=1</settings></query_log>", "query_log: partition_by"},
		"wrong partition":         {"<partition_by>toYYYYMMDD(event_date)</partition_by><ttl>event_date + INTERVAL 1 DAY DELETE</ttl><settings>ttl_only_drop_parts=1</settings></error_log>", "<partition_by>toYYYYMM(event_date)</partition_by><ttl>event_date + INTERVAL 1 DAY DELETE</ttl><settings>ttl_only_drop_parts=1</settings></error_log>", "error_log: partition_by"},
		"crash_log partitioned":   {"<crash_log><ttl>", "<crash_log><partition_by>toYYYYMMDD(event_date)</partition_by><ttl>", "crash_log: partition_by"},
		"remove zero":             {`<iceberg_metadata_log remove="1"/>`, `<iceberg_metadata_log remove="0"/>`, `iceberg_metadata_log: remove="0"`},
		"remove other word":       {`<zookeeper_connection_log remove="1"/>`, `<zookeeper_connection_log remove="remove"/>`, `zookeeper_connection_log: remove="remove"`},
		"trace level":             {"<level>warning</level>", "<level>trace</level>", `logger: level "trace"`},
		"no logger":               {"<logger><level>warning</level></logger>", "", "logger: no entry"},
		"listed twice":            {"</clickhouse>", `<metric_log remove="1"/></clickhouse>`, "metric_log: listed twice"},
		"unknown table":           {"</clickhouse>", "<foo_log/></clickhouse>", "foo_log: not a table"},
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
