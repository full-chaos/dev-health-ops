package ingressplanes

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	bigboyComposePath      = "ci/bigboy/compose.bigboy.yml"
	bigboySystemLogsSource = "./ops/docker/clickhouse-config.d/system-logs.xml"
)

// bigboy runs ci/bigboy/compose.bigboy.yml, not the root compose.yml, so the mount must be in that file too.
func bigboySystemLogsFindings(text []byte) ([]string, error) {
	var file struct {
		Services map[string]struct {
			Volumes []struct {
				Type     string `yaml:"type"`
				Source   string `yaml:"source"`
				Target   string `yaml:"target"`
				ReadOnly bool   `yaml:"read_only"`
			} `yaml:"volumes"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(text, &file); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	service, ok := file.Services[clickHouseService]
	if !ok {
		return []string{"no clickhouse service"}, nil
	}
	var findings []string
	mounted := false
	for _, volume := range service.Volumes {
		if strings.HasSuffix(volume.Target, "/docker_related_config.xml") {
			findings = append(findings, "clickhouse mounts a file over the image's docker_related_config.xml, which makes the server listen on loopback only")
		}
		if volume.Target != systemLogsFileTarget {
			continue
		}
		mounted = true
		if volume.Type != "bind" || volume.Source != bigboySystemLogsSource {
			findings = append(findings, fmt.Sprintf("clickhouse mounts %s (%s) at %s, want the checked-in file %s", volume.Source, volume.Type, systemLogsFileTarget, bigboySystemLogsSource))
		}
		if !volume.ReadOnly {
			findings = append(findings, "the system logs file is mounted read-write")
		}
	}
	if !mounted {
		findings = append(findings, fmt.Sprintf("clickhouse does not mount the system logs file at %s, so the bigboy system log tables have no ttl", systemLogsFileTarget))
	}
	return findings, nil
}

func TestBigboyComposeMountsSystemLogs(t *testing.T) {
	text, err := os.ReadFile(repoFile(t, bigboyComposePath))
	if err != nil {
		t.Fatalf("read %s: %v", bigboyComposePath, err)
	}
	findings, err := bigboySystemLogsFindings(text)
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range findings {
		t.Errorf("%s: %s", bigboyComposePath, finding)
	}
}

func TestBigboySystemLogsCheckSeesEachDefect(t *testing.T) {
	good := "services:\n  clickhouse:\n    volumes:\n    - source: clickhouse_data\n      target: /var/lib/clickhouse\n      type: volume\n" +
		"    - read_only: true\n      source: " + bigboySystemLogsSource + "\n      target: " + systemLogsFileTarget + "\n      type: bind\n"
	if findings, err := bigboySystemLogsFindings([]byte(good)); err != nil || len(findings) != 0 {
		t.Fatalf("good file: findings %v, err %v", findings, err)
	}
	for name, plant := range map[string]struct{ old, new, want string }{
		"not mounted":     {"target: " + systemLogsFileTarget, "target: /etc/other.xml", "does not mount the system logs file"},
		"read-write":      {"read_only: true", "read_only: false", "mounted read-write"},
		"other source":    {"source: " + bigboySystemLogsSource, "source: ./ops/docker/other.xml", "clickhouse mounts ./ops/docker/other.xml"},
		"named volume":    {"type: bind", "type: volume", "(volume)"},
		"loopback config": {"target: /var/lib/clickhouse", "target: /etc/clickhouse-server/config.d/docker_related_config.xml", "docker_related_config.xml"},
	} {
		changed := strings.Replace(good, plant.old, plant.new, 1)
		if changed == good {
			t.Fatalf("%s: the plant changed nothing", name)
		}
		got, err := bigboySystemLogsFindings([]byte(changed))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(strings.Join(got, "\n"), plant.want) {
			t.Errorf("%s: findings %v lack %q", name, got, plant.want)
		}
	}
	if got, _ := bigboySystemLogsFindings([]byte("services:\n  web: {}\n")); len(got) != 1 {
		t.Errorf("no clickhouse service: findings %v", got)
	}
}
