package ingressplanes

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/bigboydir"
)

// bigboy runs its own compose file, so its clickhouse probe needs the same native-port rule as compose.yml.
func bigboyClickHouseProbeFindings(text []byte) ([]string, error) {
	var file struct {
		Services map[string]yaml.Node `yaml:"services"`
	}
	if err := yaml.Unmarshal(text, &file); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	node, ok := file.Services[clickHouseService]
	if !ok {
		return []string{"no clickhouse service"}, nil
	}
	var service struct {
		Environment []string `yaml:"environment"`
		Healthcheck *struct {
			Test []string `yaml:"test"`
		} `yaml:"healthcheck"`
	}
	if err := node.Decode(&service); err != nil {
		return nil, fmt.Errorf("parse the clickhouse service: %w", err)
	}
	var test []string
	if service.Healthcheck != nil {
		test = service.Healthcheck.Test
	}
	findings := clickHouseProbeFindings(test)
	for _, name := range []string{"CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"} {
		want := name + "=${" + name + ":-"
		set := false
		for _, entry := range service.Environment {
			set = set || strings.HasPrefix(entry, want)
		}
		if !set {
			findings = append(findings, fmt.Sprintf("clickhouse health: the clickhouse environment has no %s...} entry, so the probe's clickhouse-client has no login", want))
		}
	}
	return findings, nil
}

func readBigboyCompose(t *testing.T) []byte {
	t.Helper()
	text, err := os.ReadFile(bigboydir.Path(t, repoFile(t, "."), "compose.bigboy.yml"))
	if err != nil {
		t.Fatalf("read %s: %v", bigboyComposePath, err)
	}
	return text
}

func TestBigboyComposeProbesClickHouseOnTheNativePort(t *testing.T) {
	findings, err := bigboyClickHouseProbeFindings(readBigboyCompose(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range findings {
		t.Errorf("%s: %s", bigboyComposePath, finding)
	}
}

func TestBigboyClickHouseProbeCheckSeesEachDefect(t *testing.T) {
	clean := string(readBigboyCompose(t))
	const (
		probeLine = `      - h=$$(hostname -i | cut -d' ' -f1) && [ -n "$$h" ] && clickhouse-client --host "$$h" --query 'SELECT 1'` + "\n"
		hostArg   = `--host "$$h" `
	)
	if findings, err := bigboyClickHouseProbeFindings([]byte(clean)); err != nil || len(findings) != 0 {
		t.Fatalf("checked-in file: findings %v, err %v", findings, err)
	}
	for _, plant := range []struct{ name, old, new, want string }{
		{
			name: "the HTTP ping again",
			old:  "      - CMD-SHELL\n" + probeLine,
			new:  "      - CMD\n      - wget\n      - --spider\n      - -q\n      - http://localhost:8123/ping\n",
			want: "clickhouse health: the probe is \"\"",
		},
		{
			name: "a CMD probe of the same command",
			old:  "      - CMD-SHELL\n" + probeLine,
			new:  "      - CMD\n" + probeLine,
			want: "clickhouse health: the probe is \"\"",
		},
		{
			name: "the HTTP port",
			old:  "--query 'SELECT 1'",
			new:  "--port 8123 --query 'SELECT 1'",
			want: "clickhouse health: the probe uses the HTTP port 8123",
		},
		{name: "host 127.1", old: hostArg, new: "--host 127.1 ", want: "clickhouse health: the probe host 127.1 is loopback"},
		{name: "host localhost", old: hostArg, new: "--host localhost ", want: "clickhouse health: the probe host localhost is loopback"},
		{name: "host ::1", old: hostArg, new: "--host ::1 ", want: "clickhouse health: the probe host ::1 is loopback"},
		{name: "host 0.0.0.0", old: hostArg, new: "--host 0.0.0.0 ", want: "clickhouse health: the probe host 0.0.0.0 is loopback"},
		{name: "a fixed host", old: hostArg, new: "--host 10.1.2.3 ", want: "clickhouse health: the probe host is 10.1.2.3, want"},
		{name: "no host", old: hostArg, new: "", want: "clickhouse health: the probe has no --host"},
		{name: "--password", old: hostArg, new: hostArg + "--password hunter2 ", want: "clickhouse health: the probe carries a credential (\"--password\")"},
		{name: "--user", old: hostArg, new: hostArg + "--user ch ", want: "clickhouse health: the probe carries a credential (\"--user\")"},
		{name: "-p", old: hostArg, new: hostArg + "-p hunter2 ", want: "clickhouse health: the probe carries a credential (\"-p\")"},
		{
			name: "the password variable on the command line",
			old:  hostArg,
			new:  hostArg + "--password=\"$$CLICKHOUSE_PASSWORD\" ",
			want: "clickhouse health: the probe carries a credential",
		},
		{
			name: "the host from nowhere",
			old:  `h=$$(hostname -i | cut -d' ' -f1) && [ -n "$$h" ] && `,
			new:  "h=10.1.2.3 && ",
			want: "clickhouse health: the probe does not take its host from hostname -i",
		},
		{
			name: "no healthcheck",
			old:  "      test:\n      - CMD-SHELL\n" + probeLine,
			new:  "",
			want: "clickhouse health: the probe is \"\"",
		},
		{
			name: "no login user",
			old:  "    - CLICKHOUSE_USER=${CLICKHOUSE_USER:-ch}\n",
			new:  "",
			want: "no CLICKHOUSE_USER=${CLICKHOUSE_USER:-...} entry",
		},
		{
			name: "no login password",
			old:  "    - CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-ch}\n",
			new:  "",
			want: "no CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-...} entry",
		},
	} {
		t.Run(plant.name, func(t *testing.T) {
			if strings.Count(clean, plant.old) != 1 {
				t.Fatalf("the plant's old text is in %s %d times, want once: update the plant to the file's text", bigboyComposePath, strings.Count(clean, plant.old))
			}
			got, err := bigboyClickHouseProbeFindings([]byte(strings.Replace(clean, plant.old, plant.new, 1)))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(got, "\n"), plant.want) {
				t.Errorf("findings %v lack %q", got, plant.want)
			}
		})
	}
	if got, _ := bigboyClickHouseProbeFindings([]byte("services:\n  web: {}\n")); len(got) != 1 {
		t.Errorf("no clickhouse service: findings %v", got)
	}
}
