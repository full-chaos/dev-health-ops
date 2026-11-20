package ingressplanes

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	probeHostFlag     = regexp.MustCompile(`--host[ =](\S+)`)
	probeLoopbackHost = regexp.MustCompile(`^"?(localhost|0\.0\.0\.0|::1|127(\.[0-9]+)*)"?$`)
)

// clickHouseProbeFindings checks a clickhouse healthcheck test: a CMD-SHELL clickhouse-client `SELECT 1` on the native
// port, at the container's own non-loopback address, with no credential on the command line. compose.yml and the
// bigboy compose file share it.
func clickHouseProbeFindings(test []string) []string {
	var findings []string
	found := func(format string, args ...any) { findings = append(findings, fmt.Sprintf(format, args...)) }
	probe := ""
	if len(test) == 2 && test[0] == "CMD-SHELL" {
		probe = test[1]
	}
	if !strings.Contains(probe, "clickhouse-client") || !strings.Contains(probe, "SELECT 1") {
		found("clickhouse health: the probe is %q, want a CMD-SHELL clickhouse-client `SELECT 1` on the native port: clients dial 9000, and an HTTP /ping probe passes before 9000 listens", probe)
	}
	if strings.Contains(probe, "8123") {
		found("clickhouse health: the probe uses the HTTP port 8123, want the native port")
	}
	if !strings.Contains(probe, "h=$$(hostname -i") {
		found("clickhouse health: the probe does not take its host from hostname -i: the first-boot temporary server listens on 127.0.0.1 only and would pass a loopback probe")
	}
	if match := probeHostFlag.FindStringSubmatch(probe); match == nil {
		found("clickhouse health: the probe has no --host: the client would dial loopback")
	} else if probeLoopbackHost.MatchString(match[1]) {
		found("clickhouse health: the probe host %s is loopback: the first-boot temporary server listens on 127.0.0.1 only and would pass it", match[1])
	} else if match[1] != `"$$h"` {
		found("clickhouse health: the probe host is %s, want \"$$h\" (the container's own address from hostname -i)", match[1])
	}
	for _, token := range strings.Fields(probe) {
		if token == "-u" || token == "-p" || strings.HasPrefix(token, "--user") || strings.HasPrefix(token, "--password") || strings.Contains(strings.ToUpper(token), "PASSWORD") {
			found("clickhouse health: the probe carries a credential (%q): it would show in the process list; clickhouse-client reads CLICKHOUSE_USER and CLICKHOUSE_PASSWORD from the environment", token)
		}
	}
	return findings
}
