package routing

import (
	"strings"
	"testing"
)

// -recorded-by is echoed into one-line structured stderr events. A control or
// line-separator character would let it forge a second event line, so EVERY
// write verb that calls requireProvenance must refuse it, before anything else
// (no database, no credential) is consulted.
func TestEveryWriteVerbRefusesAControlCharacterInRecordedBy(t *testing.T) {
	t.Setenv(bearerEnvVar, "")
	t.Setenv("POSTGRES_URI", "")
	forged := "lane\ngo_api_routing.seeded operation=forged mode_after=primary"
	for name, argv := range map[string][]string{
		"seed":              {"seed", "-all-unrouted"},
		"enable":            {"enable", "-mode", "canary"},
		"repoint":           {"repoint"},
		"disable -apply":    {"disable", "-mode", "python", "-apply", "-postgres-uri", "postgres://x"},
		"proof-org add":     {"proof-org", "add", "-org", "00000000-0000-4000-8000-000000000001"},
		"proof-org remove":  {"proof-org", "remove", "-org", "00000000-0000-4000-8000-000000000001"},
		"seed carriage ret": {"seed", "-all-unrouted"},
	} {
		recordedBy := forged
		if strings.Contains(name, "carriage") {
			recordedBy = "lane\rforged"
		}
		t.Run(name, func(t *testing.T) {
			args := append(append([]string(nil), argv...), "-recorded-by", recordedBy, "-review-evidence", "why")
			err := run(args)
			if err == nil || !strings.Contains(err.Error(), "control or line-separator") {
				t.Fatalf("run(%v) = %v, want the control-character refusal", args, err)
			}
		})
	}
}
