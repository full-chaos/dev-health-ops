package migrationmatrix

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// SchemaDigestPin reads contracts/graphql/v1/schema-digest.json -- the pinned canonical schema digest.
func SchemaDigestPin(path string) (string, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // caller-supplied repo path
	if err != nil {
		return "", fmt.Errorf("read schema digest pin: %w", err)
	}
	var pin struct {
		SchemaDigest string `json:"schema_digest"`
	}
	if err := json.Unmarshal(raw, &pin); err != nil {
		return "", fmt.Errorf("parse %s: %w", path, err)
	}
	if !digestRe.MatchString(pin.SchemaDigest) {
		return "", fmt.Errorf("%s: schema_digest %q is not sha256:<64 hex>", path, pin.SchemaDigest)
	}
	return pin.SchemaDigest, nil
}

// FleetReading is what one `docker inspect` sweep of the running fleet saw.
type FleetReading struct {
	// Revision is the single revision every inspected container agreed on,
	// or UnknownRevision. A DISAGREEMENT is not averaged away -- see
	// Disagreement.
	Revision string
	// PerContainer maps container name -> label value, verbatim.
	PerContainer map[string]string
	// Disagreement is set when the inspected containers do not all carry
	// the same revision. A split fleet is a real and dangerous state (the
	// migrate-ahead-of-workers lockstep failure, CHAOS-5437/5457), so it is
	// surfaced rather than collapsed.
	Disagreement string
	ReadAt       time.Time
	Source       string
}

// ReadFleetRevisions runs `docker inspect` over the named containers and
// reads org.opencontainers.image.revision from each.
//
// On a local compose build this returns "unknown", because compose never
// passes the COMMIT build-arg the Dockerfiles' LABEL interpolates. That is
// not a bug in this function and must not be worked around: "unknown" is the
// true answer to "what commit is the running fleet?", and a matrix that
// substituted a guess would be exactly the tracker that could say "done".
func ReadFleetRevisions(ctx context.Context, containers []string) (*FleetReading, error) {
	if len(containers) == 0 {
		return nil, fmt.Errorf("no containers named")
	}
	reading := &FleetReading{
		PerContainer: map[string]string{},
		ReadAt:       time.Now().UTC(),
		Source:       "docker inspect " + strings.Join(containers, " "),
	}
	args := append([]string{"inspect", "-f", `{{index .Config.Labels "org.opencontainers.image.revision"}}`}, containers...)
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker inspect: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != len(containers) {
		return nil, fmt.Errorf("docker inspect returned %d lines for %d containers", len(lines), len(containers))
	}
	distinct := map[string]bool{}
	for i, name := range containers {
		value := strings.TrimSpace(lines[i])
		if value == "" || value == "<no value>" {
			value = UnknownRevision
		}
		reading.PerContainer[name] = value
		distinct[value] = true
	}
	if len(distinct) == 1 {
		for value := range distinct {
			reading.Revision = value
		}
		return reading, nil
	}
	values := make([]string, 0, len(distinct))
	for value := range distinct {
		values = append(values, value)
	}
	sort.Strings(values)
	reading.Revision = UnknownRevision
	reading.Disagreement = fmt.Sprintf("the fleet is split across %d revisions: %s", len(values), strings.Join(values, ", "))
	return reading, nil
}
