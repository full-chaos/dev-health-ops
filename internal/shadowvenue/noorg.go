package shadowvenue

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// unjudgedMetric is the enforcement counter (internal/api/policy/enforcecounters.go); its labels are route ("METHOD
// /route") and reason.
const unjudgedMetric = "dev_health_authz_enforcement_unjudged_total"

// ParseUnjudgedReasons is the per-route, per-reason increase of the unjudged counter between two scrapes. A scrape that
// has no series of the metric has none: the counter is created at its first increment, so a go-api that never counted
// an unjudged request has no block (a body that is no go-api scrape is refused by the caller through the enforcement
// gauge and the uptime it must carry). A series that went down means the process restarted between the scrapes and is
// refused.
func ParseUnjudgedReasons(before, after []byte) (map[string]map[string]int, error) {
	b, err := unjudgedSeries(before)
	if err != nil {
		return nil, fmt.Errorf("before scrape: %w", err)
	}
	a, err := unjudgedSeries(after)
	if err != nil {
		return nil, fmt.Errorf("after scrape: %w", err)
	}
	out := map[string]map[string]int{}
	for route, reasons := range b {
		for reason, was := range reasons {
			if now, ok := a[route][reason]; !ok || now < was {
				return nil, fmt.Errorf("route %s reason %s: the counter went from %d to %d: the process restarted or the series vanished", route, reason, was, a[route][reason])
			}
		}
	}
	for route, reasons := range a {
		out[route] = map[string]int{}
		for reason, now := range reasons {
			out[route][reason] = now - b[route][reason]
		}
	}
	return out, nil
}

func unjudgedSeries(scrape []byte) (map[string]map[string]int, error) {
	out := map[string]map[string]int{}
	scanner := bufio.NewScanner(bytes.NewReader(scrape))
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, unjudgedMetric+"{") {
			continue
		}
		labels, value, ok := strings.Cut(strings.TrimPrefix(line, unjudgedMetric+"{"), "} ")
		if !ok {
			return nil, fmt.Errorf("unreadable series %q", line)
		}
		reason, route := "", ""
		for _, part := range splitLabels(labels) {
			name, val, ok := strings.Cut(part, "=")
			if !ok || len(val) < 2 || val[0] != '"' || val[len(val)-1] != '"' {
				return nil, fmt.Errorf("unreadable label %q", part)
			}
			switch name {
			case "reason":
				reason = val[1 : len(val)-1]
			case "route":
				route = val[1 : len(val)-1]
			}
		}
		n, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil || n < 0 || n != float64(int(n)) {
			return nil, fmt.Errorf("series %s has value %q", route, value)
		}
		if route == "" {
			return nil, errors.New("a no_org series has no route label")
		}
		if out[route] == nil {
			out[route] = map[string]int{}
		}
		out[route][reason] += int(n)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// splitLabels splits a label set at the commas outside quotes.
func splitLabels(s string) []string {
	var parts []string
	inQuote, start := false, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}

// UnjudgedSums is the increase of the unjudged counter over all routes by the reason the verdicts rest on, and every
// other reason together.
type UnjudgedSums struct {
	OrgRequired, Other int
}

// UnjudgedTotals sums the increase of the unjudged counter over all routes: reason org_required (a build that decides a
// token with no organization, CHAOS-10872 S1) and every other reason together, so the record shows the count the
// verdicts rest on on its own line.
func UnjudgedTotals(before, after []byte) (UnjudgedSums, error) {
	all, err := ParseUnjudgedReasons(before, after)
	if err != nil {
		return UnjudgedSums{}, err
	}
	var sums UnjudgedSums
	for _, reasons := range all {
		for reason, n := range reasons {
			if reason == "org_required" {
				sums.OrgRequired += n
			} else {
				sums.Other += n
			}
		}
	}
	return sums, nil
}

type orglessResidualKind int

const (
	residualNone orglessResidualKind = iota
	// residualOrgRequired: exactly n org_required and no other reason (an S1 build, an AdminOrg row).
	residualOrgRequired
	// residualDecided: no reason grew (an S1 build that asked the decision point).
	residualDecided
)

// orglessResidual reads the unjudged growth of one no-org route that ran n no-org superuser slots.
func orglessResidual(got map[string]int, n int) orglessResidualKind {
	grown := map[string]int{}
	for reason, delta := range got {
		if delta != 0 {
			grown[reason] = delta
		}
	}
	switch {
	case len(grown) == 0:
		return residualDecided
	case len(grown) == 1 && grown["org_required"] == n:
		return residualOrgRequired
	}
	return residualNone
}
