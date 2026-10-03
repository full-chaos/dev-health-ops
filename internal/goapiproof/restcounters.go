package goapiproof

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// This file is the COUNTER half of the REST proof (CHAOS-8182, split from
// CHAOS-6645): the same request goes to both planes and the per-route
// Prometheus counters each plane emits are compared. A route ported from
// Python to Go keeps its body (that is what the body comparison proves) but
// can silently lose the counter the Python route emitted: the Go api had no
// metrics registry at all when the first case was found (a capped Jira enable
// answers 403 on both planes, the Python counter goes to 1.0 and Go exposes
// nothing). A counter pair declared on a corpus request turns that silent
// loss into a failed run.
//
// The comparison never reads a hand-written expectation. Both numbers are
// OBSERVED: each plane's /metrics text is scraped before and after its own
// leg of the request, the per-series delta is taken, and the two deltas must
// agree series by series after the declared renames. The Python side is the
// real producer (prometheus_client / the instrumentator), the Go side is the
// real registry; the parser below reads their exposition text as it is.

// RESTCounterPair declares that a request fires one counter family on the
// baseline (Python) plane and the equivalent family on the candidate (Go)
// plane, and how their label sets line up.
type RESTCounterPair struct {
	// Name is the operator-facing name of the pair in output; never empty.
	Name string
	// BaselineFamily and CandidateFamily are the exact SAMPLE names in the two
	// exposition texts (for a counter that is the name with its _total suffix,
	// as each plane writes it). Never empty.
	BaselineFamily  string
	CandidateFamily string
	// LabelMap renames baseline label names to candidate label names
	// (for example the Python instrumentator's handler -> the Go route label).
	// A baseline label not in the map keeps its name.
	LabelMap map[string]string
	// IgnoreBaselineLabels / IgnoreCandidateLabels drop a label from every
	// series before the comparison: a label one plane carries and the other
	// has no counterpart for (for example the Go listener label).
	IgnoreBaselineLabels  []string
	IgnoreCandidateLabels []string
	// Select restricts BOTH planes to the series whose (renamed, baseline-side)
	// labels equal these values, so a route-wide counter is compared for the
	// one route the request exercises and unrelated traffic on other routes
	// cannot move the result. Keys are CANDIDATE-side label names.
	Select map[string]string
}

// CounterSample is one parsed exposition sample.
type CounterSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// RESTRefusalCounterParity is the named refusal reason of a request whose two
// planes did not move the declared counters the same way. It is NOT a no-data
// refusal: the run fails.
const RESTRefusalCounterParity = "rest_counter_parity_mismatch"

// ValidateRESTCounterPairs refuses a malformed declaration, so a pair that
// could prove nothing is caught by the corpus validation, not by a run.
func ValidateRESTCounterPairs(pairs []RESTCounterPair) error {
	seen := map[string]bool{}
	for i, p := range pairs {
		switch {
		case strings.TrimSpace(p.Name) == "":
			return fmt.Errorf("counter pair %d has no Name", i)
		case seen[p.Name]:
			return fmt.Errorf("counter pair %q is declared twice", p.Name)
		case strings.TrimSpace(p.BaselineFamily) == "" || strings.TrimSpace(p.CandidateFamily) == "":
			return fmt.Errorf("counter pair %q must name both a baseline and a candidate family", p.Name)
		}
		seen[p.Name] = true
		for from, to := range p.LabelMap {
			if from == "" || to == "" {
				return fmt.Errorf("counter pair %q has an empty label rename", p.Name)
			}
		}
		for key := range p.Select {
			if key == "" {
				return fmt.Errorf("counter pair %q has an empty Select key", p.Name)
			}
		}
	}
	return nil
}

// ParseCounterSamples reads Prometheus text exposition and returns the samples
// whose name is in names. Comments, TYPE/HELP lines and every other sample are
// skipped; a malformed line of a WANTED sample is an error (a scrape that
// cannot be read must fail the case, not read as zero).
func ParseCounterSamples(text string, names ...string) ([]CounterSample, error) {
	want := make(map[string]bool, len(names))
	for _, n := range names {
		want[n] = true
	}
	var out []CounterSample
	for lineNo, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || line[0] == '#' {
			continue
		}
		nameEnd := strings.IndexAny(line, "{ ")
		if nameEnd <= 0 {
			continue
		}
		name := line[:nameEnd]
		if !want[name] {
			continue
		}
		sample, err := parseSampleLine(name, line[nameEnd:])
		if err != nil {
			return nil, fmt.Errorf("line %d (%s): %w", lineNo+1, name, err)
		}
		out = append(out, sample)
	}
	return out, nil
}

func parseSampleLine(name, rest string) (CounterSample, error) {
	labels := map[string]string{}
	if strings.HasPrefix(rest, "{") {
		i := 1
		for {
			for i < len(rest) && (rest[i] == ' ' || rest[i] == ',') {
				i++
			}
			if i >= len(rest) {
				return CounterSample{}, fmt.Errorf("unterminated label set")
			}
			if rest[i] == '}' {
				i++
				break
			}
			eq := strings.IndexByte(rest[i:], '=')
			if eq <= 0 {
				return CounterSample{}, fmt.Errorf("label without =")
			}
			key := strings.TrimSpace(rest[i : i+eq])
			i += eq + 1
			if i >= len(rest) || rest[i] != '"' {
				return CounterSample{}, fmt.Errorf("label %q value is not quoted", key)
			}
			i++
			var val strings.Builder
			closed := false
			for i < len(rest) {
				c := rest[i]
				if c == '\\' && i+1 < len(rest) {
					switch rest[i+1] {
					case 'n':
						val.WriteByte('\n')
					case '"', '\\':
						val.WriteByte(rest[i+1])
					default:
						val.WriteByte(rest[i+1])
					}
					i += 2
					continue
				}
				if c == '"' {
					closed = true
					i++
					break
				}
				val.WriteByte(c)
				i++
			}
			if !closed {
				return CounterSample{}, fmt.Errorf("label %q value is unterminated", key)
			}
			labels[key] = val.String()
		}
		rest = rest[i:]
	}
	fields := strings.Fields(rest)
	if len(fields) < 1 {
		return CounterSample{}, fmt.Errorf("no value")
	}
	value, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return CounterSample{}, fmt.Errorf("value %q: %w", fields[0], err)
	}
	return CounterSample{Name: name, Labels: labels, Value: value}, nil
}

// seriesKey is a canonical string of a label set.
func seriesKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(labels[k]))
	}
	return b.String()
}

// normalise applies renames, ignores and Select to one plane's samples and
// returns value by canonical series key. side is "baseline" or "candidate".
func normaliseCounterSamples(samples []CounterSample, pair RESTCounterPair, side string) map[string]float64 {
	ignore := map[string]bool{}
	renames := map[string]string{}
	if side == "baseline" {
		for _, l := range pair.IgnoreBaselineLabels {
			ignore[l] = true
		}
		renames = pair.LabelMap
	} else {
		for _, l := range pair.IgnoreCandidateLabels {
			ignore[l] = true
		}
	}
	out := map[string]float64{}
	for _, s := range samples {
		labels := map[string]string{}
		for k, v := range s.Labels {
			if ignore[k] {
				continue
			}
			if to, ok := renames[k]; ok {
				k = to
			}
			labels[k] = v
		}
		keep := true
		for k, v := range pair.Select {
			if labels[k] != v {
				keep = false
				break
			}
		}
		if !keep {
			continue
		}
		out[seriesKey(labels)] += s.Value
	}
	return out
}

// CounterDeltas is the per-series increase between two scrapes of one plane.
// A series absent from before counts from zero. A series whose value went DOWN
// (a process restart between the scrapes) is reported as ok=false: a restart
// inside a leg makes the delta meaningless and the case must be re-run.
func CounterDeltas(before, after map[string]float64) (deltas map[string]float64, ok bool) {
	deltas = map[string]float64{}
	ok = true
	for key, a := range after {
		d := a - before[key]
		if d < 0 {
			ok = false
			continue
		}
		if d != 0 {
			deltas[key] = d
		}
	}
	for key, b := range before {
		if _, present := after[key]; !present && b != 0 {
			ok = false
		}
	}
	return deltas, ok
}

// CounterFinding is one disagreement (or refusal-grade condition) of a pair.
type CounterFinding struct {
	Pair      string
	Kind      string
	Series    string
	Baseline  float64
	Candidate float64
}

// String is operator-readable and carries counts and label sets only.
func (f CounterFinding) String() string {
	return fmt.Sprintf("%s: %s series {%s} baseline=%s candidate=%s", f.Pair, f.Kind, f.Series, trimFloat(f.Baseline), trimFloat(f.Candidate))
}

func trimFloat(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

// Finding kinds.
const (
	CounterKindVacuous       = "vacuous_no_counter_moved_on_the_baseline"
	CounterKindMissingOnGo   = "baseline_moved_candidate_did_not"
	CounterKindDeltaDiffers  = "delta_differs"
	CounterKindCandidateOnly = "candidate_moved_baseline_did_not"
	CounterKindScrapeReset   = "a_counter_went_down_between_scrapes"
)

// CompareCounterPair compares one pair from the four scrapes (baseline before
// and after ITS leg, candidate before and after ITS leg). The result is empty
// when the two planes moved the counter the same way and the baseline moved it
// at all; a comparison of two silent planes proves nothing and is a finding.
func CompareCounterPair(pair RESTCounterPair, baselineBefore, baselineAfter, candidateBefore, candidateAfter []CounterSample) []CounterFinding {
	bBefore := normaliseCounterSamples(baselineBefore, pair, "baseline")
	bAfter := normaliseCounterSamples(baselineAfter, pair, "baseline")
	cBefore := normaliseCounterSamples(candidateBefore, pair, "candidate")
	cAfter := normaliseCounterSamples(candidateAfter, pair, "candidate")
	bDelta, bOK := CounterDeltas(bBefore, bAfter)
	cDelta, cOK := CounterDeltas(cBefore, cAfter)
	var findings []CounterFinding
	if !bOK || !cOK {
		findings = append(findings, CounterFinding{Pair: pair.Name, Kind: CounterKindScrapeReset})
	}
	if len(bDelta) == 0 {
		findings = append(findings, CounterFinding{Pair: pair.Name, Kind: CounterKindVacuous})
	}
	keys := map[string]bool{}
	for k := range bDelta {
		keys[k] = true
	}
	for k := range cDelta {
		keys[k] = true
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)
	for _, k := range ordered {
		b, bHas := bDelta[k]
		c, cHas := cDelta[k]
		switch {
		case bHas && !cHas:
			findings = append(findings, CounterFinding{Pair: pair.Name, Kind: CounterKindMissingOnGo, Series: k, Baseline: b})
		case !bHas && cHas:
			findings = append(findings, CounterFinding{Pair: pair.Name, Kind: CounterKindCandidateOnly, Series: k, Candidate: c})
		case math.Abs(b-c) > 1e-9:
			findings = append(findings, CounterFinding{Pair: pair.Name, Kind: CounterKindDeltaDiffers, Series: k, Baseline: b, Candidate: c})
		}
	}
	return findings
}
