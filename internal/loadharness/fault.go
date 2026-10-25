package loadharness

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
)

// Bucket is one second of the run, bucketed by scheduled send time, over all
// paths.
type Bucket struct {
	Second int     `json:"second"`
	Count  int     `json:"count"`
	Errors int     `json:"errors"`
	P95Ms  float64 `json:"p95_ms"`
}

// FaultResult is the observed effect of a fault window the operator toggles
// outside the harness. Offsets are from the start of the run.
type FaultResult struct {
	Name            string `json:"name,omitempty"`
	OnMs            int64  `json:"on_ms"`
	OffMs           int64  `json:"off_ms"`
	TimeToDegradeMs *int64 `json:"time_to_degrade_ms"`
	Recovered       bool   `json:"recovered"`
	TimeToRecoverMs *int64 `json:"time_to_recover_ms"`
}

// AssertionCheck is one evaluated status or recovery assertion.
type AssertionCheck struct {
	Path   string `json:"path"`
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

type bucketAcc struct {
	lat     []time.Duration
	errors  int
	dropped int
}

func buildTimeline(acc map[int]*bucketAcc) []Bucket {
	if len(acc) == 0 {
		return nil
	}
	last := 0
	for s := range acc {
		last = max(last, s)
	}
	out := make([]Bucket, last+1)
	for s := range out {
		out[s].Second = s
		if a := acc[s]; a != nil {
			sum := Summarize(a.lat, a.errors, 0)
			out[s].Count, out[s].Errors, out[s].P95Ms = sum.Count+a.dropped, a.errors+a.dropped, toMs(sum.P95)
		}
	}
	return out
}

func (b Bucket) bad(maxErr float64) bool {
	return b.Count > 0 && float64(b.Errors)/float64(b.Count) > maxErr
}

// validateFaultWindow refuses a window that could never be judged: a threshold
// that is not a ratio in [0,1] (NaN would make every bucket look clean) or an
// off that is not after on.
func validateFaultWindow(on, off time.Duration, maxErr float64) error {
	if math.IsNaN(maxErr) || math.IsInf(maxErr, 0) || maxErr < 0 || maxErr > 1 {
		return errors.New("fault max error must be a ratio in [0,1]")
	}
	if on < 0 || off <= on {
		return errors.New("fault off must be after fault on")
	}
	return nil
}

// AnalyzeFault finds when the error ratio first exceeds maxErr after on, and
// when it is back at or under maxErr for sustain whole buckets in a row after
// off. Empty buckets neither degrade nor recover. Times are bucket starts.
func AnalyzeFault(b []Bucket, on, off time.Duration, maxErr float64, sustain int) (FaultResult, error) {
	f := FaultResult{OnMs: on.Milliseconds(), OffMs: off.Milliseconds()}
	if err := validateFaultWindow(on, off, maxErr); err != nil {
		return f, err
	}
	sustain = max(sustain, 1)
	for _, k := range b {
		if start := time.Duration(k.Second) * time.Second; start+time.Second > on && k.bad(maxErr) {
			d := max((start - on).Milliseconds(), 0)
			f.TimeToDegradeMs = &d
			break
		}
	}
	for i := range b {
		start := time.Duration(b[i].Second) * time.Second
		if start < off || i+sustain > len(b) {
			continue
		}
		ok := true
		for _, k := range b[i : i+sustain] {
			if k.Count == 0 || k.bad(maxErr) {
				ok = false
				break
			}
		}
		if ok {
			d := max((start - off).Milliseconds(), 0)
			f.Recovered, f.TimeToRecoverMs = true, &d
			break
		}
	}
	return f, nil
}

// ApplyRecoveryBound fails the report when the fault did not recover within
// bound, or when there is no fault window to judge.
func ApplyRecoveryBound(r Report, bound time.Duration) Report {
	c := AssertionCheck{Path: "fault", Name: "recover_within"}
	switch {
	case r.Fault == nil:
		c.Detail = "no fault window was given"
	case !r.Fault.Recovered || r.Fault.TimeToRecoverMs == nil:
		c.Detail = "no recovery observed"
	default:
		c.OK = *r.Fault.TimeToRecoverMs <= bound.Milliseconds()
		c.Detail = fmt.Sprintf("recovered in %dms, bound %dms", *r.Fault.TimeToRecoverMs, bound.Milliseconds())
	}
	r.Assertions = append(r.Assertions, c)
	if !c.OK {
		r.AssertOK = false
	}
	return r
}

// ApplyAssertions evaluates the forbid and require status rules of each
// target against the report.
func ApplyAssertions(r Report, targets []Target) Report {
	byName := map[string]PathReport{}
	for _, p := range r.Paths {
		byName[p.Name] = p
	}
	for _, t := range targets {
		p := byName[t.Name]
		add := func(name string, ok bool, detail string) {
			r.Assertions = append(r.Assertions, AssertionCheck{Path: t.Name, Name: name, OK: ok, Detail: detail})
			if !ok {
				r.AssertOK = false
			}
		}
		if len(t.ForbidStatus) > 0 {
			add("forbid_status", p.Forbidden == 0, fmt.Sprintf("%d forbidden responses", p.Forbidden))
		}
		if t.Forbid5xx {
			n := 0
			for s, c := range p.Statuses {
				if v, err := strconv.Atoi(s); err == nil && v >= 500 {
					n += c
				}
			}
			add("forbid_5xx", n == 0, fmt.Sprintf("%d 5xx responses", n))
		}
		if t.MaxErrorRate != nil {
			add("max_error_rate", p.ErrorRate <= *t.MaxErrorRate, fmt.Sprintf("error rate %.4f, limit %g", p.ErrorRate, *t.MaxErrorRate))
		}
		codes := make([]string, 0, len(t.MaxStatusCount))
		for k := range t.MaxStatusCount {
			codes = append(codes, k)
		}
		sort.Strings(codes)
		for _, k := range codes {
			n := p.Statuses[k]
			add("max_status_count_"+k, n <= t.MaxStatusCount[k], fmt.Sprintf("%d responses, limit %d", n, t.MaxStatusCount[k]))
		}
		req := append([]int(nil), t.RequireStatus...)
		sort.Ints(req)
		for _, s := range req {
			n := p.Statuses[strconv.Itoa(s)]
			add("require_status_"+strconv.Itoa(s), n > 0, fmt.Sprintf("%d responses", n))
		}
	}
	return r
}

// ApplyRequireDegrade fails the report when the fault window never degraded
// the run, or when there is no fault window to judge.
func ApplyRequireDegrade(r Report) Report {
	c := AssertionCheck{Path: "fault", Name: "require_degrade"}
	switch {
	case r.Fault == nil:
		c.Detail = "no fault window was given"
	case r.Fault.TimeToDegradeMs == nil:
		c.Detail = "the fault window never degraded the run"
	default:
		c.OK = true
		c.Detail = fmt.Sprintf("degraded after %dms", *r.Fault.TimeToDegradeMs)
	}
	r.Assertions = append(r.Assertions, c)
	if !c.OK {
		r.AssertOK = false
	}
	return r
}
