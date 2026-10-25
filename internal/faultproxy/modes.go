// Package faultproxy is a small TCP proxy that injects network faults between
// a service and one dependency, for fault runs on a throwaway stack. It is a
// test tool and is never shipped.
package faultproxy

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/loadharness"
)

// Mode is the fault the proxy applies.
type Mode string

const (
	Pass      Mode = "pass"
	Delay     Mode = "delay"
	Slow      Mode = "slow"
	Drop      Mode = "drop"
	Cut       Mode = "cut"
	Blackhole Mode = "blackhole"
)

const maxLatency = time.Minute

// Setting is a mode with its parameters.
type Setting struct {
	Mode        Mode
	Latency     time.Duration
	Jitter      time.Duration
	BytesPerSec int
}

// Validate refuses an unknown mode or a parameter that makes no sense.
func (s Setting) Validate() error {
	switch s.Mode {
	case Pass, Drop, Cut, Blackhole:
		return nil
	case Delay:
		if s.Latency < 0 || s.Latency > maxLatency || s.Jitter < 0 || s.Jitter > maxLatency {
			return fmt.Errorf("latency and jitter must be between 0 and %v", maxLatency)
		}
		return nil
	case Slow:
		if s.BytesPerSec <= 0 {
			return errors.New("slow needs a positive bytes per second")
		}
		return nil
	}
	return fmt.Errorf("unknown mode %q", s.Mode)
}

// Step switches the proxy to Set at offset At from the start of the schedule.
type Step struct {
	At  time.Duration
	Set Setting
}

// ParseSchedule parses "5s=blackhole,35s=pass,40s=delay:200ms,50s=slow:5000".
// Offsets must be non-negative and strictly increasing.
func ParseSchedule(spec string) ([]Step, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, errors.New("schedule is empty")
	}
	var steps []Step
	for _, part := range strings.Split(spec, ",") {
		at, rest, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("schedule step %q: want offset=mode", part)
		}
		d, err := time.ParseDuration(at)
		if err != nil || d < 0 {
			return nil, fmt.Errorf("schedule step %q: offset must be a non-negative duration", part)
		}
		if n := len(steps); n > 0 && d <= steps[n-1].At {
			return nil, fmt.Errorf("schedule step %q: offsets must be strictly increasing", part)
		}
		name, arg, hasArg := strings.Cut(rest, ":")
		set := Setting{Mode: Mode(name)}
		switch set.Mode {
		case Pass, Drop, Cut, Blackhole:
			if hasArg {
				return nil, fmt.Errorf("schedule step %q: %s takes no argument", part, name)
			}
		case Delay:
			if !hasArg {
				return nil, fmt.Errorf("schedule step %q: delay needs delay:<latency>", part)
			}
			if set.Latency, err = time.ParseDuration(arg); err != nil {
				return nil, fmt.Errorf("schedule step %q: bad latency", part)
			}
		case Slow:
			if !hasArg {
				return nil, fmt.Errorf("schedule step %q: slow needs slow:<bytes per second>", part)
			}
			if set.BytesPerSec, err = strconv.Atoi(arg); err != nil {
				return nil, fmt.Errorf("schedule step %q: bad rate", part)
			}
		default:
			return nil, fmt.Errorf("schedule step %q: unknown mode %q", part, name)
		}
		if err := set.Validate(); err != nil {
			return nil, fmt.Errorf("schedule step %q: %w", part, err)
		}
		steps = append(steps, Step{At: d, Set: set})
	}
	return steps, nil
}

// CheckAddr applies the load harness host guard to a host:port: only local
// hosts, the auth-service compose name and hosts named in allow.
func CheckAddr(addr string, allow []string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf("address %q must be host:port", addr)
	}
	return loadharness.CheckHost("http://"+net.JoinHostPort(host, port), allow)
}
