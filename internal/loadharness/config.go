package loadharness

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DefaultAllowedHosts are the hosts a run may target without --allow-host:
// the local machine and the auth-service compose service name.
var DefaultAllowedHosts = []string{"localhost", "127.0.0.1", "::1", "auth-service"}

// CheckHost refuses any base URL whose host is not in the default list or in
// allow. The match is exact on the host name (no port, no suffix).
func CheckHost(rawURL string, allow []string) error {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return errors.New("base url must be http(s) with a host")
	}
	host := strings.ToLower(u.Hostname())
	for _, a := range append(append([]string(nil), DefaultAllowedHosts...), allow...) {
		if host == strings.ToLower(a) {
			return nil
		}
	}
	return fmt.Errorf("host %q is not allowed; pass --allow-host %s for a dev host", host, host)
}

// HeaderEnv names the env var that holds a header value.
type HeaderEnv struct {
	Env    string `json:"env"`
	Prefix string `json:"prefix"`
}

// Target is one measured path.
type Target struct {
	Name          string               `json:"name"`
	Method        string               `json:"method"`
	Path          string               `json:"path"`
	Headers       map[string]string    `json:"headers"`
	HeaderEnv     map[string]HeaderEnv `json:"header_env"`
	Body          string               `json:"body"`
	ExpectStatus  []int                `json:"expect_status"`
	ForbidStatus  []int                `json:"forbid_status"`
	RequireStatus []int                `json:"require_status"`
	Forbid5xx     bool                 `json:"forbid_5xx"`
	Weight        int                  `json:"weight"`
}

// Config is the target list. The same shape serves any path, so a PDP path
// needs a config entry and no code.
type Config struct {
	BaseURL string   `json:"base_url"`
	Targets []Target `json:"targets"`
}

// ParseConfig decodes and validates a config. getenv resolves the env var
// names the config refers to; every one must be set.
func ParseConfig(data []byte, getenv func(string) string) (Config, error) {
	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if len(cfg.Targets) == 0 {
		return Config{}, errors.New("config: no targets")
	}
	seen := map[string]bool{}
	for i := range cfg.Targets {
		t := &cfg.Targets[i]
		if t.Name == "" || seen[t.Name] {
			return Config{}, fmt.Errorf("config: target %d needs a unique name", i)
		}
		seen[t.Name] = true
		if t.Method == "" {
			t.Method = "GET"
		}
		if !strings.HasPrefix(t.Path, "/") {
			return Config{}, fmt.Errorf("config: target %q path must start with /", t.Name)
		}
		if strings.Contains(t.Path, "${ENV:") || strings.Contains(t.Path, "${POOL:") || strings.Contains(t.Path, "${PAD:") {
			return Config{}, fmt.Errorf("config: target %q path may use ${SEQ} only", t.Name)
		}
		if t.Weight < 0 {
			return Config{}, fmt.Errorf("config: target %q weight is negative", t.Name)
		}
		if t.Weight == 0 {
			t.Weight = 1
		}
		if len(t.ExpectStatus) == 0 {
			t.ExpectStatus = []int{200}
		}
		for h, he := range t.HeaderEnv {
			if he.Env == "" || getenv(he.Env) == "" {
				return Config{}, fmt.Errorf("config: target %q header %q: env var %q is not set", t.Name, h, he.Env)
			}
		}
		if _, err := expand(t.Body, 0, getenv); err != nil {
			return Config{}, fmt.Errorf("config: target %q body: %w", t.Name, err)
		}
	}
	return cfg, nil
}

// MaxPad is the largest ${PAD:N} body padding in bytes.
const MaxPad = 8 << 20

// MaxExpanded is the largest body or path an expansion may produce.
const MaxExpanded = 8 << 20

// expand fills ${SEQ}, ${ENV:NAME}, ${PAD:N} (N filler bytes) and ${POOL:NAME}
// (comma list, round robin by seq).
func expand(s string, seq int, getenv func(string) string) (string, error) {
	var b strings.Builder
	for {
		i := strings.Index(s, "${")
		if i < 0 {
			if b.Len()+len(s) > MaxExpanded {
				return "", fmt.Errorf("expansion is larger than %d bytes", MaxExpanded)
			}
			b.WriteString(s)
			return b.String(), nil
		}
		j := strings.Index(s[i:], "}")
		if j < 0 {
			return "", errors.New("unterminated ${")
		}
		b.WriteString(s[:i])
		tok := s[i+2 : i+j]
		s = s[i+j+1:]
		switch {
		case tok == "SEQ":
			b.WriteString(strconv.Itoa(seq))
		case strings.HasPrefix(tok, "ENV:"):
			v := getenv(tok[4:])
			if v == "" {
				return "", fmt.Errorf("env var %q is not set", tok[4:])
			}
			b.WriteString(v)
		case strings.HasPrefix(tok, "PAD:"):
			n, err := strconv.Atoi(tok[4:])
			if err != nil || n < 0 || n > MaxPad {
				return "", fmt.Errorf("pad must be 0..%d bytes", MaxPad)
			}
			if b.Len()+n > MaxExpanded {
				return "", fmt.Errorf("expansion is larger than %d bytes", MaxExpanded)
			}
			b.WriteString(strings.Repeat("a", n))
		case strings.HasPrefix(tok, "POOL:"):
			parts := strings.Split(getenv(tok[5:]), ",")
			if len(parts) == 1 && parts[0] == "" {
				return "", fmt.Errorf("env var %q is not set", tok[5:])
			}
			b.WriteString(strings.TrimSpace(parts[seq%len(parts)]))
		default:
			return "", fmt.Errorf("unknown placeholder %q", tok)
		}
	}
}

// Profile is a load shape.
type Profile struct {
	Name        string
	Rate        float64
	Duration    time.Duration
	Concurrency int
}

// ProfileByName returns the normal or peak preset.
func ProfileByName(name string) (Profile, error) {
	switch name {
	case "normal":
		return Profile{Name: "normal", Rate: 20, Duration: 60 * time.Second, Concurrency: 16}, nil
	case "peak":
		return Profile{Name: "peak", Rate: 200, Duration: 120 * time.Second, Concurrency: 64}, nil
	}
	return Profile{}, fmt.Errorf("unknown profile %q (normal, peak)", name)
}
