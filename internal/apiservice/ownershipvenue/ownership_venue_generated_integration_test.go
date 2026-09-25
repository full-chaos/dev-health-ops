//go:build integration

package ownershipvenue

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

// The generated scenarios are the class-wide guard for the ownership decision
// (CHAOS-6748): instead of one hand-picked shape per review round, a seeded
// generator draws the stored JSON of every place the decision reads (the
// integration's config, the credential's config and decrypted payload, the
// source's metadata) from every JSON type, with values a typed decode cannot
// hold (1e400, 1e-400, -0, 30-digit integers), unused deep branches (depth
// 0..50: Python's json.loads limits its own recursion, so the named divergence
// class, a Python RecursionError, is kept out of range; the 10001-deep case is a
// fixed scenario), non-object documents, blank/invalid/valid hosts, a credential
// of another provider, an unreadable payload and both entity families. Both
// planes answer every scenario; every DIFF fails the test.
//
// OWNERSHIP_ORACLE_SEED overrides the seed to search other draws.
const generatedSeed = 6748

var hostPool = []string{"https://ghe-a.acme.test", "ghe-b.acme.test", "https://gl-c.acme.test:8443", "not a host", "   ", "", "github.com", "gitlab.com"}

func generatedSeedValue() int64 {
	if raw := os.Getenv("OWNERSHIP_ORACLE_SEED"); raw != "" {
		if seed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return seed
		}
	}
	return generatedSeed
}

func pick[T any](r *rand.Rand, values ...T) T { return values[r.Intn(len(values))] }

func quote(text string) string {
	out, _ := json.Marshal(text)
	return string(out)
}

// scalar is a JSON scalar of any type, including the ones a typed decode cannot hold.
func scalar(r *rand.Rand) string {
	return pick(r, `null`, `true`, `false`, `0`, `-0`, `0.0`, `-0.0`, `5`, `-3`, `2.5`, `1e400`, `-1e400`, `1e-400`,
		`123456789012345678901234567890`, `""`, quote("text"), quote(" x "), quote(pick(r, hostPool...)))
}

// value is any JSON value nested at most depth levels: containers hold values.
func value(r *rand.Rand, depth int) string {
	if depth <= 0 || r.Intn(3) == 0 {
		return scalar(r)
	}
	switch r.Intn(3) {
	case 0:
		items := make([]string, r.Intn(3))
		for i := range items {
			items[i] = value(r, depth-1)
		}
		return "[" + strings.Join(items, ",") + "]"
	case 1:
		members := make([]string, r.Intn(3))
		for i := range members {
			members[i] = quote(fmt.Sprintf("k%d", r.Intn(4))) + ":" + value(r, depth-1)
		}
		return "{" + strings.Join(members, ",") + "}"
	default:
		// A chain of `levels` single-element arrays or objects around a scalar.
		levels := r.Intn(51)
		wrap := pick(r, "[", "{")
		var open, closer strings.Builder
		for i := 0; i < levels; i++ {
			if wrap == "[" {
				open.WriteString("[")
				closer.WriteString("]")
			} else {
				open.WriteString(`{"d":`)
				closer.WriteString("}")
			}
		}
		return open.String() + scalar(r) + closer.String()
	}
}

// object is a JSON object holding some of the wanted keys with values of any
// type (a host string most of the time) and unused extra keys, or, one time in
// ten, a document that is not an object.
func object(r *rand.Rand, wanted []string) string {
	if r.Intn(10) == 0 {
		return value(r, 3)
	}
	var members []string
	for _, key := range wanted {
		switch r.Intn(4) {
		case 0:
			continue
		case 1:
			members = append(members, quote(key)+":"+value(r, 4))
		default:
			members = append(members, quote(key)+":"+quote(pick(r, hostPool...)))
		}
	}
	for i := r.Intn(4); i > 0; i-- {
		members = append(members, quote(fmt.Sprintf("extra%d", i))+":"+value(r, 50))
	}
	return "{" + strings.Join(members, ",") + "}"
}

// generatedScenarios is n scenarios drawn from the seed.
func generatedScenarios(n int) []scenario {
	r := rand.New(rand.NewSource(generatedSeedValue()))
	var out []scenario
	for i := 0; i < n; i++ {
		system := pick(r, "github", "github", "gitlab", "gitlab", "linear", "jira")
		s := scenario{name: fmt.Sprintf("gen-%02d %s", i, system), system: system, sourceOn: r.Intn(5) != 0, intActive: r.Intn(5) != 0}
		operational := (system == "github" || system == "gitlab") && r.Intn(10) < 7
		if !operational {
			s.family = "legacy"
			s.host = "acme/managed"
		}
		s.intConfig = object(r, []string{system + "_instance_url", system + "_url"})
		if system == "gitlab" || system == "linear" {
			s.metadata = object(r, []string{"path_with_namespace", "org_wide_placeholder"})
			if r.Intn(3) == 0 {
				s.metadata = `{"path_with_namespace":"group/private","org_wide_placeholder":` + pick(r, "true", "false", "1", `"true"`) + `,"unused":` + value(r, 50) + `}`
				s.host = pick(r, "group/private", "acme/managed")
			}
		}
		if operational {
			switch r.Intn(4) {
			case 0: // no credential: the environment decides
			default:
				s.credProvider = pick(r, system, system, system, map[string]string{"github": "gitlab", "gitlab": "github"}[system])
				s.credConfig = object(r, []string{system + "_url", "url", "base_url"})
				switch r.Intn(4) {
				case 0:
					s.cred = noPayload
				case 1:
					s.cred = garbled
				default:
					s.cred = encrypted(object(r, []string{system + "_url", "url", "base_url"}))
				}
			}
			s.host = pick(r, "ghe-a.acme.test", "ghe-b.acme.test", "gl-c.acme.test:8443", "")
		}
		out = append(out, s)
	}
	return out
}
