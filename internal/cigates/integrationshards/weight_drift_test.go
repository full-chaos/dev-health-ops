//go:build !race

package integrationshards_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const driftWarning = "::warning title=INTEGRATION WEIGHT DRIFT::"

// okGo is a go on PATH whose `go test` prints one `ok` line per package argument (seconds from FAKE_OK, "pkg=sec;pkg=sec")
// and exits FAKE_RC; every other go command runs the real one. Its docker lists no container, so the session reap passes
// whatever docker the host has.
func okGo(t *testing.T) string {
	t.Helper()
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	script := "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"${1-}\" = test ]; then\n" +
		"  for a in \"$@\"; do\n    case \"${a}\" in ./*)\n" +
		"      sec=\"$(printf '%s' \"${FAKE_OK}\" | tr ';' '\\n' | sed -n \"s|^${a#./}=||p\")\"\n" +
		"      printf 'ok  \\tgithub.com/full-chaos/dev-health-ops/%s\\t%ss\\n' \"${a#./}\" \"${sec:-1}\" ;;\n    esac\n  done\n" +
		"  exit \"${FAKE_RC:-0}\"\nfi\n" +
		"exec '" + realGo + "' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// driftShard picks one packages shard of the real plan: its number, its packages' weights, the heaviest package and a
// package that weighs under 30 s.
func driftShard(t *testing.T) (shard int, weights map[string]int, heavy, light string) {
	t.Helper()
	stdout, _ := realPlan(t)
	byShard := map[int]map[string]int{}
	for _, l := range strings.Split(stdout, "\n") {
		m := shardRow.FindStringSubmatch(l)
		if m == nil || splitPackages[m[2]] {
			continue
		}
		s, _ := strconv.Atoi(m[1])
		w, _ := strconv.Atoi(m[3])
		if byShard[s] == nil {
			byShard[s] = map[string]int{}
		}
		byShard[s][m[2]] = w
	}
	for s := 1; s <= 64; s++ {
		rows := byShard[s]
		if len(rows) < 2 {
			continue
		}
		heavy, light = "", ""
		for p, w := range rows {
			if heavy == "" || w > rows[heavy] || (w == rows[heavy] && p < heavy) {
				heavy = p
			}
			if w < 30 && (light == "" || p < light) {
				light = p
			}
		}
		if rows[heavy] >= 60 && light != "" {
			return s, rows, heavy, light
		}
	}
	t.Fatal("no packages shard holds a package of 60 s or more and a package under 30 s")
	return 0, nil, "", ""
}

func driftRun(t *testing.T, shard int, fake string, seconds map[string]int, rc int) cmdResult {
	t.Helper()
	var parts []string
	for p, s := range seconds {
		parts = append(parts, fmt.Sprintf("%s=%d", p, s))
	}
	return runCheckGoSharding(t, shardEnv{extra: []string{
		"PATH=" + fake + string(os.PathListSeparator) + os.Getenv("PATH"), "FAKE_OK=" + strings.Join(parts, ";"),
		"FAKE_RC=" + strconv.Itoa(rc), "TMPDIR=" + t.TempDir(),
	}}, "integration-shard", "packages", strconv.Itoa(shard))
}

func TestAShardRunWarnsOnlyForAMeasuredTimeFarFromItsWeight(t *testing.T) {
	t.Parallel()
	shard, weights, heavy, light := driftShard(t)
	fake := okGo(t)
	base := map[string]int{}
	for p, w := range weights {
		base[p] = w
	}
	with := func(p string, sec int) map[string]int {
		m := map[string]int{}
		for k, v := range base {
			m[k] = v
		}
		m[p] = sec
		return m
	}
	line := func(p string, sec, w int) string {
		return fmt.Sprintf("%s%s measured %ds, weight %ds (%+.0f%%)", driftWarning, p, sec, w, float64(sec-w)*100/float64(w))
	}
	for _, c := range []struct {
		name    string
		seconds map[string]int
		rc      int
		want    string
		wantRC  int
	}{
		{"no warning when every time equals its weight", base, 0, "", 0},
		{"a time inside 50% of its weight", with(heavy, weights[heavy]*14/10), 0, "", 0},
		{"a time 60 s or more and over 50% away", with(heavy, weights[heavy]*2), 0, line(heavy, weights[heavy]*2, weights[heavy]), 0},
		{"a time far under a 60 s or heavier weight", with(heavy, 1), 0, line(heavy, 1, weights[heavy]), 0},
		{"a failing go test keeps its status and still warns", with(heavy, weights[heavy]*2), 3, line(heavy, weights[heavy]*2, weights[heavy]), 3},
		{"a failing go test with no drift keeps its status", base, 3, "", 3},
		{"both sides under 60 s", with(light, 59), 0, "", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := driftRun(t, shard, fake, c.seconds, c.rc)
			if r.code != c.wantRC {
				t.Errorf("exit %d, want %d: %s%s", r.code, c.wantRC, r.stdout, r.stderr)
			}
			got := strings.Count(r.stdout, driftWarning)
			switch {
			case c.want == "" && got != 0:
				t.Errorf("%d drift warning(s), want none: %s", got, r.stdout)
			case c.want != "" && (got != 1 || !strings.Contains(r.stdout, c.want+"\n")):
				t.Errorf("want exactly the warning %q, got %d warning(s): %s", c.want, got, r.stdout)
			}
		})
	}
}
