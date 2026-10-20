package bigboy_test

import (
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const (
	repinEnvNames    = "ci/bigboy/container-env-names.sh"
	repinEnvTemplate = `{{range .Config.Env}}{{index (split . "=") 0}}{{println}}{{end}}`
)

func repinEnvNamesRun(g *guard, docker string, args ...string) scriptRun {
	dir, _ := repinBin(g, map[string]string{"docker": docker})
	return repinRun(g, map[string]string{"DOCKER": filepath.Join(dir, "docker")}, append([]string{g.path(repinEnvNames)}, args...)...)
}

func checkRepinEnvTemplateCutsName(g *guard) {
	argv := filepath.Join(repinDir(g, "argv"), "argv")
	r := repinEnvNamesRun(g, `printf "%s\n" "$@" > "`+argv+`"`+"\nprintf \"A_NAME\\n\"\n", "synthetic")
	g.expect(r.code == 0, "container-env-names.sh exited %d on a names-only inspect: %s", r.code, r.stderr)
	args := strings.Split(strings.TrimSuffix(repinReadFile(g, argv), "\n"), "\n")
	cut, typed := false, false
	for i, a := range args {
		cut = cut || strings.Contains(a, `{{index (split . "=") 0}}`)
		typed = typed || (a == "--type" && i+1 < len(args) && args[i+1] == "container")
	}
	g.expect(cut, "docker was not handed the name-cutting template {{index (split . \"=\") 0}}: argv %q", args)
	g.expect(typed, "docker inspect was not restricted to --type container: argv %q", args)
	g.expect(r.stdout == "A_NAME\n", "stdout %q, want %q", r.stdout, "A_NAME\n")
}

func checkRepinEnvLeakWithheld(g *guard) {
	s := repinSentinel()
	r := repinEnvNamesRun(g, `printf "GOOD\nLEAK=`+s+`\n"`+"\n", "synthetic")
	g.expect(r.code == 3, "a NAME=value line exited %d, want 3", r.code)
	g.expect(!strings.Contains(r.repinOut(), s), "the sentinel value reached stdout or stderr")
	g.expect(strings.Contains(r.stdout, "<non-name-line>"), "the NAME=value line was not replaced with <non-name-line>: %q", r.stdout)
	g.expect(strings.Contains(r.stdout, "GOOD"), "the good name was not printed: %q", r.stdout)
}

func checkRepinEnvInspectFailure(g *guard) {
	s := repinSentinel()
	r := repinEnvNamesRun(g, `echo "boom `+s+`" >&2; exit 1`+"\n", "absent-container")
	g.expect(r.code == 2, "an inspect failure exited %d, want 2", r.code)
	g.expect(strings.Contains(r.stderr, "absent-container"), "the inspect failure does not name the container: %q", r.stderr)
	g.expect(!strings.Contains(r.repinOut(), s), "docker's own error text (with the sentinel value) reached stdout or stderr")
}

func checkRepinEnvUsage(g *guard) {
	r := repinEnvNamesRun(g, "exit 0\n")
	g.expect(r.code == 2, "no argument exited %d, want 2 (usage)", r.code)
	g.expect(strings.Contains(r.stderr, "usage"), "no argument printed no usage on stderr: %q", r.stderr)
}

// Stands in for the live-daemon leg (no docker here): the stub cuts names only when handed the cutting template.
func checkRepinEnvSyntheticContainer(g *guard) {
	s := repinSentinel()
	env := []string{"SYNTH_WITH_EQUALS=a=" + s + "=b", "SYNTH_SECRET_TOKEN=" + s, "PATH=/usr/bin", "SYNTH_SECRET_TOKEN=" + s}
	var names, full strings.Builder
	for _, e := range env {
		names.WriteString(strings.SplitN(e, "=", 2)[0] + "\n")
		full.WriteString(e + "\n")
	}
	r := repinEnvNamesRun(g, "fmt=\nfor a in \"$@\"; do [[ $a == *'{{range'* ]] && fmt=$a; done\n"+
		"if [[ $fmt == '"+repinEnvTemplate+"' ]]; then printf '%s' '"+names.String()+"'; else printf '%s' '"+full.String()+"'; fi\n", "env-names-test")
	g.expect(r.code == 0, "container-env-names.sh exited %d on a synthetic container: %s", r.code, r.stderr)
	got := strings.Split(strings.TrimSuffix(r.stdout, "\n"), "\n")
	g.expect(sort.StringsAreSorted(got), "names are not sorted: %q", got)
	g.expect(equal(got, []string{"PATH", "SYNTH_SECRET_TOKEN", "SYNTH_WITH_EQUALS"}), "names %q, want PATH SYNTH_SECRET_TOKEN SYNTH_WITH_EQUALS once each", got)
	g.expect(!strings.Contains(r.repinOut(), s), "the sentinel value reached stdout or stderr")
	g.expect(!strings.Contains(r.stdout, "="), "stdout carries a '=': %q", r.stdout)
}

func TestRepinContainerEnvNames(t *testing.T) {
	runReal(t,
		check{"template-cuts-the-name-inside-docker", checkRepinEnvTemplateCutsName},
		check{"leaked-value-line-is-withheld-and-fails", checkRepinEnvLeakWithheld},
		check{"inspect-failure-is-named-and-nonzero", checkRepinEnvInspectFailure},
		check{"missing-argument-is-usage-error", checkRepinEnvUsage},
		check{"synthetic-container-names-only", checkRepinEnvSyntheticContainer},
	)
}

func TestRepinContainerEnvNamesPlanted(t *testing.T) {
	c := []string{repinEnvNames}
	runPlanted(t, []planted{
		{
			name:   "template-prints-whole-entry",
			copy:   c,
			mutate: mut(repinEnvNames, `{{index (split . "=") 0}}`, `{{.}}`),
			check:  checkRepinEnvTemplateCutsName,
			want:   "docker was not handed the name-cutting template",
		},
		{
			name:   "inspect-not-typed",
			copy:   c,
			mutate: mut(repinEnvNames, `inspect --type container \`, `inspect \`),
			check:  checkRepinEnvTemplateCutsName,
			want:   "not restricted to --type container",
		},
		{
			name:   "non-name-line-echoed",
			copy:   c,
			mutate: mut(repinEnvNames, `out+="<non-name-line>"$'\n'`, `out+="$line"$'\n'`),
			check:  checkRepinEnvLeakWithheld,
			want:   "the sentinel value reached stdout or stderr",
		},
		{
			name:   "non-name-line-exits-zero",
			copy:   c,
			mutate: mut(repinEnvNames, "    bad=1\n", ""),
			check:  checkRepinEnvLeakWithheld,
			want:   "a NAME=value line exited 0, want 3",
		},
		{
			name:   "inspect-stderr-passed-through",
			copy:   c,
			mutate: mut(repinEnvNames, `"$c" 2>/dev/null); then`, `"$c"); then`),
			check:  checkRepinEnvInspectFailure,
			want:   "docker's own error text",
		},
		{
			name:   "missing-argument-exits-zero",
			copy:   c,
			mutate: mut(repinEnvNames, `<container>" >&2; exit 2; }`, `<container>" >&2; exit 0; }`),
			check:  checkRepinEnvUsage,
			want:   "no argument exited 0, want 2",
		},
		{
			name:   "names-not-sorted",
			copy:   c,
			mutate: mut(repinEnvNames, "printf '%s' \"$out\" | sort -u", "printf '%s' \"$out\""),
			check:  checkRepinEnvSyntheticContainer,
			want:   "names are not sorted",
		},
	})
}
