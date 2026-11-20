package run

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const realDir = "testdata/real"

// fake replays one Output and records every argv.
type fake struct {
	out   Output
	calls [][]string
}

func (f *fake) Run(_ context.Context, c Cmd) (Output, error) {
	f.calls = append(f.calls, c.Argv)
	return f.out, nil
}

// finalLine is the kit step_rc final-line rule as a typed parser: the last
// non-blank line must match the step's regex; the record is that line.
type finalLine string

func (f finalLine) Record() string { return string(f) }

var blank = regexp.MustCompile(`^[[:space:]]*$`)

func kitFinal(re *regexp.Regexp) func(Output) (finalLine, error) {
	return func(o Output) (finalLine, error) {
		last := ""
		sc := bufio.NewScanner(bytes.NewReader(o.Stdout))
		for sc.Scan() {
			if !blank.MatchString(sc.Text()) {
				last = sc.Text()
			}
		}
		if !re.MatchString(last) {
			return "", errors.New("the final line is not the tool's PASS line")
		}
		return finalLine(last), nil
	}
}

type kitStep struct {
	step, fixture string
	re            *regexp.Regexp
}

func readTSV(t *testing.T, name string) [][]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(realDir, name))
	if err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	for _, l := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		rows = append(rows, strings.Split(l, "\t"))
	}
	return rows
}

// kitSteps reads the 40 real prod outputs: MANIFEST.tsv (the kit's), the
// step -> final-line regex map read from prodroll.sh v3z21 with the kit's own
// bash verdict (kitsteps.tsv), and SHA256SUMS (the kit's lines for these files).
func kitSteps(t *testing.T) []kitStep {
	t.Helper()
	sums := map[string]string{}
	for _, l := range strings.Split(strings.TrimSuffix(mustRead(t, "SHA256SUMS"), "\n"), "\n") {
		sum, name, ok := strings.Cut(l, "  ")
		if !ok {
			t.Fatalf("SHA256SUMS line %q", l)
		}
		sums[name] = sum
	}
	files, err := os.ReadDir(realDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.Name() == "SHA256SUMS" || f.Name() == "kitsteps.tsv" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(realDir, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(b)
		if sums[f.Name()] != hex.EncodeToString(h[:]) {
			t.Fatalf("%s: sha256 is not the kit SHA256SUMS line", f.Name())
		}
		delete(sums, f.Name())
	}
	if len(sums) != 0 {
		t.Fatalf("SHA256SUMS lines with no file: %v", sums)
	}
	manifest := readTSV(t, "MANIFEST.tsv")
	steps := readTSV(t, "kitsteps.tsv")
	if len(manifest) != 40 || len(steps) != 40 || len(files) != 43 {
		t.Fatalf("want 40 manifest rows, 40 step rows, 40 fixtures + 3 pins; got %d, %d, %d files", len(manifest), len(steps), len(files))
	}
	var out []kitStep
	for i, m := range manifest {
		s := steps[i]
		if len(s) != 5 || s[0] != m[0] || s[1] != m[1] || s[3] != "y" || s[4] != "0" {
			t.Fatalf("kitsteps row %d %q is not manifest row %q with the kit verdict y 0", i, s, m[:2])
		}
		out = append(out, kitStep{step: s[0], fixture: s[1], re: regexp.MustCompile(s[2])})
	}
	return out
}

func mustRead(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(realDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func newOut(t *testing.T) *Out {
	t.Helper()
	return &Out{Dir: t.TempDir()}
}

// G15 on the 40 real prod outputs: each passes its kit step rule as a typed
// step and only the record reaches the .out file; a missing PASS line, a
// FAIL/REFUSED/HOLD word or a wrong rc on the same bytes is a STOP.
func TestTheRealOutputsPassTheirStepRule(t *testing.T) {
	c := newClock()
	for _, k := range kitSteps(t) {
		t.Run(k.step, func(t *testing.T) {
			raw := []byte(mustRead(t, k.fixture))
			step := Step[finalLine]{Label: k.step, Cmd: Cmd{Argv: []string{"helm", "x"}}, Parse: kitFinal(k.re)}
			o, f, log := newOut(t), &fake{out: Output{Stdout: raw}}, &bytes.Buffer{}
			v, err := Do(context.Background(), f, o, log, c.now, step)
			if err != nil {
				t.Fatalf("real output: %v", err)
			}
			rec, err := os.ReadFile(filepath.Join(o.Dir, k.step+".out"))
			if err != nil || string(rec) != v.Record()+"\n" {
				t.Fatalf(".out = %q (%v), want only the record %q", rec, err, v.Record())
			}

			lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
			noPass := []byte(strings.Join(append(slices.Clone(lines[:len(lines)-1]), "x"), "\n") + "\n")
			word := []byte("note: FAIL\n" + string(raw))
			for name, out := range map[string]Output{
				"final line replaced": {Stdout: noPass},
				"FAIL word":           {Stdout: word},
				"rc 1":                {Stdout: raw, RC: 1},
			} {
				o := newOut(t)
				_, err := Do(context.Background(), &fake{out: out}, o, &bytes.Buffer{}, c.now, step)
				wantCode(t, err, CodeStop, name)
				rec, _ := os.ReadFile(filepath.Join(o.Dir, k.step+".out"))
				if !strings.HasPrefix(string(rec), "STOP: "+k.step) {
					t.Fatalf("%s: .out = %q, want the STOP line", name, rec)
				}
			}
		})
	}
}

func TestAStepRunsOnceAndWritesOnlyItsRecord(t *testing.T) {
	c := newClock()
	sentinel := "SENTINEL-6d1f0c"
	step := Step[finalLine]{Label: "I1", Cmd: Cmd{Argv: []string{"helm", "history"}}, OK: []int{0, 3},
		Parse: kitFinal(regexp.MustCompile(`^STATE LIVEREV=[0-9]+$`))}
	o, log := newOut(t), &bytes.Buffer{}
	f := &fake{out: Output{Stdout: []byte(sentinel + "\nSTATE LIVEREV=449\n"), Stderr: []byte(sentinel), RC: 3}}
	if _, err := Do(context.Background(), f, o, log, c.now, step); err != nil {
		t.Fatalf("rc 3 is allowed: %v", err)
	}
	rec, _ := os.ReadFile(filepath.Join(o.Dir, "I1.out"))
	if strings.Contains(string(rec)+log.String(), sentinel) || string(rec) != "STATE LIVEREV=449\n" {
		t.Fatalf("raw output reached a record: .out %q log %q", rec, log.String())
	}
	f2 := &fake{out: f.out}
	_, err := Do(context.Background(), f2, o, log, c.now, step)
	wantCode(t, err, CodeStop, "an existing .out")
	if len(f2.calls) != 0 {
		t.Fatalf("an existing .out must stop before the exec; calls %q", f2.calls)
	}
	f3 := &fake{out: Output{Stdout: []byte("STATE LIVEREV=449\n"), RC: 2}}
	_, err = Do(context.Background(), f3, newOut(t), log, c.now, step)
	wantCode(t, err, CodeStop, "rc 2 is not allowed")
	bad := step
	bad.Label = "../x"
	_, err = Do(context.Background(), &fake{}, newOut(t), log, c.now, bad)
	wantCode(t, err, CodeStop, "a label with a path")
}
