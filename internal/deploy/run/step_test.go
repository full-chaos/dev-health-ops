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
	"strconv"
	"strings"
	"testing"
)

const stepsDir = "testdata/steps"

// fake replays one Output and records every argv.
type fake struct {
	out   Output
	err   error
	calls [][]string
}

func (f *fake) Run(_ context.Context, c Cmd) (Output, error) {
	f.calls = append(f.calls, c.Argv)
	return f.out, f.err
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

type stepCase struct {
	step, fixture string
	re            *regexp.Regexp
}

func readTSV(t *testing.T, name string) [][]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(stepsDir, name))
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

// synthSteps reads the 40 SYNTHETIC step outputs (made-up values in the
// shapes of the prod roll steps: padded kubectl columns, helm tables with
// tabs, multi-line tool logs): steps.tsv (step, file, final-line regex) and
// SHA256SUMS (a pin of every file).
func synthSteps(t *testing.T) []stepCase {
	t.Helper()
	sums := map[string]string{}
	for _, l := range strings.Split(strings.TrimSuffix(mustRead(t, "SHA256SUMS"), "\n"), "\n") {
		sum, name, ok := strings.Cut(l, "  ")
		if !ok {
			t.Fatalf("SHA256SUMS line %q", l)
		}
		sums[name] = sum
	}
	files, err := os.ReadDir(stepsDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.Name() == "SHA256SUMS" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(stepsDir, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(b)
		if sums[f.Name()] != hex.EncodeToString(h[:]) {
			t.Fatalf("%s: sha256 is not its SHA256SUMS line", f.Name())
		}
		delete(sums, f.Name())
	}
	if len(sums) != 0 {
		t.Fatalf("SHA256SUMS lines with no file: %v", sums)
	}
	steps := readTSV(t, "steps.tsv")
	if len(steps) != 40 || len(files) != 42 {
		t.Fatalf("want 40 step rows, 40 outputs + steps.tsv + SHA256SUMS; got %d rows, %d files", len(steps), len(files))
	}
	var out []stepCase
	listed := map[string]bool{}
	for i, s := range steps {
		if len(s) != 3 || listed[s[1]] {
			t.Fatalf("steps.tsv row %d %q", i, s)
		}
		listed[s[1]] = true
		out = append(out, stepCase{step: s[0], fixture: s[1], re: regexp.MustCompile(s[2])})
	}
	return out
}

func mustRead(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(stepsDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func newOut(t *testing.T) *Out {
	t.Helper()
	return &Out{Dir: t.TempDir()}
}

// G15 on the 40 synthetic step outputs: each passes its step rule as a typed
// step and only the record reaches the .out file; a missing PASS line, a
// FAIL/REFUSED/HOLD word or a wrong rc on the same bytes is a STOP.
func TestTheStepOutputsPassTheirStepRule(t *testing.T) {
	c := newClock()
	for _, k := range synthSteps(t) {
		t.Run(k.step, func(t *testing.T) {
			raw := []byte(mustRead(t, k.fixture))
			step := Step[finalLine]{Label: k.step, Cmd: Cmd{Argv: []string{"helm", "x"}}, Parse: kitFinal(k.re)}
			o, f, log := newOut(t), &fake{out: Output{Stdout: raw}}, &bytes.Buffer{}
			v, err := Do(context.Background(), f, o, log, c.now, step)
			if err != nil {
				t.Fatalf("step output: %v", err)
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

type number int

func (n number) Record() string { return "N=" + strconv.Itoa(int(n)) }

// r1 P1: a STOP is fixed text: the text of a parser error or a runner error
// (both can quote tool output) reaches neither .out, the log nor the error.
func TestAStopLineHoldsNoToolText(t *testing.T) {
	c := newClock()
	const sentinel = "raw-tool-bytes-5be1"
	atoi := func(o Output) (number, error) {
		n, err := strconv.Atoi(strings.TrimSpace(string(o.Stdout)))
		return number(n), err
	}
	for name, f := range map[string]*fake{
		"a parser error quoting stdout": {out: Output{Stdout: []byte(sentinel + "\n")}},
		"a runner error quoting stderr": {out: Output{Stderr: []byte(sentinel)}, err: errors.New("exit: " + sentinel)},
	} {
		t.Run(name, func(t *testing.T) {
			o, log := newOut(t), &bytes.Buffer{}
			_, err := Do(context.Background(), f, o, log, c.now, Step[number]{Label: "N", Cmd: Cmd{Argv: []string{"helm"}}, Parse: atoi})
			wantCode(t, err, CodeStop, name)
			rec, _ := os.ReadFile(filepath.Join(o.Dir, "N.out"))
			if strings.Contains(string(rec)+log.String()+err.Error(), sentinel) || !strings.HasPrefix(string(rec), "STOP: N") {
				t.Fatalf("tool text reached a record: .out %q, log %q, err %q", rec, log.String(), err)
			}
		})
	}
	o := newOut(t)
	v, err := Do(context.Background(), &fake{out: Output{Stdout: []byte("7\n")}}, o, &bytes.Buffer{}, c.now, Step[number]{Label: "N", Cmd: Cmd{Argv: []string{"helm"}}, Parse: atoi})
	if err != nil || v != 7 {
		t.Fatalf("a good parse: %v %v", v, err)
	}
}

// failClose makes every checked Close of a run file fail (the file is still closed).
func failClose(t *testing.T) {
	t.Helper()
	orig := closeFile
	closeFile = func(f *os.File) error {
		_ = f.Close()
		return errors.New("close failed")
	}
	t.Cleanup(func() { closeFile = orig })
}

// A record whose Close fails is a STOP on the success path; on a STOP path
// the first STOP stands.
func TestARecordCloseFailureIsAStop(t *testing.T) {
	failClose(t)
	c := newClock()
	atoi := func(o Output) (number, error) {
		n, err := strconv.Atoi(strings.TrimSpace(string(o.Stdout)))
		return number(n), err
	}
	log := &bytes.Buffer{}
	_, err := Do(context.Background(), &fake{out: Output{Stdout: []byte("7\n")}}, newOut(t), log, c.now, Step[number]{Label: "N", Cmd: Cmd{Argv: []string{"helm"}}, Parse: atoi})
	if Code(err) != CodeStop || err.Error() != "N: cannot close the record" || !strings.Contains(log.String(), "STOP: N: cannot close the record") || strings.Contains(log.String(), "N=7") {
		t.Fatalf("a failed record Close on the success path: %v, log %q", err, log.String())
	}
	_, err = Do(context.Background(), &fake{out: Output{Stdout: []byte("x\n")}}, newOut(t), &bytes.Buffer{}, c.now, Step[number]{Label: "N", Cmd: Cmd{Argv: []string{"helm"}}, Parse: atoi})
	if Code(err) != CodeStop || err.Error() != "N rc=0 but its output is not the PASS form" {
		t.Fatalf("on a STOP path the first STOP must stand: %v", err)
	}
}
