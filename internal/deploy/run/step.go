package run

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"time"
)

// Record is a typed step result. Record() is the only text a step writes to
// its .out file and the run log; the raw tool output stays in memory.
type Record interface {
	Record() string
}

// Step is one exec with its success rule: an allowed rc AND a typed parse
// that succeeds AND no FAIL/REFUSED/HOLD word in stdout (kit step_rc).
type Step[T Record] struct {
	Label string
	Cmd   Cmd
	OK    []int
	Parse func(Output) (T, error)
}

var stopWord = regexp.MustCompile(`\b(FAIL|REFUSED|HOLD)\b`)

// Do runs s once. <label>.out is created O_EXCL first (an existing one is a
// STOP and nothing runs). A STOP names the label and the rule, never the
// tool output.
func Do[T Record](ctx context.Context, r Runner, out *Out, log io.Writer, now func() time.Time, s Step[T]) (T, error) {
	var zero T
	f, err := out.create(s.Label)
	if err != nil {
		return zero, err
	}
	defer f.Close()
	fmt.Fprintf(log, "== %s %s\n", s.Label, now().UTC().Format(time.RFC3339))
	o, err := r.Run(ctx, s.Cmd)
	if err != nil {
		return zero, stopRecord(f, log, Stop("%s: %v", s.Label, err))
	}
	fmt.Fprintf(log, "== %s rc=%d\n", s.Label, o.RC)
	if !rcAllowed(o.RC, s.OK) {
		return zero, stopRecord(f, log, Stop("%s rc=%d", s.Label, o.RC))
	}
	v, err := s.Parse(o)
	if err != nil {
		return zero, stopRecord(f, log, Stop("%s rc=%d but its output is not the PASS form: %v", s.Label, o.RC, err))
	}
	if stopWord.Match(o.Stdout) {
		return zero, stopRecord(f, log, Stop("%s printed FAIL, REFUSED or HOLD", s.Label))
	}
	rec := v.Record()
	if _, err := io.WriteString(f, rec+"\n"); err != nil {
		return zero, stopRecord(f, log, Stop("%s: cannot write its record", s.Label))
	}
	fmt.Fprintln(log, rec)
	return v, nil
}

func stopRecord(f io.Writer, log io.Writer, e *Error) *Error {
	line := "STOP: " + e.Msg
	fmt.Fprintln(f, line)
	fmt.Fprintln(log, line)
	return e
}

func rcAllowed(rc int, ok []int) bool {
	if len(ok) == 0 {
		return rc == 0
	}
	for _, c := range ok {
		if rc == c {
			return true
		}
	}
	return false
}
