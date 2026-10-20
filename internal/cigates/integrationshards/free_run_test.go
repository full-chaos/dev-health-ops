package integrationshards_test

import (
	"bytes"
	"errors"
	"os/exec"
)

// freeSpec runs a child process like cmdSpec but without the one-at-a-time cap, so the heavy script guards can run their
// cases side by side: at most freeSlots children run at once, which keeps a small runner from being starved.
var freeSlots = make(chan struct{}, 4)

type freeSpec struct {
	dir   string
	env   []string
	stdin []byte
}

func (c freeSpec) run(name string, args ...string) cmdResult {
	freeSlots <- struct{}{}
	defer func() { <-freeSlots }()
	cmd := exec.Command(name, args...)
	cmd.Dir = c.dir
	cmd.Env = c.env
	if c.stdin != nil {
		cmd.Stdin = bytes.NewReader(c.stdin)
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	code := 0
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			code = exit.ExitCode()
		} else {
			code = -1
			errb.WriteString(err.Error())
		}
	}
	return cmdResult{code: code, stdout: out.String(), stderr: errb.String()}
}
