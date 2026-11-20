// Package deploycli is the `dho deploy` vertical: the one stable prod entry
// point (permission rule `Bash(ssh prod dho deploy *)`). The argv is a fixed
// grammar of enums; the per-run variation is the inputs buffer on stdin
// ("-"), read once, hashed and parsed from that buffer. Every run prints line 1
// (build commit, sha256 of the binary, the pin result, sha256 of the inputs)
// and a last line `DHO-DEPLOY END rc=<n>`.
package deploycli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/deploy/inputs"
	deployrun "github.com/full-chaos/dev-health-ops/internal/deploy/run"
	"github.com/full-chaos/dev-health-ops/internal/deploy/selfpin"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// host is what a run reads from the machine; tests replace it.
type host struct {
	pinPath string
	selfExe string
	commit  string
	harden  func() error
}

func prodHost() host {
	return host{pinPath: selfpin.PinPath, selfExe: selfpin.SelfExe, commit: version.Current("dho").Commit, harden: deployrun.Harden}
}

var rollPhases = map[string][]string{
	"dev-health":    {"pre", "deploy", "wait", "post"},
	"ingress-nginx": {"pre", "upgrade", "post"},
	"acr":           {"pre", "upgrade", "post"},
}

// Command is the `deploy` group.
func Command() cli.Command { return command(prodHost()) }

func command(h host) cli.Command {
	return cli.Command{
		Name:    "deploy",
		Kind:    cli.Group,
		Summary: "the prod roll entry point: inputs on stdin (-), gates built in (exit 0 pass, 1 stop, 2 usage, 3 refused)",
		Children: []cli.Command{
			{
				Name:    "self-check",
				Kind:    cli.Verb,
				Summary: "print the build, sha256 of this binary and the pin check; with -, parse the inputs on stdin; writes nothing",
				Run:     func(_ context.Context, env cli.Env) int { return selfCheck(h, env) },
			},
			{
				Name:    "roll",
				Kind:    cli.Verb,
				Summary: "roll <dev-health|ingress-nginx|acr> <phase> - : not built in this binary yet (the kit runs rolls)",
				Run:     func(_ context.Context, env cli.Env) int { return roll(h, env) },
			},
		},
	}
}

// positional checks the raw words before anything reads them: a lone help
// word prints the usage (exit 0); any other word that starts with "-", except
// the stdin word "-" itself, is a usage error that never echoes the word
// ("--" included, so no parser can drop a token).
func positional(usage string, env cli.Env) ([]string, int, bool) {
	if len(env.Args) == 1 {
		switch env.Args[0] {
		case "-h", "-help", "--help":
			fmt.Fprintln(env.Stderr, "usage: dho deploy "+usage)
			return nil, cli.ExitOK, false
		}
	}
	for _, a := range env.Args {
		if a != "-" && strings.HasPrefix(a, "-") {
			fmt.Fprintln(env.Stderr, "unknown flag (dho deploy takes no flags)")
			fmt.Fprintln(env.Stderr, "usage: dho deploy "+usage)
			return nil, cli.ExitUsage, false
		}
	}
	return env.Args, cli.ExitOK, true
}

type run struct {
	out       io.Writer
	cmd       string
	pin       selfpin.Result
	inputsSHA string
	hasInputs bool
}

func (r *run) line1(commit string) {
	pin := "ok"
	if !r.pin.OK {
		pin = "refused"
	}
	src, sum := "-", "-"
	if r.hasInputs {
		src, sum = "stdin", r.inputsSHA
	}
	fmt.Fprintf(r.out, "DHO-DEPLOY %s sha256(dho)=%s pin=%s inputs=%s sha256(inputs)=%s cmd=%s\n",
		short(commit), r.pin.SHA256, pin, src, sum, r.cmd)
	if r.pin.OK {
		fmt.Fprintf(r.out, "PIN: PASS (sha256 and commit = %s, path %s)\n", selfpin.PinPath, r.pin.ExePath)
	} else {
		fmt.Fprintf(r.out, "PIN: REFUSED: %s\n", r.pin.Reason)
	}
}

func (r *run) end(code int) int {
	fmt.Fprintf(r.out, "DHO-DEPLOY END rc=%d\n", code)
	return code
}

func short(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}

// readInputs takes stdin once; the sha256 printed and the parse use that buffer.
func readInputs(env cli.Env) ([]byte, error) {
	return inputs.Read(env.Stdin)
}

func selfCheck(h host, env cli.Env) int {
	words, code, ok := positional("self-check [-]", env)
	if !ok {
		return code
	}
	if len(words) > 1 || (len(words) == 1 && words[0] != "-") {
		fmt.Fprintln(env.Stderr, "usage: dho deploy self-check [-] (nothing read)")
		return cli.ExitUsage
	}
	r := &run{out: env.Stdout, cmd: strings.TrimSpace("self-check " + strings.Join(words, " "))}
	var buf []byte
	if len(words) == 1 {
		var err error
		if buf, err = readInputs(env); err != nil {
			fmt.Fprintln(env.Stdout, "INPUTS: REFUSED: stdin is unreadable")
			return cli.ExitRefused
		}
		r.hasInputs, r.inputsSHA = true, inputs.SHA256(buf)
	}
	r.pin = selfpin.Check(h.pinPath, h.selfExe, h.commit)
	r.line1(h.commit)
	rc := cli.ExitOK
	if !r.pin.OK {
		rc = cli.ExitRefused
	}
	if r.hasInputs {
		in, err := inputs.Parse(buf)
		if err != nil {
			fmt.Fprintf(r.out, "INPUTS: REFUSED: %v\n", err)
			rc = cli.ExitRefused
		} else {
			fmt.Fprintf(r.out, "INPUTS: PASS %s\n", summary(in))
		}
	}
	return r.end(rc)
}

func summary(in *inputs.Inputs) string {
	s := fmt.Sprintf("target=%s run_tag=%s author=%s", in.Target, in.RunTag, in.Author)
	if dh := in.DevHealth; dh != nil {
		s += fmt.Sprintf(" step=%d svc=%s rollback=%s revs=%d->%d img=%s->%s rows=%d",
			dh.Step, short(dh.SvcSHA), short(dh.RollbackSHA), dh.ExpStartRev, dh.ExpectNewestRev, short(dh.ImgOld), short(dh.ImgNew), len(dh.Rows))
	}
	return s
}

func roll(h host, env cli.Env) int {
	words, code, ok := positional("roll <target> <phase> -", env)
	if !ok {
		return code
	}
	if !rollGrammar(words) {
		fmt.Fprintln(env.Stderr, "usage: dho deploy roll <dev-health pre|deploy|wait|post | ingress-nginx pre|upgrade|post | acr pre|upgrade|post> - (nothing read)")
		return cli.ExitUsage
	}
	if err := h.harden(); err != nil {
		fmt.Fprintln(env.Stdout, "DHO-DEPLOY REFUSED: cannot set umask 077 and cd /; nothing read")
		return cli.ExitRefused
	}
	buf, err := readInputs(env)
	if err != nil {
		fmt.Fprintln(env.Stdout, "INPUTS: REFUSED: stdin is unreadable")
		return cli.ExitRefused
	}
	r := &run{out: env.Stdout, cmd: "roll " + strings.Join(words, " "), hasInputs: true, inputsSHA: inputs.SHA256(buf)}
	r.pin = selfpin.Check(h.pinPath, h.selfExe, h.commit)
	r.line1(h.commit)
	if !r.pin.OK {
		return r.end(cli.ExitRefused)
	}
	in, err := inputs.Parse(buf)
	if err != nil {
		fmt.Fprintf(r.out, "INPUTS: REFUSED: %v\n", err)
		return r.end(cli.ExitRefused)
	}
	if in.Target != words[0] {
		fmt.Fprintln(r.out, "INPUTS: REFUSED: TARGET is not the target of the command")
		return r.end(cli.ExitRefused)
	}
	fmt.Fprintf(r.out, "INPUTS: PASS %s\n", summary(in))
	fmt.Fprintln(r.out, "DHO-DEPLOY REFUSED: roll is not built in this binary (not in dho yet: use the kit); nothing ran")
	return r.end(cli.ExitRefused)
}

func rollGrammar(words []string) bool {
	if len(words) != 3 || words[2] != "-" {
		return false
	}
	for _, p := range rollPhases[words[0]] {
		if p == words[1] {
			return true
		}
	}
	return false
}
