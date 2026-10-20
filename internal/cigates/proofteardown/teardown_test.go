package proofteardown_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// CHAOS-5025/5362: drives the SHIPPED teardown functions, sliced by unique anchors, with real groups and signals.

const prelude = "set -euo pipefail\nTEARDOWN_WAIT_SECS=1\n"

var (
	libFuncs    = []string{"service_pgid", "stop_service", "stop_worker_stack"}
	scriptFuncs = []string{"cleanup", "on_signal"}
	junk        = []string{"abc", "12.5", "0", "-5", "999999", "60s"}
)

// teardownSource is service_pgid + stop_service + stop_worker_stack, sliced out of the shared library.
func teardownSource(g *guard) (string, bool) {
	if !g.definedOnce(libRel, libFuncs...) {
		return "", false
	}
	return g.slice(libRel, `^service_pgid\(\) \{`, `^seed_and_finalize_sync_targets\(\) \{`, false)
}

// waitSecsSource is the script's real mapping line plus validate_teardown_wait_secs, in the script's order.
func waitSecsSource(g *guard) (string, bool) {
	mapping, ok1 := g.line(scriptRel, `^TEARDOWN_WAIT_SECS=.*$`)
	fn, ok2 := g.slice(libRel, `^validate_teardown_wait_secs\(\) \{`, `^service_pgid\(\) \{`, false)
	return mapping + fn, ok1 && ok2
}

// composedSource is the library mechanism plus the script's cleanup/on_signal/trap wiring through its last trap line.
func composedSource(g *guard) (string, bool) {
	lib, ok1 := teardownSource(g)
	ok2 := g.definedOnce(scriptRel, scriptFuncs...)
	script, ok3 := g.slice(scriptRel, `^CLEANUP_DONE=""$`, `^trap 'on_signal TERM' TERM$`, true)
	return lib + script, ok1 && ok2 && ok3
}

func sub(body string, kv ...string) string { return strings.NewReplacer(kv...).Replace(body) }

func definedExactlyOnce(g *guard) {
	g.definedOnce(libRel, libFuncs...)
	g.definedOnce(scriptRel, scriptFuncs...)
}

const trapProbe = `trap 'echo CAUGHT' INT
kill -INT $$
sleep 0.2
echo AFTER
`

func shellCanTrapSIGINT(g *guard) {
	r, ok := g.bash(g.launch, trapProbe, 30*time.Second)
	if !ok {
		return
	}
	g.expect(strings.Contains(r.stdout, "CAUGHT"),
		"the shell under test could not trap SIGINT: it inherited SIGINT as IGNORED (the test was launched backgrounded, `cmd &`, and the launcher no longer resets it). stdout=%q stderr=%q", r.stdout, r.stderr)
}

func sigintIgnoredCannotBeTrapped(g *guard) {
	r, ok := g.bash(g.ignoring, trapProbe, 30*time.Second)
	if !ok {
		return
	}
	g.expect(!strings.Contains(r.stdout, "CAUGHT") && strings.Contains(r.stdout, "AFTER"),
		"a shell that inherited SIGINT ignored trapped it after all: %q", r.stdout)
}

func descendantReapedWhenServiceLeadsGroup(g *guard) {
	src, ok := teardownSource(g)
	if !ok {
		return
	}
	marker := filepath.Join(g.tmp, "child.pid")
	body := prelude + src + sub(`
set -m
( bash -c 'sh -c "trap \"\" TERM; while :; do sleep 1; done" & echo $! > @MARKER@; trap "exit 0" TERM; while :; do sleep 1; done' ) &
LEADER=$!
set +m
sleep 1
stop_service worker "$LEADER"
echo REACHED_END
`, "@MARKER@", marker)
	r, ok := g.bash(g.launch, body, 60*time.Second)
	if !ok {
		return
	}
	g.expect(strings.Contains(r.stdout, "REACHED_END"), "stop_service did not return: %s", r.stderr)
	child, ok := readPid(g, marker)
	if !ok {
		return
	}
	g.expect(diedWithin(child, 2*time.Second), "descendant %d outlived stop_service(): the service's process group was never swept", child)
}

func fallbackSignalsNonLeader(g *guard) {
	src, ok := teardownSource(g)
	if !ok {
		return
	}
	body := prelude + src + `
set +m
bash -c 'trap : TERM; while :; do sleep 1; done' & SERVICE=$!
sleep 0.3
stop_service fallback "$SERVICE"
echo REACHED_END
if kill -0 "$SERVICE" 2>/dev/null; then echo SERVICE_ALIVE; kill -KILL "$SERVICE"; else echo SERVICE_DEAD; fi
`
	r, ok := g.bash(g.launch, body, 60*time.Second)
	if !ok {
		return
	}
	if r.code != 0 || !strings.Contains(r.stdout, "REACHED_END") {
		g.failf("stop_service aborted before signalling a non-group-leader service (exit %d): %s", r.code, r.stderr)
		return
	}
	g.expect(strings.Contains(r.stdout, "SERVICE_DEAD"), "stop_service left a non-group-leader service running: %s", r.stdout)
}

func escalationWarning(g *guard) {
	src, ok := teardownSource(g)
	if !ok {
		return
	}
	body := prelude + src + `
set -m
( trap "" TERM; while :; do sleep 1; done ) & SERVICE=$!
set +m
sleep 0.3
stop_service stubborn "$SERVICE"
`
	r, ok := g.bash(g.launch, body, 60*time.Second)
	if !ok {
		return
	}
	g.expect(strings.Contains(r.stderr, "ignored SIGTERM") && strings.Contains(r.stderr, "escalated to SIGKILL"),
		"no escalation warning for a SIGTERM-ignoring service: %q", r.stderr)
}

func neverSignalsInitOrBogusPid(g *guard) {
	src, ok := teardownSource(g)
	if !ok {
		return
	}
	log := filepath.Join(g.tmp, "kill.log")
	body := prelude + src + sub(`
kill() { echo "KILL $*" >> @LOG@; return 0; }
stop_service none ""
stop_service bogus "not-a-pid"
stop_service negative "-1"
stop_service zero "0"
stop_service init "1"
echo REACHED_END
`, "@LOG@", log)
	r, ok := g.bash(g.launch, body, 60*time.Second)
	if !ok {
		return
	}
	g.expect(r.code == 0 && strings.Contains(r.stdout, "REACHED_END"), "stop_service failed on a refused pid (exit %d): %s", r.code, r.stderr)
	recorded, _ := readIfExists(log)
	g.expect(recorded == "", "stop_service signalled something it must refuse: %q", recorded)
}

func readIfExists(p string) (string, bool) {
	b, err := os.ReadFile(p)
	return string(b), err == nil
}

func waitSecs(g *guard, value string) (shResult, bool) {
	src, ok := waitSecsSource(g)
	if !ok {
		return shResult{}, false
	}
	body := "set -euo pipefail\nMETRICS_PROOF_TEARDOWN_WAIT_SECS=\"" + value + "\"\n" + src +
		"\nvalidate_teardown_wait_secs\necho \"resolved=${TEARDOWN_WAIT_SECS}\"\n"
	return g.bash(g.launch, body, 30*time.Second)
}

func waitSecsFailsClosed(bad string) func(*guard) {
	return func(g *guard) {
		r, ok := waitSecs(g, bad)
		if !ok {
			return
		}
		g.expect(r.code == 0, "validate_teardown_wait_secs aborted on %q (exit %d): %s", bad, r.code, r.stderr)
		g.expect(strings.Contains(r.stdout, "resolved=60") && strings.Contains(r.stderr, "WARNING"),
			"TEARDOWN_WAIT_SECS=%q did not fail closed to 60s with a WARNING: stdout %q stderr %q", bad, r.stdout, r.stderr)
	}
}

func emptyDefaultsSilently(g *guard) {
	r, ok := waitSecs(g, "")
	if !ok {
		return
	}
	g.expect(r.code == 0, "validate_teardown_wait_secs aborted on an empty override (exit %d): %s", r.code, r.stderr)
	g.expect(strings.Contains(r.stdout, "resolved=60") && !strings.Contains(r.stderr, "WARNING"),
		"an empty override must default to 60s silently: stdout %q stderr %q", r.stdout, r.stderr)
}

func validOverrideAccepted(g *guard) {
	r, ok := waitSecs(g, "45")
	if !ok {
		return
	}
	g.expect(strings.Contains(r.stdout, "resolved=45"), "a valid override 45 was not accepted: stdout %q stderr %q", r.stdout, r.stderr)
}

// shutdownTimeoutAtContractMinimum runs start_worker_stack against a stub dho that records its argv.
func shutdownTimeoutAtContractMinimum(g *guard) {
	src, ok := g.slice(libRel, `^start_worker_stack\(\) \{`, `^validate_teardown_wait_secs\(\) \{`, false)
	if !ok || !g.definedOnce(libRel, "start_worker_stack") {
		return
	}
	bin := filepath.Join(g.tmp, "bin")
	writeExec(g, filepath.Join(bin, "dho"), "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"${ARGS_DIR}/$1.args\"\n")
	body := sub(`set -euo pipefail
export ARGS_DIR=@TMP@
BIN_DIR=@BIN@ ROOT_DIR=@TMP@
RIVER_DOMAIN_ROLE=d RIVER_DOMAIN_PASSWORD=p RIVER_QUEUE_ROLE=q RIVER_QUEUE_PASSWORD=p
RIVER_COORDINATOR_ROLE=c RIVER_COORDINATOR_PASSWORD=p POSTGRES_HOST=h POSTGRES_PORT=1 POSTGRES_DB=db
CLICKHOUSE_URI_NATIVE=x VALKEY_HOST=h VALKEY_PORT=1 SETTINGS_ENCRYPTION_KEY=k WORKER_HTTP_PORT=1 RECONCILER_HTTP_PORT=2
WORKER_PID="" RECONCILER_PID=""
wait_for_http_ready() { :; }
`, "@TMP@", g.tmp, "@BIN@", bin) + src + sub(`
start_worker_stack @TMP@/worker.log @TMP@/reconciler.log
wait "$WORKER_PID"
wait "$RECONCILER_PID"
`, "@TMP@", g.tmp)
	r, ok := g.bash(g.launch, body, 30*time.Second)
	if !ok {
		return
	}
	args, err := os.ReadFile(filepath.Join(g.tmp, "worker.args"))
	if err != nil {
		g.failf("start_worker_stack did not run the worker (exit %d): %s", r.code, r.stderr)
		return
	}
	var got []string
	for _, a := range strings.Split(string(args), "\n") {
		if strings.HasPrefix(a, "--shutdown-timeout") {
			got = append(got, a)
		}
	}
	g.expect(len(got) == 1 && got[0] == "--shutdown-timeout=7260s",
		"the worker's --shutdown-timeout must stay at the contract minimum 7260s (7200s longest selected timeout + 60s workerFinalizationBuffer; see internal/workerservice/dependencies.go), got %q", got)
}

func cancelledRunTearsDownAndExits(g *guard) {
	src, ok := composedSource(g)
	if !ok {
		return
	}
	marker := filepath.Join(g.tmp, "resumed")
	workerPid := filepath.Join(g.tmp, "worker.pid")
	body := `set -euo pipefail
TMP_DIR="$(mktemp -d)"
API_PID=""; WORKER_PID=""; RECONCILER_PID=""
TEARDOWN_WAIT_SECS=1
` + src + sub(`
set -m
( trap "" TERM; while :; do sleep 1; done ) >/dev/null 2>&1 & WORKER_PID=$!
echo "$WORKER_PID" > @WPID@
set +m
( sleep 0.3; kill -INT $$ ) &
sleep 5
echo RESUMED > @MARKER@
`, "@WPID@", workerPid, "@MARKER@", marker)
	// ~6.1 s: bash defers the INT trap until sleep 5 ends, then the teardown waits 1 s; 20 s is a hang.
	r, ok := g.bash(g.launch, body, 20*time.Second)
	if !ok {
		return
	}
	if _, err := os.ReadFile(marker); err == nil {
		g.failf("the script resumed after a cancellation signal (elapsed %s)", r.elapsed.Round(100*time.Millisecond))
	}
	g.expect(r.signal == syscall.SIGINT || r.code == 130,
		"want death-by-SIGINT so the run reports cancellation rather than success, got exit %d signal %v (stderr %q)", r.code, r.signal, r.stderr)
	pid, ok := readPid(g, workerPid)
	if !ok {
		return
	}
	g.expect(diedWithin(pid, 3*time.Second), "the cancelled run exited but left the worker (pid %d) running: the trap did not tear down the services", pid)
}

// boundSecs is TEARDOWN_WAIT_SECS for the disposition check: long enough that paying it is unmistakable.
const boundSecs = 8

func teardownFastUnderCleanupDisposition(g *guard) {
	src, ok := teardownSource(g)
	if !ok {
		return
	}
	// Services are forked FIRST (they must not inherit the ignore), then the disposition is set as cleanup() does.
	body := "set -euo pipefail\nTEARDOWN_WAIT_SECS=" + strconv.Itoa(boundSecs) + "\n" + src + `
set -m
( trap "" TERM; while :; do sleep 1; done ) & STUBBORN=$!
( while :; do sleep 1; done ) & PROMPT=$!
set +m
sleep 0.3
trap '' INT TERM
start=$SECONDS
stop_service prompt "$PROMPT"
echo "PROMPT_ELAPSED=$((SECONDS - start))"
start=$SECONDS
stop_service stubborn "$STUBBORN"
echo "STUBBORN_ELAPSED=$((SECONDS - start))"
`
	r, ok := g.bash(g.launch, body, 60*time.Second)
	if !ok {
		return
	}
	g.expect(r.code == 0, "the disposition run failed (exit %d): %s", r.code, r.stderr)
	prompt, ok1 := elapsedOf(g, r.stdout, "PROMPT_ELAPSED")
	stubborn, ok2 := elapsedOf(g, r.stdout, "STUBBORN_ELAPSED")
	if !ok1 || !ok2 {
		return
	}
	g.expect(prompt <= boundSecs*3/8,
		"a service that exits promptly took %ds against a %ds bound: the watchdog was not cancelled (SIGTERM ignored via the inherited disposition?)", prompt, boundSecs)
	g.expect(stubborn >= boundSecs,
		"a SIGTERM-ignoring service took only %ds against a %ds bound: the bound is not being honoured", stubborn, boundSecs)
	g.expect(strings.Contains(r.stderr, "ignored SIGTERM"), "no escalation warning for the stubborn service: %q", r.stderr)
}

func elapsedOf(g *guard, stdout, key string) (int, bool) {
	m := regexp.MustCompile(key + `=(\d+)`).FindStringSubmatch(stdout)
	if m == nil {
		g.failf("no %s in %q: the script died before reaching it", key, stdout)
		return 0, false
	}
	n, _ := strconv.Atoi(m[1])
	return n, true
}

func checks() []check {
	cs := []check{
		{"defined_exactly_once", definedExactlyOnce},
		{"shell_can_trap_sigint", shellCanTrapSIGINT},
		{"sigint_ignored_cannot_be_trapped", sigintIgnoredCannotBeTrapped},
		{"descendant_reaped_when_service_leads_group", descendantReapedWhenServiceLeadsGroup},
		{"fallback_signals_non_group_leader", fallbackSignalsNonLeader},
		{"escalation_warning_for_sigterm_ignoring_service", escalationWarning},
		{"never_signals_init_or_bogus_pid", neverSignalsInitOrBogusPid},
		{"empty_override_defaults_silently", emptyDefaultsSilently},
		{"valid_override_accepted", validOverrideAccepted},
		{"shutdown_timeout_at_contract_minimum", shutdownTimeoutAtContractMinimum},
		{"cancelled_run_tears_down_and_exits", cancelledRunTearsDownAndExits},
		{"teardown_fast_under_cleanup_disposition", teardownFastUnderCleanupDisposition},
	}
	for _, bad := range junk {
		cs = append(cs, check{"wait_secs_fails_closed/" + bad, waitSecsFailsClosed(bad)})
	}
	return cs
}

func TestTeardownRealTree(t *testing.T) {
	t.Parallel()
	runReal(t, checks()...)
}

func TestTeardownPlantedDefects(t *testing.T) {
	t.Parallel()
	const (
		pgidLine = "  pgid=\"$(service_pgid \"${pid}\")\" || pgid=\"\"\n"
		waitLine = "  wait \"${pid}\" >/dev/null 2>&1 || true\n"
	)
	var cases []planted
	for _, decoy := range []struct{ name, text string }{
		{"paren", "stop_service() {\n  return 0\n}\n"},
		{"paren-spaced", "stop_service  ()  {\n  return 0\n}\n"},
		{"keyword", "function stop_service {\n  return 0\n}\n"},
		{"keyword-paren", "function stop_service() {\n  return 0\n}\n"},
	} {
		// Bash runs the LAST definition, so a shadow after the real one replaces it while a slice tests the first.
		cases = append(cases, planted{
			name: "shadowing stop_service/" + decoy.name, copy: subject,
			mutate: mut(libRel, "# seed_and_finalize_sync_targets target...", decoy.text+"# seed_and_finalize_sync_targets target..."),
			check:  definedExactlyOnce, want: "stop_service is defined 2x",
		})
	}
	cases = append(cases,
		planted{
			name: "shadowing on_signal in the script", copy: subject,
			mutate: mut(scriptRel, "trap cleanup EXIT\n", "function on_signal {\n  :\n}\ntrap cleanup EXIT\n"),
			check:  definedExactlyOnce, want: "on_signal is defined 2x",
		},
		planted{
			name: "a shadow inside the slice also stops the executed checks", copy: subject,
			mutate: mut(libRel, "stop_worker_stack() {", "function stop_service {\n  return 0\n}\nstop_worker_stack() {"),
			check:  fallbackSignalsNonLeader, want: "stop_service is defined 2x",
		},
		planted{
			name: "duplicate slice anchor", copy: subject,
			mutate: mut(libRel, "# seed_and_finalize_sync_targets target...", "seed_and_finalize_sync_targets() {\n  :\n}\n# seed_and_finalize_sync_targets target..."),
			check:  escalationWarning, want: "occurs 2x, want exactly 1",
		},
		planted{
			name: "launcher no longer resets an inherited SIGINT ignore", copy: subject,
			launch: ignoredSIGINT, check: shellCanTrapSIGINT, want: "could not trap SIGINT",
		},
		planted{
			name: "control launcher no longer leaves SIGINT ignored", copy: subject,
			ignoring: defaultSignals, check: sigintIgnoredCannotBeTrapped, want: "trapped it after all",
		},
		planted{
			// Historical (CHAOS-5025): ps reports nothing for a reaped leader, so the group is never swept.
			name: "pgid resolved after wait", copy: subject,
			mutate: all(mut(libRel, pgidLine, ""), mut(libRel, waitLine, waitLine+pgidLine)),
			check:  descendantReapedWhenServiceLeadsGroup, want: "outlived stop_service",
		},
		planted{
			name: "group not swept after the leader exits", copy: subject,
			mutate: mut(libRel, waitLine+"  local elapsed=$((SECONDS - started))\n\n  if [ -n \"${pgid}\" ]; then kill -KILL -- -\"${pgid}\" >/dev/null 2>&1 || true; fi\n",
				waitLine+"  local elapsed=$((SECONDS - started))\n\n"),
			check: descendantReapedWhenServiceLeadsGroup, want: "outlived stop_service",
		},
		planted{
			// Historical (codex r2 P2): needs the && ending AND the then-unguarded caller assignment to bite.
			name: "service_pgid ends on an && chain under set -e", copy: subject,
			mutate: all(
				mut(libRel, "  if [ -n \"${pgid}\" ] && [ \"${pgid}\" = \"${pid}\" ]; then\n    printf '%s' \"${pgid}\"\n  fi\n  return 0\n}",
					"  [ -n \"${pgid}\" ] && [ \"${pgid}\" = \"${pid}\" ] && printf '%s' \"${pgid}\"\n}"),
				mut(libRel, pgidLine, "  pgid=\"$(service_pgid \"${pid}\")\"\n")),
			check: fallbackSignalsNonLeader, want: "stop_service aborted",
		},
		planted{
			name: "elapsed measured after wait", copy: subject,
			mutate: all(mut(libRel, "  local started=\"${SECONDS}\"\n", ""),
				mut(libRel, waitLine, waitLine+"  local started=\"${SECONDS}\"\n")),
			check: escalationWarning, want: "no escalation warning",
		},
		planted{
			name: "pid 0/1 guard dropped from stop_service", copy: subject,
			mutate: mut(libRel, "  [ \"${pid}\" -gt 1 ] || return 0\n  kill -0", "  kill -0"),
			check:  neverSignalsInitOrBogusPid, want: "signalled something it must refuse",
		},
		planted{
			name: "integer check dropped", copy: subject,
			mutate: mut(libRel, "if ! printf '%s' \"${TEARDOWN_WAIT_SECS}\" | grep -Eq '^[0-9]+$' \\\n     || [", "if ["),
			check:  waitSecsFailsClosed("abc"), want: "did not fail closed",
		},
		planted{
			name: "lower bound dropped", copy: subject,
			mutate: mut(libRel, " \\\n     || [ \"${TEARDOWN_WAIT_SECS}\" -lt 1 ]", " \\\n    "),
			check:  waitSecsFailsClosed("0"), want: "did not fail closed",
		},
		planted{
			name: "upper bound dropped", copy: subject,
			mutate: mut(libRel, " || [ \"${TEARDOWN_WAIT_SECS}\" -gt 3600 ]", ""),
			check:  waitSecsFailsClosed("999999"), want: "did not fail closed",
		},
		planted{
			name: "caller maps an empty override as set", copy: subject,
			mutate: mut(scriptRel, "${METRICS_PROOF_TEARDOWN_WAIT_SECS:-60}", "${METRICS_PROOF_TEARDOWN_WAIT_SECS-60}"),
			check:  emptyDefaultsSilently, want: "default to 60s silently",
		},
		planted{
			name: "upper bound too low", copy: subject,
			mutate: mut(libRel, "-gt 3600", "-gt 30"),
			check:  validOverrideAccepted, want: "valid override 45 was not accepted",
		},
		planted{
			name: "shutdown timeout lowered", copy: subject,
			mutate: mut(libRel, "--shutdown-timeout=7260s", "--shutdown-timeout=30s"),
			check:  shutdownTimeoutAtContractMinimum, want: "contract minimum 7260s",
		},
		planted{
			name: "on_signal does not re-raise", copy: subject,
			mutate: mut(scriptRel, "  kill -\"${sig}\" \"$$\"\n", ""),
			check:  cancelledRunTearsDownAndExits, want: "resumed after a cancellation signal",
		},
		planted{
			name: "cleanup bound directly to INT", copy: subject,
			mutate: mut(scriptRel, "trap 'on_signal INT' INT\n", "trap cleanup INT\n"),
			check:  cancelledRunTearsDownAndExits, want: "resumed after a cancellation signal",
		},
		planted{
			name: "cleanup does not stop the worker stack", copy: subject,
			mutate: mut(scriptRel, "  stop_worker_stack\n", ""),
			check:  cancelledRunTearsDownAndExits, want: "left the worker",
		},
		planted{
			// Historical (run 33869945044): the watchdog inherits cleanup()'s ignored TERM, so TERM cannot cancel it.
			name: "watchdog cancelled with SIGTERM", copy: subject,
			mutate: mut(libRel, "  kill -KILL \"${watchdog}\"", "  kill -TERM \"${watchdog}\""),
			check:  teardownFastUnderCleanupDisposition, want: "watchdog was not cancelled",
		},
		planted{
			name: "stop_service does not wait for the service", copy: subject,
			mutate: mut(libRel, waitLine, ""),
			check:  teardownFastUnderCleanupDisposition, want: "bound is not being honoured",
		},
	)
	runPlanted(t, cases)
}
