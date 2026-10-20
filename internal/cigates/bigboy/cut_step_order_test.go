package bigboy_test

import (
	"strings"
	"testing"
)

const cutScript = "ci/bigboy/bigboy-cut.sh"

// The values-driven STEPs (and so the first compose config read) run only with a pinned DEPLOY_SHA.
var cutDeployEnv = []string{"DEPLOY_SHA=" + strings.Repeat("d", 40)}

func cutOperatorResolved(c cutCall) bool {
	return c.kind == "docker" && c.has("--raw") && strings.Contains(c.line(), "dev-health-go-operator:sha-")
}

func cutCheckExportBeforeConfigRead(g *guard) {
	r := cutRunCut(g, cutOpts{env: cutDeployEnv})
	i := cutFind(r.calls, 0, cutTool("compose-config-redacted.sh"))
	if i < 0 {
		g.failf("the compose config was never read: %s", r.dump())
		return
	}
	g.expect(r.calls[i].opImage == cutOperatorRef,
		"BIGBOY_OPERATOR_IMAGE is %q at the first compose config read, want the resolved operator %q", r.calls[i].opImage, cutOperatorRef)
}

func cutCheckExportOnce(g *guard) {
	r := cutRunCut(g, cutOpts{env: cutDeployEnv})
	at := cutFind(r.calls, 0, cutOperatorResolved)
	if at < 0 || len(r.calls) <= at+1 {
		g.failf("the operator digest was never resolved, or nothing ran after it: %s", r.dump())
		return
	}
	for _, c := range r.calls[at+1:] {
		g.expect(c.opImage == cutOperatorRef, "BIGBOY_OPERATOR_IMAGE changed after the operator resolve: %q at `%s`, want %q", c.opImage, c.line(), cutOperatorRef)
	}
}

func cutCheckEmptyOperatorDigest(g *guard) {
	r := cutRunCut(g, cutOpts{env: cutDeployEnv, digests: cutWithout(cutDefaultDigests(), "dev-health-go-operator:sha-")})
	g.expect(r.code == 1, "an empty operator digest must fail the cut rc=1, got rc=%d", r.code)
	g.expect(strings.Contains(r.stdout, "STEP operator-resolve rc=1"), "no `STEP operator-resolve rc=1` line: %s", r.stdout)
	g.expect(strings.Contains(r.stderr, "FAIL: no operator digest"), "the empty operator digest is not named on stderr: %s", r.stderr)
	if at := cutFind(r.calls, 0, cutOperatorResolved); at >= 0 && at+1 < len(r.calls) {
		g.failf("the cut went on after an empty operator digest: `%s`", r.calls[at+1].line())
	}
}

func cutCheckConfigFailureNotBlank(g *guard) {
	r := cutRunCut(g, cutOpts{env: append([]string{cutVar("CUT_RC", "compose-config-redacted.sh") + "=1"}, cutDeployEnv...)})
	g.expect(strings.Contains(r.stdout, "STEP query-api-secret-refs-nonblank rc=1"), "a failed compose config read must fail the secret-refs STEP: %s", r.stdout)
	g.expect(strings.Contains(r.stderr, "compose config did not resolve"), "a failed compose config read is not named as a config failure: %s", r.stderr)
	g.expect(!strings.Contains(r.stderr, "resolve BLANK"), "a failed compose config read was read as a blank-secret verdict: %s", r.stderr)
}

func TestCutStepOrder(t *testing.T) {
	runReal(t,
		check{"operator_image_exported_before_first_compose_config_read", cutCheckExportBeforeConfigRead},
		check{"operator_image_exported_once", cutCheckExportOnce},
		check{"empty_operator_digest_fails_loud", cutCheckEmptyOperatorDigest},
		check{"config_read_failure_is_not_a_blank_verdict", cutCheckConfigFailureNotBlank},
	)
}

const cutExportLine = `export BIGBOY_OPERATOR_IMAGE=ghcr.io/full-chaos/dev-health-go-operator@$BIGBOY_OPERATOR_DIGEST; `

func TestCutStepOrderPlanted(t *testing.T) {
	cp := []string{"ci/bigboy"}
	runPlanted(t, []planted{
		{
			name: "operator export moved after the config read", copy: cp, check: cutCheckExportBeforeConfigRead,
			mutate: all(mut(cutScript, cutExportLine, ""), mut(cutScript, `docker pull -q "$BIGBOY_OPERATOR_IMAGE"`, cutExportLine+`docker pull -q "$BIGBOY_OPERATOR_IMAGE"`)),
			want:   "at the first compose config read",
		},
		{
			name: "operator image exported a second time", copy: cp, check: cutCheckExportOnce,
			mutate: mut(cutScript, `"${DC[@]}" run --rm --no-deps -T migrate`,
				"export BIGBOY_OPERATOR_IMAGE=ghcr.io/full-chaos/dev-health-go-operator@sha256:"+strings.Repeat("f", 64)+"\n"+`"${DC[@]}" run --rm --no-deps -T migrate`),
			want: "BIGBOY_OPERATOR_IMAGE changed",
		},
		{
			name: "empty operator digest does not exit", copy: cp, check: cutCheckEmptyOperatorDigest,
			mutate: mut(cutScript, `st operator-resolve 1; echo "FAIL: no operator digest for sha-$S7" >&2; exit 1 ;;`, `st operator-resolve 1 ;;`),
			want:   "went on after an empty operator digest",
		},
		{
			name: "config read status dropped", copy: cp, check: cutCheckConfigFailureNotBlank,
			mutate: mut(cutScript, `RED_RC=$?`, `RED_RC=0`),
			want:   "read as a blank-secret verdict",
		},
		{
			name: "BLANK decided before the config failure", copy: cp, check: cutCheckConfigFailureNotBlank,
			mutate: mut(cutScript, `if [ "$RED_RC" -ne 0 ]; then`, `if [ -n "$BLANK" ]; then echo "DRIFT: resolve BLANK:$BLANK" >&2; elif [ "$RED_RC" -ne 0 ]; then`),
			want:   "read as a blank-secret verdict",
		},
	})
}
