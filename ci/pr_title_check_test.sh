#!/usr/bin/env bash
# CHAOS-12791: cells for ci/pr_title_check.sh (the house PR-title rule, root AGENTS.md).
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
script="$repo_root/ci/pr_title_check.sh"
fails=0

# run <name> <want: pass|fail> <author> <title>
run() {
  local name=$1 want=$2 author=$3 title=$4 rc=0
  EVENT=pull_request AUTHOR="$author" TITLE="$title" bash "$script" >/dev/null 2>&1 || rc=$?
  if { [ "$want" = pass ] && [ "$rc" = 0 ]; } || { [ "$want" = fail ] && [ "$rc" != 0 ]; }; then
    echo "ok   $name"
  else
    echo "FAIL $name (want $want, rc=$rc)"; fails=$((fails + 1))
  fi
}

# 1. titles from the evidence list (no type, no id) fail
run bad-evidence-no-id-no-type fail chaos 'authruntime: pre-fault control login of the JWKS stall test needs no p'
run bad-evidence-docs-no-id fail chaos 'docs: one-off operator Pod for dho workers matches the prod form'
run bad-evidence-typed-no-id fail chaos 'chore(ops): roll go-api to the ops c2097f94 build'
# 2. a valid title passes
run valid pass chaos 'chore(ci): CHAOS-12791 add a required PR-title check'
run valid-every-type pass chaos 'enhancement(deploy): CHAOS-1 roll web'
# 3. dependabot is exempt by author, the same title from a human fails
run dependabot-exempt pass 'dependabot[bot]' 'Bump actions/checkout from 4 to 5'
run human-same-title fail chaos 'Bump actions/checkout from 4 to 5'
# 4. two ids fail (different and repeated)
run two-ids fail chaos 'fix(ci): CHAOS-1 CHAOS-2 roll web'
run same-id-twice fail chaos 'fix(ci): CHAOS-1 roll web for CHAOS-1'
# every allowed type passes (the full type set is pinned)
for ty in feat bug enhancement chore fix docs; do
  run "every-type-$ty" pass chaos "$ty(ci): CHAOS-1 add the check"
done
# the exemption is exactly dependabot[bot]
run other-bot-invalid fail 'otherbot[bot]' 'Bump x from 1 to 2'
run other-bot-valid pass 'otherbot[bot]' 'chore(ci): CHAOS-1 add x'
# the first word of the description is not an article, determiner or pronoun
run declarative-the fail chaos 'fix(ci): CHAOS-7 the process sometimes exits incorrectly'
run declarative-we fail chaos 'fix(ci): CHAOS-7 we need the title rule'
run declarative-upper fail chaos 'fix(ci): CHAOS-7 The title rule is wrong'
run declarative-its fail chaos 'fix(ci): CHAOS-7 its exit code is wrong'
run imperative-add pass chaos 'fix(ci): CHAOS-7 add a required check'
run imperative-make pass chaos 'fix(ci): CHAOS-7 make the check required'
run imperative-keep pass chaos 'fix(ci): CHAOS-7 keep the pattern in one place'
# extras
run bad-type fail chaos 'feature(ci): CHAOS-1 roll web'
run no-description fail chaos 'fix(ci): CHAOS-1'
run no-area fail chaos 'fix: CHAOS-1 roll web'

# CHAOS-13180: the title's ticket must not own a live temporary or decision row of the exception ledger
ledger_dir=$(mktemp -d)
printf '# id\tkind\tpath\tneedle\towner\towner_state\treason\nrow-a\ttemporary\tp\tn\tCHAOS-77\topen\tr\nrow-d\tdecision\tp\tn\tCHAOS-78\topen\tr\nrow-p\tpermanent\tp\tn\tCHAOS-79\t-\tr\n# row-c\ttemporary\tp\tn\tCHAOS-80\topen\tr\n' >"$ledger_dir/ledger.tsv"
# lg <name> <want> <title> [ledger]
lg() {
  local name=$1 want=$2 title=$3 path=${4:-$ledger_dir/ledger.tsv} rc=0
  EVENT=pull_request AUTHOR=chaos TITLE="$title" EXCEPTION_LEDGER="$path" bash "$script" >/dev/null 2>&1 || rc=$?
  if { [ "$want" = pass ] && [ "$rc" = 0 ]; } || { [ "$want" = fail ] && [ "$rc" != 0 ]; }; then
    echo "ok   $name"
  else
    echo "FAIL $name (want $want, rc=$rc)"; fails=$((fails + 1))
  fi
}
lg ledger-temporary-owner fail 'chore(ci): CHAOS-77 add the check'
lg ledger-decision-owner fail 'chore(ci): CHAOS-78 add the check'
lg ledger-permanent-owner pass 'chore(ci): CHAOS-79 add the check'
lg ledger-commented-row pass 'chore(ci): CHAOS-80 add the check'
lg ledger-prefix-is-not-the-id pass 'chore(ci): CHAOS-7 add the check'
lg ledger-other-id pass 'chore(ci): CHAOS-1 add the check'
lg ledger-missing fail 'chore(ci): CHAOS-1 add the check' "$ledger_dir/absent.tsv"
lg ledger-empty-title fail ''
lg ledger-real-file pass 'chore(ci): CHAOS-1 add the check' "$repo_root/ci/auth_exception_ledger.tsv"
rm -rf "$ledger_dir"

# merge_group: the PR is read by REST (stub gh); the gh-readonly-queue ref names the PR number
fakebin=$(mktemp -d)
trap 'rm -rf "$fakebin"' EXIT
cat >"$fakebin/gh" <<'GH'
#!/usr/bin/env bash
printf '%s\t%s\n' "$FAKE_GH_AUTHOR" "$FAKE_GH_TITLE"
GH
chmod +x "$fakebin/gh"
# mg <name> <want> <head_ref> <author> <title>
mg() {
  local name=$1 want=$2 ref=$3 author=$4 title=$5 rc=0
  PATH="$fakebin:$PATH" EVENT=merge_group REPO=full-chaos/dev-health-ops HEAD_REF="$ref" FAKE_GH_AUTHOR="$author" FAKE_GH_TITLE="$title" \
    bash "$script" >/dev/null 2>&1 || rc=$?
  if { [ "$want" = pass ] && [ "$rc" = 0 ]; } || { [ "$want" = fail ] && [ "$rc" != 0 ]; }; then
    echo "ok   $name"
  else
    echo "FAIL $name (want $want, rc=$rc)"; fails=$((fails + 1))
  fi
}
mg merge-group-valid pass refs/heads/gh-readonly-queue/main/pr-5600-abc123 chaos 'fix(ci): CHAOS-1 roll web'
mg merge-group-bad-title fail refs/heads/gh-readonly-queue/main/pr-5600-abc123 chaos 'authruntime: no id'
mg merge-group-dependabot pass refs/heads/gh-readonly-queue/main/pr-5600-abc123 'dependabot[bot]' 'Bump x from 1 to 2'
mg merge-group-unknown-ref fail refs/heads/other chaos 'fix(ci): CHAOS-1 roll web'

[ "$fails" = 0 ] || { echo "$fails cell(s) failed" >&2; exit 1; }
