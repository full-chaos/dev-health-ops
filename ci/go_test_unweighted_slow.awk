# CHAOS-10498: fail a plain (non-race) test leg when one of its packages ran >= thresh seconds and has no row in the test weights.
#
#   awk -v weights=<tsv> -v thresh=<seconds> -v label=<name for messages> -f ci/go_test_unweighted_slow.awk <plan> <go test log>
#
# plan: the shard's "module<TAB>package<TAB>key" lines (check_test_shard_plan); log: the leg's plain go test output, whose
# "ok"/"FAIL" summary lines are "<status><TAB><package><TAB><seconds>s" (a cached package has no seconds and is skipped).
# Weights rows are read as ci/go_race_shard.awk reads them: TAB-split, column 1, exact key, no comment or blank row. The failure
# prints the row to add (the measured seconds, rounded) so it can be pasted into the weights file.
# partial=1 (a leg whose go test run already failed: its log is cut short) keeps only the slow-package diagnostic and skips the
# coverage proof. A package of a nested module cannot be given a row (the weights are keyed by root-module package and the table
# test refuses any other key), so it is named with that reason and no row.
# Fail closed: a log with no package summary line, or with none for a package of the plan (a truncated log), cannot prove coverage
# and is refused. Exit 0 clean, 1 a package is named, 2 the weights, the plan or the log are unusable.
BEGIN {
  FS = "\t"
  if (weights == "") { print "go_test_unweighted_slow: weights file not given" > "/dev/stderr"; bad = 2; exit 2 }
  if (thresh == "") thresh = 30
  if (label == "") label = weights
  nrows = 0
  while ((getline line < weights) > 0) {
    if (line ~ /^[[:space:]]*(#|$)/) continue
    split(line, f, "\t")
    w[f[1]] = 1
    nrows++
  }
  close(weights)
  if (nrows == 0) { print "go_test_unweighted_slow: weights file holds no row: " weights > "/dev/stderr"; bad = 2; exit 2 }
}
FILENAME == ARGV[1] {
  if (NF >= 3) { key[$2] = $3; modof[$2] = $1; nplan++ }
  next
}
$1 ~ /^(ok|FAIL|\?)[ ]*$/ && NF >= 3 { nsummary++; seen[$2] = 1 }
$1 ~ /^(ok|FAIL)[ ]*$/ && ($2 in key) && $3 ~ /^[0-9]+(\.[0-9]+)?s$/ {
  sec = substr($3, 1, length($3) - 1) + 0
  if (sec >= thresh + 0 && !(key[$2] in w) && !($2 in named)) {
    named[$2] = 1
    if (modof[$2] != ".") {
      printf "nested-module-slow-package: %s took %ss (>= %s s) in module %s and has no row in %s; no row can be added: the weights are keyed by root-module package only; move the test to the root module or extend the weights table to nested modules\n", $2, sec, thresh, modof[$2], label > "/dev/stderr"
      rc = 1
      next
    }
    printf "unweighted-slow-package: %s took %ss (>= %s s) in this test leg and has no row in %s; add this row (measured seconds):\n%s\t%d\n", $2, sec, thresh, label, key[$2], int(sec + 0.5) > "/dev/stderr"
    rc = 1
  }
}
END {
  if (bad) exit bad
  if (nplan == 0) { print "go_test_unweighted_slow: the shard plan is empty: the guard read no package" > "/dev/stderr"; exit 2 }
  if (partial + 0) exit rc
  if (nsummary == 0) { print "go_test_unweighted_slow: the go test log holds no package summary line: the guard saw nothing" > "/dev/stderr"; exit 2 }
  for (p in key) if (!(p in seen)) { nmissing++; if (nmissing <= 5) missing = missing (missing == "" ? "" : ", ") p }
  if (nmissing > 0) {
    printf "go_test_unweighted_slow: the go test log has no summary line for %d of %d package(s) of the plan (truncated log?): %s%s\n", nmissing, nplan, missing, (nmissing > 5 ? ", ..." : "") > "/dev/stderr"
    rc = 2
  }
  exit rc
}
