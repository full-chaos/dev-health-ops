# CHAOS-10498: fail a plain (non-race) test leg when one of its packages ran >= thresh seconds and has no row in the test weights.
#
#   awk -v weights=<tsv> -v thresh=<seconds> -v label=<name for messages> -f ci/go_test_unweighted_slow.awk <plan> <go test log>
#
# plan: the shard's "module<TAB>package<TAB>key" lines (check_test_shard_plan); log: the leg's plain go test output, whose
# "ok"/"FAIL" summary lines are "<status><TAB><package><TAB><seconds>s" (a cached package has no seconds and is skipped).
# Weights rows are read as ci/go_race_shard.awk reads them: TAB-split, column 1, exact key, no comment or blank row. The failure
# prints the row to add (the measured seconds, rounded) so it can be pasted into the weights file.
# Exit 0 clean, 1 a package is named, 2 the weights or the plan are unusable.
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
FNR == NR {
  if (NF >= 3) { key[$2] = $3; nplan++ }
  next
}
$1 ~ /^(ok|FAIL)[ ]*$/ && ($2 in key) && $3 ~ /^[0-9]+(\.[0-9]+)?s$/ {
  sec = substr($3, 1, length($3) - 1) + 0
  if (sec >= thresh + 0 && !(key[$2] in w) && !(key[$2] in named)) {
    named[key[$2]] = 1
    printf "unweighted-slow-package: %s took %ss (>= %s s) in this test leg and has no row in %s; add this row (measured seconds):\n%s\t%d\n", $2, sec, thresh, label, key[$2], int(sec + 0.5) > "/dev/stderr"
    rc = 1
  }
}
END {
  if (bad) exit bad
  if (nplan == 0) { print "go_test_unweighted_slow: the shard plan is empty: the guard read no package" > "/dev/stderr"; exit 2 }
  exit rc
}
