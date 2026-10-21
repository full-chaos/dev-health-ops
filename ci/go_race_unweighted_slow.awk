# CHAOS-10481: fail the providersync race leg when a top-level test took >= thresh seconds and has no row in the weights file.
#
#   awk -v weights=<tsv> -v thresh=<seconds> -v label=<name for messages> -f ci/go_race_unweighted_slow.awk <go test -json file>
#
# Rows are looked up exactly as ci/go_providersync_race_shard.awk looks them up: TAB-split, column 1, exact name, no
# comment or blank row. Only top-level tests count (a name with "/" is a subtest: its parent's Elapsed holds it). A top-level
# test that began (run) and never ended is named too: a package timeout or a hang leaves exactly such a test.
# Exit 0 clean, 1 a test is named, 2 the input or the weights are unusable (a guard that read nothing must not pass).
function field(line, key,    m) {
  if (match(line, "\"" key "\":\"[^\"]*\"")) { m = substr(line, RSTART + length(key) + 4, RLENGTH - length(key) - 5); return m }
  return ""
}
BEGIN {
  if (weights == "") { print "go_race_unweighted_slow: weights file not given" > "/dev/stderr"; bad = 2; exit 2 }
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
  if (nrows == 0) { print "go_race_unweighted_slow: weights file holds no row: " weights > "/dev/stderr"; bad = 2; exit 2 }
}
/^\{("Time":"[^"]*",)?"Action":"(run|pass|fail|skip)"/ {
  test = field($0, "Test")
  if (test == "" || index(test, "/") > 0) next
  action = field($0, "Action")
  if (action == "run") { running[test] = 1; next }
  delete running[test]
  results++
  if (action == "skip") next
  if (match($0, /"Elapsed":[0-9.eE+-]+/)) {
    sec = substr($0, RSTART + 10, RLENGTH - 10)
    if (sec + 0 >= thresh + 0 && !(test in w)) slow[test] = sec
  }
}
END {
  if (bad) exit bad
  if (results == 0 && length(running) == 0) {
    print "go_race_unweighted_slow: the go test -json stream holds no top-level test result: the guard saw nothing" > "/dev/stderr"
    exit 2
  }
  rc = 0
  for (t in slow) {
    printf "unweighted-slow-test: %s took %ss (>= %s s) and has no row in %s: add a row to ci/go_providersync_race_weights.tsv (median ms of 3 race runs)\n", t, slow[t], thresh, label > "/dev/stderr"
    rc = 1
  }
  for (t in running) {
    printf "unfinished-test: %s began and never ended (timeout or hang)%s\n", t, (t in w) ? "" : "; it has no row in " label > "/dev/stderr"
    rc = 1
  }
  exit rc
}
