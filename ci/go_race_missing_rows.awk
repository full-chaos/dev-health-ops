# CHAOS-8135: prints every package of the input that has NO row in the weights file (ci/go_race_weights.tsv).
#
#   awk -v mod=<module path> -v weights=<tsv> -f ci/go_race_missing_rows.awk < packages
#
# Reads Go import paths (one per line, as `go list ./...` prints them). A package with no row would silently weigh the
# shard awk's default (2 s) and hide its cost from the shard plan; the leg that gets it can then pass the 600 s go test
# limit (httpguard: 324 s, no row). The check is static, so it is deterministic: no timing, no runner noise. An unreadable
# or empty weights file, or an empty package list, is itself an error: nothing would be measured.
BEGIN {
  FS = "\t"
  if (weights == "") { print "go_race_missing_rows: weights file not given" > "/dev/stderr"; bad = 1; exit 2 }
  nrows = 0
  while ((getline line < weights) > 0) {
    if (line ~ /^[[:space:]]*(#|$)/) continue
    split(line, f, "\t")
    w[f[1]] = 1
    nrows++
  }
  close(weights)
  if (nrows == 0) { print "go_race_missing_rows: weights file holds no row: " weights > "/dev/stderr"; bad = 1; exit 2 }
}
$0 != "" {
  n++
  key = $0
  if (key == mod) key = "."
  else if (index(key, mod "/") == 1) key = substr(key, length(mod) + 2)
  if (!(key in w)) print key
}
END {
  if (bad) exit 2
  if (n == 0) { print "go_race_missing_rows: the package list is empty" > "/dev/stderr"; exit 2 }
}
