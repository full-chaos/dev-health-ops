# CHAOS-8135: names every package that took `minsec` seconds or more (default 90: 15% of the 600 s go test limit; a lower line flaps on runner noise, queryapiservice ran 12-36 s across CI legs) in a `go test -race` run and has NO row in the
# weights file (ci/go_race_weights.tsv). Such a package silently weighs the shard awk's default (2 s), so the shard plan
# hides its cost and the leg it lands on can pass the 600 s go test limit with no warning (httpguard: 324 s, no row).
#
#   awk -v mod=<module path> -v weights=<tsv> -v minsec=90 -f ci/go_race_unweighted.awk < go-test-output
#
# Reads `go test` result lines ("ok<TAB>pkg<TAB>12.3s", also FAIL); prints one "pkg<TAB>seconds" line per offender. An
# unreadable weights file is itself an error: no weights would make every package an offender, so it must fail loudly.
BEGIN {
  FS = "\t"
  if (minsec == "") minsec = 90
  if (weights == "") { print "go_race_unweighted: weights file not given" > "/dev/stderr"; bad = 1; exit 2 }
  nrows = 0
  while ((getline line < weights) > 0) {
    if (line ~ /^[[:space:]]*(#|$)/) continue
    split(line, f, "\t")
    w[f[1]] = 1
    nrows++
  }
  close(weights)
  if (nrows == 0) { print "go_race_unweighted: weights file holds no row: " weights > "/dev/stderr"; bad = 1; exit 2 }
}
$1 ~ /^(ok|FAIL)/ && $3 ~ /^[0-9.]+s/ {
  pkg = $2
  sec = $3
  sub(/s.*$/, "", sec)
  key = pkg
  if (key == mod) key = "."
  else if (index(key, mod "/") == 1) key = substr(key, length(mod) + 2)
  else next   # not a package of this module
  if ((sec + 0) >= (minsec + 0) && !(key in w)) printf "%s\t%s\n", key, sec
}
END { if (bad) exit 2 }
