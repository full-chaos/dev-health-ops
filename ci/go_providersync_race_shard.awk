# CHAOS-8166: weight-balanced test-name shard of internal/providersync for `check_go.sh race SHARD COUNT`.
#
#   awk -v shard=K -v count=N -v weights=<tsv> -v defw=<default ms> -f ci/go_providersync_race_shard.awk < test-names
#
# Reads top-level Test names (one per line, as `go test -list` prints them) and prints the ones of shard K of N (1-based).
# Longest-processing-time assignment: tests sorted by (weight desc, name asc), each handed to the currently lightest shard
# (lowest index on a tie). The result is a PARTITION of the input whatever the weights say: a wrong weight only unbalances
# the shards, it can never drop or duplicate a test. An unreadable or empty weights file is an error.
BEGIN {
  FS = "\t"
  if (weights == "") { print "go_providersync_race_shard: weights file not given" > "/dev/stderr"; bad = 1; exit 2 }
  if (defw == "") defw = 20
  nrows = 0
  while ((getline line < weights) > 0) {
    if (line ~ /^[[:space:]]*(#|$)/) continue
    split(line, f, "\t")
    w[f[1]] = f[2] + 0
    nrows++
  }
  close(weights)
  if (nrows == 0) { print "go_providersync_race_shard: weights file holds no row: " weights > "/dev/stderr"; bad = 1; exit 2 }
}
$0 ~ /^Test/ {
  n++
  name[n] = $0
  weight[n] = ($0 in w) ? w[$0] : defw + 0
}
END {
  if (bad) exit 2
  for (i = 1; i <= n; i++) order[i] = i
  for (i = 2; i <= n; i++) {
    v = order[i]
    j = i - 1
    while (j >= 1 && (weight[order[j]] < weight[v] || (weight[order[j]] == weight[v] && name[order[j]] > name[v]))) {
      order[j + 1] = order[j]
      j--
    }
    order[j + 1] = v
  }
  for (s = 1; s <= count; s++) load[s] = 0
  for (i = 1; i <= n; i++) {
    k = order[i]
    best = 1
    for (s = 2; s <= count; s++) if (load[s] < load[best]) best = s
    load[best] += weight[k]
    owner[k] = best
  }
  for (i = 1; i <= n; i++) if (owner[i] == shard) print name[i]
}
