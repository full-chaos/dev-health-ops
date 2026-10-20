# Deterministic, weight-balanced package shard for `check_go.sh race SHARD COUNT`
# (CHAOS-6690). Reads Go import paths (one per line) on stdin and prints the
# ones that belong to shard `shard` of `count` (both 1-based).
#
#   awk -v mod=<module path> -v shard=K -v count=N -v weights=<tsv> \
#       -v defw=<default weight> [-v solo=<tsv>] [-v part=solo] -f ci/go_race_shard.awk < packages
#
# Longest-processing-time assignment: packages sorted by (weight desc, path
# asc), each handed to the currently lightest shard (lowest index on a tie).
# The result is a PARTITION of the input for a given (input, weights, count),
# whatever the weights say: a wrong weight only unbalances the shards, it can
# never drop or duplicate a package.
#
# solo=<tsv> (CHAOS-10289, ci/go_race_solo.tsv, package<TAB>reason): the listed
# packages take no part in the longest-processing-time split, because their
# measured time depends on what runs beside them (internal/fixturescli: 10.7 s
# with idle co-tenants, 600 s with heavy ones) and no weight can place a package
# away from every heavy neighbour. Each solo package is placed FIRST, on the then-lightest
# shard (its weight pre-loads that shard, so the split of the other packages leaves it less),
# and `part=solo` prints exactly those packages of the
# shard while the default part prints the rest. The caller runs the solo
# packages in their own `go test` invocation once the shard's other packages
# have finished, so nothing else of the leg runs beside them. Without `solo`,
# every package is split as before. Still a partition: the two parts of every
# shard together hold each package exactly once.
BEGIN {
  FS = "\t"
  if (weights != "") {
    while ((getline line < weights) > 0) {
      if (line ~ /^[[:space:]]*(#|$)/) continue
      split(line, f, "\t")
      w[f[1]] = f[2] + 0
    }
    close(weights)
  }
  if (solo != "") {
    while ((getline line < solo) > 0) {
      if (line ~ /^[[:space:]]*(#|$)/) continue
      split(line, f, "\t")
      isSolo[f[1]] = 1
    }
    close(solo)
  }
  if (defw == "") defw = 2
}
{
  pkg = $0
  key = pkg
  if (key == mod) key = "."
  else if (index(key, mod "/") == 1) key = substr(key, length(mod) + 2)
  n++
  path[n] = pkg
  weight[n] = (key in w) ? w[key] : defw + 0
  soloPkg[n] = (key in isSolo) ? 1 : 0
}
END {
  # insertion sort by (weight desc, path asc): n is a few hundred at most
  for (i = 1; i <= n; i++) order[i] = i
  for (i = 2; i <= n; i++) {
    v = order[i]
    j = i - 1
    while (j >= 1 && (weight[order[j]] < weight[v] || (weight[order[j]] == weight[v] && path[order[j]] > path[v]))) {
      order[j + 1] = order[j]
      j--
    }
    order[j + 1] = v
  }
  for (s = 1; s <= count; s++) load[s] = 0
  for (pass = 1; pass <= 2; pass++) {
    for (i = 1; i <= n; i++) {
      k = order[i]
      if (soloPkg[k] != (pass == 1)) continue
      best = 1
      for (s = 2; s <= count; s++) if (load[s] < load[best]) best = s
      load[best] += weight[k]
      if (best == shard) keep[k] = 1
    }
  }
  # print in input order so the package order handed to go test is stable
  for (k = 1; k <= n; k++) if ((k in keep) && ((part == "solo") == (soloPkg[k] == 1))) print path[k]
}
