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
# pin=<tsv> (CHAOS-13422, ci/go_test_pins.tsv, package<TAB>reason): the listed packages are placed before everything else, each on the
# then-lightest shard, heaviest first, so as many pinned packages as there are shards land on distinct shards. A pinned package
# is split like any other (same part, same run): only its placement differs. Still a partition.
# splits=<tsv> (CHAOS-13520, ci/go_test_splits.tsv, package<TAB>parts): a listed package becomes PARTS items, part 1..PARTS, each
# of weight ceil(weight / PARTS), placed like packages; such an item prints as "<path><TAB><part><TAB><parts>" and check_go.sh runs
# that part's share of the package's test names. A split package that is also pinned or solo is refused (exit 2). Still a
# partition: every part of every split package lands in exactly one shard.
# serial=<key> serialx=<factor> (CHAOS-13422): the package check_go.sh runs alone after the shard's other packages. Its whole wall
# adds to the shard, while the other packages overlap (sum / factor), so its weight counts factor times. A pinned serial package is
# placed after the other pins.
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
  if (pin != "") {
    while ((getline line < pin) > 0) {
      if (line ~ /^[[:space:]]*(#|$)/) continue
      split(line, f, "\t")
      isPin[f[1]] = 1
    }
    close(pin)
  }
  if (splits != "") {
    while ((getline line < splits) > 0) {
      if (line ~ /^[[:space:]]*(#|$)/) continue
      nf = split(line, f, "\t")
      if (nf != 2 || f[2] !~ /^[0-9]+$/ || f[2] + 0 < 2) { print "go_race_shard.awk: bad split row: " line > "/dev/stderr"; bad = 1; exit 2 }
      if (f[1] in parts) { print "go_race_shard.awk: split row repeats " f[1] > "/dev/stderr"; bad = 1; exit 2 }
      parts[f[1]] = f[2] + 0
    }
    close(splits)
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
  np = (key in parts) ? parts[key] : 1
  if (np > 1 && ((key in isPin) || (key in isSolo))) {
    print "go_race_shard.awk: split package " key " is also pinned or solo" > "/dev/stderr"; bad = 1; exit 2
  }
  if (np > 1) splitSeen[key] = 1
  for (pp = 1; pp <= np; pp++) {
    n++
    path[n] = pkg
    pidx[n] = (np > 1) ? pp : 0
    nparts[n] = np
    weight[n] = (key in w) ? w[key] : defw + 0
    if (np > 1) weight[n] = int((weight[n] + np - 1) / np)
    if (serial != "" && key == serial && serialx + 0 > 0) weight[n] = weight[n] * serialx
    serialPkg[n] = (serial != "" && key == serial) ? 1 : 0
    soloPkg[n] = (key in isSolo) ? 1 : 0
    pinPkg[n] = ((key in isPin) && !(key in isSolo)) ? 1 : 0
  }
}
END {
  if (bad) exit 2
  # insertion sort by (weight desc, path asc): n is a few hundred at most
  for (i = 1; i <= n; i++) order[i] = i
  for (i = 2; i <= n; i++) {
    v = order[i]
    j = i - 1
    while (j >= 1 && (weight[order[j]] < weight[v] || (weight[order[j]] == weight[v] && (path[order[j]] > path[v] || (path[order[j]] == path[v] && pidx[order[j]] > pidx[v]))))) {
      order[j + 1] = order[j]
      j--
    }
    order[j + 1] = v
  }
  for (s = 1; s <= count; s++) load[s] = 0
  for (ppass = 0; ppass <= 1; ppass++) {
    for (i = 1; i <= n; i++) {
      k = order[i]
      if (!pinPkg[k] || serialPkg[k] != ppass) continue
      best = 1
      for (s = 2; s <= count; s++) if (load[s] < load[best]) best = s
      load[best] += weight[k]
      if (best == shard) keep[k] = 1
    }
  }
  for (pass = 1; pass <= 2; pass++) {
    for (i = 1; i <= n; i++) {
      k = order[i]
      if (pinPkg[k]) continue
      if (soloPkg[k] != (pass == 1)) continue
      best = 1
      for (s = 2; s <= count; s++) if (load[s] < load[best]) best = s
      load[best] += weight[k]
      if (best == shard) keep[k] = 1
    }
  }
  # print in weight-descending order (the order of the split, ties by path): go test starts packages in the order given, so the
  # longest ones start first and no heavy package starts late and ends the leg alone. The order is a pure function of
  # (input, weights, count), so it is stable.
  for (i = 1; i <= n; i++) {
    k = order[i]
    if (!(k in keep) || ((part == "solo") != (soloPkg[k] == 1))) continue
    if (pidx[k] > 0) print path[k] "\t" pidx[k] "\t" nparts[k]
    else print path[k]
  }
}
