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
# chain=1 (CHAOS-13664, the test legs only): check_go.sh runs a test shard's split-package jobs one after another (a chain) beside one
# go test of its other packages (the rest, which overlap about 2x), then the serial package alone, so a leg's wall is about
# max(chain, rest / 2) + serial. The split parts and the serial package are placed first, heaviest first, each on the shard of the
# least chain + serial load. Then each other package, heaviest first: among the shards it does not lift above the highest
# modelled wall so far, the one of the least rest (so the rest spreads under the chains); when it lifts every shard, the shard of
# the least wall after it (ties: the least rest). Ties: the lowest index.
# serialx is not used. Pins: pinned packages first (rest rule), then a pinned serial one (chain rule). solo is refused. A
# "<key>#<part>" weights row is refused: part weights are derived (tests=). Without chain, nothing above changes the split. Still a
# partition.
# tests=<tsv> (CHAOS-13668, ci/go_test_split_weights.tsv, package<TAB>TestName<TAB>seconds): the measured tests of split packages.
# cut(): the measured names of a package go longest first (ties: name), each to the lightest part (ties: the lowest index); a name
# with no row goes to part djb2(name) mod PARTS + 1 (h = h * 33 + byte mod 2^32 from 5381). So a part depends on the rows and the
# name alone: adding or removing a test with no row moves no other test. With chain=1, a part weighs the rows cut to it plus
# ceil(max(weight - all rows, 0) / PARTS) (the unmeasured rest, spread as the hash spreads its names), at least 1; a split package
# with fewer rows than parts is refused (a part could be empty). Without chain (the race legs; the auth legs, CHAOS-13693, pass
# their own tests table), a part weighs ceil(weight / PARTS) and tests= only refuses a malformed row or a row of no split package.
# cutkey=<key> cutparts=<n> [cutpart=<p>] (names mode, run under LC_ALL=C): stdin holds the names `go test -list` reports for the
# package; prints the names of part p (or "name<TAB>part" for every name without cutpart). A row that names no listed test, or
# fewer rows than parts, exits 2: a stale row fails instead of describing a part that no longer exists.
# restpart=<r> (names mode, CHAOS-13833, the race-leg parts): a name with no row goes to part r (1..n) instead of its hash part.
# wall2(s, x): twice the modelled wall of shard s with x more rest weight: max(2 * chain, rest + x) + 2 * serial.
# rserial=1 [rk=<overlap>] (CHAOS-13958, the race legs): a race leg runs its packages in one parallel go test, then each race part and
# each solo package alone, so its wall is about max(its largest package, its package sum / rk) + the sum of its parts and solo
# packages (rk: the overlap of the parallel batch, 3.5 by default; the measured warm legs of Go quality 44019931820, 44022785905 and
# 44025688889 run 1.7-3.9x their package sum in the batch, which ends at the largest package). The parts and solo packages are placed
# first, heaviest first, each on the shard of the least modelled wall; then each package, heaviest first, on the shard whose modelled
# wall it raises least (ties: the least package sum, the lowest index). rcap=<weight> (CHAOS-13958): a shard whose row weight (its
# packages plus its parts) would pass rcap takes the item only when no shard fits; then the shard that takes it prints "WARN race
# plan: <package> over the leg weight cap on leg <n>/<count>" on stderr. rcap counts the packages of this one run (one module); check_go.sh passes it for the
# root module only (the budget guard's scope). Pinned packages and chain=1 are not used with it. Without
# rserial nothing above changes the split. Still a partition.
# apart=<tsv> (CHAOS-13958, ci/go_race_apart.tsv, rserial only): "heavy<TAB>package<TAB>seconds" rows name the packages that run
# slower beside each other than rk models (measured: three of them in one batch took 1.2-2.0x their time alone), and
# "part<TAB>package<TAB>part" rows name serial race parts. The parts and solo packages are placed first, as above; then the heavy
# packages, heaviest first, each on a shard that holds no heavy package and no listed part (the same choice as above among those
# shards); then every other package. A heavy package with no such shard goes where the rule above puts it and its shard prints
# "WARN race plan: <package> heavy, no leg without a heavy package or a listed part, on leg <n>/<count>" on stderr. A heavy row
# of a split, solo or pinned package, a part row of no split package or of no part of it, and a repeated row are refused.
# rwall(m, l, x): the modelled batch wall of a shard whose largest package is m and package sum l, with one more package of weight x.
function rwall(m, l, x,   k, v) {
  k = (rk + 0 > 0) ? rk + 0 : 3.5
  v = (m > x) ? m : x
  return ((l + x) / k > v) ? (l + x) / k : v
}
function die(msg) {
  print "go_race_shard.awk: " msg > "/dev/stderr"
  bad = 1
  exit 2
}
function djb2(name,    i, h) {
  h = 5381
  for (i = 1; i <= length(name); i++) h = (h * 33 + ord[substr(name, i, 1)]) % 4294967296
  return h
}
function cut(key, np,    i, j, m, v, o, p, best) {
  if (key in cutDone) return
  cutDone[key] = 1
  m = tn[key] + 0
  for (i = 1; i <= m; i++) o[i] = tname[key, i]
  for (i = 2; i <= m; i++) {
    v = o[i]
    j = i - 1
    while (j >= 1 && (trow[key, o[j]] < trow[key, v] || (trow[key, o[j]] == trow[key, v] && o[j] > v))) {
      o[j + 1] = o[j]
      j--
    }
    o[j + 1] = v
  }
  for (p = 1; p <= np; p++) psum[key, p] = 0
  mtot[key] = 0
  for (i = 1; i <= m; i++) {
    best = 1
    for (p = 2; p <= np; p++) if (psum[key, p] < psum[key, best]) best = p
    mpart[key, o[i]] = best
    psum[key, best] += trow[key, o[i]]
    mtot[key] += trow[key, o[i]]
  }
}
function partOf(key, np, name) {
  if ((key, name) in mpart) return mpart[key, name]
  return (restpart != "") ? restpart + 0 : djb2(name) % np + 1
}
function wall2(s, x,    r) {
  r = rl[s] + x
  return (2 * cl[s] > r ? 2 * cl[s] : r) + 2 * sl[s]
}
BEGIN {
  FS = "\t"
  if (chain && solo != "") { print "go_race_shard.awk: chain=1 takes no solo file" > "/dev/stderr"; bad = 1; exit 2 }
  if (weights != "") {
    while ((getline line < weights) > 0) {
      if (line ~ /^[[:space:]]*(#|$)/) continue
      split(line, f, "\t")
      w[f[1]] = f[2] + 0
      if (chain && index(f[1], "#")) die("part row " f[1] " in " weights ": part weights are derived from the test rows (tests=), never kept by hand")
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
  if (tests != "") {
    for (i = 1; i < 256; i++) ord[sprintf("%c", i)] = i
    while ((getline line < tests) > 0) {
      if (line ~ /^[[:space:]]*(#|$)/) continue
      nf = split(line, f, "\t")
      if (nf != 3 || f[2] !~ /^(Test|Example|Fuzz)/ || f[3] !~ /^[0-9]+$/ || f[3] + 0 < 1) die("bad test row: " line)
      if ((f[1], f[2]) in trow) die("test row repeats " f[1] " " f[2])
      if (splits != "" && !(f[1] in parts)) die("test row " f[1] " " f[2] ": " f[1] " is no split package (" splits ")")
      trow[f[1], f[2]] = f[3] + 0
      tn[f[1]]++
      tname[f[1], tn[f[1]]] = f[2]
    }
    close(tests)
  }
  if (cutkey != "") {
    np = cutparts + 0
    if (np < 2 || tests == "") die("names mode needs cutparts >= 2 and tests")
    if (restpart != "" && (restpart !~ /^[0-9]+$/ || restpart + 0 < 1 || restpart + 0 > np)) die("restpart must be a part number 1.." np ", got '" restpart "'")
    while ((getline line) > 0) {
      if (line == "") continue
      listed[line] = 1
      nl++
      lname[nl] = line
    }
    if (tn[cutkey] + 0 < np) die("split package " cutkey " has " (tn[cutkey] + 0) " test row(s) in " tests ", fewer than its " np " parts")
    for (i = 1; i <= tn[cutkey]; i++) {
      if (!(tname[cutkey, i] in listed)) die("split package " cutkey ": the row of " tname[cutkey, i] " in " tests " names a test go test -list no longer reports (rename or remove the row)")
    }
    cut(cutkey, np)
    for (i = 1; i <= nl; i++) {
      p = partOf(cutkey, np, lname[i])
      if (cutpart == "") print lname[i] "\t" p
      else if (p == cutpart + 0) print lname[i]
    }
    cutDoneMode = 1
    exit 0
  }
  if (solo != "") {
    while ((getline line < solo) > 0) {
      if (line ~ /^[[:space:]]*(#|$)/) continue
      split(line, f, "\t")
      isSolo[f[1]] = 1
    }
    close(solo)
  }
  if (apart != "") {
    while ((getline line < apart) > 0) {
      if (line ~ /^[[:space:]]*(#|$)/) continue
      nf = split(line, f, "\t")
      if (nf != 3 || f[3] !~ /^[0-9]+$/ || f[3] + 0 < 1) die("bad apart row: " line)
      if (f[1] == "heavy") {
        if (f[2] in isHeavy) die("apart row repeats heavy " f[2])
        if (f[2] in parts) die("apart row: heavy " f[2] " is a split package")
        isHeavy[f[2]] = 1
      } else if (f[1] == "part") {
        if (!(f[2] in parts) || f[3] + 0 > parts[f[2]]) die("apart row: part " f[3] " of " f[2] " is no race split part")
        if ((f[2] "#" f[3]) in isApartPart) die("apart row repeats part " f[3] " of " f[2])
        isApartPart[f[2] "#" f[3]] = 1
      } else die("bad apart row: " line)
    }
    close(apart)
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
    if (chain && np > 1 && tests != "") {
      cut(key, np)
      if (tn[key] + 0 < np) die("split package " key " has " (tn[key] + 0) " test row(s) in " tests ", fewer than its " np " parts")
      left = weight[n] - mtot[key]
      weight[n] = psum[key, pp] + int(((left > 0 ? left : 0) + np - 1) / np)
      if (weight[n] < 1) weight[n] = 1
    } else if (np > 1) weight[n] = int((weight[n] + np - 1) / np)
    if (!chain && serial != "" && key == serial && serialx + 0 > 0) weight[n] = weight[n] * serialx
    serialPkg[n] = (serial != "" && key == serial) ? 1 : 0
    soloPkg[n] = (key in isSolo) ? 1 : 0
    pinPkg[n] = ((key in isPin) && !(key in isSolo)) ? 1 : 0
    heavyPkg[n] = (key in isHeavy) ? 1 : 0
    apartPart[n] = (np > 1 && ((key "#" pp) in isApartPart)) ? 1 : 0
  }
  if ((key in isHeavy) && ((key in isSolo) || (key in isPin))) die("apart row: heavy " key " is solo or pinned")
}
END {
  if (bad) exit 2
  if (cutDoneMode) exit 0
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
  if (chain) {
    for (s = 1; s <= count; s++) { cl[s] = 0; rl[s] = 0; sl[s] = 0 }
    for (cpass = 1; cpass <= 4; cpass++) {
      for (i = 1; i <= n; i++) {
        k = order[i]
        onChain = (nparts[k] > 1 || serialPkg[k])
        if (cpass == 1 && !(pinPkg[k] && !onChain)) continue
        if (cpass == 2 && !(pinPkg[k] && onChain)) continue
        if (cpass == 3 && !(!pinPkg[k] && onChain)) continue
        if (cpass == 4 && !(!pinPkg[k] && !onChain)) continue
        best = 1
        if (onChain) {
          for (s = 2; s <= count; s++) if (cl[s] + sl[s] < cl[best] + sl[best]) best = s
          if (serialPkg[k]) sl[best] += weight[k]
          else cl[best] += weight[k]
        } else {
          top = 0
          for (s = 1; s <= count; s++) if (wall2(s, 0) > top) top = wall2(s, 0)
          best = 0
          for (s = 1; s <= count; s++) if (wall2(s, weight[k]) <= top && (best == 0 || rl[s] < rl[best])) best = s
          if (best == 0) {
            best = 1
            for (s = 2; s <= count; s++) {
              if (wall2(s, weight[k]) < wall2(best, weight[k]) || (wall2(s, weight[k]) == wall2(best, weight[k]) && rl[s] < rl[best])) best = s
            }
          }
          rl[best] += weight[k]
        }
        if (best == shard) keep[k] = 1
      }
    }
  }
  for (ppass = 0; ppass <= 1 && !chain; ppass++) {
    for (i = 1; i <= n; i++) {
      k = order[i]
      if (!pinPkg[k] || serialPkg[k] != ppass) continue
      best = 1
      for (s = 2; s <= count; s++) if (load[s] < load[best]) best = s
      load[best] += weight[k]
      if (best == shard) keep[k] = 1
    }
  }
  # rserial=1 (CHAOS-13958): see the header.
  if (rserial && !chain) {
    for (s = 1; s <= count; s++) { mx[s] = 0; ser[s] = 0; heavyOn[s] = 0; partOn[s] = 0 }
    # pass 1: parts and solo packages; pass 2: heavy packages (apart=); pass 3: every other package.
    for (pass = 1; pass <= 3; pass++) {
      for (i = 1; i <= n; i++) {
        k = order[i]
        if (pinPkg[k]) continue
        isSer = (soloPkg[k] || nparts[k] > 1)
        if ((isSer ? 1 : (heavyPkg[k] ? 2 : 3)) != pass) continue
        # rcap: a shard whose row weight (packages + parts) would pass the cap is used only when no shard fits (then WARN).
        fits = 0
        for (s = 1; s <= count; s++) {
          capOk[s] = (rcap + 0 <= 0 || load[s] + ser[s] + weight[k] <= rcap + 0)
          if (capOk[s]) fits = 1
        }
        if (!fits) for (s = 1; s <= count; s++) capOk[s] = 1
        apartOk = 1
        if (pass == 2) {
          apartOk = 0
          for (s = 1; s <= count; s++) if (capOk[s] && !heavyOn[s] && !partOn[s]) apartOk = 1
          for (s = 1; s <= count; s++) allowed[s] = capOk[s] && (!apartOk || (!heavyOn[s] && !partOn[s]))
        } else for (s = 1; s <= count; s++) allowed[s] = capOk[s]
        best = 0
        if (isSer) {
          for (s = 1; s <= count; s++) if (capOk[s] && (best == 0 || mx[s] + ser[s] < mx[best] + ser[best])) best = s
          ser[best] += weight[k]
          if (apartPart[k]) partOn[best] = 1
        } else {
          for (s = 1; s <= count; s++) {
            if (!allowed[s]) continue
            if (best == 0) { best = s; continue }
            a = rwall(mx[s], load[s], weight[k]) + ser[s]; b = rwall(mx[best], load[best], weight[k]) + ser[best]
            if (a < b || (a == b && load[s] < load[best])) best = s
          }
          if (weight[k] > mx[best]) mx[best] = weight[k]
          load[best] += weight[k]
          if (heavyPkg[k]) heavyOn[best] = 1
        }
        if (best == shard) {
          keep[k] = 1
          if (!fits) printf "WARN race plan: %s%s over the leg weight cap on leg %d/%d\n", path[k], (pidx[k] > 0 ? " part " pidx[k] "/" nparts[k] : ""), shard, count > "/dev/stderr"
          if (!apartOk) printf "WARN race plan: %s heavy, no leg without a heavy package or a listed part, on leg %d/%d\n", path[k], shard, count > "/dev/stderr"
        }
      }
    }
  }
  for (pass = 1; pass <= 2 && !chain && !rserial; pass++) {
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
