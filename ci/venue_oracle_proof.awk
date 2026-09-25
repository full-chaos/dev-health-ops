# The proof half of the venue-oracles registry guard (CHAOS-6806). Reads .go
# files (the registry's `run` packages plus every file importing the harness,
# so a helper package is included) and prints "PROOF <package dir> <Test>" for
# every top-level Test function that can reach a proof writer:
# venueoracle.Diff (writes the "executed" proof), venueoracle.DiffRecorded (Go-only
# proof against a frozen recording, CHAOS-6817), venueoracle.WriteProof or
# venueoracle.WriteGoOnlyProof. Reach = the function's body names a proof
# call, or names another top-level function that does (resolved in the same
# package first, then by name in any scanned package; a name shared by two
# packages can only make the check looser, never stricter). A registry `run`
# row whose test prints no PROOF line leaves no proof file in the venue, so
# the hosted venue-oracles verb fails on main -- run at PR time instead
# (the venue is not a PR gate, R430). Static and conservative: it proves the
# test CAN write a proof, not that a given run does (the venue proves that).
# Comments are ignored; the harness import alias is honoured per file.
function dirname(p) { sub(/\/[^\/]*$/, "", p); return p }
FNR == 1 {
  dir = dirname(FILENAME); alias = "venueoracle"; cur = ""
}
/^import[ \t]/ || /^[ \t]+([A-Za-z0-9_.]+[ \t]+)?"[^"]*"[ \t]*$/ {
  if (index($0, "/internal/testsupport/venueoracle\"") > 0) {
    line = $0
    if (match(line, /[A-Za-z0-9_]+[ \t]+"/)) { a = substr(line, RSTART, RLENGTH); sub(/[ \t]+"$/, "", a); if (a != "import") alias = a }
  }
}
cur == "" && /^func / {
  n = $0; sub(/^func[ \t]+/, "", n)
  if (n ~ /^\(/) sub(/^\([^)]*\)[ \t]*/, "", n)
  sub(/[^A-Za-z0-9_].*$/, "", n)
  if (n == "") next
  cur = dir SUBSEP n; names[cur] = 1; byname[n] = byname[n] " " cur; direct[cur] = direct[cur] + 0
  single = ($0 ~ /\{.*\}[ \t]*$/)
  body = $0
  finish = single
} 
cur != "" && !(/^func / && body == $0) {
  body = body "\n" $0
}
cur != "" {
  if (!finish && $0 ~ /^\}[ \t]*$/) finish = 1
  if (finish) {
    text = body; gsub(/\/\/[^\n]*/, "", text)
    re = "(^|[^A-Za-z0-9_.])" alias "[ \t\n]*\\.[ \t\n]*(Diff|DiffRecorded|WriteProof|WriteGoOnlyProof)[ \t\n]*\\("
    if (text ~ re) direct[cur] = 1
    gsub(/[^A-Za-z0-9_]/, " ", text); toks[cur] = " " text " "
    cur = ""; body = ""; finish = 0
  }
}
END {
  for (f in names) reach[f] = direct[f]
  do {
    changed = 0
    for (f in names) {
      if (reach[f]) continue
      split(f, part, SUBSEP)
      m = split(toks[f], tk, /[ \t\n]+/)
      for (i = 1; i <= m && !reach[f]; i++) {
        t = tk[i]; if (t == "" || t == part[2]) continue
        if (((part[1] SUBSEP t) in names)) { if (reach[part[1] SUBSEP t]) reach[f] = 1 }
        else if (t in byname) {
          k = split(byname[t], cand, " ")
          for (j = 1; j <= k; j++) if (reach[cand[j]]) { reach[f] = 1; break }
        }
      }
      if (reach[f]) changed = 1
    }
  } while (changed)
  for (f in names) {
    split(f, part, SUBSEP)
    if (reach[f] && part[2] ~ /^Test/) print "PROOF", part[1], part[2]
  }
}
