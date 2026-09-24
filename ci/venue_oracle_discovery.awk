# The venue-oracles discovery of ci/check_go.sh (see "Discovery" there): reads
# one package's *_test.go files and prints "RUN <Test>" for each top-level test
# whose code reaches internal/testsupport/venueoracle, and "LOCAL <Test> <file>"
# for such a test carrying the local-only marker (-v marker=...) on the line
# directly above it. A file, not a here-document: a here-document over the
# measured pipe budget can wedge (tests/tooling/test_local_validate_heredocs.py).
function flush() {
  if (decl != "") { body[decl] = text; decl = "" ; text = "" }
}
FNR == 1 { flush(); prev = "" }
/^(func|var|const|type|import)([ (]|$)/ {
  flush()
  if ($0 ~ /^import/) { skipping = 1; prev = $0; next }
  skipping = 0
  ndecl++; decl = ndecl
  line = $0; gsub(/"([^"\\]|\\.)*"/, "\"\"", line); sub(/\/\/.*/, "", line)
  text = line
  block = 0
  if (line ~ /^func \(/) {
    # A method is reached through a value of its type (whose name
    # propagates), never by its bare name.
    names[decl] = ""
  } else if (line ~ /^func /) {
    n = line; sub(/^func /, "", n); sub(/[^A-Za-z0-9_].*/, "", n)
    names[decl] = n
    if (line ~ /^func Test[A-Za-z0-9_]*[(\[]/ && n != "TestMain") {
      test[decl] = n; testfile[decl] = FILENAME
      if (index(prev, marker) == 1) local[decl] = 1
    }
  } else if (line ~ /^(var|const|type) \(/) {
    names[decl] = ""; block = 1
  } else {
    n = line; sub(/^(var|const|type) /, "", n); sub(/[^A-Za-z0-9_].*/, "", n)
    names[decl] = n; block = 0
  }
  prev = $0; next
}
{
  if (!skipping && decl != "") {
    code = $0; gsub(/"([^"\\]|\\.)*"/, "\"\"", code); sub(/\/\/.*/, "", code)
    text = text "\n" code
    if ($0 ~ /^\)/) block = 0
    if (block && $0 ~ /^\t[A-Za-z_][A-Za-z0-9_]*/) {
      n = $0; sub(/^\t/, "", n); sub(/[^A-Za-z0-9_].*/, "", n); names[decl] = names[decl] " " n
    }
  }
  prev = $0
}
END {
  flush()
  for (d in body) if (index(body[d], "venueoracle.")) uses[d] = 1
  do {
    changed = 0
    for (d in uses) { split(names[d], ns, " "); for (i in ns) if (ns[i] != "") used[ns[i]] = 1 }
    for (d in body) {
      if (d in uses) continue
      b = body[d]
      b = " " b
      while (match(b, /[^.A-Za-z0-9_][A-Za-z_][A-Za-z0-9_]*/)) {
        tok = substr(b, RSTART + 1, RLENGTH - 1); b = substr(b, RSTART + RLENGTH)
        if ((tok in used) && tok != names[d]) { uses[d] = 1; changed = 1; break }
      }
    }
  } while (changed)
  for (d in test) if (d in uses) {
    if (d in local) print "LOCAL " test[d] " " testfile[d]
    else print "RUN " test[d]
  }
}
