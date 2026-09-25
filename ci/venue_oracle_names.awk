# The by-NAME half of the venue-oracles registry guard (see "Registry" in
# ci/check_go.sh). Reads *_test.go files and prints "<Test> <file> run|local"
# for every top-level test whose name contains "VenueOracle", whatever it
# imports or calls. ci/venue_oracle_discovery.awk finds venue tests by
# STRUCTURE (a call reaching the harness); this finds them by NAME, so a
# venue test in a package that never imports the harness (Trap #392:
# TestVenueOracleQueryAPIResponseModels) still has to be in the registry.
# "local" = the local-only marker (-v marker=...) is on the line directly
# above the func line.
FNR == 1 { prev = "" }
/^func Test[A-Za-z0-9_]*VenueOracle[A-Za-z0-9_]*[(\[]/ {
  n = $0; sub(/^func /, "", n); sub(/[^A-Za-z0-9_].*/, "", n)
  print n, FILENAME, (index(prev, marker) == 1 ? "local" : "run")
}
{ prev = $0 }
