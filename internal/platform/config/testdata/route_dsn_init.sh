encode() {
  python3 -c 'import sys, urllib.parse; sys.stdout.write(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}
# codex review (r4, P1 -- executed, fixed): a URI host is NOT
# just another percent-encoded component -- an IPv6 literal
# (RFC 3986 IP-literal) is syntax the AUTHORITY part carries
# verbatim inside brackets, "[<addr>]". Percent-encoding it
# (e.g. the domain encode() above, applied to the host in r3)
# escapes the colons that make it parseable as an address at
# all -- confirmed: a real operator connect against an
# unbracketed IPv4-mapped IPv6 host returns
# {"error":{"code":"database_unavailable"}}, the SAME class of
# silent-wrong-target failure as r3's unencoded database name,
# just for a host the operator legitimately needs to reach
# (docker/k8s IPv6-only networking). ipaddress.IPv6Address
# correctly classifies genuine IPv6 literals (DNS names and
# IPv4 dotted-quads both raise ValueError there); only THOSE
# get bracketed, verbatim, no percent-encoding -- everything
# else still goes through the same encode() as before.
encode_host() {
  python3 -c 'import ipaddress, sys, urllib.parse
h = sys.argv[1]
try:
    ipaddress.IPv6Address(h)
    sys.stdout.write("[" + h + "]")
except ValueError:
    sys.stdout.write(urllib.parse.quote(h, safe=""))' "$1"
}
# codex review (r3, P1 -- executed, fixed, class-swept): the r3
# P1 fix encoded ONLY the database name -- team-lead/chris's
# amendment to the prompt of record names this exact shape
# (an encoding fix applied to one field of a class, never swept
# to its siblings) as its own method failure. Every remaining
# URI component an operator can set to an arbitrary string --
# the three roles and the host, same as the three passwords and
# the database -- now goes through the SAME encoder (host uses
# the authority-aware encode_host above). Port is excluded: it
# is always numeric (dev-health.goPgbouncerPostgresPort),
# never operator-set to an arbitrary string.
domain_role=$(encode "$RIVER_DOMAIN_DATABASE_ROLE")
queue_role=$(encode "$RIVER_QUEUE_DATABASE_ROLE")
coordinator_role=$(encode "$RIVER_COORDINATOR_DATABASE_ROLE")
domain_pw=$(encode "$RIVER_DOMAIN_DATABASE_PASSWORD")
queue_pw=$(encode "$RIVER_QUEUE_DATABASE_PASSWORD")
coordinator_pw=$(encode "$RIVER_COORDINATOR_DATABASE_PASSWORD")
host=$(encode_host "$POSTGRES_HOST")
db=$(encode "$POSTGRES_DB")
printf '%s' "postgresql://${domain_role}:${domain_pw}@${host}:${POSTGRES_PORT}/${db}" > /run/route-dsn/POSTGRES_URI
printf '%s' "postgresql://${queue_role}:${queue_pw}@${host}:${POSTGRES_PORT}/${db}" > /run/route-dsn/WORKER_DATABASE_URI
printf '%s' "postgresql://${coordinator_role}:${coordinator_pw}@${host}:${POSTGRES_PORT}/${db}" > /run/route-dsn/COORDINATOR_DATABASE_URI
