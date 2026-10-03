"""Host side of the admin2 pass stream (CHAOS-8369, D4566 class rules).

Reads the framed stream pass-bigboy-admin2-run.py writes on stdout and unpacks it into <out>:
"@@FILE\t<name>\t<nbytes>\n<bytes>\n" per file, then one "@@END\t<nfiles>\t<status>\n".
Strict on purpose (a measurement that did not happen must FAIL): no END frame, a short body,
a name outside the closed grammar, a file count that disagrees with END, or a status other
than "ok" is a nonzero exit. Nothing is written outside <out>.
"""

import re
import sys
from pathlib import Path

NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,200}$")


def fail(msg):
    print(f"UNPACK FAILED: {msg}", file=sys.stderr)
    sys.exit(2)


def main():
    out = Path(sys.argv[1])
    data = sys.stdin.buffer.read()
    pos = 0
    n = 0
    while True:
        nl = data.find(b"\n", pos)
        if nl < 0:
            fail("stream ended without an END frame")
        head = data[pos:nl].decode("utf-8", "replace").split("\t")
        pos = nl + 1
        if head[0] == "@@END":
            if len(head) != 3 or not head[1].isdigit():
                fail(f"malformed END frame {head!r}")
            if int(head[1]) != n:
                fail(f"END says {head[1]} files, stream carried {n}")
            if pos != len(data):
                fail("bytes after the END frame")
            if head[2] != "ok":
                fail(f"run status {head[2]!r}")
            print(f"unpacked {n} files", file=sys.stderr)
            return
        if len(head) != 3 or head[0] != "@@FILE" or not head[2].isdigit():
            fail(f"malformed frame header {head!r}")
        name, size = head[1], int(head[2])
        if not NAME.match(name) or ".." in name:
            fail(f"file name outside the closed grammar: {name!r}")
        body = data[pos : pos + size]
        if len(body) != size or data[pos + size : pos + size + 1] != b"\n":
            fail(f"short or unterminated body for {name}")
        (out / name).write_bytes(body)
        pos += size + 1
        n += 1


main()
