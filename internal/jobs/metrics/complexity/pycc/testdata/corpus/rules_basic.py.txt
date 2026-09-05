"""Corpus: one function per radon rule, so a divergence names its own rule.

Every function here is deliberately small. When the parity test fails it
reports the block name, and a block named `boolop_chain` points straight at
`BoolOp += len(values) - 1` rather than at a 200-line file that disagrees
by 3 somewhere.
"""


def plain():
    """No decision points: the base case, complexity 1."""
    return 1


def single_if(x):
    if x:
        return 1
    return 0


def if_elif_else(x):
    """`else` on an if adds NOTHING -- the If nodes already counted."""
    if x == 1:
        return "a"
    elif x == 2:
        return "b"
    elif x == 3:
        return "c"
    else:
        return "d"


def for_with_else(items):
    """`else` on a for DOES add 1 (bool(node.orelse))."""
    for item in items:
        if item:
            break
    else:
        return None
    return items


def while_with_else(n):
    while n > 0:
        n -= 1
    else:
        return 0
    return n


def try_except_else_finally(x):
    """Try adds len(handlers) + bool(orelse); finally adds nothing."""
    try:
        return int(x)
    except ValueError:
        return 0
    except TypeError:
        return -1
    else:
        return None
    finally:
        pass


def boolop_chain(a, b, c, d):
    """One BoolOp with 4 values is +3; two chains are counted separately."""
    if a and b and c and d:
        return 1
    if a or b:
        return 2
    return 0


def ternary(x):
    """IfExp is +1, exactly like a statement `if`."""
    return "yes" if x else "no"


def comprehensions(items):
    """comprehension += len(ifs) + 1, so the `for` and each `if` count."""
    a = [i for i in items]
    b = [i for i in items if i]
    c = [i for i in items if i if i > 2]
    d = {i: j for i in items for j in items}
    return a, b, c, d


def asserts(x):
    assert x
    assert x > 0
    return x


def lambda_not_counted(items):
    """Lambda itself is NOT a decision point (radon #68), but a ternary
    inside its body still counts toward THIS function."""
    f = lambda v: v
    g = lambda v: 1 if v else 0
    return f, g


def nested_loops(rows):
    total = 0
    for row in rows:
        for cell in row:
            if cell:
                total += 1
            while cell > 10:
                cell -= 1
    return total


async def async_paths(items):
    """AsyncFor is treated exactly as For."""
    async for item in items:
        if item:
            return item
    return None


def closure_excluded(x):
    """A closure's complexity does NOT reach this function, and the closure
    is NOT reported as its own block."""

    def inner(y):
        if y:
            return 1
        if y > 2:
            return 2
        return 0

    if x:
        return inner
    return None
