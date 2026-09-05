"""Corpus: class/method block algebra.

radon's class rule folds each method's FULL complexity into the class, and
then reports the methods as blocks in their own right. A class and its
methods therefore overlap on purpose, and `functions_count` counts both.
This file exists so that overlap is pinned rather than rediscovered.
"""


class Empty:
    pass


class Simple:
    def method_a(self, x):
        if x:
            return 1
        return 0

    def method_b(self, x):
        for i in x:
            if i:
                return i
        return None


class WithBodyLogic:
    """Class-body decision points count toward the class, not any method."""

    if True:
        FLAG = 1
    else:
        FLAG = 2

    def only_method(self, x):
        return 1 if x else 0


class Outer:
    class Inner:
        def inner_method(self, x):
            if x:
                return 1
            return 0

    def outer_method(self, x):
        if x:
            return 1
        return 0


class WithClosure:
    def factory(self, x):
        def made(y):
            if y:
                return 1
            return 0

        if x:
            return made
        return None


def function_holding_a_class(x):
    """A class nested in a function is still reported as a block."""

    class Nested:
        def m(self, y):
            if y:
                return 1
            return 0

    if x:
        return Nested
    return None


class DecoratedMethods:
    @staticmethod
    def stat(x):
        if x:
            return 1
        return 0

    @property
    def prop(self):
        return self._v if hasattr(self, "_v") else None
