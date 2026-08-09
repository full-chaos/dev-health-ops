"""CHAOS-3617 graph-arm tests.

A package (unlike some sibling test directories) so ``live_gate`` can be
imported as ``tests.context_fabric.live_gate`` rather than relying on
pytest's rootdir sys.path insertion, which mypy cannot follow.
"""
