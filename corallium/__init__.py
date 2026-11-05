"""Corallium server package — see PROTOCOL §13.

The storage model, the derivation rules, the write path, maintenance mode, the
HTTP surface and the registry are separate modules on purpose: `server.py` is
1588 lines of one file, and two agents editing it cannot see each other's
changes (git merges them line-by-line and says nothing).

`Store` is composed from the mixins below, so `self.` calls cross module
boundaries exactly as they crossed method boundaries before.
"""
