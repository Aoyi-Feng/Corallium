"""Corallium server package — see PROTOCOL §13.

The storage model, the derivation rules, the write path, maintenance mode, the
HTTP surface and the registry are separate modules on purpose: the old single file was
1588 lines, and two agents editing it could not see each other's changes
(git merges line-by-line and says nothing).

`Store` is composed from the mixins below, so `self.` calls cross module
boundaries exactly as they crossed method boundaries before.
"""
