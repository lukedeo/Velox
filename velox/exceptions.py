#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
## `velox.exceptions`

The `velox.exceptions` submodule defines all the exceptions that are specific 
to the Velox ecosystem.
"""


class VeloxCreationError(Exception):
    """
    Raised in the event of a class instantiation that does not follow
    protocol.
    """
    pass


class VeloxConstraintError(Exception):
    """
    Raised in the event of no matches for loading a model
    """
    pass
