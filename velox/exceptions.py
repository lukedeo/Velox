#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
file: exceptions.py
description: all exceptions for Velox
author: Luke de Oliveira (lukedeo@vaitech.io)
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
