import logging

# supress "No handlers could be found for logger XXXXX" error
logging.getLogger('velox').addHandler(logging.NullHandler())

__version__ = '0.1.0'

from .obj import VeloxObject, register_model
