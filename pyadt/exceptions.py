# pyadt.exceptions

"""Implements the exceptions module."""

class BaseException(Exception):
    """Base exception for all other exceptions."""
    def __init__(self, *args):
        self.args = args

class ClosedDataException(BaseException):
    """The data source is not open."""
