"""Utility functions for the Insightly API client."""

from insightly.insightly import Insightly

insightly = Insightly()


def get_singleton() -> Insightly:
    """
    Get the singleton instance of the Insightly class.

    Returns
    -------
    Insightly
        The singleton instance of the Insightly class.
    """
    return insightly
