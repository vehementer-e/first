"""Provides wrappers for project-wide used functions/methods."""

from functools import wraps
import time
from src.utils.logging import DualLogger

logger = DualLogger(name=__name__)

def timing_decorator(func):
    """
    A wrapper for function to measure execution time
    :param func: any function
    :return: a function wrapper calculating time execution
    """
    @wraps(func)
    def wrapper(*args, **kwargs):

        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Function {func.__name__} took {end_time - start_time:.2f} seconds to execute")
        return result

    return wrapper
