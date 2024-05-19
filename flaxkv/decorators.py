# Copyright (c) 2023 K.Y. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import logging
import time
from functools import wraps
from typing import TYPE_CHECKING

from rich import print
from rich.text import Text

if TYPE_CHECKING:
    from flaxkv import FlaxKV

ENABLED_MEASURE_TIME_DECORATOR = True


def class_measure_time(logger=None, level=logging.INFO, prec=3):
    def decorate(func):
        """Log the runtime of the decorated function."""

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not ENABLED_MEASURE_TIME_DECORATOR:
                return func(self, *args, **kwargs)
            start = time.perf_counter()
            value = func(self, *args, **kwargs)
            end = time.perf_counter()
            cost_time = end - start
            time_str = f"{cost_time:.{int(prec)}E}"
            msg = f"{func.__name__}:{self._db_manager.db_type}"
            if logger:
                show_string = f"Finished {msg} in {time_str} secs."
                logger.log(level, show_string)
            else:
                rgb_cost_time = Text(time_str, style='green')
                rgb_msg = Text(f"{msg}", style="cyan")
                str_tuple = (f"Finished", rgb_msg, "in", rgb_cost_time, "secs.")
                print(*str_tuple, sep=' ')
            return value

        return wrapper

    return decorate


def msg_encoder(func):
    from .pack import encode

    @wraps(func)
    async def wrapper(*args, **kwargs):
        result = await func(*args, **kwargs)
        return encode(result)

    return wrapper


def retry(max_retries=3, delay=1, backoff=2, exceptions=(Exception,)):
    """
    A decorator for automatically retrying a function upon encountering specified exceptions.

    Args:
        max_retries (int): The maximum number of times to retry the function.
        delay (float): The initial delay between retries in seconds.
        backoff (float): The multiplier by which the delay should increase after each retry.
        exceptions (tuple): A tuple of exception classes upon which to retry.

    Returns:
        The return value of the wrapped function, if it succeeds.
        Raises the last encountered exception if the function never succeeds.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries == max_retries:
                        raise
                    print(
                        f"Retrying `{func.__name__}` after {current_delay} seconds, retry : {retries}\n"
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff

        return wrapper

    return decorator


def cache(db: FlaxKV = None):
    """Keep a cache of previous function calls."""

    if db is None:
        db = {}

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            if key in db:
                return db[key]
            result = func(*args, **kwargs)
            db[key] = result
            return result

        return wrapper

    return decorator


def singleton(cls):
    instances = {}

    @wraps(cls)
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance
