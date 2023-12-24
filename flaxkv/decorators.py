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


import logging
import time
from functools import wraps

from rich import print
from rich.text import Text

from .pack import encode

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
    @wraps(func)
    async def wrapper(*args, **kwargs):
        result = await func(*args, **kwargs)
        return encode(result)

    return wrapper
