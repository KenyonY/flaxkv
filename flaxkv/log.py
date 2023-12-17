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

import os
import sys
import time
from datetime import datetime

import pytz
from loguru import logger


def setting_log(level=None, multi_process=True, save_file=True, stdout=True):
    """
    Configures the logging settings for the application.
    """
    if level is None:
        # https://loguru.readthedocs.io/en/stable/api/logger.html
        # level = "CRITICAL"
        level = "INFO"
        save_file = False

    tz = os.environ.get("TZ", "").strip()
    if tz and hasattr(time, "tzset"):

        def get_utc_offset(timezone_str):
            timezone = pytz.timezone(timezone_str)
            offset_seconds = timezone.utcoffset(datetime.now()).total_seconds()
            offset_hours = offset_seconds // 3600
            return f"UTC{-int(offset_hours):+d}"

        try:
            os.environ["TZ"] = get_utc_offset(tz)
        except:
            pass
        time.tzset()
    config_handlers = []
    if stdout:
        config_handlers += [
            {
                "sink": sys.stdout,
                "level": level,
                "filter": lambda record: "flaxkv" in record["extra"],
            },
        ]
    if save_file:
        config_handlers += [
            {
                "sink": f"./Log/flaxkv.log",
                "enqueue": multi_process,
                "rotation": "100 MB",
                "level": level,
                "filter": lambda record: "flaxkv" in record["extra"],
            }
        ]
    return config_handlers


def enable_logging():
    logger.enable("flaxkv")
