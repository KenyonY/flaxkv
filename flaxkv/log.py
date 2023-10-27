import os
import sys
import time
from datetime import datetime

import pytz
from loguru import logger


def setting_log(
    save_file=False,
    multi_process=True,
):
    """
    Configures the logging settings for the application.
    """

    tz = os.environ.get("TZ", "").strip()
    if tz and hasattr(time, "tzset"):

        def get_utc_offset(timezone_str):
            timezone = pytz.timezone(timezone_str)
            offset_seconds = timezone.utcoffset(datetime.now()).total_seconds()
            offset_hours = offset_seconds // 3600
            return f"UTC{-int(offset_hours):+d}"

        os.environ["TZ"] = get_utc_offset(tz)
        time.tzset()

    config_handlers = [
        {
            "sink": sys.stdout,
            "level": "DEBUG",
            "filter": lambda record: "flaxkv" in record["extra"],
        },
    ]
    if save_file:
        config_handlers += [
            {
                "sink": f"./Log/flaxkv.log",
                "enqueue": multi_process,
                "rotation": "100 MB",
                "level": "DEBUG",
                "filter": lambda record: "flaxkv" in record["extra"],
            }
        ]

    logger_config = {"handlers": config_handlers}
    logger.configure(**logger_config)
