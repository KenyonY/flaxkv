from __future__ import annotations

import time

import pandas as pd


def wait_for_server_to_start(url, timeout=5):
    import httpx

    start_time = time.time()
    while True:
        try:
            response = httpx.get(url)
            response.raise_for_status()
            break
        except Exception:
            time.sleep(0.5)
            if time.time() - start_time > timeout:
                raise RuntimeError("Server didn't start in time")


def plot(df: pd.DataFrame, title: str, log=False):
    import matplotlib.pyplot as plt

    # df.plot(kind="bar", figsize=(10, 7))
    df.reset_index(inplace=True)
    plt.figure(figsize=(10, 6))
    write_color = '#ADD8E6'
    read_color = '#3EB489'
    bars_write = plt.bar(
        df["index"],
        df["write"],
        width=0.4,
        color=write_color,
        label='Write',
        align='center',
    )
    bars_read = plt.bar(
        df["index"],
        df["read"],
        width=0.4,
        color=read_color,
        label='Read',
        align='edge',
    )

    plt.title(title)
    plt.xlabel("DB Type")
    plt.ylabel("Time (seconds)")
    if log:
        plt.yscale('log')
    plt.xticks(rotation=20)
    plt.legend(title="Operation")

    for bar in bars_write + bars_read:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            yval,
            "{:.2e}".format(yval),
            ha='center',
            va='bottom',
        )

    plt.show()
