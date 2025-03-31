from __future__ import annotations

import time
import asyncio
import logging
from typing import Dict, List, Tuple, Optional, Any, Union

import pandas as pd
import matplotlib.pyplot as plt


def wait_for_server_to_start(url: str, timeout: int = 10, retry_interval: float = 0.5):
    """Wait for a server to be ready by polling its health check endpoint.
    
    Args:
        url: The URL to check (typically a health check endpoint)
        timeout: Maximum time in seconds to wait
        retry_interval: Time in seconds between retries
        
    Raises:
        RuntimeError: If the server doesn't start within the timeout
    """
    import aiohttp
    import requests
    
    print(f"Waiting for server at {url} to be ready...")
    start_time = time.time()
    
    while True:
        try:
            # First try with requests (synchronous)
            response = requests.get(url, timeout=1)
            if response.status_code == 200:
                print(f"Server is ready! (after {time.time() - start_time:.2f}s)")
                return
        except requests.RequestException:
            # If requests fails, try with aiohttp (async)
            try:
                async def check_async():
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, timeout=1) as response:
                            return response.status == 200
                
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                success = loop.run_until_complete(check_async())
                loop.close()
                
                if success:
                    print(f"Server is ready! (after {time.time() - start_time:.2f}s)")
                    return
            except Exception:
                pass
        
        # Check if we've exceeded the timeout
        if time.time() - start_time > timeout:
            raise RuntimeError(f"Server didn't start within {timeout}s timeout")
        
        time.sleep(retry_interval)


def format_bytes(size: int) -> str:
    """Format bytes to human-readable format.
    
    Args:
        size: Size in bytes
        
    Returns:
        Human-readable string representation of the size
    """
    power = 1024
    n = 0
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size > power and n <= 4:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}"


def plot(df: pd.DataFrame, title: str, log: bool = False, output_file: Optional[str] = None):
    """Plot benchmark results as a bar chart.
    
    Args:
        df: DataFrame with benchmark results
        title: Plot title
        log: Whether to use logarithmic scale for y-axis
        output_file: If provided, save the plot to this file instead of showing it
    """
    # Reset index to make the index a column
    df.reset_index(inplace=True)
    plt.figure(figsize=(12, 7))
    
    # Colors for bars
    write_color = '#ADD8E6'  # Light blue
    read_color = '#3EB489'   # Mint green
    
    # Create bars for write and read operations
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

    # Set title and labels
    plt.title(title, fontsize=14, fontweight='bold')
    plt.xlabel("Database Type", fontsize=12)
    plt.ylabel("Time (seconds)", fontsize=12)
    
    # Set logarithmic scale if requested
    if log:
        plt.yscale('log')
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=20, fontsize=10)
    plt.legend(title="Operation", fontsize=10)

    # Add value annotations to bars
    for bar in bars_write + bars_read:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            yval,
            "{:.2e}".format(yval),
            ha='center',
            va='bottom',
            fontsize=9,
        )

    # Adjust layout to make room for annotations
    plt.tight_layout()
    
    # Save or show the plot
    if output_file:
        plt.savefig(output_file, dpi=300)
        print(f"Plot saved to {output_file}")
    else:
        plt.show()
