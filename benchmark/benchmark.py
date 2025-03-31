#!/usr/bin/env python3
"""
FlaxKV2 Benchmark Suite

This script runs comprehensive benchmarks for different database backends,
including FlaxKV2, and compares their performance for various operations.
"""

import argparse
import os
import sys
import time
import tempfile
import shutil
from pathlib import Path
import json
import logging
from datetime import datetime
import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn

# Add parent directory to path to allow importing from benchmark package
sys.path.append(str(Path(__file__).parent.parent))

from benchmark.run import test_benchmark
from benchmark.helpers import plot, format_bytes

console = Console()


def setup_logging(log_level):
    """Setup logging configuration."""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="FlaxKV2 Benchmark Suite")
    
    parser.add_argument(
        "--n-items", 
        type=int, 
        default=1500,
        help="Number of items to write/read (default: 1500)"
    )
    
    parser.add_argument(
        "--vector-dim", 
        type=int, 
        default=1000,
        help="Dimension of random vectors (default: 1000)"
    )
    
    parser.add_argument(
        "--repeat", 
        type=int, 
        default=3,
        help="Number of times to repeat each benchmark for averaging (default: 3)"
    )
    
    parser.add_argument(
        "--db-types",
        nargs="+",
        default=["dict", "SQLite", "RocksDict", "Shelve", "FlaxKV2-LevelDB", "FlaxKV2-Remote"],
        help="Database types to benchmark (default: all available types)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="benchmark_results",
        help="Directory to save results (default: benchmark_results)"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["debug", "info", "warning", "error", "critical"],
        default="info",
        help="Logging level (default: info)"
    )
    
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Don't clean up temporary files after benchmarking"
    )
    
    parser.add_argument(
        "--save-plot",
        action="store_true",
        help="Save the benchmark plot to a file instead of displaying it"
    )
    
    return parser.parse_args()


def run_benchmarks(args):
    """Run the benchmark suite with the specified parameters."""
    # Import benchmark functions
    import pytest
    from benchmark.run import benchmark, N, VECTOR_DIM, BENCHMARK_REPEAT
    from benchmark.dbclass import (
        RocksDict, ShelveDict, SQLiteDict, FlaxKV2LevelDB, FlaxKV2Remote
    )
    from benchmark.run import start_flaxkv2_server, find_free_port
    
    # Preserve original values
    original_N = N
    original_VECTOR_DIM = VECTOR_DIM
    original_BENCHMARK_REPEAT = BENCHMARK_REPEAT
    
    # Update global benchmark parameters in the run module
    import benchmark.run as run_module
    run_module.N = args.n_items
    run_module.VECTOR_DIM = args.vector_dim
    run_module.BENCHMARK_REPEAT = args.repeat
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run timestamp for uniquely identifying this benchmark run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Store benchmark results
    benchmark_info = {}
    
    # Setup progress tracking
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console
    ) as progress:
        task = progress.add_task("Running benchmarks...", total=len(args.db_types))
        
        # Run benchmarks for each database type
        for db_type in args.db_types:
            progress.update(task, description=f"Benchmarking {db_type}...")
            
            try:
                if db_type == "dict":
                    db = {}
                    write_cost, read_cost = benchmark(db, db_type, args.n_items)
                
                elif db_type == "SQLite":
                    # Ensure clean state for benchmark
                    if os.path.exists("sqlite_db.db"):
                        os.remove("sqlite_db.db")
                    db = SQLiteDict()
                    write_cost, read_cost = benchmark(db, db_type, args.n_items)
                    db.destroy()
                
                elif db_type == "RocksDict":
                    # Ensure clean state for benchmark
                    if os.path.exists("test_rocksdict"):
                        shutil.rmtree("test_rocksdict")
                    db = RocksDict()
                    write_cost, read_cost = benchmark(db, db_type, args.n_items)
                    db.destroy()
                
                elif db_type == "Shelve":
                    # Ensure clean state for benchmark
                    if os.path.exists("shelve_db"):
                        shutil.rmtree("shelve_db")
                    db = ShelveDict()
                    write_cost, read_cost = benchmark(db, db_type, args.n_items)
                    db.destroy()
                
                elif db_type == "FlaxKV2-LevelDB":
                    db = FlaxKV2LevelDB()
                    write_cost, read_cost = benchmark(db, db_type, args.n_items)
                    db.destroy()
                
                elif db_type == "FlaxKV2-Remote":
                    port = find_free_port()
                    with start_flaxkv2_server(port=port) as server_url:
                        db = FlaxKV2Remote(server_url=server_url)
                        write_cost, read_cost = benchmark(db, db_type, args.n_items)
                        db.destroy()
                
                else:
                    progress.update(task, description=f"Skipping unknown DB type: {db_type}")
                    continue
                
                # Store benchmark results
                benchmark_info[db_type] = {"write": write_cost, "read": read_cost}
                progress.update(task, description=f"Completed {db_type}")
            
            except Exception as e:
                console.print(f"[bold red]Error benchmarking {db_type}: {e}[/bold red]")
                logging.exception(f"Error in benchmark for {db_type}")
                # Continue with other database types
            
            finally:
                progress.advance(task)
    
    # Restore original values
    run_module.N = original_N
    run_module.VECTOR_DIM = original_VECTOR_DIM
    run_module.BENCHMARK_REPEAT = original_BENCHMARK_REPEAT
    
    if not benchmark_info:
        console.print("[bold red]No benchmarks completed successfully![/bold red]")
        return
    
    # Convert to DataFrame and sort
    df = pd.DataFrame(benchmark_info).T
    df = df.sort_values(by="write", ascending=True)
    
    # Display results table
    table = Table(title="Benchmark Results")
    table.add_column("Database Type", justify="left", style="cyan")
    table.add_column("Write Time (s)", justify="right", style="green")
    table.add_column("Read Time (s)", justify="right", style="magenta")
    table.add_column("Write:Read Ratio", justify="right", style="yellow")
    
    for index, row in df.iterrows():
        ratio = row['write'] / row['read'] if row['read'] > 0 else float('inf')
        table.add_row(
            index,
            f"{row['write']:.6f}",
            f"{row['read']:.6f}",
            f"{ratio:.2f}"
        )
    
    console.print(table)
    
    # Save results to CSV
    results_file = output_dir / f"benchmark_results_{timestamp}.csv"
    df.to_csv(results_file)
    console.print(f"Results saved to [bold]{results_file}[/bold]")
    
    # Plot results
    plot_file = output_dir / f"benchmark_plot_{timestamp}.png" if args.save_plot else None
    title = f"Read and Write Performance - {args.n_items} items with {args.vector_dim}-dim vectors"
    plot(df, title, log=True, output_file=plot_file)
    
    # Save benchmark parameters
    params = {
        "timestamp": timestamp,
        "n_items": args.n_items,
        "vector_dim": args.vector_dim,
        "benchmark_repeat": args.repeat,
        "db_types": args.db_types,
        "system_info": {
            "python_version": sys.version,
            "platform": sys.platform,
            "cpu_count": os.cpu_count()
        }
    }
    
    params_file = output_dir / f"benchmark_params_{timestamp}.json"
    with open(params_file, "w") as f:
        json.dump(params, f, indent=2)
    
    console.print(f"Benchmark parameters saved to [bold]{params_file}[/bold]")


def main():
    """Main entry point for the benchmark script."""
    args = parse_args()
    setup_logging(args.log_level)
    
    console.print("[bold]FlaxKV2 Benchmark Suite[/bold]")
    console.print(f"Running benchmarks with {args.n_items} items of {args.vector_dim} dimensions")
    console.print(f"Database types: {', '.join(args.db_types)}")
    
    try:
        run_benchmarks(args)
    except KeyboardInterrupt:
        console.print("\n[bold red]Benchmark interrupted by user[/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[bold red]Error running benchmarks: {e}[/bold red]")
        raise
    
    console.print("\n[bold green]Benchmarks completed successfully![/bold green]")


if __name__ == "__main__":
    main() 