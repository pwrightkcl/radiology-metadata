import argparse
from pathlib import Path
import json

import pandas as pd


def stats_json_to_dataframe(stats_files):
    """Read a list of *_stats.json files and concatenate them into a dataframe.

    Args:
        stats_files: List of paths to *_stats.json files.

    Returns:
        pd.DataFrame: A dataframe containing the concatenated data from the stats files.
    """
    data_dicts = []

    for file in stats_files:
        with file.open() as f:
            d = json.load(f)
        data_dicts.append(d)

    return pd.DataFrame(data_dicts)


def parse_query_stats(query_dir: Path):
    """Find all *_stats.json files in a query directory and concatenate them into a single CSV.
    
    Args:
        query_dir: Path to the query directory containing *_stats.json files.
    
    Returns:
        None. Saves the concatenated dataframe to a CSV file in the query directory.
    """
    # Check if metadata directory exists
    if not query_dir.exists():
        raise ValueError(f"Query directory {query_dir} does not exist.")

    # Check if metadata directory is a directory
    if not query_dir.is_dir():
        raise ValueError(f"Query directory {query_dir} is not a directory.")

    # Load metadata files
    stats_files = list(query_dir.glob("*_stats.json"))
    if not stats_files:
        raise ValueError(f"No *_stats.json files found in {query_dir}.")

    stats_df = stats_json_to_dataframe(stats_files)

    # Save the concatenated dataframe to a parquet file
    output_file = query_dir / "stats.csv"
    stats_df.to_csv(output_file, index=False)
    print(f"Query stats table saved to {output_file}")


if __name__ == "__main__":
    # Take metadata directory as argument
    parser = argparse.ArgumentParser(description="Parse *_stats.json in query directory.")
    parser.add_argument(
        "query_dir",
        type=Path,
        help="Query directory.",
    )
    args = parser.parse_args()
    parse_query_stats(args.query_dir)
