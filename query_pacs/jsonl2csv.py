import argparse
import json
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd


def jsonl_to_csv(jsonl_file: Path, csv_file: Path) -> None:
    """Convert a JSONL file to a CSV file.

    Parameters:
        jsonl_file (Path): The path to the input JSONL file.
        csv_file (Path): The path to the output CSV file.

    Returns:
        None

    Outputs:
        A CSV file with the converted data.
    """
    # Read JSONL file and load data into a list of dictionaries
    data: List[Dict[str, Any]] = []
    with jsonl_file.open('r', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line))

    # Convert list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(data)

    # Save DataFrame to CSV file
    df.to_csv(csv_file, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert a JSONL file to a CSV file.')
    parser.add_argument('jsonl_file', type=Path, help='Path to the input JSONL file')
    parser.add_argument('csv_file', type=Path, help='Path to the output CSV file', nargs='?', default=None)
    args = parser.parse_args()

    if not args.csv_file:
        args.csv_file = args.jsonl_file.with_suffix('.csv')

    jsonl_to_csv(args.jsonl_file, args.csv_file)
