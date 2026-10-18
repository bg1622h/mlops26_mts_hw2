#!/usr/bin/env python3
"""Export the query result to CSV using clickhouse-client."""
import argparse
import subprocess
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query-file", default="sql/get_max_transactions_base.sql")
    parser.add_argument("--output", default="results/results.csv")
    parser.add_argument("--container", default="hw2-clickhouse")
    args = parser.parse_args()
    query = Path(args.query_file).read_text(encoding="utf-8")
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["docker", "exec", "-i", args.container, "clickhouse-client", "--format", "CSVWithNames", "--query", query],
        check=True, capture_output=True, text=True
    )
    output.write_text(result.stdout, encoding="utf-8")
    print(f"Saved query result to {output} ({len(result.stdout.splitlines()) - 1} data rows)")

if __name__ == "__main__":
    main()
