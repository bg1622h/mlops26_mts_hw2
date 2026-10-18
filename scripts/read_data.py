#!/usr/bin/env python3
"""read csv and send to kafka topic"""
import argparse
import csv
import json
from kafka import KafkaProducer
from tqdm import tqdm
from datetime import datetime
from uuid import uuid4

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="data/transactions.csv")
    parser.add_argument("--bootstrap-servers", default="localhost:9095")
    parser.add_argument("--topic", default="transactions")
    args = parser.parse_args()
    print("Intialize kafka producer")
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda row: json.dumps(row).encode("utf-8"),
    )
    print("Intialization done")
    count = 0
    try:
        with open(args.csv, newline="", encoding="utf-8") as file:
            total_rows = sum(1 for _ in file) - 1

        with open(args.csv, newline="", encoding="utf-8") as file:
            for row in tqdm(csv.DictReader(file), total = total_rows, desc = 'Loading data in kafka topic'):
                row["amount"] = float(row["amount"])
                row["is_fraud"] = int(row.get("is_fraud", 0))
                message = {
                    "transaction_id": str(uuid4()),
                    "transaction_time": datetime.strptime(row["transaction_time"], "%Y-%m-%d %H:%M").strftime("%Y-%m-%d %H:%M:%S"),
                    "category": row["cat_id"],
                    "state": row["us_state"],
                    "amount": float(row["amount"]),
                    "is_fraud": int(row.get("is_fraud", 0)),
                }
                producer.send(args.topic, message)
                count += 1
        print("Sending data...")
        producer.flush()
        print("Done")
    finally:
        producer.close()
    print(f"Sent {count} rows to Kafka topic '{args.topic}'")

if __name__ == "__main__":
    main()
