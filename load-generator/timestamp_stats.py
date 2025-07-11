import csv
import logging
from datetime import datetime
from dateutil import parser

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Change this to your CSV file path
csv_file_path = "./output/escooter_trips_simple.csv"

min_ts = None
max_ts = None
sum_ts = 0
count = 0

with open(csv_file_path, "r", newline="") as f:
    reader = csv.reader(f)

    # Read header
    try:
        header = next(reader)
    except StopIteration:
        logging.error("The CSV file is empty.")
        exit(1)

    try:
        ts_index = header.index("timestamp")
    except ValueError:
        logging.error("No 'timestamp' column found in header!")
        exit(1)

    for row_num, row in enumerate(reader, start=2):  # start=2 to account for header row
        if len(row) <= ts_index:
            logging.warning(
                f"Row {row_num} malformed or missing timestamp column. Skipping."
            )
            continue

        value = row[ts_index]
        if value.strip() == "":
            logging.warning(f"Row {row_num} has empty timestamp field. Skipping.")
            continue

        if value.strip().lower() == "timestamp":
            # logging.info(f"Row {row_num} appears to be a header row. Skipping.")
            continue

        try:
            # Parse ISO timestamp to datetime
            dt = parser.isoparse(value)
            ts = dt.timestamp()  # convert to seconds
        except ValueError as e:
            logging.warning(
                f"Row {row_num} has invalid timestamp format: {value}. Skipping. Error: {e}"
            )
            continue

        # Min
        if min_ts is None or ts < min_ts:
            min_ts = ts

        # Max
        if max_ts is None or ts > max_ts:
            max_ts = ts

        # Sum and count
        sum_ts += ts
        count += 1

if count == 0:
    logging.warning("No valid timestamp records found in the file.")
    mean_ts = None
else:
    mean_ts = sum_ts / count

# Convert min, max, mean back to readable dates
min_dt = datetime.fromtimestamp(min_ts) if min_ts is not None else None
max_dt = datetime.fromtimestamp(max_ts) if max_ts is not None else None
mean_dt = datetime.fromtimestamp(mean_ts) if mean_ts is not None else None

print(f"Count: {count}")
print(f"Min timestamp: {min_dt}")
print(f"Max timestamp: {max_dt}")
print(f"Mean timestamp: {mean_dt}")
