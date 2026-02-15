import json
import sys
from pathlib import Path
from typing import Final, cast

import pandas as pd
from loguru import logger

MIN_PERIODS: Final = 1
ROLLING_WINDOW: Final = 14


def process_location(df: pd.DataFrame, location: str, target_year: int, prev_year: int) -> dict:
    logger.info("Processing {}...", location)
    location_data = {
        "max_temp": {},
        "max_humidity": {}
    }
    
    for column in ["max_temp", "max_humidity"]:
        try:
            df_location = df[df.location == location]

            if df_location.empty:
                logger.warning("No data for {}", location)
                continue

            df_rolling = df_location.rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS).mean(numeric_only=True)[column]

            y_target = df_rolling[df_rolling.index.year == target_year]
            y_prev = df_rolling[df_rolling.index.year == prev_year]

            if y_target.empty:
                logger.warning("No data for {} in {}", location, target_year)
                continue

            # We'll use the target year's dates (day/month) as labels
            # To ensure we match by day of year, we'll just take the values
            # and handle alignment in the frontend if needed, but for now
            # let's just provide the raw rolling values for both years.
            
            location_data[column][str(target_year)] = y_target.tolist()
            location_data[column][str(prev_year)] = y_prev.tolist()
            location_data["labels"] = y_target.index.strftime("%d-%b").tolist()

        except (ValueError, KeyError, RuntimeError, OSError):
            logger.exception("Error processing {} - {}", location, column)
    
    return location_data


def main() -> None:
    try:
        df = pd.read_csv("temp_all.csv", parse_dates=["date"])
    except FileNotFoundError:
        logger.error("temp_all.csv not found. Please run make_all.py first.")
        sys.exit(1)

    df = df.set_index("date")

    if df.empty:
        logger.error("Dataset is empty.")
        sys.exit(1)

    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    df_index = cast("pd.DatetimeIndex", df.index)
    max_year = int(df_index.year.max())
    target_year = max_year
    prev_year = target_year - 1

    logger.info("Latest year in dataset: {}", max_year)
    logger.info("Comparing {} vs {}", prev_year, target_year)

    locations = [
        "NAMBOUR DAFF - HILLSIDE",
        "SYDNEY (OBSERVATORY HILL)",
        "BRISBANE",
        "CANBERRA AIRPORT",
        "DARWIN AIRPORT",
        "ADELAIDE (WEST TERRACE _ NGAYIRDAPIRA)",
        "HOBART AIRPORT",
        "MELBOURNE (OLYMPIC PARK)",
        "PERTH METRO",
        "ALICE SPRINGS AIRPORT",
        "CAIRNS AIRPORT",
        "TOWNSVILLE AERO",
        "BROOME AIRPORT",
        "GOLD COAST SEAWAY",
        "NEWCASTLE NOBBYS SIGNAL STATION AWS",
        "PORT HEDLAND AIRPORT",
    ]

    all_data = {
        "metadata": {
            "target_year": target_year,
            "prev_year": prev_year,
            "locations": []
        },
        "locations": {}
    }

    for location in locations:
        short_name = location.split(maxsplit=1)[0]
        data = process_location(df, location, target_year, prev_year)
        if data["max_temp"] or data["max_humidity"]:
            all_data["locations"][short_name] = data
            if short_name not in all_data["metadata"]["locations"]:
                all_data["metadata"]["locations"].append(short_name)

    all_data["metadata"]["locations"] = sorted(list(set(all_data["metadata"]["locations"])))

    Path("public").mkdir(exist_ok=True)
    with open("public/data.json", "w") as f:
        json.dump(all_data, f)
    
    logger.success("Saved public/data.json")


if __name__ == "__main__":
    main()
