import sys
from typing import Final, cast

import matplotlib.pyplot as plt
import pandas as pd
from loguru import logger

MIN_PERIODS: Final = 1
ROLLING_WINDOW: Final = 14
MIN_XTICK_DIST: Final = 10
MIN_XTICKS_FOR_REMOVAL: Final = 2
DPI: Final = 400


def process_location(df: pd.DataFrame, location: str, target_year: int, prev_year: int) -> None:
    logger.info("Processing %s...", location)
    for column, name in {
        "max_temp": "Maximum Temperature",
        "max_humidity": "Maximum Humidity",
    }.items():
        try:
            df_location = df[df.location == location]

            if df_location.empty:
                logger.warning("No data for %s", location)
                continue

            df_rolling = df_location.rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS).mean(numeric_only=True)[column]

            # Get data for target and previous years
            # We filter by year using the index
            y_target = df_rolling[df_rolling.index.year == target_year]
            y_prev = df_rolling[df_rolling.index.year == prev_year]

            if y_target.empty:
                logger.warning("No data for %s in %s", location, target_year)
                continue

            # X-axis is based on target year dates formatted as dd/mm
            x = y_target.index.strftime("%d/%m").to_list()

            # We need to plot y_prev against x.
            # Ideally, we match by day-of-year, but for simple visualization,
            # we'll slice y_prev to match x length or vice versa.
            # If target year (e.g. 2026) is partial, x is short.
            # y_prev (2025) is likely full. We take the first len(x) of y_prev.

            plot_len = min(len(x), len(y_prev))

            plt.figure(figsize=(10, 6))
            plt.plot(x[:plot_len], y_prev.iloc[:plot_len], label=prev_year)
            plt.plot(x[:plot_len], y_target.iloc[:plot_len], label=target_year)

            plt.legend(loc="upper left")
            plt.xlabel("Date")

            ylabel = "Temperature (°C)" if "temp" in column else "Humidity (%)"
            plt.ylabel(ylabel)

            title_loc = location.split(maxsplit=1)[0]
            plt.title(f"{title_loc}\nRolling 14 Day Average {name}")

            # X-ticks logic
            xticks = []
            for idx, day in enumerate(x[:plot_len]):
                if day.startswith(("01", "15")):
                    xticks.append(idx)

            # Ensure last point is included if sensible
            if plot_len > 0 and (plot_len - 1) not in xticks:
                xticks.append(plot_len - 1)

            # Remove second to last if too close to last
            if len(xticks) >= MIN_XTICKS_FOR_REMOVAL and xticks[-1] - xticks[-2] < MIN_XTICK_DIST:
                xticks.pop(-2)

            plt.xticks(xticks, rotation=45)
            plt.tight_layout()

            filename = f"images/{title_loc}_{column}.png"
            plt.savefig(filename, dpi=DPI)
            plt.close()
            logger.success("Saved %s", filename)

        except Exception:
            logger.exception("Error processing %s - %s", location, column)


def main() -> None:
    try:
        df = pd.read_csv("temp_all.csv", parse_dates=["date"])
    except FileNotFoundError:
        logger.error(  # noqa: TRY400
            "temp_all.csv not found. Please run make_all.py first."
        )
        sys.exit(1)

    # Set index to date but keep the column for safety if needed,
    # though we can use index.
    df = df.set_index("date")

    # Determine years dynamically
    if df.empty:
        logger.error("Dataset is empty.")
        sys.exit(1)

    # Ensure index is DatetimeIndex for type checkers and logic
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    # Explicit cast to satisfy static type checkers
    df_index = cast("pd.DatetimeIndex", df.index)
    max_year = df_index.year.max()  # type: ignore[unresolved-attribute]
    target_year = int(max_year)
    prev_year = target_year - 1

    logger.info("Latest year in dataset: %s", max_year)
    logger.info("Comparing %s vs %s", prev_year, target_year)

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
    ]

    for location in locations:
        process_location(df, location, target_year, prev_year)


if __name__ == "__main__":
    main()
