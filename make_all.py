import csv
import datetime
import shutil
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Any

from loguru import logger

BOM_URL = "ftp://ftp.bom.gov.au/anon/gen/clim_data/IDCKWCDEA0.tgz"
ARCHIVE_NAME = "IDCKWCDEA0.tgz"
MIN_ROW_LEN = 9


def get_data() -> None:
    logger.info("Downloading New File")
    urllib.request.urlretrieve(BOM_URL, ARCHIVE_NAME)  # noqa: S310
    logger.info("Unpacking File")
    shutil.unpack_archive(ARCHIVE_NAME)
    logger.success("File Unpacked")


def process_file(p: Path, data: dict[str, Any], lock: Lock) -> None:
    local_data = []
    try:
        with p.open(encoding="unicode_escape") as f:
            r = csv.reader(f)
            for row in r:
                if not row:
                    continue
                try:
                    # date format: dd/mm/YYYY
                    date_obj = datetime.datetime.strptime(row[1], "%d/%m/%Y").replace(tzinfo=datetime.UTC).date()
                except ValueError, IndexError:
                    continue
                # row[5]: max_temp, row[6]: min_temp, row[7]: max_humidity, row[8]: min_humidity
                if len(row) < MIN_ROW_LEN:
                    continue

                new_row = [row[5], row[6], row[7], row[8]]
                clean_row = ["" if x == " " else x for x in new_row]
                local_data.append((row[0], date_obj, clean_row))
    except Exception:  # noqa: BLE001
        logger.warning("Failed to process %s", p)
        return

    if local_data:
        with lock:
            for location, date, values in local_data:
                if location not in data:
                    data[location] = {}
                data[location][date] = values


def main() -> None:
    archive_path = Path(ARCHIVE_NAME)
    if not archive_path.is_file():
        get_data()
    else:
        try:
            size1 = archive_path.stat().st_size
            with urllib.request.urlopen(BOM_URL) as response:  # noqa: S310
                size2 = int(response.info()["Content-length"])
            if size1 != size2:
                get_data()
        except Exception as e:  # noqa: BLE001
            logger.warning("Could not check remote file size: %s", e)

    data: dict[str, Any] = {}
    data_lock = Lock()

    logger.info("Start Reading Files")
    files = [p for p in Path("tables").rglob("*") if p.suffix == ".csv"]
    total_files = len(files)

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_file, p, data, data_lock) for p in files]
        for i, _ in enumerate(as_completed(futures), 1):
            if i % 100 == 0:
                logger.info("Processed %s/%s files (%.2f%%)", i, total_files, i / total_files * 100)

    logger.info("Creating CSV")
    csv_path = Path("temp_all.csv")
    with csv_path.open("w", encoding="utf-8", newline="") as out_file:
        w = csv.writer(out_file)
        w.writerow(["location", "date", "max_temp", "min_temp", "max_humidity", "min_humidity"])
        for location, datum in data.items():
            for date, value in datum.items():
                w.writerow([location, date, *value])
    logger.success("Done")


if __name__ == "__main__":
    main()
