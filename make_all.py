import csv
import datetime
import ftplib
import shutil
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import urlparse

from loguru import logger

BOM_URL = "ftp://ftp.bom.gov.au/anon/gen/clim_data/IDCKWCDEA0.tgz"
ARCHIVE_NAME = "IDCKWCDEA0.tgz"
MIN_ROW_LEN = 9
PROGRESS_STEP = 100
PERCENT_MAX = 100


def download_progress(block_num: int, block_size: int, total_size: int) -> None:
    if total_size <= 0:
        return
    downloaded = block_num * block_size
    progress = min(float(PERCENT_MAX), downloaded / total_size * PERCENT_MAX)
    if block_num % PROGRESS_STEP == 0 or progress >= PERCENT_MAX:
        logger.info(
            "Download progress: {:.2f}% ({:.2f} MB / {:.2f} MB)",
            progress,
            downloaded / 1024 / 1024,
            total_size / 1024 / 1024,
        )


def get_data() -> None:
    logger.info("Downloading New File from {}", BOM_URL)
    urllib.request.urlretrieve(BOM_URL, ARCHIVE_NAME, reporthook=download_progress)  # noqa: S310
    logger.info("Unpacking File: {}", ARCHIVE_NAME)
    shutil.unpack_archive(ARCHIVE_NAME)
    logger.success("File Unpacked Successfully")


def process_file(p: Path, data: dict[str, Any], lock: Lock) -> None:
    local_data = []
    utc_tz = datetime.UTC
    try:
        with p.open(encoding="unicode_escape") as f:
            r = csv.reader(f)
            # Skip header row more efficiently
            for row in islice(r, 1, None):
                if not row or len(row) < MIN_ROW_LEN:
                    continue
                try:
                    # date format: dd/mm/YYYY
                    date_obj = datetime.datetime.strptime(row[1], "%d/%m/%Y").replace(tzinfo=utc_tz).date()
                except ValueError, IndexError:
                    continue

                new_row = [row[5], row[6], row[7], row[8]]
                clean_row = ["" if x == " " else x for x in new_row]
                local_data.append((row[0], date_obj, clean_row))
    except csv.Error, UnicodeDecodeError, ValueError, IndexError, OSError:
        logger.warning("Failed to process {}", p)
        return

    if local_data:
        with lock:
            for location, date, values in local_data:
                if location not in data:
                    data[location] = {}
                data[location][date] = values


def get_remote_size() -> int:
    parsed = urlparse(BOM_URL)
    hostname = parsed.hostname
    if not hostname:
        msg = "Could not parse hostname from BOM_URL"
        raise ValueError(msg)

    with ftplib.FTP(hostname) as ftp:  # noqa: S321
        ftp.login()
        size = ftp.size(parsed.path)

    if size is None:
        msg = "Could not retrieve remote file size"
        raise ValueError(msg)

    return size


def main() -> None:
    archive_path = Path(ARCHIVE_NAME)
    if not archive_path.is_file():
        logger.info("Archive not found at {}. Starting download...", ARCHIVE_NAME)
        get_data()
    else:
        logger.info("Checking for remote updates (this may take a moment)...")
        try:
            size1 = archive_path.stat().st_size
            size2 = get_remote_size()

            logger.info("Local file size: {:.2f} MB", size1 / 1024 / 1024)
            logger.info("Remote file size: {:.2f} MB", size2 / 1024 / 1024)

            if size1 != size2:
                logger.info("Remote file size differs from local file size. Downloading update...")
                get_data()
            else:
                logger.info("Archive is up to date (sizes match).")
        except (ValueError, OSError, *ftplib.all_errors) as e:
            logger.warning("Could not check remote file size: {}", e)
            logger.info("Proceeding with existing archive.")

    data: dict[str, Any] = {}
    data_lock = Lock()

    logger.info("Searching for data files...")
    files = [p for p in Path("tables").rglob("*") if p.suffix == ".csv"]
    total_files = len(files)
    logger.info("Found {} files to process.", total_files)

    logger.info("Start Reading Files")
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_file, p, data, data_lock) for p in files]
        for i, _ in enumerate(as_completed(futures), 1):
            if i % 100 == 0:
                logger.info("Processed {}/{} files ({:.2f}%)", i, total_files, i / total_files * 100)

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
