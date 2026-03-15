import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import logging
from datetime import date, timedelta
from io import StringIO

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
MASTER_FILE = os.path.join(DATA_DIR, "daily_master.csv")


def scrape_daily(target_date: str) -> pd.DataFrame | None:
    """
    Scrapes Box Office Mojo daily chart for a given date.
    Args:
        target_date: Date string in YYYY-MM-DD format
    Returns:
        DataFrame of results or None if failed
    """
    url = f"https://www.boxofficemojo.com/date/{target_date}/"
    logger.info(f"Scraping {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {target_date}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")

    if not table:
        logger.warning(f"No table found for {target_date}")
        return None


    df = pd.read_html(StringIO(str(table)))[0]
    df["date"] = target_date
    logger.info(f"Found {len(df)} rows for {target_date}")
    return df


def get_last_scraped_date() -> str | None:
    """
    Checks the master CSV for the most recent date already scraped.
    """
    if not os.path.exists(MASTER_FILE):
        return None
    df = pd.read_csv(MASTER_FILE)
    return df["date"].max()


def upsert_to_master(new_df: pd.DataFrame):
    """
    Merges new data into the master CSV, updating existing rows
    for the same date to handle estimate vs actual revisions.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(MASTER_FILE):
        master_df = pd.read_csv(MASTER_FILE)
        dates_to_update = new_df["date"].unique()
        master_df = master_df[~master_df["date"].isin(dates_to_update)]
        combined = pd.concat([master_df, new_df], ignore_index=True)
    else:
        combined = new_df

    combined.sort_values("date", inplace=True)
    combined.to_csv(MASTER_FILE, index=False)
    logger.info(f"Master file updated: {len(combined)} total rows")


def run_incremental(days_back: int = 7):
    """
    Main entry point. Scrapes from the last scraped date up to today.
    Args:
        days_back: How many days back to scrape on first run
    """
    today = date.today()
    last_scraped = get_last_scraped_date()

    if last_scraped:
        start = date.fromisoformat(last_scraped) + timedelta(days=1)
        logger.info(f"Incremental run: picking up from {start}")
    else:
        start = today - timedelta(days=days_back)
        logger.info(f"First run: scraping last {days_back} days from {start}")

    all_frames = []
    current = start

    while current <= today:
        df = scrape_daily(str(current))
        if df is not None:
            all_frames.append(df)
        current += timedelta(days=1)

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        upsert_to_master(combined)
    else:
        logger.info("No new data to save.")


if __name__ == "__main__":
    run_incremental(days_back=7)