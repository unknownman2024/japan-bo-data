#!/usr/bin/env python3
"""
Fetch box office data, process into movie‑slug, daywise (one file per date),
and yearly index JSONs. Uses asyncio + aiohttp for fast concurrent fetching.

Handles the fact that the "Final" entries in the API response are always for
the previous day, while "2PM" and "7PM" entries are for the requested date.
"""

import asyncio
import aiohttp
import aiofiles
import json
import re
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://japanapi.text2024mail.workers.dev/?date={}"
START_DATE = datetime(2019, 1, 1)          # Full fetch from this date
DATA_DIR = Path("data")
DATABASE_DIR = Path("database")
DAYWISE_DIR = DATA_DIR / "daywise"
STATE_FILE = Path("state.json")

# Semaphore to limit concurrent requests
SEMAPHORE = asyncio.Semaphore(45)

# Time mapping and ordering
TIME_MAP = {
    "14:00": "2PM",
    "19:00": "7PM",
    None: "Final"
}
TIME_ORDER = ["2PM", "7PM", "Final"]

# Ordinal suffix for day strings
ORDINAL_SUFFIX = {1: "st", 2: "nd", 3: "rd"}

def ordinal(n):
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = ORDINAL_SUFFIX.get(n % 10, "th")
    return f"{n}{suffix}"

def make_slug(name, jp_name):
    """
    Generate a short, filesystem‑safe slug (max 50 characters).
    If name is empty or too long, uses a hash to ensure uniqueness.
    """
    if not name:
        name = jp_name or "unknown"

    # Convert to lower-case, replace non-alnum with hyphens
    base = re.sub(r'[^a-z0-9]+', '-', name.lower().strip())
    base = base.strip('-')
    if not base:
        base = "unknown"

    # Truncate to avoid filesystem length limits (50 chars is safe)
    max_len = 50
    if len(base) > max_len:
        # Use a short hash of the Japanese name (or English if not available)
        hash_source = jp_name if jp_name else name
        h = hashlib.md5(hash_source.encode('utf-8')).hexdigest()[:6]
        base = base[:max_len - 7] + '-' + h   # 43 + '-' + 6 = 50
    return base

def parse_date_str(date_str):
    return datetime.strptime(date_str, "%d-%m-%Y")

def format_date_str(dt):
    return dt.strftime("%d-%m-%Y")

def week_day_string(release_date, current_date):
    if release_date is None:
        return "Unknown"
    diff = (current_date - release_date).days
    week_num = diff // 7 + 1
    weekday = current_date.strftime("%a")
    return f"{ordinal(week_num)} {weekday}"

class MovieStore:
    def __init__(self):
        self.movies = {}          # slug -> {movie_name, jp_name, releaseDate, entries}
        self.dirty = set()
        self.lock = asyncio.Lock()
        self.slug_cache = {}      # (movie_name, jp_name) -> slug

    async def load_all(self):
        DATABASE_DIR.mkdir(parents=True, exist_ok=True)
        for filepath in DATABASE_DIR.glob("*.json"):
            try:
                async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                    data = json.loads(await f.read())
                slug = filepath.stem
                self.movies[slug] = data
                self.slug_cache[(data["movie_name"], data["jp_name"])] = slug
            except Exception as e:
                logger.warning(f"Failed to load {filepath}: {e}")

    async def get_or_create(self, movie_name, jp_name, date_obj):
        key = (movie_name, jp_name)
        if key in self.slug_cache:
            slug = self.slug_cache[key]
        else:
            base_slug = make_slug(movie_name, jp_name)
            # Check for conflicts (same English name but different Japanese name)
            conflicting = [s for s, m in self.movies.items() 
                           if m["movie_name"] == movie_name and s != base_slug]
            if conflicting:
                h = hashlib.md5(jp_name.encode('utf-8')).hexdigest()[:6]
                slug = f"{base_slug[:40]}-{h}"  # ensure total length <= 50
            else:
                slug = base_slug

            # Final uniqueness check
            if slug in self.movies and self.movies[slug]["jp_name"] != jp_name:
                h = hashlib.md5(jp_name.encode('utf-8')).hexdigest()[:6]
                slug = f"{base_slug[:40]}-{h}"

            self.slug_cache[key] = slug

        if slug not in self.movies:
            self.movies[slug] = {
                "movie_name": movie_name,
                "jp_name": jp_name,
                "releaseDate": format_date_str(date_obj),
                "entries": []
            }
            self.dirty.add(slug)
        else:
            existing = self.movies[slug]
            # Update names if changed
            if existing["movie_name"] != movie_name or existing["jp_name"] != jp_name:
                existing["movie_name"] = movie_name
                existing["jp_name"] = jp_name
                self.dirty.add(slug)
            # Update release date if earlier
            existing_release = datetime.strptime(existing["releaseDate"], "%d-%m-%Y")
            if date_obj < existing_release:
                existing["releaseDate"] = format_date_str(date_obj)
                self.dirty.add(slug)

        return slug, self.movies[slug]

    async def add_entry(self, slug, entry):
        async with self.lock:
            movie = self.movies[slug]
            # Update or insert
            for i, e in enumerate(movie["entries"]):
                if e["date"] == entry["date"] and e["time"] == entry["time"]:
                    movie["entries"][i] = entry
                    break
            else:
                movie["entries"].append(entry)
            self.dirty.add(slug)

    async def flush(self):
        async with self.lock:
            for slug in list(self.dirty):
                filepath = DATABASE_DIR / f"{slug}.json"
                try:
                    async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                        await f.write(json.dumps(self.movies[slug], indent=2, ensure_ascii=False))
                except OSError as e:
                    logger.error(f"OS error writing {filepath}: {e}")
                except Exception as e:
                    logger.error(f"Failed to write {filepath}: {e}")
                else:
                    self.dirty.remove(slug)

    def get_release_date(self, slug):
        if slug in self.movies:
            return datetime.strptime(self.movies[slug]["releaseDate"], "%d-%m-%Y")
        return None

    def get_all_movies(self):
        return list(self.movies.values())

# Global store
movie_store = MovieStore()

async def fetch_date(session, date_str):
    url = BASE_URL.format(date_str)
    try:
        async with SEMAPHORE:
            async with session.get(url, timeout=30) as resp:
                if resp.status != 200:
                    logger.warning(f"Date {date_str} returned {resp.status}")
                    return None
                return await resp.json()
    except Exception as e:
        logger.error(f"Error fetching {date_str}: {e}")
        return None

def ensure_daywise_date(daywise_acc, date_obj):
    """Ensure the daywise accumulator has an entry for the given date."""
    date_str = format_date_str(date_obj)
    if date_str not in daywise_acc:
        daywise_acc[date_str] = {
            "date": date_str,
            "times": {t: [] for t in TIME_ORDER}
        }
    return date_str

async def process_api_response(date_str, data, daywise_acc):
    """
    Process a single API response.
    
    - 2PM and 7PM entries are assigned to the requested date.
    - Final entries are assigned to the previous day.
    - Updates the movie store and the daywise accumulator.
    """
    if not data or "entries" not in data:
        return

    api_date = datetime.strptime(date_str, "%Y-%m-%d")
    start_date = START_DATE.date()
    today = datetime.today().date()

    for entry in data["entries"]:
        if not entry.get("include_independents", False):
            continue

        time_raw = entry.get("time")
        time_label = TIME_MAP.get(time_raw, "Final")

        # Determine the actual date for these entries
        if time_label == "Final":
            actual_date = api_date - timedelta(days=1)
        else:
            actual_date = api_date

        # Skip if the actual date is outside our desired range
        if actual_date.date() < start_date or actual_date.date() > today:
            continue

        actual_date_str = format_date_str(actual_date)
        # Ensure daywise entry exists
        ensure_daywise_date(daywise_acc, actual_date)

        for movie in entry.get("data", []):
            movie_en = movie.get("movie_en") or movie["movie"]
            jp_name = movie["movie"]
            sales = movie["sales"]
            seats = movie["seats"]
            showtimes = movie["showtimes"]
            theaters = movie["theaters"]
            rank = movie["rank"]
            last_week_ratio = movie.get("last_week_ratio")

            # Get or create movie in store
            slug, _ = await movie_store.get_or_create(movie_en, jp_name, actual_date)
            release_date = movie_store.get_release_date(slug)

            # Build entry for movie store
            entry_obj = {
                "title": "Including Independents",
                "date": actual_date_str,
                "time": time_label,
                "rank": rank,
                "sales": sales,
                "seats": seats,
                "showtimes": showtimes,
                "theaters": theaters,
                "last_week_ratio": last_week_ratio,
                "day": week_day_string(release_date, actual_date)
            }
            await movie_store.add_entry(slug, entry_obj)

            # Add to daywise accumulator
            daywise_entry = {
                "moviename": movie_en,
                "japanese name": jp_name,
                "sales": sales,
                "seats": seats,
                "showtimes": showtimes,
                "theaters": theaters,
                "rank": rank,
                "last_week_ratio": last_week_ratio
            }
            daywise_acc[actual_date_str]["times"][time_label].append(daywise_entry)

async def write_daywise(daywise_acc):
    """Write daywise files: one JSON per date from the accumulator."""
    for date_str, daywise_obj in daywise_acc.items():
        # Convert the times dict to the expected list format
        times_list = [
            {
                "title": "Including Independents",
                "time": time_label,
                "data": daywise_obj["times"][time_label]
            }
            for time_label in TIME_ORDER
        ]
        # Remove entries with empty data? Keep them as empty lists.
        file_data = {
            "date": date_str,
            "times": times_list
        }
        filepath = DAYWISE_DIR / f"{date_str}.json"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(file_data, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Failed to write {filepath}: {e}")

async def build_yearly_index():
    """Rebuild data/YYYY/index.json from all movie records."""
    all_movies = movie_store.get_all_movies()
    yearly = defaultdict(list)

    for movie in all_movies:
        release_date = datetime.strptime(movie["releaseDate"], "%d-%m-%Y")
        year = release_date.year

        total_sales = sum(e["sales"] for e in movie["entries"])
        total_seats = sum(e["seats"] for e in movie["entries"])
        total_showtimes = sum(e["showtimes"] for e in movie["entries"])
        total_theaters = sum(e["theaters"] for e in movie["entries"])

        yearly[year].append({
            "movie_name": movie["movie_name"],
            "releaseDate": movie["releaseDate"],
            "total_sales": total_sales,
            "total_seats": total_seats,
            "total_showtimes": total_showtimes,
            "total_theaters": total_theaters
        })

    for year, movies in yearly.items():
        year_dir = DATA_DIR / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        filepath = year_dir / "index.json"
        try:
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(movies, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Failed to write {filepath}: {e}")

    logger.info("Yearly indices rebuilt.")

async def main(full_fetch=False):
    await movie_store.load_all()

    today = datetime.today().date()
    if full_fetch:
        start_date = START_DATE.date()
        end_date = today
        logger.info("Performing full fetch from %s to %s", start_date, end_date)
    else:
        start_date = today - timedelta(days=1)
        if start_date < START_DATE.date():
            start_date = START_DATE.date()
        end_date = today
        logger.info("Incremental fetch from %s to %s", start_date, end_date)

    date_list = []
    current = start_date
    while current <= end_date:
        date_list.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    daywise_acc = {}

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_date(session, d) for d in date_list]
        responses = await asyncio.gather(*tasks)

        for date_str, data in zip(date_list, responses):
            if data:
                await process_api_response(date_str, data, daywise_acc)
                logger.info("Processed %s", date_str)

    if daywise_acc:
        await write_daywise(daywise_acc)

    await movie_store.flush()
    await build_yearly_index()

    # Update state file
    state = {"last_run": today.isoformat()}
    try:
        async with aiofiles.open(STATE_FILE, 'w') as f:
            await f.write(json.dumps(state))
    except Exception as e:
        logger.error(f"Failed to write state file: {e}")

    logger.info("All done!")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", 
                        help="Perform full fetch from 2019-01-01")
    args = parser.parse_args()
    asyncio.run(main(full_fetch=args.full))
