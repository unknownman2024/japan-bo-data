#!/usr/bin/env python3
"""
Fetch box office data, process into movie‑slug, daywise (one file per date),
and yearly index JSONs. Uses asyncio + aiohttp for fast concurrent fetching.
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

async def process_date(date_str, data):
    """Process a single day's JSON and update movie_store and collect daywise data."""
    if not data or "entries" not in data:
        return None

    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d-%m-%Y")

    # Structure: time_label -> list of movie entries
    time_data = {time: [] for time in TIME_ORDER}

    for entry in data["entries"]:
        if not entry.get("include_independents", False):
            continue
        time_raw = entry.get("time")
        time_label = TIME_MAP.get(time_raw, "Final")

        for movie in entry.get("data", []):
            movie_en = movie.get("movie_en") or movie["movie"]
            jp_name = movie["movie"]
            sales = movie["sales"]
            seats = movie["seats"]
            showtimes = movie["showtimes"]
            theaters = movie["theaters"]
            rank = movie["rank"]
            last_week_ratio = movie.get("last_week_ratio")

            slug, _ = await movie_store.get_or_create(movie_en, jp_name, date_obj)
            release_date = movie_store.get_release_date(slug)

            # Add to movie's own entries
            entry_obj = {
                "title": "Including Independents",
                "date": formatted_date,
                "time": time_label,
                "rank": rank,
                "sales": sales,
                "seats": seats,
                "showtimes": showtimes,
                "theaters": theaters,
                "last_week_ratio": last_week_ratio,
                "day": week_day_string(release_date, date_obj)
            }
            await movie_store.add_entry(slug, entry_obj)

            # Add to daywise data for this date
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
            time_data[time_label].append(daywise_entry)

    # Build the daywise object for this date
    times_list = [
        {
            "title": "Including Independents",
            "time": time_label,
            "data": time_data[time_label]
        }
        for time_label in TIME_ORDER
    ]

    return {
        "date": formatted_date,
        "times": times_list
    }

async def write_daywise(daywise_collection):
    """Write daywise files: one JSON per date."""
    for date_str, daywise_obj in daywise_collection.items():
        filepath = DAYWISE_DIR / f"{date_str}.json"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(daywise_obj, indent=2, ensure_ascii=False))
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

    daywise_collection = {}

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_date(session, d) for d in date_list]
        responses = await asyncio.gather(*tasks)

        for date_str, data in zip(date_list, responses):
            if data:
                daywise_obj = await process_date(date_str, data)
                if daywise_obj:
                    daywise_collection[date_str] = daywise_obj
                logger.info("Processed %s", date_str)

    if daywise_collection:
        await write_daywise(daywise_collection)

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
