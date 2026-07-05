#!/usr/bin/env python3
"""
Fetch box office data, process into movie‑slug, daywise (one file per date),
and yearly index JSONs. Uses asyncio + aiohttp for fast concurrent fetching.

Handles:
- "Final" entries are for the previous day, "2PM"/"7PM" for the requested date.
- Japanese name is the unique key; English name is used to generate a
  human‑readable slug. If two movies have the same English name, a hash of
  the Japanese name is appended for uniqueness.
- Persistent mapping file (movieslug.json) stores Japanese name → {slug, english_name}.
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
START_DATE = datetime(2019, 1, 1)
DATA_DIR = Path("data")
DATABASE_DIR = Path("database")
DAYWISE_DIR = DATA_DIR / "daywise"
STATE_FILE = Path("state.json")
MOVIE_SLUG_MAP_FILE = DATA_DIR / "movieslug.json"

SEMAPHORE = asyncio.Semaphore(45)

TIME_MAP = {"14:00": "2PM", "19:00": "7PM", None: "Final"}
TIME_ORDER = ["2PM", "7PM", "Final"]

ORDINAL_SUFFIX = {1: "st", 2: "nd", 3: "rd"}

def ordinal(n):
    if 10 <= n % 100 <= 20:
        return "th"
    return ORDINAL_SUFFIX.get(n % 10, "th")

def format_date_str(dt):
    return dt.strftime("%d-%m-%Y")

def parse_date_str(date_str):
    return datetime.strptime(date_str, "%d-%m-%Y")

def week_day_string(release_date, current_date):
    if release_date is None:
        return "Unknown"
    diff = (current_date - release_date).days
    week_num = diff // 7 + 1
    weekday = current_date.strftime("%a")
    return f"{week_num}{ordinal(week_num)} {weekday}"

def generate_slug_from_english(english_name, jp_name=None):
    """
    Generate a filesystem‑safe slug from the English name.
    If the English name is empty, fallback to Japanese.
    Returns a base slug (without uniqueness suffix).
    """
    if not english_name:
        name = jp_name or "unknown"
    else:
        name = english_name

    # Convert to lower-case, replace spaces and non-alnum with hyphens
    slug = re.sub(r'[^a-z0-9]+', '-', name.lower().strip())
    slug = slug.strip('-')
    if not slug:
        slug = "movie"
    # Limit length to 50 characters for filesystem safety
    return slug[:50]

class MovieSlugMapper:
    """
    Persistent mapping: Japanese name -> {slug, english_name}.
    Loaded from / saved to movieslug.json.
    """
    def __init__(self):
        self.map = {}          # jp_name -> {slug, english_name}
        self.dirty = False
        self.lock = asyncio.Lock()

    async def load(self):
        if not MOVIE_SLUG_MAP_FILE.exists():
            return
        try:
            async with aiofiles.open(MOVIE_SLUG_MAP_FILE, 'r', encoding='utf-8') as f:
                data = json.loads(await f.read())
            self.map = data
        except Exception as e:
            logger.warning(f"Failed to load movie slug map: {e}")

    async def save(self):
        if not self.dirty:
            return
        async with self.lock:
            try:
                MOVIE_SLUG_MAP_FILE.parent.mkdir(parents=True, exist_ok=True)
                async with aiofiles.open(MOVIE_SLUG_MAP_FILE, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(self.map, indent=2, ensure_ascii=False))
                self.dirty = False
            except Exception as e:
                logger.error(f"Failed to write movie slug map: {e}")

    def get_slug(self, jp_name):
        entry = self.map.get(jp_name)
        return entry["slug"] if entry else None

    def get_english_name(self, jp_name):
        entry = self.map.get(jp_name)
        return entry["english_name"] if entry else None

    def ensure_movie(self, jp_name, english_name):
        """
        Ensure the movie exists in the map.
        Returns (slug, is_new). For new movies, generates a slug from the
        English name (or Japanese if English is empty). If the generated slug
        is already taken, appends a short hash of the Japanese name.
        If existing, returns its slug and optionally updates english_name if
        it was previously empty.
        """
        if jp_name in self.map:
            slug = self.map[jp_name]["slug"]
            stored_eng = self.map[jp_name]["english_name"]
            # If we have a better English name now, update it (but keep the slug)
            if english_name and (not stored_eng or english_name != stored_eng):
                self.map[jp_name]["english_name"] = english_name
                self.dirty = True
            return slug, False
        else:
            # New movie: generate a slug from the English name
            base_slug = generate_slug_from_english(english_name, jp_name)
            # Check for collisions with existing slugs
            existing_slugs = {v["slug"] for v in self.map.values()}
            slug = base_slug
            if slug in existing_slugs:
                # Append a short hash of the Japanese name to make it unique
                h = hashlib.md5(jp_name.encode('utf-8')).hexdigest()[:6]
                # Ensure we don't exceed 50 chars
                suffix = f"-{h}"
                max_base_len = 50 - len(suffix)
                slug = base_slug[:max_base_len] + suffix
                # In the rare case that even that collides, add a counter
                if slug in existing_slugs:
                    for i in range(1, 100):
                        alt = f"{base_slug[:48]}-{i}"[:50]
                        if alt not in existing_slugs:
                            slug = alt
                            break
            self.map[jp_name] = {"slug": slug, "english_name": english_name or jp_name}
            self.dirty = True
            return slug, True

class MovieStore:
    def __init__(self, mapper):
        self.mapper = mapper
        self.movies = {}          # slug -> {movie_name, jp_name, releaseDate, entries}
        self.dirty = set()
        self.lock = asyncio.Lock()

    async def load_all(self):
        """
        Load all movie JSON files. Groups by Japanese name, merges entries,
        and writes a single consolidated file per movie. Removes duplicates.
        """
        DATABASE_DIR.mkdir(parents=True, exist_ok=True)

        # Temporary grouping: jp_name -> list of (slug, data)
        groups = defaultdict(list)
        for filepath in DATABASE_DIR.glob("*.json"):
            try:
                async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                    data = json.loads(await f.read())
                slug = filepath.stem
                jp_name = data.get("jp_name")
                if not jp_name:
                    logger.warning(f"File {filepath.name} missing jp_name, skipping")
                    continue
                groups[jp_name].append((slug, data))
            except Exception as e:
                logger.warning(f"Failed to load {filepath}: {e}")

        # Process each group
        for jp_name, items in groups.items():
            # Get canonical slug from mapper
            canonical_slug = self.mapper.get_slug(jp_name)
            if not canonical_slug:
                # Generate a new slug using the first item's English name
                first_eng = items[0][1].get("movie_name", jp_name)
                canonical_slug, _ = self.mapper.ensure_movie(jp_name, first_eng)
                # mapper.ensure_movie already sets dirty
            else:
                # Ensure the mapper has the correct English name (use the first item)
                first_eng = items[0][1].get("movie_name", jp_name)
                self.mapper.ensure_movie(jp_name, first_eng)  # may update English name

            # Merge entries from all files for this Japanese name
            merged_entries = []
            seen = set()  # (date, time) to avoid duplicates
            for _, data in items:
                for entry in data.get("entries", []):
                    key = (entry["date"], entry["time"])
                    if key not in seen:
                        merged_entries.append(entry)
                        seen.add(key)
            # Sort entries by date, then by time order
            def entry_sort_key(e):
                try:
                    dt = datetime.strptime(e["date"], "%d-%m-%Y")
                except:
                    dt = datetime.min
                time_priority = TIME_ORDER.index(e["time"]) if e["time"] in TIME_ORDER else 999
                return (dt, time_priority)
            merged_entries.sort(key=entry_sort_key)

            # Determine release date: earliest date in entries
            release_dates = [datetime.strptime(e["date"], "%d-%m-%Y") for e in merged_entries if "date" in e]
            release_date = min(release_dates) if release_dates else datetime.now()

            # Build consolidated movie data
            movie_data = {
                "movie_name": self.mapper.get_english_name(jp_name) or items[0][1].get("movie_name", jp_name),
                "jp_name": jp_name,
                "releaseDate": format_date_str(release_date),
                "entries": merged_entries
            }

            # Write to canonical slug file
            filepath = DATABASE_DIR / f"{canonical_slug}.json"
            try:
                async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(movie_data, indent=2, ensure_ascii=False))
                logger.info(f"Consolidated {jp_name} -> {canonical_slug}.json ({len(merged_entries)} entries)")
            except Exception as e:
                logger.error(f"Failed to write consolidated file {filepath}: {e}")
                continue

            # Delete all other files for this jp_name
            for slug, _ in items:
                if slug != canonical_slug:
                    old_file = DATABASE_DIR / f"{slug}.json"
                    try:
                        old_file.unlink()
                        logger.info(f"Removed duplicate {old_file.name}")
                    except Exception as e:
                        logger.warning(f"Could not delete {old_file}: {e}")

            # Store in self.movies
            self.movies[canonical_slug] = movie_data

        # Also load any files that may not have been grouped (should not happen)
        for filepath in DATABASE_DIR.glob("*.json"):
            slug = filepath.stem
            if slug not in self.movies:
                try:
                    async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                        data = json.loads(await f.read())
                    self.movies[slug] = data
                    jp_name = data.get("jp_name")
                    if jp_name and not self.mapper.get_slug(jp_name):
                        # Add to mapper
                        self.mapper.map[jp_name] = {"slug": slug, "english_name": data.get("movie_name", jp_name)}
                        self.mapper.dirty = True
                except Exception as e:
                    logger.warning(f"Failed to load {filepath}: {e}")

        await self.mapper.save()

    async def get_or_create(self, movie_name, jp_name, date_obj):
        """Get or create a movie entry. Returns (slug, movie_data)."""
        slug, is_new = self.mapper.ensure_movie(jp_name, movie_name)
        if is_new:
            movie_data = {
                "movie_name": movie_name,
                "jp_name": jp_name,
                "releaseDate": format_date_str(date_obj),
                "entries": []
            }
            self.movies[slug] = movie_data
            self.dirty.add(slug)
        else:
            movie_data = self.movies.get(slug)
            if not movie_data:
                movie_data = {
                    "movie_name": movie_name,
                    "jp_name": jp_name,
                    "releaseDate": format_date_str(date_obj),
                    "entries": []
                }
                self.movies[slug] = movie_data
                self.dirty.add(slug)
            else:
                # Update release date if earlier
                existing_release = datetime.strptime(movie_data["releaseDate"], "%d-%m-%Y")
                if date_obj < existing_release:
                    movie_data["releaseDate"] = format_date_str(date_obj)
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
                except Exception as e:
                    logger.error(f"Failed to write {filepath}: {e}")
                else:
                    self.dirty.remove(slug)
        await self.mapper.save()

    def get_release_date(self, slug):
        if slug in self.movies:
            return datetime.strptime(self.movies[slug]["releaseDate"], "%d-%m-%Y")
        return None

    def get_all_movies(self):
        return list(self.movies.values())

# Global instances
movie_slug_mapper = MovieSlugMapper()
movie_store = MovieStore(movie_slug_mapper)

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
    date_str = format_date_str(date_obj)
    if date_str not in daywise_acc:
        daywise_acc[date_str] = {
            "date": date_str,
            "times": {t: [] for t in TIME_ORDER}
        }
    return date_str

async def process_api_response(date_str, data, daywise_acc):
    if not data or "entries" not in data:
        return

    api_date = datetime.strptime(date_str, "%Y-%m-%d")
    today = datetime.today().date()
    start_date = START_DATE.date()

    for entry in data["entries"]:
        if not entry.get("include_independents", False):
            continue
        time_raw = entry.get("time")
        time_label = TIME_MAP.get(time_raw, "Final")

        # Determine actual date for this time slot
        if time_label == "Final":
            actual_date = api_date - timedelta(days=1)
        else:
            actual_date = api_date

        if actual_date.date() < start_date or actual_date.date() > today:
            continue

        actual_date_str = format_date_str(actual_date)
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

            slug, _ = await movie_store.get_or_create(movie_en, jp_name, actual_date)
            release_date = movie_store.get_release_date(slug)

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
    for date_str, daywise_obj in daywise_acc.items():
        times_list = [
            {
                "title": "Including Independents",
                "time": time_label,
                "data": daywise_obj["times"][time_label]
            }
            for time_label in TIME_ORDER
        ]
        file_data = {"date": date_str, "times": times_list}
        filepath = DAYWISE_DIR / f"{date_str}.json"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(file_data, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Failed to write {filepath}: {e}")

async def build_yearly_index():
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
    await movie_slug_mapper.load()
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
