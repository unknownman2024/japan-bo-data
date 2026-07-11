#!/usr/bin/env python3
"""
Fetch box office data, process into movie‑slug, daywise (one file per date),
and yearly index JSONs. Uses asyncio + aiohttp for fast concurrent fetching.

Supports custom date ranges via --start and --end.
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
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://japanapi.bfilmyisback.workers.dev/?date={}"
START_DATE = datetime(2015, 2, 1)          # earliest data we have

# Use absolute paths based on script location
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATABASE_DIR = BASE_DIR / "database"
DAYWISE_DIR = DATA_DIR / "daywise"
STATE_FILE = BASE_DIR / "state.json"
MOVIE_SLUG_MAP_FILE = DATA_DIR / "movieslug.json"
CORRECTIONS_FILE = BASE_DIR / "correctedslug.json"

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
    if not english_name:
        name = jp_name or "unknown"
    else:
        name = english_name
    slug = re.sub(r'[^a-z0-9]+', '-', name.lower().strip())
    slug = slug.strip('-')
    if not slug:
        slug = "movie"
    return slug[:50]

def entry_sort_key(e):
    try:
        dt = datetime.strptime(e["date"], "%d-%m-%Y")
    except:
        dt = datetime.min
    time_priority = TIME_ORDER.index(e["time"]) if e["time"] in TIME_ORDER else 999
    return (dt, time_priority)

class MovieSlugMapper:
    def __init__(self):
        self.map = {}
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

    def resolve(self, jp_name):
        current = jp_name
        seen = set()
        while current in self.map and "redirect" in self.map[current]:
            if current in seen:
                break
            seen.add(current)
            current = self.map[current]["redirect"]
        return current

    def get_slug(self, jp_name):
        primary = self.resolve(jp_name)
        entry = self.map.get(primary)
        return entry["slug"] if entry else None

    def get_english_name(self, jp_name):
        primary = self.resolve(jp_name)
        entry = self.map.get(primary)
        return entry["english_name"] if entry else None

    def ensure_movie(self, jp_name, english_name):
        """
        Ensure the movie exists in the map.
        Returns (slug, is_new).
        """
        primary = self.resolve(jp_name)
        if primary in self.map:
            entry = self.map[primary]
            slug = entry["slug"]
            stored_eng = entry.get("english_name", "")
            if english_name and not stored_eng and not entry.get("manual_english", False):
                entry["english_name"] = english_name
                self.dirty = True
            return slug, False
        else:
            base_slug = generate_slug_from_english(english_name, primary)
            existing_slugs = {v["slug"] for v in self.map.values() if "slug" in v}
            slug = base_slug
            if slug in existing_slugs:
                h = hashlib.md5(primary.encode('utf-8')).hexdigest()[:6]
                suffix = f"-{h}"
                max_base_len = 50 - len(suffix)
                slug = base_slug[:max_base_len] + suffix
                if slug in existing_slugs:
                    for i in range(1, 100):
                        alt = f"{base_slug[:48]}-{i}"[:50]
                        if alt not in existing_slugs:
                            slug = alt
                            break
            self.map[primary] = {
                "slug": slug,
                "english_name": english_name or primary,
                "manual_english": False,
                "manual_slug": False
            }
            self.dirty = True
            return slug, True

class MovieStore:
    def __init__(self, mapper):
        self.mapper = mapper
        self.movies = {}
        self.dirty = set()
        self.lock = asyncio.Lock()

    async def load_all(self):
        DATABASE_DIR.mkdir(parents=True, exist_ok=True)
        groups = defaultdict(list)
        for filepath in DATABASE_DIR.glob("*.json"):
            try:
                async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                    data = json.loads(await f.read())
                slug = filepath.stem
                jp_name = data.get("jp_name")
                if not jp_name:
                    continue
                primary = self.mapper.resolve(jp_name)
                groups[primary].append((slug, data))
            except Exception as e:
                logger.warning(f"Failed to load {filepath}: {e}")

        for primary_jp, items in groups.items():
            canonical_slug = self.mapper.get_slug(primary_jp)
            if not canonical_slug:
                first_eng = items[0][1].get("movie_name", primary_jp)
                canonical_slug, _ = self.mapper.ensure_movie(primary_jp, first_eng)
                await self.mapper.save()

            first_eng = items[0][1].get("movie_name", primary_jp)
            self.mapper.ensure_movie(primary_jp, first_eng)

            merged_entries = []
            seen = set()
            for _, data in items:
                for entry in data.get("entries", []):
                    key = (entry["date"], entry["time"])
                    if key not in seen:
                        merged_entries.append(entry)
                        seen.add(key)
            merged_entries.sort(key=entry_sort_key)

            release_dates = [datetime.strptime(e["date"], "%d-%m-%Y") for e in merged_entries if "date" in e]
            release_date = min(release_dates) if release_dates else datetime.now()

            movie_data = {
                "movie_name": self.mapper.get_english_name(primary_jp) or items[0][1].get("movie_name", primary_jp),
                "jp_name": primary_jp,
                "releaseDate": format_date_str(release_date),
                "entries": merged_entries
            }

            filepath = DATABASE_DIR / f"{canonical_slug}.json"
            try:
                async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(movie_data, indent=2, ensure_ascii=False))
                logger.info(f"Consolidated {primary_jp} -> {canonical_slug}.json ({len(merged_entries)} entries)")
            except Exception as e:
                logger.error(f"Failed to write consolidated file {filepath}: {e}")
                continue

            for slug, _ in items:
                if slug != canonical_slug:
                    old_file = DATABASE_DIR / f"{slug}.json"
                    try:
                        old_file.unlink()
                        logger.info(f"Removed duplicate {old_file.name}")
                    except Exception as e:
                        logger.warning(f"Could not delete {old_file}: {e}")

            self.movies[canonical_slug] = movie_data

        for filepath in DATABASE_DIR.glob("*.json"):
            slug = filepath.stem
            if slug not in self.movies:
                try:
                    async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                        data = json.loads(await f.read())
                    self.movies[slug] = data
                    jp_name = data.get("jp_name")
                    if jp_name and not self.mapper.get_slug(jp_name):
                        self.mapper.map[jp_name] = {
                            "slug": slug,
                            "english_name": data.get("movie_name", jp_name),
                            "manual_english": False,
                            "manual_slug": False
                        }
                        self.mapper.dirty = True
                except Exception as e:
                    logger.warning(f"Failed to load {filepath}: {e}")

        await self.mapper.save()

    async def get_or_create(self, movie_name, jp_name, date_obj):
        primary_jp = self.mapper.resolve(jp_name)
        slug, is_new = self.mapper.ensure_movie(primary_jp, movie_name)
        if is_new:
            movie_data = {
                "movie_name": movie_name,
                "jp_name": primary_jp,
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
                    "jp_name": primary_jp,
                    "releaseDate": format_date_str(date_obj),
                    "entries": []
                }
                self.movies[slug] = movie_data
                self.dirty.add(slug)
            else:
                existing_release = datetime.strptime(movie_data["releaseDate"], "%d-%m-%Y")
                if date_obj < existing_release:
                    movie_data["releaseDate"] = format_date_str(date_obj)
                    self.dirty.add(slug)
        return slug, self.movies[slug]

    async def add_entry(self, slug, entry):
        async with self.lock:
            movie = self.movies[slug]
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

async def process_api_response(date_str, data, daywise_acc, start_date):
    if not data or "entries" not in data:
        return

    api_date = datetime.strptime(date_str, "%Y-%m-%d")
    today = datetime.today().date()
    # Use the supplied start_date (not global)
    start = start_date if isinstance(start_date, datetime) else start_date

    for entry in data["entries"]:
        if not entry.get("include_independents", False):
            continue
        time_raw = entry.get("time")
        time_label = TIME_MAP.get(time_raw, "Final")

        if time_label == "Final":
            actual_date = api_date - timedelta(days=1)
        else:
            actual_date = api_date

        if actual_date.date() < start.date() or actual_date.date() > today:
            continue

        actual_date_str = format_date_str(actual_date)
        ensure_daywise_date(daywise_acc, actual_date)

        for movie in entry.get("data", []):
            movie_en = movie.get("movie_en") or movie["movie"]
            raw_jp = movie["movie"]
            primary_jp = movie_slug_mapper.resolve(raw_jp)

            slug, _ = await movie_store.get_or_create(movie_en, primary_jp, actual_date)

            definitive_eng = movie_slug_mapper.get_english_name(primary_jp)
            if not definitive_eng:
                definitive_eng = movie_en

            sales = movie["sales"]
            seats = movie["seats"]
            showtimes = movie["showtimes"]
            theaters = movie["theaters"]
            rank = movie["rank"]
            last_week_ratio = movie.get("last_week_ratio")

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
                "moviename": definitive_eng,
                "japanese name": primary_jp,
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

async def rebuild_daywise_from_database():
    logger.info("Rebuilding all daywise files from database...")
    movies = movie_store.get_all_movies()
    daywise_data = defaultdict(lambda: {t: [] for t in TIME_ORDER})

    for movie in movies:
        jp_name = movie["jp_name"]
        movie_name = movie["movie_name"]
        for entry in movie["entries"]:
            date_str = entry["date"]
            time_label = entry["time"]
            if time_label not in TIME_ORDER:
                continue
            day_entry = {
                "moviename": movie_name,
                "japanese name": jp_name,
                "sales": entry["sales"],
                "seats": entry["seats"],
                "showtimes": entry["showtimes"],
                "theaters": entry["theaters"],
                "rank": entry["rank"],
                "last_week_ratio": entry.get("last_week_ratio")
            }
            daywise_data[date_str][time_label].append(day_entry)

    for date_str, times_dict in daywise_data.items():
        times_list = [
            {
                "title": "Including Independents",
                "time": time_label,
                "data": times_dict[time_label]
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
            logger.error(f"Failed to write daywise {filepath}: {e}")

    logger.info(f"Rebuilt {len(daywise_data)} daywise files.")

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

async def apply_corrections():
    if not CORRECTIONS_FILE.exists():
        logger.info("No corrections file found, skipping.")
        return False

    try:
        async with aiofiles.open(CORRECTIONS_FILE, 'r', encoding='utf-8') as f:
            corrections = json.loads(await f.read())
        logger.info(f"Loaded corrections file with {len(corrections)} entries.")
    except Exception as e:
        logger.error(f"Failed to read corrections file: {e}")
        return False

    await movie_slug_mapper.load()
    changed = False

    for primary_jp, data in corrections.items():
        primary = movie_slug_mapper.resolve(primary_jp)
        if primary != primary_jp:
            logger.warning(f"Correction key '{primary_jp}' resolves to '{primary}'; using primary.")

        if primary not in movie_slug_mapper.map:
            movie_slug_mapper.map[primary] = {
                "slug": generate_slug_from_english(data.get("new_english_name", primary), primary),
                "english_name": data.get("new_english_name", primary),
                "manual_english": False,
                "manual_slug": False
            }
            changed = True

        entry = movie_slug_mapper.map[primary]

        if "new_slug" in data:
            new_slug = data["new_slug"]
            if entry.get("slug") != new_slug:
                logger.info(f"Updating slug for '{primary}': '{entry.get('slug')}' -> '{new_slug}'")
                entry["slug"] = new_slug
                entry["manual_slug"] = True
                changed = True

        if "new_english_name" in data:
            new_name = data["new_english_name"]
            if entry.get("english_name") != new_name:
                logger.info(f"Updating English name for '{primary}': '{entry.get('english_name')}' -> '{new_name}'")
                entry["english_name"] = new_name
                entry["manual_english"] = True
                changed = True

        if "merge" in data:
            for merged_jp in data["merge"]:
                if merged_jp == primary:
                    continue
                if movie_slug_mapper.map.get(merged_jp, {}).get("redirect") != primary:
                    logger.info(f"Redirecting '{merged_jp}' -> '{primary}'")
                    movie_slug_mapper.map[merged_jp] = {"redirect": primary}
                    changed = True

    if changed:
        await movie_slug_mapper.save()
        await reconsolidate_movies()
        await rebuild_daywise_from_database()
        logger.info("Corrections applied, database and daywise rebuilt.")
    else:
        logger.info("No corrections needed (already up‑to‑date).")

    return changed

async def reconsolidate_movies():
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)

    groups = defaultdict(list)
    for filepath in DATABASE_DIR.glob("*.json"):
        try:
            async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                data = json.loads(await f.read())
            slug = filepath.stem
            jp_name = data.get("jp_name")
            if not jp_name:
                continue
            primary = movie_slug_mapper.resolve(jp_name)
            groups[primary].append((slug, data))
        except Exception as e:
            logger.warning(f"Failed to read {filepath}: {e}")

    for primary_jp, items in groups.items():
        canonical_slug = movie_slug_mapper.get_slug(primary_jp)
        if not canonical_slug:
            eng = movie_slug_mapper.get_english_name(primary_jp) or primary_jp
            canonical_slug, _ = movie_slug_mapper.ensure_movie(primary_jp, eng)
            await movie_slug_mapper.save()

        merged_entries = []
        seen = set()
        for _, data in items:
            for entry in data.get("entries", []):
                key = (entry["date"], entry["time"])
                if key not in seen:
                    merged_entries.append(entry)
                    seen.add(key)
        merged_entries.sort(key=entry_sort_key)

        release_dates = [datetime.strptime(e["date"], "%d-%m-%Y") for e in merged_entries if "date" in e]
        release_date = min(release_dates) if release_dates else datetime.now()

        movie_data = {
            "movie_name": movie_slug_mapper.get_english_name(primary_jp) or primary_jp,
            "jp_name": primary_jp,
            "releaseDate": format_date_str(release_date),
            "entries": merged_entries
        }

        new_file = DATABASE_DIR / f"{canonical_slug}.json"
        try:
            async with aiofiles.open(new_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(movie_data, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Failed to write {new_file}: {e}")
            continue

        for slug, _ in items:
            if slug != canonical_slug:
                old_file = DATABASE_DIR / f"{slug}.json"
                try:
                    old_file.unlink()
                    logger.info(f"Removed old file {old_file.name}")
                except Exception as e:
                    logger.warning(f"Could not delete {old_file}: {e}")

        movie_store.movies[canonical_slug] = movie_data

async def main(start_date=None, end_date=None, full_fetch=False):
    """
    Fetch data between start_date and end_date (inclusive).
    If not provided:
      - full_fetch: from START_DATE to today
      - otherwise: only yesterday
    """
    await movie_slug_mapper.load()
    await movie_store.load_all()
    await apply_corrections()

    today = datetime.today().date()

    if start_date is None:
        if full_fetch:
            start_date = START_DATE.date()
        else:
            start_date = today - timedelta(days=1)
            if start_date < START_DATE.date():
                start_date = START_DATE.date()
    else:
        # ensure it's a date object
        if isinstance(start_date, datetime):
            start_date = start_date.date()

    if end_date is None:
        if full_fetch:
            end_date = today
        else:
            end_date = start_date   # single day
    else:
        if isinstance(end_date, datetime):
            end_date = end_date.date()

    if start_date > end_date:
        logger.error("Start date must be before or equal to end date.")
        return

    # Clip to START_DATE (we never go before that)
    if start_date < START_DATE.date():
        start_date = START_DATE.date()

    logger.info("Fetching from %s to %s", start_date, end_date)

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
                await process_api_response(date_str, data, daywise_acc, start_date)
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
    parser = argparse.ArgumentParser(description="Fetch box office data for a date range.")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD). Default: yesterday (or START_DATE if --full).")
    parser.add_argument("--end", help="End date (YYYY-MM-DD). Default: same as start (or today if --full).")
    parser.add_argument("--full", action="store_true", help="Fetch from START_DATE to today (ignores --start/--end).")
    args = parser.parse_args()

    start = None
    end = None
    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d")
    if args.end:
        end = datetime.strptime(args.end, "%Y-%m-%d")

    asyncio.run(main(start_date=start, end_date=end, full_fetch=args.full))
