#!/usr/bin/env python3
"""
RSS loader for VC-Sourcing-Tool.

- Loads startup-related RSS feeds (TechCrunch Startups, EU-Startups)
- Filters for funding-relevant items using keyword heuristics
- Extracts normalized fields
- Persists results to SQLite (with upsert-like behavior on unique link)
- Writes an append-safe, deduplicated CSV backup

Usage:
    python etl/rss_loader.py [--max-items N] [--since-days D]

Requirements:
    - feedparser
    - python-dateutil
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import feedparser
from dateutil import parser as date_parser

# ---------------------------
# Configuration
# ---------------------------

FEEDS: List[Tuple[str, str]] = [
    ("https://techcrunch.com/startups/feed/", "techcrunch"),
    ("https://www.eu-startups.com/feed/", "eu-startups"),
    ("https://venturebeat.com/category/startups/feed/", "venturebeat"),
    ("https://sifted.eu/feed/", "sifted"),
    ("https://news.crunchbase.com/feed/", "crunchbase-news"),
    ("https://www.businessinsider.com/sai/rss", "businessinsider"),
    ("https://eu.vc/feed/", "eu-vc"),
    ("http://news.ycombinator.com/rss", "hackernews"),
    ("http://firstround.com/review/feed.xml", "firstround"),
    ("http://feed.onstartups.com/onstartups", "onstartups"),
    ("https://bothsidesofthetable.com/feed", "bothsides"),
    ("http://steveblank.com/feed/", "steveblank"),
    ("http://ben-evans.com/benedictevans?format=rss", "benedictevans"),
    ("http://andrewchen.co/feed/", "andrewchen"),
    ("http://blog.samaltman.com/posts.atom", "samaltman"),
]

FUNDING_KEYWORDS: List[str] = [
    "raises",
    "funding",
    "series",
    "seed",
    "round",
    "secures",
    "secured",
    "closes",
    "closing",
    "invests",
    "investment",
    "backs",
    "backed",
]

DB_PATH: str = os.path.join("data", "vc_tool.db")
CSV_PATH: str = os.path.join("data", "news_clean.csv")

SQL_CREATE_TABLE: str = """
CREATE TABLE IF NOT EXISTS news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    link TEXT NOT NULL UNIQUE,
    published_utc TEXT,
    source TEXT NOT NULL,
    company TEXT,
    amount_value REAL,
    amount_currency TEXT,
    stage TEXT,
    inserted_at_utc TEXT NOT NULL
);
"""

SQL_INSERT_NEWS: str = """
INSERT OR IGNORE INTO news (
    title, link, published_utc, source, company,
    amount_value, amount_currency, stage, inserted_at_utc
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

CSV_HEADERS: List[str] = [
    "title",
    "link",
    "published_utc",
    "source",
    "company",
    "amount_value",
    "amount_currency",
    "stage",
    "inserted_at_utc",
]


# ---------------------------
# Data structures
# ---------------------------

@dataclass
class NewsItem:
    title: str
    link: str
    published_utc: Optional[str]
    source: str  # "techcrunch" | "eu-startups"
    company: Optional[str]
    amount_value: Optional[float]
    amount_currency: Optional[str]  # USD | EUR | GBP
    stage: Optional[str]  # Seed | Pre-Seed | Series A/B/C...
    inserted_at_utc: str


# ---------------------------
# Logging
# ---------------------------

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


# ---------------------------
# Utilities
# ---------------------------

def ensure_data_dir() -> None:
    """Ensure the data directory exists."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def is_funding_related(title: str) -> bool:
    """Check if title contains funding-related keywords."""
    text = title.lower()
    return any(keyword in text for keyword in FUNDING_KEYWORDS)


def to_iso8601_utc(dt: datetime) -> str:
    """Return ISO8601 string with timezone info (UTC)."""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def parse_published_utc(entry: Dict[str, Any]) -> Optional[str]:
    """
    Parse entry published time into ISO8601 UTC string, if available.
    Tries 'published_parsed', then 'updated_parsed', then text parsing.
    """
    try:
        if "published_parsed" in entry and entry["published_parsed"]:
            dt = datetime(*entry["published_parsed"][:6], tzinfo=timezone.utc)
            return to_iso8601_utc(dt)
        if "updated_parsed" in entry and entry["updated_parsed"]:
            dt = datetime(*entry["updated_parsed"][:6], tzinfo=timezone.utc)
            return to_iso8601_utc(dt)

        # Fallback to text fields
        for key in ("published", "updated"):
            if key in entry and entry[key]:
                dt = date_parser.parse(entry[key])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return to_iso8601_utc(dt)
    except Exception as exc:
        logging.error("Failed to parse published date: %s", exc)
    return None


def parse_amount_and_currency(title: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Extract funding amount and currency from title.

    Supports:
      - Currency symbols: $, €, £
      - Currency codes: USD, EUR, GBP
      - Magnitudes: K, M, B (case-insensitive)
      - Numbers with commas/decimals

    Examples:
      "$5M", "€3.2 million", "GBP 500k", "USD 1.5B", "£750,000"
    """
    text = title
    # Normalize common words to magnitudes
    norm = re.sub(r"\bmillion(s)?\b", "M", text, flags=re.IGNORECASE)
    norm = re.sub(r"\bbillion(s)?\b", "B", norm, flags=re.IGNORECASE)
    norm = re.sub(r"\bthousand(s)?\b", "K", norm, flags=re.IGNORECASE)

    currency_map = {"$": "USD", "€": "EUR", "£": "GBP"}
    code_pattern = r"\b(USD|EUR|GBP)\b"
    symbol_pattern = r"[\$\€\£]"
    number_pattern = r"(\d{1,3}(?:,\d{3})*|\d+)(?:\.\d+)?"
    magnitude_pattern = r"[KkMmBb]?"

    # Combined patterns to catch various orders (symbol/code position)
    patterns = [
        rf"({symbol_pattern})\s*{number_pattern}\s*({magnitude_pattern})",
        rf"{number_pattern}\s*({magnitude_pattern})\s*({symbol_pattern})",
        rf"({code_pattern})\s*{number_pattern}\s*({magnitude_pattern})",
        rf"{number_pattern}\s*({magnitude_pattern})\s*({code_pattern})",
    ]

    match: Optional[re.Match[str]] = None
    used_currency: Optional[str] = None
    raw_num: Optional[str] = None
    magnitude: str = ""

    for pat in patterns:
        m = re.search(pat, norm, flags=re.IGNORECASE)
        if m:
            groups = m.groups()
            # Deduce which group is currency and which is number
            # Strategy: Find first currency symbol/code and first number
            symbol = None
            code = None
            nums = re.findall(number_pattern, m.group(0))
            if nums:
                raw_num = nums[0]
            mag_match = re.search(magnitude_pattern, m.group(0), flags=re.IGNORECASE)
            magnitude = mag_match.group(0) if mag_match else ""

            sym_match = re.search(symbol_pattern, m.group(0))
            if sym_match:
                symbol = sym_match.group(0)
            code_match = re.search(code_pattern, m.group(0), flags=re.IGNORECASE)
            if code_match:
                code = code_match.group(0).upper()

            if code:
                used_currency = code
            elif symbol:
                used_currency = currency_map.get(symbol)

            match = m
            break

    if not match or not raw_num:
        return None, None

    # Clean number: "1,200.5" -> 1200.5
    num_str = raw_num.replace(",", "")
    try:
        value = float(num_str)
    except ValueError:
        return None, used_currency

    mag = magnitude.upper()
    if mag == "K":
        value *= 1_000
    elif mag == "M":
        value *= 1_000_000
    elif mag == "B":
        value *= 1_000_000_000

    # Round to cents precision to avoid 1.2000000002
    value = float(f"{value:.2f}")

    return value, used_currency


def parse_stage(title: str) -> Optional[str]:
    """
    Extract funding stage from title.

    Supports:
      - Pre-Seed / Pre Seed
      - Seed
      - Series A/B/C/... (case-insensitive)
    """
    text = title

    # Pre-Seed variations
    if re.search(r"\bpre[\s\-]?seed\b", text, flags=re.IGNORECASE):
        return "Pre-Seed"

    # Seed, but exclude Pre-Seed (handled above)
    if re.search(r"\bseed\b", text, flags=re.IGNORECASE):
        return "Seed"

    # Series A/B/C...
    m = re.search(r"\bseries\s+([A-Z])\b", text, flags=re.IGNORECASE)
    if m:
        letter = m.group(1).upper()
        return f"Series {letter}"

    # Series with words (rare): "Series Seed" etc. Keep simple here.
    return None


def parse_company(title: str) -> Optional[str]:
    """
    Heuristic to extract probable company name from title.

    Approach:
      - Take the first segment before separators (":", " - ", "–", "—")
      - Find first proper-noun-like token sequence:
        capitalized words, allowing &, -, digits inside
      - Exclude generic funding words (Raises, Funding, Series ...)
    """
    separators = [":", " - ", " – ", " — ", "–", "—", " | "]
    segment = title
    for sep in separators:
        if sep in segment:
            segment = segment.split(sep, 1)[0]
            break

    # Candidates of capitalized sequences
    # Example: "Acme Robotics raises $5M" -> "Acme Robotics"
    proper_noun_pattern = re.compile(
        r"\b([A-Z][A-Za-z0-9&\-]*(?:\s+[A-Z][A-Za-z0-9&\-]*)*)\b"
    )

    exclude_tokens = {
        "Raises",
        "Raise",
        "Raising",
        "Raised",
        "Funding",
        "Funded",
        "Series",
        "Seed",
        "Round",
        "Pre-Seed",
        "Pre",
        "SeedRound",
        "A",
        "B",
        "C",
        "D",
        "E",
    }

    for match in proper_noun_pattern.finditer(segment):
        candidate = match.group(1).strip()
        # Filter obviously non-company sequences
        if any(tok in exclude_tokens for tok in candidate.split()):
            continue
        # Avoid source names that may appear at start
        if candidate.lower() in {"techcrunch", "eu-startups", "eu startups"}:
            continue
        # Reasonable min length
        if len(candidate) >= 2:
            return candidate

    return None


def within_since_days(published_iso: Optional[str], since_days: int) -> bool:
    """Return True if item is within 'since_days' from now. If no date, keep by default."""
    if published_iso is None:
        return True
    try:
        dt = date_parser.isoparse(published_iso)
        now = datetime.now(timezone.utc)
        return dt >= now - timedelta(days=since_days)
    except Exception:
        return True


# ---------------------------
# Persistence
# ---------------------------

def init_db(conn: sqlite3.Connection) -> None:
    """Create tables if not exist."""
    conn.execute(SQL_CREATE_TABLE)
    conn.commit()


def insert_news_items(conn: sqlite3.Connection, items: Iterable[NewsItem]) -> int:
    """Insert items into DB with OR IGNORE. Returns count of newly inserted rows."""
    cur = conn.cursor()
    new_count = 0
    for it in items:
        try:
            cur.execute(
                SQL_INSERT_NEWS,
                (
                    it.title,
                    it.link,
                    it.published_utc,
                    it.source,
                    it.company,
                    it.amount_value,
                    it.amount_currency,
                    it.stage,
                    it.inserted_at_utc,
                ),
            )
            if cur.rowcount > 0:
                new_count += 1
        except sqlite3.Error as exc:
            logging.error("SQLite insert error for link=%s: %s", it.link, exc)
    conn.commit()
    return new_count


def load_existing_csv_links(csv_path: str) -> set[str]:
    """Load existing links from CSV to deduplicate appends."""
    if not os.path.exists(csv_path):
        return set()
    links: set[str] = set()
    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                link = row.get("link")
                if link:
                    links.add(link)
    except Exception as exc:
        logging.error("Failed reading existing CSV for dedupe: %s", exc)
    return links


def append_items_to_csv(csv_path: str, items: Iterable[NewsItem]) -> int:
    """Append items to CSV, deduplicating by link. Returns count of new rows written."""
    existing = load_existing_csv_links(csv_path)
    is_new_file = not os.path.exists(csv_path)
    written = 0

    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)

        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            if is_new_file:
                writer.writeheader()
            for it in items:
                if it.link in existing:
                    continue
                writer.writerow(
                    {
                        "title": it.title,
                        "link": it.link,
                        "published_utc": it.published_utc or "",
                        "source": it.source,
                        "company": it.company or "",
                        "amount_value": it.amount_value if it.amount_value is not None else "",
                        "amount_currency": it.amount_currency or "",
                        "stage": it.stage or "",
                        "inserted_at_utc": it.inserted_at_utc,
                    }
                )
                existing.add(it.link)
                written += 1
    except Exception as exc:
        logging.error("Failed writing CSV: %s", exc)
    return written


# ---------------------------
# Feed loading and parsing
# ---------------------------

def fetch_feed(url: str) -> feedparser.FeedParserDict:
    """Fetch and parse an RSS/Atom feed."""
    try:
        parsed = feedparser.parse(url)
        if parsed.bozo:
            logging.error("Feed parsing issue for %s: %s", url, getattr(parsed, "bozo_exception", "Unknown"))
        return parsed
    except Exception as exc:
        logging.error("Failed to fetch feed %s: %s", url, exc)
        return feedparser.FeedParserDict(entries=[])


def normalize_entry(entry: Dict[str, Any], source: str, since_days: int) -> Optional[NewsItem]:
    """Normalize a feed entry to NewsItem, or return None if not funding-related or too old."""
    title = entry.get("title") or ""
    link = entry.get("link") or ""

    if not title or not link:
        return None

    if not is_funding_related(title):
        return None

    published_utc = parse_published_utc(entry)

    if not within_since_days(published_utc, since_days):
        return None

    company = parse_company(title)
    amount_value, amount_currency = parse_amount_and_currency(title)
    stage = parse_stage(title)
    inserted_at_utc = to_iso8601_utc(datetime.now(timezone.utc))

    return NewsItem(
        title=title.strip(),
        link=link.strip(),
        published_utc=published_utc,
        source=source,
        company=company,
        amount_value=amount_value,
        amount_currency=amount_currency,
        stage=stage,
        inserted_at_utc=inserted_at_utc,
    )


def collect_news(max_items: Optional[int], since_days: int) -> List[NewsItem]:
    """Fetch feeds and collect normalized, filtered news items."""
    items: List[NewsItem] = []
    for url, source in FEEDS:
        logging.info("Fetching feed: %s (%s)", url, source)
        feed = fetch_feed(url)
        entries = feed.entries or []

        if max_items is not None:
            entries = entries[:max_items]

        for entry in entries:
            try:
                item = normalize_entry(entry, source, since_days)
                if item:
                    items.append(item)
            except Exception as exc:
                logging.error("Failed to normalize entry from %s: %s", source, exc)

    # Deduplicate by link within this run
    seen: set[str] = set()
    unique_items: List[NewsItem] = []
    for it in items:
        if it.link in seen:
            continue
        seen.add(it.link)
        unique_items.append(it)

    logging.info("Collected %d funding-related items", len(unique_items))
    return unique_items


# ---------------------------
# CLI
# ---------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load and normalize funding-related RSS items.")
    parser.add_argument("--max-items", type=int, default=None, help="Max items to process per feed.")
    parser.add_argument("--since-days", type=int, default=90, help="Only include items within the last N days.")
    return parser.parse_args(argv)


# ---------------------------
# Main
# ---------------------------

def main(argv: Optional[List[str]] = None) -> int:
    setup_logging()
    args = parse_args(argv)
    ensure_data_dir()

    try:
        conn = sqlite3.connect(DB_PATH)
    except sqlite3.Error as exc:
        logging.error("Failed to open SQLite database at %s: %s", DB_PATH, exc)
        return 1

    try:
        init_db(conn)

        items = collect_news(max_items=args.max_items, since_days=args.since_days)

        # Write to DB
        new_db = insert_news_items(conn, items)

        # Write to CSV
        new_csv = append_items_to_csv(CSV_PATH, items)

        logging.info(
            "Inserted %d new records into SQLite; appended %d new rows to CSV.",
            new_db,
            new_csv,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())


