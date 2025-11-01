#!/usr/bin/env python3
"""
YC Companies loader (YC OSS API) for VC-Sourcing-Tool.

- Lädt alle Companies von https://yc-oss.github.io/api/companies/all.json
- Mappt relevante Felder und speichert sie in SQLite (yc_companies)
- Dedupliziert via company_url (UNIQUE)

Nutzung:
    python etl/yc_loader.py [--max-companies N]

Anforderungen:
    - requests
    - sqlite3
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests

DB_PATH: str = os.path.join("data", "vc_tool.db")
DEFAULT_MAX_COMPANIES: int = 100
ALL_COMPANIES_URL: str = "https://yc-oss.github.io/api/companies/all.json"

SQL_CREATE_TABLE: str = """
CREATE TABLE IF NOT EXISTS yc_companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    yc_api_id INTEGER,
    name TEXT,
    description TEXT,
    batch TEXT,
    industry TEXT,
    industries TEXT,
    location TEXT,
    status TEXT,
    website_url TEXT,
    social_links TEXT,
    founding_date TEXT,
    company_url TEXT UNIQUE,
    inserted_at_utc TEXT
);
"""

SQL_INSERT_COMPANY: str = """
INSERT OR IGNORE INTO yc_companies (
    yc_api_id, name, description, batch, industry, industries, location, status,
    website_url, social_links, founding_date, company_url, inserted_at_utc
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


@dataclass
class YcCompany:
    yc_api_id: Optional[int]
    name: Optional[str]
    description: Optional[str]
    batch: Optional[str]
    industry: Optional[str]
    industries: Optional[str]  # JSON array as TEXT
    location: Optional[str]
    status: Optional[str]
    website_url: Optional[str]
    social_links: Optional[str]  # JSON array as TEXT
    founding_date: Optional[str]
    company_url: Optional[str]
    inserted_at_utc: str


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def to_iso8601_utc(ts: Optional[int]) -> Optional[str]:
    """Convert a Unix timestamp (seconds) to ISO8601 UTC without microseconds."""
    if ts is None:
        return None
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return dt.replace(microsecond=0).isoformat()
    except Exception:
        return None


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_data_dir() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def init_db(conn: sqlite3.Connection) -> None:
    """Create table and add missing columns if needed (idempotent)."""
    conn.execute(SQL_CREATE_TABLE)
    conn.commit()
    # Ensure all expected columns exist (handles older schema versions)
    cur = conn.execute("PRAGMA table_info(yc_companies);")
    existing_cols = {row[1] for row in cur.fetchall()}
    expected_cols: List[tuple[str, str]] = [
        ("yc_api_id", "INTEGER"),
        ("name", "TEXT"),
        ("description", "TEXT"),
        ("batch", "TEXT"),
        ("industry", "TEXT"),
        ("industries", "TEXT"),
        ("location", "TEXT"),
        ("status", "TEXT"),
        ("website_url", "TEXT"),
        ("social_links", "TEXT"),
        ("founding_date", "TEXT"),
        ("company_url", "TEXT"),
        ("inserted_at_utc", "TEXT"),
    ]
    for col_name, col_type in expected_cols:
        if col_name not in existing_cols:
            try:
                conn.execute(f"ALTER TABLE yc_companies ADD COLUMN {col_name} {col_type};")
            except sqlite3.Error as exc:
                logging.error("Migration failed (ADD COLUMN %s): %s", col_name, exc)
    conn.commit()


def fetch_all_companies(max_companies: Optional[int]) -> List[Dict[str, Any]]:
    """Fetch all companies from YC OSS API. Optionally cap to max_companies."""
    logging.info("Fetching YC OSS companies: %s", ALL_COMPANIES_URL)
    try:
        with requests.get(ALL_COMPANIES_URL, timeout=120) as resp:
            if resp.status_code != 200:
                logging.error("Non-200 from YC OSS: %s", resp.status_code)
                return []
            data = resp.json()
            if not isinstance(data, list):
                logging.error("Unexpected JSON shape from YC OSS (not a list)")
                return []
            if max_companies is not None:
                return data[: max_companies]
            return data
    except requests.RequestException as exc:
        logging.error("Request error: %s", exc)
        return []
    except ValueError as exc:
        logging.error("JSON parse error: %s", exc)
        return []


def map_company(obj: Dict[str, Any]) -> YcCompany:
    """Map a YC OSS company object to our YcCompany dataclass."""
    yc_api_id = obj.get("id") if isinstance(obj.get("id"), int) else None
    name = (obj.get("name") or None) if isinstance(obj.get("name"), str) else None

    # description from one_liner
    description = (obj.get("one_liner") or None) if isinstance(obj.get("one_liner"), str) else None

    batch = (obj.get("batch") or None) if isinstance(obj.get("batch"), str) else None
    industry = (obj.get("industry") or None) if isinstance(obj.get("industry"), str) else None

    industries_field = obj.get("industries")
    industries_json: Optional[str] = None
    if isinstance(industries_field, list):
        try:
            industries_json = json.dumps([str(x) for x in industries_field if x is not None], ensure_ascii=False)
        except Exception:
            industries_json = None

    location = (obj.get("all_locations") or None) if isinstance(obj.get("all_locations"), str) else None
    status = (obj.get("status") or None) if isinstance(obj.get("status"), str) else None
    website_url = (obj.get("website") or None) if isinstance(obj.get("website"), str) else None

    # Optional: social links from tags if they look like URLs
    social_links_json: Optional[str] = None
    tags = obj.get("tags")
    if isinstance(tags, list):
        socials = [t for t in tags if isinstance(t, str) and t.startswith("http")]
        if socials:
            try:
                social_links_json = json.dumps(socials, ensure_ascii=False)
            except Exception:
                social_links_json = None

    founding_date = to_iso8601_utc(obj.get("launched_at"))
    company_url = (obj.get("url") or None) if isinstance(obj.get("url"), str) else None

    return YcCompany(
        yc_api_id=yc_api_id,
        name=name,
        description=description,
        batch=batch,
        industry=industry,
        industries=industries_json,
        location=location,
        status=status,
        website_url=website_url,
        social_links=social_links_json,
        founding_date=founding_date,
        company_url=company_url,
        inserted_at_utc=now_iso_utc(),
    )


def insert_companies(conn: sqlite3.Connection, companies: Iterable[YcCompany]) -> int:
    """Insert companies with INSERT OR IGNORE; return number of newly inserted rows."""
    cur = conn.cursor()
    new_count = 0
    for c in companies:
        if not c.company_url:
            continue
        try:
            cur.execute(
                SQL_INSERT_COMPANY,
                (
                    c.yc_api_id,
                    c.name,
                    c.description,
                    c.batch,
                    c.industry,
                    c.industries,
                    c.location,
                    c.status,
                    c.website_url,
                    c.social_links,
                    c.founding_date,
                    c.company_url,
                    c.inserted_at_utc,
                ),
            )
            if cur.rowcount > 0:
                new_count += 1
        except sqlite3.Error as exc:
            logging.error("SQLite insert error for url=%s: %s", c.company_url, exc)
    conn.commit()
    return new_count


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load YC companies from YC OSS API into SQLite.")
    parser.add_argument(
        "--max-companies",
        type=int,
        default=DEFAULT_MAX_COMPANIES,
        help=f"Max number of companies to process (default: {DEFAULT_MAX_COMPANIES}).",
    )
    return parser.parse_args(argv)


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
        raw = fetch_all_companies(max_companies=args.max_companies)
        if not raw:
            logging.info("No companies fetched from YC OSS API.")
            return 0
        mapped = [map_company(obj) for obj in raw if isinstance(obj, dict)]
        new_rows = insert_companies(conn, mapped)
        logging.info("Inserted %d new YC company rows into SQLite.", new_rows)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
