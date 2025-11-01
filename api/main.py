"""FastAPI backend for VC-Sourcing-Tool.

Exposes:
- GET /health
- GET /news?source=...&since_days=...&limit=...

Run locally:
  uvicorn api.main:app --reload
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from fastapi.responses import JSONResponse


# ---------------------------
# Logging
# ---------------------------

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
)


# ---------------------------
# App Init
# ---------------------------

app = FastAPI(title="VC-Sourcing-Tool API", version="0.1.0")

DB_PATH = os.path.join("data", "vc_tool.db")


def _utc_now_iso() -> str:
    """Return current time as ISO8601 in UTC without microseconds."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_db_connection() -> sqlite3.Connection:
    """Return a SQLite connection with row factory set to dict-like rows."""
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"SQLite database not found at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------
# Pydantic models
# ---------------------------

class HealthResponse(BaseModel):
    status: str = Field(..., description="Service status")
    time: str = Field(..., description="Current UTC time in ISO8601 format")


class NewsItem(BaseModel):
    title: str
    link: str
    published_utc: Optional[str] = None
    source: str
    company: Optional[str] = None
    amount_value: Optional[float] = None
    amount_currency: Optional[str] = None
    stage: Optional[str] = None


# ---------------------------
# Routes
# ---------------------------

@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Simple health probe returning status and current UTC time."""
    return HealthResponse(status="ok", time=_utc_now_iso())


@app.get("/news", response_model=List[NewsItem])
def get_news(
    source: Optional[str] = Query(None, description="Filter by source, e.g. 'techcrunch'"),
    since_days: int = Query(90, ge=0, description="Only include items within last N days"),
    limit: int = Query(50, gt=0, le=500, description="Max number of items to return"),
) -> List[NewsItem]:
    """Return normalized funding-related news items from SQLite as JSON.

    - Filters by optional `source`
    - Filters by `since_days`; items with NULL `published_utc` are included
    - Limits results with `limit`
    """
    cutoff_iso: Optional[str] = None
    if since_days and since_days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        cutoff_iso = cutoff.replace(microsecond=0).isoformat()

    try:
        conn = get_db_connection()
    except FileNotFoundError as exc:
        logging.error("DB not found: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
    except sqlite3.Error as exc:
        logging.error("SQLite error opening DB: %s", exc)
        raise HTTPException(status_code=500, detail="Database error")

    try:
        clauses = []
        params: list = []

        if source:
            clauses.append("source = ?")
            params.append(source)

        if cutoff_iso is not None:
            # include rows with no published_utc as well
            clauses.append("(published_utc IS NULL OR published_utc >= ?)")
            params.append(cutoff_iso)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = (
            "SELECT title, link, published_utc, source, company, "
            "amount_value, amount_currency, stage "
            "FROM news "
            f"{where_sql} "
            "ORDER BY COALESCE(published_utc, '') DESC, id DESC "
            "LIMIT ?"
        )
        params.append(limit)

        cur = conn.execute(sql, params)
        rows = cur.fetchall()

        items: List[NewsItem] = []
        for row in rows:
            items.append(
                NewsItem(
                    title=row["title"],
                    link=row["link"],
                    published_utc=row["published_utc"],
                    source=row["source"],
                    company=row["company"],
                    amount_value=row["amount_value"],
                    amount_currency=row["amount_currency"],
                    stage=row["stage"],
                )
            )

        return items
    except sqlite3.Error as exc:
        logging.error("SQLite query error: %s", exc)
        raise HTTPException(status_code=500, detail="Database query error")
    finally:
        try:
            conn.close()
        except Exception:
            pass



# ---------------------------
# YC Companies
# ---------------------------

class YcCompanyItem(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    batch: Optional[str] = None
    industry: Optional[str] = None
    location: Optional[str] = None
    status: Optional[str] = None
    website_url: Optional[str] = None
    company_url: Optional[str] = None
    inserted_at_utc: str


@app.get("/yc/companies", response_model=List[YcCompanyItem])
def get_yc_companies(
    batch: Optional[str] = Query(None, description="Filter by batch, e.g. 'W25' or 'S24'"),
    industry: Optional[str] = Query(None, description="Filter by primary industry, e.g. 'Fintech'"),
    status: Optional[str] = Query(None, description="Filter by status, e.g. 'Active' or 'Acquired'"),
    limit: int = Query(50, gt=0, le=500, description="Max number of items to return"),
) -> List[YcCompanyItem]:
    """Return YC companies from SQLite with optional filters.

    Filters apply as equality on `batch`, `industry`, and `status` if provided.
    Results are ordered by `inserted_at_utc` DESC.
    """
    try:
        conn = get_db_connection()
    except FileNotFoundError as exc:
        logging.error("DB not found: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
    except sqlite3.Error as exc:
        logging.error("SQLite error opening DB: %s", exc)
        raise HTTPException(status_code=500, detail="Database error")

    try:
        clauses = []
        params: list = []

        if batch:
            clauses.append("batch = ?")
            params.append(batch)
        if industry:
            clauses.append("industry = ?")
            params.append(industry)
        if status:
            clauses.append("status = ?")
            params.append(status)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = (
            "SELECT name, description, batch, industry, location, status, website_url, company_url, inserted_at_utc "
            "FROM yc_companies "
            f"{where_sql} "
            "ORDER BY inserted_at_utc DESC, id DESC "
            "LIMIT ?"
        )
        params.append(limit)

        cur = conn.execute(sql, params)
        rows = cur.fetchall()

        items: List[YcCompanyItem] = []
        for row in rows:
            items.append(
                YcCompanyItem(
                    name=row["name"],
                    description=row["description"],
                    batch=row["batch"],
                    industry=row["industry"],
                    location=row["location"],
                    status=row["status"],
                    website_url=row["website_url"],
                    company_url=row["company_url"],
                    inserted_at_utc=row["inserted_at_utc"],
                )
            )

        return items
    except sqlite3.Error as exc:
        logging.error("SQLite query error: %s", exc)
        raise HTTPException(status_code=500, detail="Database query error")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------
# Stats
# ---------------------------

class StatsNewsSection(BaseModel):
    total: int
    by_source: Dict[str, int]


class StatsYcSection(BaseModel):
    total: int
    by_batch: Dict[str, int]
    by_industry: Dict[str, int]
    by_status: Dict[str, int]


class StatsResponse(BaseModel):
    news: Optional[StatsNewsSection] = None
    yc: Optional[StatsYcSection] = None


def _fetch_total(conn: sqlite3.Connection, query: str) -> int:
    try:
        logging.info("Executing stats total query: %s", query)
        cur = conn.execute(query)
        row = cur.fetchone()
        if not row or row[0] is None:
            return 0
        return int(row[0])
    except sqlite3.Error as exc:
        logging.error("Stats total query failed (%s): %s", query, exc)
        return 0


def _fetch_group_counts(conn: sqlite3.Connection, query: str) -> Dict[str, int]:
    results: Dict[str, int] = {}
    try:
        logging.info("Executing stats group query: %s", query)
        cur = conn.execute(query)
        for row in cur.fetchall():
            key = row[0]
            value = row[1]
            if key is None or value is None:
                continue
            results[str(key)] = int(value)
    except sqlite3.Error as exc:
        logging.error("Stats group query failed (%s): %s", query, exc)
    return results


@app.get("/stats", response_model=StatsResponse)
def get_stats(
    stats_type: Optional[Literal["news", "yc"]] = Query(
        None,
        description="Limit stats to a specific dataset: 'news' or 'yc'. Leave empty for both.",
    )
) -> JSONResponse:
    """Return aggregated statistics for news and/or YC companies."""
    try:
        conn = get_db_connection()
    except FileNotFoundError as exc:
        logging.error("DB not found: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
    except sqlite3.Error as exc:
        logging.error("SQLite error opening DB: %s", exc)
        raise HTTPException(status_code=500, detail="Database error")

    try:
        include_news = stats_type in (None, "news")
        include_yc = stats_type in (None, "yc")

        response_payload: Dict[str, BaseModel] = {}

        if include_news:
            news_total = _fetch_total(conn, "SELECT COUNT(*) FROM news;")
            news_by_source = _fetch_group_counts(
                conn,
                "SELECT source, COUNT(*) FROM news GROUP BY source ORDER BY COUNT(*) DESC;",
            )
            response_payload["news"] = StatsNewsSection(
                total=news_total,
                by_source=news_by_source,
            )

        if include_yc:
            yc_total = _fetch_total(conn, "SELECT COUNT(*) FROM yc_companies;")
            yc_by_batch = _fetch_group_counts(
                conn,
                "SELECT batch, COUNT(*) FROM yc_companies WHERE batch IS NOT NULL GROUP BY batch ORDER BY COUNT(*) DESC;",
            )
            yc_by_industry = _fetch_group_counts(
                conn,
                "SELECT industry, COUNT(*) FROM yc_companies WHERE industry IS NOT NULL GROUP BY industry ORDER BY COUNT(*) DESC;",
            )
            yc_by_status = _fetch_group_counts(
                conn,
                "SELECT status, COUNT(*) FROM yc_companies WHERE status IS NOT NULL GROUP BY status ORDER BY COUNT(*) DESC;",
            )
            response_payload["yc"] = StatsYcSection(
                total=yc_total,
                by_batch=yc_by_batch,
                by_industry=yc_by_industry,
                by_status=yc_by_status,
            )

        stats_response = StatsResponse(**{k: v for k, v in response_payload.items()})
        return JSONResponse(content=stats_response.dict(exclude_none=True))
    finally:
        try:
            conn.close()
        except Exception:
            pass

