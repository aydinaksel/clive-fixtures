#!/usr/bin/env python3

import os
import re
import time
import sqlite3
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from ics import Calendar, Event

# ─── Configuration ─────────────────────────────────────────────────────────────

BASE_URL = "https://footballmundial.com"
LEAGUE_PAGE_URL = "https://footballmundial.com/find_league"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible;)"}

OUTPUT_DIRECTORY = "docs"
DB_PATH = os.path.join(OUTPUT_DIRECTORY, "fixtures.db")

DEFAULT_EVENT_LOCATION = "301 Huntington Rd, Huntington, York YO32 9WT"
TIME_ZONE = ZoneInfo("Europe/London")
UTC = ZoneInfo("UTC")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use a single Session for all HTTP calls
SESSION = requests.Session()
SESSION.headers.update(HTTP_HEADERS)

# Simple in-memory cache for venues to avoid re-fetching
_venue_cache: dict[str, str] = {}


# ─── SQLite Schema & Helpers ───────────────────────────────────────────────────


def init_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    # 1) league_group
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS league_group (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        slug TEXT NOT NULL UNIQUE
    );
    """)

    # 2) venue
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS venue (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        url     TEXT UNIQUE,
        name    TEXT,
        address TEXT
    );
    """)

    # 3) league
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS league (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        league_group_id INTEGER NOT NULL REFERENCES league_group(id) ON DELETE CASCADE,
        name            TEXT NOT NULL,
        url             TEXT UNIQUE,
        slug            TEXT NOT NULL UNIQUE
    );
    """)

    # 4) team
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS team (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        slug TEXT NOT NULL UNIQUE
    );
    """)

    # 5) fixture
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fixture (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        league_id     INTEGER NOT NULL REFERENCES league(id) ON DELETE CASCADE,
        venue_id      INTEGER REFERENCES venue(id),
        home_team_id  INTEGER NOT NULL REFERENCES team(id),
        away_team_id  INTEGER NOT NULL REFERENCES team(id),
        dt_utc        TEXT NOT NULL,
        result        TEXT,
        UNIQUE(league_id, home_team_id, away_team_id, dt_utc)
    );
    """)

    conn.commit()
    return conn


def slugify(text: str) -> str:
    return re.sub(r"\W+", "_", text.strip().lower()).strip("_")


def get_or_create_league_group(conn: sqlite3.Connection, group_name: str) -> int:
    slug = slugify(group_name)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO league_group(name, slug) VALUES (?, ?)",
        (group_name, slug),
    )
    conn.commit()
    cur.execute("SELECT id FROM league_group WHERE slug = ?", (slug,))
    return cur.fetchone()[0]


def get_or_create_league(
    conn: sqlite3.Connection, group_id: int, league_name: str, league_url: str
) -> int:
    slug = slugify(league_name)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO league(league_group_id, name, url, slug) VALUES (?, ?, ?, ?)",
        (group_id, league_name, league_url, slug),
    )
    conn.commit()
    cur.execute("SELECT id FROM league WHERE slug = ?", (slug,))
    return cur.fetchone()[0]


def get_or_create_venue(conn: sqlite3.Connection, venue_dict: dict) -> int | None:
    url = venue_dict.get("url")
    if not url:
        return None

    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO venue(url, name, address) VALUES (?, ?, ?)",
        (url, venue_dict["name"], venue_dict["address"]),
    )
    conn.commit()
    cur.execute("SELECT id FROM venue WHERE url = ?", (url,))
    row = cur.fetchone()
    return row[0] if row else None


def get_or_create_team(conn: sqlite3.Connection, team_name: str) -> int:
    slug = slugify(team_name)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO team(name, slug) VALUES (?, ?)", (team_name, slug)
    )
    conn.commit()
    cur.execute("SELECT id FROM team WHERE slug = ?", (slug,))
    return cur.fetchone()[0]


def insert_fixture(
    conn: sqlite3.Connection,
    league_id: int,
    venue_id: int,
    home_id: int,
    away_id: int,
    dt_iso: str,
    result: str,
) -> int | None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO fixture(league_id, venue_id, home_team_id, away_team_id, dt_utc, result)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (league_id, venue_id, home_id, away_id, dt_iso, result),
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        return None


# ─── HTTP Helpers ───────────────────────────────────────────────────────────────


def safe_get(full_url: str) -> requests.Response | None:
    """
    Perform up to 3 attempts to GET full_url. On success, return Response.
    On repeated failure, log an error and return None.
    """
    for attempt in range(1, 4):
        try:
            resp = SESSION.get(full_url)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.warning("GET %s failed (attempt %d/3): %s", full_url, attempt, e)
            time.sleep(2 ** (attempt - 1))  # 1s, then 2s, then 4s
    logger.error("Giving up on %s after 3 attempts.", full_url)
    return None


# ─── Fetch & Parse Helpers ────────────────────────────────────────────────────


def fetch_league_names_and_urls() -> list[tuple[str, str]]:
    """
    Return a list of (league_group_name, league_group_relative_url).
    """
    resp = safe_get(LEAGUE_PAGE_URL)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    select = soup.find(
        "select",
        attrs={"onchange": "location = this.options[this.selectedIndex].value;"},
    )
    if not select:
        logger.error(
            "Could not find the league selection dropdown on %s", LEAGUE_PAGE_URL
        )
        return []

    leagues: list[tuple[str, str]] = []
    for opt in select.find_all("option"):
        name = opt.get_text(strip=True)
        relative = opt.get("value", "").strip()
        if relative and relative != "/find_league":
            leagues.append((name, relative))
    return leagues


def parse_league_group(group_name: str, group_url: str) -> list[dict]:
    """
    Parse a single league group page (BASE_URL + group_url) and return
    [{"name": league_name, "url": league_relative_url}, ...].
    """
    full = BASE_URL + group_url
    resp = safe_get(full)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    parsed = []
    # Use a CSS selector to find all <h4 class="panel-title"> under the correct container
    for league_h4 in soup.select("div.col-lg-12 div.panel-heading h4.panel-title"):
        link = league_h4.find("a", href=True)
        if not link:
            continue
        league_name = league_h4.get_text(strip=True).split("View Fixtures")[0].strip()
        league_rel = link["href"]
        if league_rel:
            parsed.append({"name": league_name, "url": league_rel})
    return parsed


def fetch_venue_address(venue_url: str) -> str:
    """
    Given a relative venue URL, return the address text. Cached in _venue_cache.
    """
    if not venue_url:
        return DEFAULT_EVENT_LOCATION

    if venue_url in _venue_cache:
        return _venue_cache[venue_url]

    full = BASE_URL + venue_url
    resp = safe_get(full)
    if not resp:
        # If the GET failed repeatedly, fallback to default
        _venue_cache[venue_url] = DEFAULT_EVENT_LOCATION
        return DEFAULT_EVENT_LOCATION

    soup = BeautifulSoup(resp.text, "html.parser")
    # Look for <p> that is exactly "Address"
    address_block = soup.find("p", string=re.compile(r"^\s*Address\s*$"))
    if not address_block:
        _venue_cache[venue_url] = DEFAULT_EVENT_LOCATION
        return DEFAULT_EVENT_LOCATION

    container = address_block.find_parent("div")
    if not container:
        _venue_cache[venue_url] = DEFAULT_EVENT_LOCATION
        return DEFAULT_EVENT_LOCATION

    lines = []
    for p in container.find_all("p")[1:]:
        txt = p.get_text(strip=True)
        if txt:
            lines.append(txt)
    address = ", ".join(lines) if lines else DEFAULT_EVENT_LOCATION
    _venue_cache[venue_url] = address
    return address


def extract_fixtures_from_league(league_url: str) -> tuple[list[dict], dict]:
    """
    Hit BASE_URL + league_url and parse all fixtures (past + upcoming).
    Return (fixtures_list, venue_dict).
    fixtures_list: [
       { "dt": datetime(tz=TIME_ZONE),
         "home_name": str, "home_url": str,
         "away_name": str, "away_url": str,
         "result": str | None }
     , …]
    venue_dict: { "name": str, "url": str|None, "address": str }
    """
    full = BASE_URL + league_url
    resp = safe_get(full)
    if not resp:
        return [], {"name": "Unknown", "url": None, "address": DEFAULT_EVENT_LOCATION}

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1) Find venue link & address
    venue_link = soup.find("a", href=re.compile(r"^/info/venues/\d+"))
    if venue_link:
        venue = {
            "name": venue_link.get_text(strip=True),
            "url": venue_link["href"],
            "address": None,
        }
        venue["address"] = fetch_venue_address(venue["url"])
    else:
        venue = {"name": "Unknown", "url": None, "address": DEFAULT_EVENT_LOCATION}

    fixtures: list[dict] = []

    # 2) Two accordion sections: upcoming & results
    for section_id in ("fixtures_accordion_fixtures", "fixtures_accordion_results"):
        acc = soup.find("div", id=section_id)
        if not acc:
            continue
        # Each date header is <div class="panel-heading"><h4 class="panel-title">DD-MM-YYYY</h4>…
        for panel in acc.find_all("div", class_="panel-heading"):
            title_tag = panel.find("h4", class_="panel-title")
            if not title_tag:
                continue
            date_text = title_tag.get_text(strip=True).replace("View:", "").strip()
            try:
                match_date = datetime.strptime(date_text, "%d-%m-%Y").date()
            except ValueError:
                continue

            # The sibling <div class="panel-collapse"> contains a table
            content_div = panel.find_next_sibling("div", class_="panel-collapse")
            if not content_div:
                continue
            table = content_div.find("table", class_="table-striped")
            if not table:
                continue

            for row in table.select("tbody tr"):
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue
                time_str = cells[0].get_text(strip=True)
                try:
                    fixture_time = datetime.strptime(time_str, "%H:%M").time()
                except ValueError:
                    continue

                dt_local = datetime.combine(match_date, fixture_time).replace(
                    tzinfo=TIME_ZONE
                )

                home_link = cells[1].find("a", href=True)
                away_link = cells[3].find("a", href=True)
                if not home_link or not away_link:
                    continue

                result_text = None
                if section_id == "fixtures_accordion_results":
                    result_text = cells[2].get_text(strip=True)

                fixtures.append(
                    {
                        "dt": dt_local,
                        "home_name": home_link.get_text(strip=True),
                        "home_url": home_link["href"],
                        "away_name": away_link.get_text(strip=True),
                        "away_url": away_link["href"],
                        "result": result_text,
                    }
                )

    return fixtures, venue


# ─── Main Crawl Loop ───────────────────────────────────────────────────────────


def crawl_and_populate_db(limit: int | None = None):
    """
    Crawl up to `limit` league groups (or all if None), parse leagues & fixtures,
    insert everything into SQLite. Uses a single DB transaction for the entire run.
    """
    conn = init_db(DB_PATH)
    league_groups = fetch_league_names_and_urls()
    if limit:
        league_groups = league_groups[:limit]

    # Batch everything in one transaction to reduce commits
    with conn:
        for group_name, group_url in league_groups:
            logger.info("Processing league group: %s", group_name)
            lg_id = get_or_create_league_group(conn, group_name)

            parsed_leagues = parse_league_group(group_name, group_url)
            for league_info in parsed_leagues:
                league_name = league_info["name"]
                league_url = league_info["url"]
                logger.info("  └── League: %s", league_name)

                league_id = get_or_create_league(conn, lg_id, league_name, league_url)

                fixtures, venue_dict = extract_fixtures_from_league(league_url)
                venue_id = get_or_create_venue(conn, venue_dict)

                for fx in fixtures:
                    home_tid = get_or_create_team(conn, fx["home_name"])
                    away_tid = get_or_create_team(conn, fx["away_name"])
                    dt_utc = fx["dt"].astimezone(UTC).isoformat()
                    insert_fixture(
                        conn,
                        league_id,
                        venue_id,
                        home_tid,
                        away_tid,
                        dt_utc,
                        fx["result"],
                    )

                # Throttle—reduce if the site allows faster
                time.sleep(0.5)

    conn.close()
    logger.info("Done crawling & populating SQLite.")


# ─── ICS‐Building Helpers (DRY’d) ──────────────────────────────────────────────


def _build_ics(
    conn: sqlite3.Connection,
    query: str,
    params: tuple,
    creator_text: str,
    event_namer,
    output_path: str,
):
    """
    Generic ICS builder.
    - `query, params` fetch (fid, h_id, h_name, a_id, a_name, dt_utc, result, venue_addr).
    - `creator_text` goes into Calendar.creator.
    - `event_namer` is a callable(f_row) → (event_name:str, event_desc:str|None).
    """
    cur = conn.cursor()
    rows = cur.execute(query, params).fetchall()
    if not rows:
        logger.info("No data for ICS at %s", output_path)
        return

    cal = Calendar()
    cal.creator = creator_text

    for row in rows:
        fid, h_id, h_name, a_id, a_name, dt_iso, result, venue_addr = row
        dt_obj = datetime.fromisoformat(dt_iso).astimezone(TIME_ZONE)
        venue_loc = venue_addr or DEFAULT_EVENT_LOCATION

        name, desc = event_namer(row)
        e = Event()
        e.name = name
        e.begin = dt_obj
        e.duration = timedelta(hours=1)
        e.location = venue_loc
        if desc:
            e.description = desc
        cal.events.add(e)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.writelines(cal)
    logger.info("Wrote ICS: %s", output_path)


def build_league_ics(conn: sqlite3.Connection, league_slug: str, output_path: str):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT l.id AS league_id, lg.name AS group_name
        FROM league l
        JOIN league_group lg ON l.league_group_id = lg.id
        WHERE l.slug = ?
    """,
        (league_slug,),
    )
    row = cur.fetchone()
    if not row:
        logger.warning("No league found for slug=%s", league_slug)
        return
    league_id, group_name = row

    query = """
        SELECT f.id,
               f.home_team_id, t1.name AS home_name,
               f.away_team_id, t2.name AS away_name,
               f.dt_utc, f.result,
               COALESCE(v.address, ?) AS address
        FROM fixture f
        JOIN team t1 ON f.home_team_id = t1.id
        JOIN team t2 ON f.away_team_id = t2.id
        LEFT JOIN venue v ON f.venue_id = v.id
        WHERE f.league_id = ?
        ORDER BY f.dt_utc ASC
    """
    params = (DEFAULT_EVENT_LOCATION, league_id)
    creator = f"-//Fixtures for {group_name} → {league_slug}//EN"

    def event_namer(r):
        _fid, _h_id, h_name, _a_id, a_name, _dt_iso, result, _venue_addr = r
        name = f"{h_name} vs {a_name}"
        desc = f"Result: {result}" if result else None
        return name, desc

    _build_ics(conn, query, params, creator, event_namer, output_path)


def build_team_ics(conn: sqlite3.Connection, team_slug: str, output_path: str):
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM team WHERE slug = ?", (team_slug,))
    row = cur.fetchone()
    if not row:
        logger.warning("No team found for slug=%s", team_slug)
        return
    team_id, team_name = row

    query = """
        SELECT f.id,
               f.home_team_id, t1.name AS home_name,
               f.away_team_id, t2.name AS away_name,
               f.dt_utc, f.result,
               COALESCE(v.address, ?) AS address
        FROM fixture f
        JOIN team t1 ON f.home_team_id = t1.id
        JOIN team t2 ON f.away_team_id = t2.id
        LEFT JOIN venue v ON f.venue_id = v.id
        WHERE f.home_team_id = ? OR f.away_team_id = ?
        ORDER BY f.dt_utc ASC
    """
    params = (DEFAULT_EVENT_LOCATION, team_id, team_id)
    creator = f"-//Fixtures for team {team_name}//EN"

    def event_namer(r):
        _fid, h_id, h_name, a_id, a_name, _dt_iso, result, _venue_addr = r
        if h_id == team_id:
            opponent = a_name
            name = f"{team_name} vs {opponent}"
        else:
            opponent = h_name
            name = f"{team_name} vs {opponent}"
        desc = f"Result: {result}" if result else None
        return name, desc

    _build_ics(conn, query, params, creator, event_namer, output_path)


# ─── Entrypoint ────────────────────────────────────────────────────────────────


def main():
    crawl_and_populate_db(None)

    with sqlite3.connect(DB_PATH) as conn:
        # Generate all league ICS
        cur = conn.cursor()

        cur.execute("SELECT slug FROM team;")
        for (slug,) in cur.fetchall():
            ics_path = os.path.join(OUTPUT_DIRECTORY, f"{slug}.ics")
            build_team_ics(conn, slug, ics_path)

    logger.info("All ICS files generated.")


if __name__ == "__main__":
    main()
