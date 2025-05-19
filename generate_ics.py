#!/usr/bin/env python3

import requests
import re
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from ics import Calendar, Event

URL = "https://footballmundial.com/info/teams/770267"
TEAM_NAME = "CLIVE OWEN & CO"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible;)"}
OUTPUT_PATH = os.path.join("docs", "clive_owen_fixtures.ics")
EVENT_LOCATION = "301 Huntington Rd, Huntington, York YO32 9WT"
EVENT_TIMEZONE = ZoneInfo("Europe/London")


def fetch_fixtures():
    resp = requests.get(URL, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    heading = soup.find(
        "h4", class_="panel-title", string=re.compile(r"\s*CLIVE OWEN & CO Fixtures\s*")
    )
    if not heading:
        raise RuntimeError("Couldn't find the CLIVE OWEN & CO Fixtures heading")

    container = heading.find_parent("div", class_="col-lg-6")
    rows = container.select("table.table-striped tbody tr")

    fixtures = []
    for tr in rows:
        raw = tr.td.get_text(separator=" ").strip()  # e.g. "19/05/25 20:45"
        d, t = raw.split()
        dt = datetime.strptime(f"{d} {t}", "%d/%m/%y %H:%M").replace(
            tzinfo=EVENT_TIMEZONE
        )

        home = tr.select("td")[1].get_text(strip=True)
        away = tr.select("td")[3].get_text(strip=True)
        opponent = away if home == TEAM_NAME else home if away == TEAM_NAME else None
        if opponent:
            fixtures.append((dt, opponent))

    return fixtures


def build_ical(fixtures):
    cal = Calendar()
    cal.extra.append(("PRODID", "-//Clive Owen Fixtures//github.com//"))
    for dt, opp in fixtures:
        ev = Event()
        ev.name = f"Match Versus {opp}"
        ev.begin = dt
        ev.duration = timedelta(hours=1)
        ev.location = EVENT_LOCATION
        cal.events.add(ev)
    return cal


def main():
    fixtures = fetch_fixtures()
    cal = build_ical(fixtures)
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        f.writelines(cal)
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
