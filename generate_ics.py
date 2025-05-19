#!/usr/bin/env python3

import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from ics import Calendar, Event

FIXTURES_SOURCE_URL = "https://footballmundial.com/info/teams/770267"
TEAM_NAME = "CLIVE OWEN & CO"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible;)"}
OUTPUT_DIRECTORY = os.path.join("docs")
ICS_OUTPUT_FILENAME = "clive_owen_fixtures.ics"
ICS_OUTPUT_PATH = os.path.join(OUTPUT_DIRECTORY, ICS_OUTPUT_FILENAME)
DEFAULT_EVENT_LOCATION = "301 Huntington Rd, Huntington, York YO32 9WT"
TIME_ZONE = ZoneInfo("Europe/London")


def fetch_team_fixtures() -> list[tuple[datetime, str]]:
    """
    Retrieve upcoming fixtures for the team from the remote schedule page.
    """
    response = requests.get(FIXTURES_SOURCE_URL, headers=HTTP_HEADERS)
    response.raise_for_status()
    page_soup = BeautifulSoup(response.text, "html.parser")

    fixtures_heading = page_soup.find(
        "h4", class_="panel-title", string=re.compile(r"\s*CLIVE OWEN & CO Fixtures\s*")
    )
    if fixtures_heading is None:
        raise RuntimeError("Unable to locate the fixtures section for the home team.")

    fixtures_container = fixtures_heading.find_parent("div", class_="col-lg-6")
    table_rows = fixtures_container.select("table.table-striped tbody tr")

    fixtures_list: list[tuple[datetime, str]] = []
    for table_row in table_rows:
        raw_datetime = table_row.td.get_text(separator=" ").strip()
        date_string, time_string = raw_datetime.split()
        fixture_datetime = datetime.strptime(
            f"{date_string} {time_string}", "%d/%m/%y %H:%M"
        ).replace(tzinfo=TIME_ZONE)

        home_team_cell = table_row.select("td")[1].get_text(strip=True)
        away_team_cell = table_row.select("td")[3].get_text(strip=True)

        if home_team_cell == TEAM_NAME:
            opponent_team = away_team_cell
        elif away_team_cell == TEAM_NAME:
            opponent_team = home_team_cell
        else:
            continue

        fixtures_list.append((fixture_datetime, opponent_team))

    return fixtures_list


def build_calendar(fixtures: list[tuple[datetime, str]]) -> Calendar:
    """
    Construct an iCalendar object from the list of fixtures.
    """
    calendar = Calendar()
    calendar.creator = "-//Aydin Aksel//Clive Owen Fixtures//EN"

    for fixture_datetime, opponent_team in fixtures:
        event = Event()
        event.name = f"Match Versus {opponent_team}"
        event.begin = fixture_datetime
        event.duration = timedelta(hours=1)
        event.location = DEFAULT_EVENT_LOCATION
        calendar.events.add(event)

    return calendar


def main() -> None:
    """
    Fetch fixtures and write them to an ICS file for import into calendar applications.
    """
    fixtures = fetch_team_fixtures()
    calendar = build_calendar(fixtures)

    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    with open(ICS_OUTPUT_PATH, "w") as output_file:
        output_file.writelines(calendar)

    print(f"Wrote fixture calendar to {ICS_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
