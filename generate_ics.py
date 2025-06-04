#!/usr/bin/env python3

import os
import re
import time
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from ics import Calendar, Event

BASE_URL = "https://footballmundial.com"

LEAGUE_PAGE_URL = "https://footballmundial.com/find_league"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible;)"}

FIXTURES_SOURCE_URL = "https://footballmundial.com/info/teams/770267"
TEAM_NAME = "CLIVE OWEN & CO"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible;)"}
OUTPUT_DIRECTORY = os.path.join("docs")
ICS_OUTPUT_FILENAME = "clive_owen_fixtures.ics"
ICS_OUTPUT_PATH = os.path.join(OUTPUT_DIRECTORY, ICS_OUTPUT_FILENAME)
DEFAULT_EVENT_LOCATION = "301 Huntington Rd, Huntington, York YO32 9WT"
TIME_ZONE = ZoneInfo("Europe/London")


def fetch_league_names_and_urls():
    """
    Extract league names and their relative URLs from the league dropdown.
    """
    response = requests.get(LEAGUE_PAGE_URL, headers=HTTP_HEADERS)
    response.raise_for_status()

    page_soup = BeautifulSoup(response.text, "html.parser")

    select_element = page_soup.find(
        "select",
        attrs={"onchange": "location = this.options[this.selectedIndex].value;"},
    )
    if not select_element:
        raise RuntimeError("Could not find the league dropdown.")

    leagues = []
    for option in select_element.find_all("option"):
        league_name = option.text.strip()
        relative_url = option.get("value", "").strip()

        if relative_url == "/find_league" or not relative_url:
            continue

        leagues.append((league_name, relative_url))

    return leagues


def parse_league_group(group_name: str, group_url: str):
    full_url = BASE_URL + group_url
    response = requests.get(full_url, headers=HTTP_HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    league_data = {"group_name": group_name, "group_url": group_url, "leagues": []}

    panels = soup.find_all("div", class_="col-lg-12")
    for panel in panels:
        title_div = panel.find("div", class_="panel-heading")
        if not title_div:
            continue

        league_name_tag = title_div.find("h4", class_="panel-title")
        if not league_name_tag:
            continue

        league_name = (
            league_name_tag.get_text(strip=True).split("View Fixtures")[0].strip()
        )
        link_tag = league_name_tag.find("a", href=True)
        league_url = link_tag["href"] if link_tag else None
        if not league_url:
            continue

        table = panel.find("table", class_="table-striped")
        team_names = []
        if table:
            for row in table.select("tbody tr"):
                team_div = row.find("div", class_="team_name_with_colour")
                if team_div:
                    team_names.append(team_div.get_text(strip=True))

        league_data["leagues"].append(
            {"name": league_name, "url": league_url, "teams": team_names}
        )

    return league_data


def extract_fixtures_from_league(league_url: str):
    response = requests.get(BASE_URL + league_url, headers=HTTP_HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    venue_link = soup.find("a", href=re.compile(r"^/info/venues/\d+"))
    venue = {
        "name": venue_link.text.strip() if venue_link else "Unknown Venue",
        "url": venue_link["href"] if venue_link else None,
        "address": None,
    }

    if venue["url"]:
        venue["address"] = fetch_venue_address(venue["url"])

    fixtures = []

    for section_id in ["fixtures_accordion_fixtures", "fixtures_accordion_results"]:
        accordion = soup.find("div", id=section_id)
        if not accordion:
            continue

        # Look for all date headers
        date_panels = accordion.find_all("div", class_="panel-heading")
        for panel in date_panels:
            title_tag = panel.find("h4", class_="panel-title")
            if not title_tag:
                continue

            date_text = title_tag.get_text(strip=True).replace("View:", "").strip()
            try:
                match_date = datetime.strptime(date_text, "%d-%m-%Y").date()
            except ValueError:
                continue

            # Get the div with fixtures for that matchday
            content_div = panel.find_next_sibling("div", class_="panel-collapse")
            if not content_div:
                continue

            table = content_div.find("table", class_="table-striped")
            if not table:
                continue

            rows = table.select("tbody tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue

                time_str = cells[0].text.strip()
                try:
                    fixture_time = datetime.strptime(time_str, "%H:%M").time()
                except ValueError:
                    continue

                fixture_datetime = datetime.combine(match_date, fixture_time).replace(
                    tzinfo=TIME_ZONE
                )

                home_link = cells[1].find("a")
                away_link = cells[3].find("a")
                if not home_link or not away_link:
                    continue

                fixture = {
                    "datetime": fixture_datetime,
                    "home": {"name": home_link.text.strip(), "url": home_link["href"]},
                    "away": {"name": away_link.text.strip(), "url": away_link["href"]},
                    "result": None,
                    "league_url": league_url,
                }

                # Add result if present (only in past results section)
                if section_id == "fixtures_accordion_results":
                    fixture["result"] = cells[2].text.strip()

                fixtures.append(fixture)

    return fixtures, venue


def fetch_venue_address(venue_url: str) -> str:
    """
    Given a relative venue URL like /info/venues/3940, return a cleaned-up address string.
    """
    if not venue_url:
        return DEFAULT_EVENT_LOCATION

    full_url = BASE_URL + venue_url
    response = requests.get(full_url, headers=HTTP_HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    address_block = soup.find("p", string=re.compile(r"^\s*Address\s*$"))
    if not address_block:
        return DEFAULT_EVENT_LOCATION

    container = address_block.find_parent("div")
    if not container:
        return DEFAULT_EVENT_LOCATION

    p_tags = container.find_all("p")
    address_lines = []

    for p in p_tags[1:]:
        text = p.get_text(strip=True)
        if text:
            address_lines.append(text)

    return ", ".join(address_lines) if address_lines else DEFAULT_EVENT_LOCATION


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


def build_leaguegroup_fixture_manifest(limit: int = None):
    all_data = []

    league_groups = fetch_league_names_and_urls()
    if limit is not None:
        league_groups = league_groups[:limit]

    for group_name, group_url in league_groups:
        print(f"Processing league group: {group_name} ({group_url})")
        group_info = {"group_name": group_name, "group_url": group_url, "leagues": []}

        parsed_group = parse_league_group(group_name, group_url)
        for league in parsed_group["leagues"]:
            league_fixtures, venue_info = extract_fixtures_from_league(league["url"])

            team_map = {}
            for fx in league_fixtures:
                for side in ["home", "away"]:
                    name = fx[side]["name"]
                    team_map.setdefault(name, []).append(fx)

            group_info["leagues"].append(
                {
                    "league_name": league["name"],
                    "league_url": league["url"],
                    "venue": venue_info,
                    "fixtures": league_fixtures,
                    "teams": team_map,
                }
            )

        all_data.append(group_info)

        time.sleep(1)

    return all_data


def build_team_calendar(
    fixtures: list[dict], team_name: str, location: str = DEFAULT_EVENT_LOCATION
) -> Calendar:
    cal = Calendar()
    cal.creator = f"Fixtures for {team_name}"
    for fx in fixtures:
        opponent = (
            fx["away"]["name"]
            if fx["home"]["name"] == team_name
            else fx["home"]["name"]
        )
        event = Event()
        event.name = f"{team_name} vs {opponent}"
        event.begin = fx["datetime"]
        event.duration = timedelta(hours=1)
        event.location = location
        if fx["result"]:
            event.description = f"Result: {fx['result']}"
        cal.events.add(event)
    return cal


def write_all_calendars(manifest):
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

    for group in manifest:
        group_name = group["group_name"].replace(" ", "_").lower()

        for league in group["leagues"]:
            league_name = league["league_name"].replace(" ", "_").lower()

            # League-wide calendar
            league_ics_path = os.path.join(
                OUTPUT_DIRECTORY, f"{group_name}__{league_name}.ics"
            )

            venue_address = league.get("venue", {}).get(
                "address", DEFAULT_EVENT_LOCATION
            )

            league_calendar = build_team_calendar(
                league["fixtures"], team_name="", location=venue_address
            )
            with open(league_ics_path, "w") as f:
                f.writelines(league_calendar)

            # Per-team calendars
            for team_name, team_fixtures in league["teams"].items():
                safe_team = team_name.replace(" ", "_").lower()
                team_ics_path = os.path.join(OUTPUT_DIRECTORY, f"{safe_team}.ics")
                team_calendar = build_team_calendar(
                    team_fixtures, team_name, location=venue_address
                )
                with open(team_ics_path, "w") as f:
                    f.writelines(team_calendar)


def main() -> None:
    """
    Crawl all league groups, extract fixtures, and generate .ics files
    for each league, and each team within each league.
    """
    # leagues = fetch_league_names_and_urls()
    # for league in leagues:
    #    print(f"League Name: {league[0]}, URL: {league[1]}")

    # league_info = parse_league_group("Accrington - Weds", "/info/leaguegroups/28805")
    # from pprint import pprint
    # pprint(league_info)

    # league_fixtures = extract_fixtures_from_league("/info/leagues/11882")
    # for f in league_fixtures:
    #    print(
    # f"{f['datetime']} — {f['home']['name']} vs {f['away']['name']} — Result: {f['result']}"
    # )

    print("Building fixture manifest from all league groups...")
    manifest = build_leaguegroup_fixture_manifest(limit=3)
    # manifest = build_leaguegroup_fixture_manifest(None)
    print("Finished building manifest.\n")
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    with open(os.path.join(OUTPUT_DIRECTORY, "manifest.json"), "w") as f:
        json.dump(
            manifest, f, indent=2, default=str
        )  # default=str to serialize datetime

    print("Manifest saved to docs/manifest.json")

    print("Writing calendars to output directory...")
    write_all_calendars(manifest)
    print("All calendars written successfully.\n")
    # fixtures = fetch_team_fixtures()
    # calendar = build_calendar(fixtures)

    # os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    # with open(ICS_OUTPUT_PATH, "w") as output_file:
    #    output_file.writelines(calendar)

    # print(f"Wrote fixture calendar to {ICS_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
